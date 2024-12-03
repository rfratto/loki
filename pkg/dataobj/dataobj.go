// Package dataobj holds utilities for working with data objects.
package dataobj

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/loki/pkg/push"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/objstore"
)

// BuilderConfig configures a data object [Builder].
type BuilderConfig struct {
	// MaxPageSize sets a maximum size for encoded pages within the data object.
	// MaxPageSize accounts for encoding, but not for compression.
	//
	// Pages may can go above this size if a single entry within a column exceeds
	// MaxPageSize.
	MaxPageSize flagext.Bytes

	// MaxMetadataSize sets a maximum size for metadata within the data object.
	//
	// Metadata may go above this size if there is user-provided data (e.g.,
	// labels) which exceeds MaxMetadataSize.
	MaxMetadataSize flagext.Bytes

	// MaxObjectSizeBytes sets a maximum size for each data object. Once buffered
	// data in memory exceeds this size, a flush will be triggered.
	//
	// Only buffered data that fits within MaxObjectSizeBytes is flushed.
	// Remaining data can be flushed manually by calling [Builder.Flush].
	MaxObjectSizeBytes flagext.Bytes
}

// A Builder builds data objects from a set of incoming log data. Log data is
// appended to a builder by calling [Builder.Append]. Builders occasionally
// flush appended data to object storage after an Append based on its
// configuration. A flush can be manually triggered by calling [Builder.Flush].
//
// Once a builder is no longer needed, call [Builder.Close] to trigger a final
// flush and release resources.
//
// Methods on Builder are not goroutine safe; callers are responsible for
// synchronizing calls.
type Builder struct {
	cfg    BuilderConfig
	bucket objstore.Bucket

	tenants map[string]*tenant
}

// NewBuilder creates a new builder which stores data objects in the provided
// bucket.
func NewBuilder(cfg BuilderConfig, bucket objstore.Bucket) *Builder {
	return &Builder{
		cfg:    cfg,
		bucket: bucket,
	}
}

// Append buffers entries to be written as a data object. If enough data has
// been accumulated, Append will trigger a flush to object storage.
func (b *Builder) Append(ctx context.Context, tenantID string, entries push.PushRequest) error {
	if b.tenants == nil {
		b.tenants = make(map[string]*tenant)
	}
	tenant, ok := b.tenants[tenantID]
	if !ok {
		tenant = b.newTenant(tenantID)
		b.tenants[tenantID] = tenant
	}

	// TODO(rfratto): Check if we need to flush after appending.
	//
	// We should flush if the set of data across all tenants exceeds
	// MaxObjectSizeBytes. For now, we will have each tenant calculate its own
	// size (and perhaps cache it since not every tenant gets updated at once).
	//
	// TODO(rfratto): Add a comment that we're not counting the size of metadata,
	// but perhaps we should in the future.

	return tenant.Append(ctx, entries)
}

// Flush triggers a flush of any appended data to object storage. Calling flush
// may result in a no-op if there is no buffered data to flush.
func (b *Builder) Flush(ctx context.Context) error {
	return errors.New("not implemented")
}

// Close triggers a final [Builder.Flush] before releasing resources. New data
// may not be appended to a closed Builder.
func (b *Builder) Close(ctx context.Context) error {
	return errors.New("not implemented")
}

// uncompressedSize returns the uncompressed size of all data in the builder.
func (b *Builder) uncompressedSize() int {
	var total int
	for _, tenant := range b.tenants {
		total += tenant.UncompressedSize()
	}
	return total
}

// compressedSize returns the compressed size of all data in the builder. If
// includeHead is true, the current uncompressed data in head pages are counted
// in the result.
func (b *Builder) compressedSize(includeHead bool) int {
	var total int
	for _, tenant := range b.tenants {
		total += tenant.CompressedSize(includeHead)
	}
	return total
}

type tenant struct {
	builder *Builder
	ID      string
	streams map[string]*stream
}

// newTenant creates a new tenant buffer with the provided ID.
func (b *Builder) newTenant(ID string) *tenant {
	return &tenant{
		builder: b,
		ID:      ID,
	}
}

func (t *tenant) Append(ctx context.Context, entries push.PushRequest) error {
	if t.streams == nil {
		t.streams = make(map[string]*stream)
	}

	var errs []error

	for _, pushStream := range entries.Streams {
		dataStream, ok := t.streams[pushStream.Labels]
		if !ok {
			newStream, err := t.builder.newStream(pushStream.Labels)
			if err != nil {
				errs = append(errs, fmt.Errorf("creating stream %q: %w", pushStream.Labels, err))
				continue
			}

			dataStream = newStream
			t.streams[pushStream.Labels] = dataStream
		}

		if err := dataStream.Append(ctx, pushStream.Entries); err != nil {
			errs = append(errs, fmt.Errorf("appending to stream %q: %w", pushStream.Labels, err))
		}
	}

	return errors.Join(errs...)
}

// UncompressedSize returns the uncompressed size of all streams in the tenant.
func (t *tenant) UncompressedSize() int {
	var total int
	for _, stream := range t.streams {
		total += stream.UncompressedSize()
	}
	return total
}

// CompressedSize returns the compressed size of all streams in the tenant. If
// includeHead is true, the current uncompressed data in head pages are counted
// in the result.
func (t *tenant) CompressedSize(includeHead bool) int {
	var total int
	for _, stream := range t.streams {
		total += stream.CompressedSize(includeHead)
	}
	return total
}

type stream struct {
	builder *Builder
	labels  labels.Labels

	timestamp *column[time.Time]
	metadata  map[string]*column[string]
	logColumn *column[string]
	rows      int
}

func (b *Builder) newStream(labels string) (*stream, error) {
	lbls, err := parser.ParseMetric(labels)
	if err != nil {
		return nil, err
	}

	return &stream{
		builder:   b,
		labels:    lbls,
		timestamp: newTimeColumn(uint64(b.cfg.MaxPageSize)),
		metadata:  make(map[string]*column[string]),
		logColumn: newTextColumn(uint64(b.cfg.MaxPageSize)),
	}, nil
}

func (s *stream) Iter() iter.Seq2[push.Entry, error] {
	// Before we iterate, we must backfill all columns to guarantee all columns
	// have the same number of rows.
	s.Backfill()

	return func(yield func(push.Entry, error) bool) {
		pullTs, stopTs := iter.Pull2(s.timestamp.Iter())
		defer stopTs()

		pullColumns := s.metadataPullers()
		defer func() {
			for _, col := range pullColumns {
				col.Stop()
			}
		}()

		pullLog, stopLog := iter.Pull2(s.logColumn.Iter())
		defer stopLog()

		for {
			var ent push.Entry

			ts, err, ok := pullTs()
			if err != nil {
				yield(push.Entry{}, err)
				return
			} else if !ok {
				return
			}
			ent.Timestamp = ts

			for _, pullMetadata := range pullColumns {
				val, err, ok := pullMetadata.NextValue()
				if err != nil {
					yield(push.Entry{}, err)
					return
				} else if !ok {
					return
				} else if val == "" {
					continue
				}

				ent.StructuredMetadata = append(ent.StructuredMetadata, push.LabelAdapter{
					Name:  pullMetadata.Key,
					Value: val,
				})
			}

			log, err, ok := pullLog()
			if err != nil {
				yield(push.Entry{}, err)
				return
			} else if !ok {
				return
			}

			ent.Line = log
			if !yield(ent, nil) {
				return
			}
		}
	}
}

type metadataPuller struct {
	Key       string
	NextValue func() (string, error, bool)
	Stop      func()
}

// metadataPullers returns a sequence of metadataPuller instances for each
// column.
func (s *stream) metadataPullers() []metadataPuller {
	columnNames := slices.Collect(maps.Keys(s.metadata))

	pullers := make([]metadataPuller, 0, len(columnNames))
	for _, name := range columnNames {
		next, stop := iter.Pull2(s.metadata[name].Iter())

		puller := metadataPuller{
			Key:       name,
			NextValue: next,
			Stop:      stop,
		}
		pullers = append(pullers, puller)
	}
	return pullers
}

// TODO(rfratto): before flushing, we want to make sure all columns have the same number of rows.

func (s *stream) Append(ctx context.Context, entries []push.Entry) error {
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return ctx.Err()
		}

		s.timestamp.Append(s.rows, entry.Timestamp)
		for _, kvp := range entry.StructuredMetadata {
			// Providing the row count here allows backfilling columns with NULLs.
			s.getOrAddTextColumn(kvp.Name).Append(s.rows, kvp.Value)
		}
		s.logColumn.Append(s.rows, entry.Line)

		s.rows++
	}

	return nil
}

func (s *stream) getOrAddTextColumn(key string) *column[string] {
	col, ok := s.metadata[key]
	if !ok {
		col = newTextColumn(uint64(s.builder.cfg.MaxPageSize))
		s.metadata[key] = col
	}
	return col
}

// Backfill ensures all optional columns have the same number of rows as other columns.
func (s *stream) Backfill() {
	for _, col := range s.metadata {
		for col.Count() < s.rows {
			// s.rows is a count, so we need to subtract 1 to get the last row index.
			col.Backfill(s.rows - 1)
		}
	}
}

// UncompressedSize returns the uncompressed size of all columns in the stream.
func (s *stream) UncompressedSize() int {
	var total int
	total += s.timestamp.UncompressedSize()
	for _, md := range s.metadata {
		total += md.UncompressedSize()
	}
	total += s.logColumn.UncompressedSize()
	return total
}

// CompressedSize returns the compressed size of all columns in the stream. If
// includeHead is true, the current uncompressed data in head pages are counted
// in the result.
func (s *stream) CompressedSize(includeHead bool) int {
	var total int
	total += s.timestamp.CompressedSize(includeHead)
	for _, md := range s.metadata {
		total += md.CompressedSize(includeHead)
	}
	total += s.logColumn.CompressedSize(includeHead)
	return total
}
