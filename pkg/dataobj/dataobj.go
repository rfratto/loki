// Package dataobj holds utilities for working with data objects.
package dataobj

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoder"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
	"github.com/thanos-io/objstore"
)

// TODO(rfratto): validate the BuilderConfig with min/max limits; page size and
// metadata size should be >4KB and object size should be <3GB (there's an
// aboluslte hard limit at 4GB).

// BuilderConfig configures a data object [Builder].
type BuilderConfig struct {
	// SHAPrefixSize sets the number of bytes of the SHA filename to use as a
	// folder path.
	SHAPrefixSize int

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

	// MaxObjectSizeBytes sets a soft maximum size for data objects. Once
	// buffered data in memory exceeds this size, a flush will be triggered.
	//
	// A flush can be triggered regardless of the amount of buffered data by
	// calling [Builder.Flush].
	MaxObjectSizeBytes flagext.Bytes

	// TODO(rfratto): Should MaxObjectSizeBytes be called FlushSizeBytes?
}

// A Builder builds data objects from a set of incoming log data. Log data is
// appended to a builder by calling [Builder.Append]. Builders occasionally
// flush appended data to object storage after an Append based on its
// configuration. A flush can be manually triggered by calling [Builder.Flush].
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

	// TODO(rfratto): is it safe for tenant.Append to fail based on context? The
	// caller won't know what subset of entries were appended, and might cause
	// duplicates to get appended.
	if err := tenant.Append(ctx, entries); err != nil {
		return err
	}

	if tenant.CompressedSize(true) > int(b.cfg.MaxObjectSizeBytes) {
		return b.flush(ctx, tenantID)
	}
	return nil
}

// Flush forces writing all data remaining buffered via Append to object
// storage. Calling Flush may result in a no-op if there is no buffered data to
// flush.
func (b *Builder) Flush(ctx context.Context) error {
	var errs []error

	for tenantID := range b.tenants {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := b.flush(ctx, tenantID); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (b *Builder) flush(ctx context.Context, tenantID string) error {
	// TODO(rfratto): skip if there's no data?

	tenant, ok := b.tenants[tenantID]
	if !ok {
		// No work to do.
		return nil
	}

	enc := encoder.New()
	streams, err := enc.OpenStreams()
	if err != nil {
		return fmt.Errorf("encoding streams section: %w", err)
	}

	// newStreams closes the current streams section and opens a new one.
	newStreams := func() error {
		if err := streams.Close(); err != nil {
			return err
		}

		streams, err = enc.OpenStreams()
		return err
	}

	// TODO(rfratto): what happens if a column is empty? what about if a page is
	// empty? Does the encoder handle it gracefully? What about the decoder?

	for _, streamName := range tenant.StreamNames() {
		stream := tenant.streams[streamName]

		// Cut all head pages to ensure that all data is available for encoding.
		//
		// TODO(rfratto): reset columns after we finish encoding the page to avoid
		// leaking columns.
		stream.CutHead()

		encStream, err := streams.OpenStream(stream.ID())
		if streams.MetadataSize() > int(b.cfg.MaxMetadataSize) {
			// We hit too many streams for a single streams section. Let's create a
			// new streams section and recreate encStream there.
			_ = encStream.Discard()

			// We don't check for the metadata size enc here; there's really no way
			// for there to be enough sections in the file that we exceed the size
			// limit.
			if err := newStreams(); err != nil {
				panic("Builder.flush: unexpected error opening section: " + err.Error())
			}

			encStream, err = streams.OpenStream(stream.ID())
			if err != nil {
				panic("Builder.flush: unexpected error opening stream: " + err.Error())
			}
		}

		{
			encTimestamp, err := encStream.OpenColumn(stream.Timestamp().Info(false))
			if err != nil {
				panic("Builder.flush: unexpected error opening column: " + err.Error())
			}
			for _, page := range stream.Timestamp().Pages() {
				if err := encTimestamp.AppendPage(page); err != nil {
					panic("Unexpected error appending page: " + err.Error())
				}
			}
			if err := encTimestamp.Close(); err != nil {
				panic("Unexpected error closing column: " + err.Error())
			}
		}

		{
			encLogs, err := encStream.OpenColumn(stream.LogLine().Info(false))
			if err != nil {
				panic("Builder.flush: unexpected error opening column: " + err.Error())
			}
			for _, page := range stream.LogLine().Pages() {
				if err := encLogs.AppendPage(page); err != nil {
					panic("Unexpected error appending page: " + err.Error())
				}
			}
			if err := encLogs.Close(); err != nil {
				panic("Unexpected error closing column: " + err.Error())
			}
		}

		// TODO(rfratto): Check for metadata size of encStream here. If we exceeded
		// the metadata size limit (too many columns), we should encode an
		// aggregated column instead.
		for _, metadataName := range stream.MetadataNames() {
			encMetadata, err := encStream.OpenColumn(stream.Metadata(metadataName).Info(false))
			if err != nil {
				panic("Builder.flush: unexpected error opening column: " + err.Error())
			}
			for _, page := range stream.Metadata(metadataName).Pages() {
				if err := encMetadata.AppendPage(page); err != nil {
					panic("Unexpected error appending page: " + err.Error())
				}
			}
			if err := encMetadata.Close(); err != nil {
				panic("Unexpected error closing column: " + err.Error())
			}
		}

		if err := encStream.Close(); err != nil {
			panic("Unexpected error closing stream: " + err.Error())
		}
	}

	if err := streams.Close(); err != nil {
		panic("Unexpected error closing streams: " + err.Error())
	}

	var buf bytes.Buffer
	if _, err := enc.WriteTo(&buf); err != nil {
		return err
	}

	// Get the SHA of the data to use as the filename.
	sum := sha256.Sum224(buf.Bytes())
	sumStr := hex.EncodeToString(sum[:])

	path := fmt.Sprintf("tenant-%s/objects/%s/%s", tenantID, sumStr[:b.cfg.SHAPrefixSize], sumStr[b.cfg.SHAPrefixSize:])
	return b.bucket.Upload(ctx, path, bytes.NewReader(buf.Bytes()))
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
	streams map[string]*streams.Stream
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
		t.streams = make(map[string]*streams.Stream)
	}

	var errs []error

	for _, pushStream := range entries.Streams {
		dataStream, ok := t.streams[pushStream.Labels]
		if !ok {
			newStream, err := streams.NewStream(uint64(t.builder.cfg.MaxPageSize), pushStream.Labels)
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

// StreamNames returns the names of all streams in the tenant in sorted order.
func (t *tenant) StreamNames() []string {
	res := slices.Collect(maps.Keys(t.streams))
	slices.Sort(res)
	return res
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
