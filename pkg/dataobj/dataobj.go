// Package dataobj holds utilities for working with data objects.
package dataobj

import (
	"context"
	"errors"
	"fmt"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
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

	// TODO(rfratto): Check if we need to flush after appending. As flushing is
	// per-tenant, we only need to check the size of the current tenant.
	//
	// TODO(rfratto): Add a comment that we're not counting the size of metadata,
	// but perhaps we should in the future.

	return tenant.Append(ctx, entries)
}

// Flush forces writing data buffered via Append to object storage, up to
// MaxObjectSizeBytes. Calling Flush may result in a no-op if there is no
// buffered data to flush.
//
// MaxObjectSizeBytes is a soft limit; flushes may slightly exceed
// MaxObjectSizeBytes based on the amount of metadata.
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
