package dataobj

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/decoder"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/logstreamsmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
)

var errIterationStopped = errors.New("iteration stopped")

// Reader connects to an object store bucket and supports reading data objects.
type Reader struct {
	bucket objstore.Bucket
}

// NewReader creates a new Reader that answers questions about data objects
// inside bucket.
func NewReader(bucket objstore.Bucket) *Reader {
	return &Reader{
		bucket: bucket,
	}
}

// Objects returns an iterator over all data objects for the provided tenant.
// If ctx is canceled during iteration, the iterator stops immediately.
//
// If r's bucket does not support iteration, no objects are returned.
func (r *Reader) Objects(ctx context.Context, tenant string) iter.Seq[string] {
	tenantPath := fmt.Sprintf("tenant-%s/objects/", tenant)

	return func(yield func(string) bool) {
		r.bucket.Iter(ctx, tenantPath, func(name string) error {
			if !yield(name) {
				return errIterationStopped
			}
			return nil
		}, objstore.WithRecursiveIter())
	}
}

// Streams returns an iterator over all streams for the provided object. If an
// error is encountered, the iterator returns the error and stops.
func (r *Reader) Streams(ctx context.Context, object string) result.Seq[labels.Labels] {
	return result.Iter(func(yield func(labels.Labels) bool) error {
		for res := range r.streams(ctx, object) {
			stream, err := res.Value()
			if err != nil {
				return fmt.Errorf("iterating streams: %w", err)
			}

			lbls := logstreamsmd.Labels(stream.Identifier)
			if !yield(lbls) {
				return nil
			}
		}

		return nil
	})
}

func (r *Reader) streams(ctx context.Context, object string) result.Seq[*logstreamsmd.StreamInfo] {
	dec := decoder.BucketDecoder(r.bucket, object)

	return result.Iter(func(yield func(*logstreamsmd.StreamInfo) bool) error {
		sections, err := dec.Sections(ctx)
		if err != nil {
			return fmt.Errorf("reading sections: %w", err)
		}

		for _, sec := range sections {
			if sec.Type != filemd.SECTION_TYPE_LOG_STREAMS {
				continue
			}

			streamsDec := dec.StreamsDecoder()
			streams, err := streamsDec.Streams(ctx, sec)
			if err != nil {
				return fmt.Errorf("reading streams: %w", err)
			}

			for _, stream := range streams {
				if !yield(stream) {
					return nil
				}
			}
		}

		return nil
	})
}

// EntriesOptions customizes the filtering behaviour of [Reader.Entries].
type EntriesOptions struct {
	// MinTime and MaxTime are inclusive bounds for the time range of
	// [push.Entry] instances to return.
	//
	// If MinTime or MaxTime is nil, the bound is not applied.
	MinTime, MaxTime *time.Time

	GetAllMetadata bool     // Gets all metadata if true. Mutually exclusive with GetMetadata.
	GetMetadata    []string // Gets only the specified metadata keys. Mutually exclusive with GetAllMetadata.
	GetLine        bool     // Gets the log line if true.

	// MetadtaFilter is an optional filtering function which filters out entire
	// entries based on metadata key-value pairs. MetadataFilter is invoked once
	// for each retrieved metadata key-value pair encountered. If MetadataFilter
	// returns false, the entry is filtered out.
	MetadataFilter func(name, value string) bool

	// TODO(rfratto): how far should we go with filtering options? Should it
	// explicitly support log filters? The MetadataFilter should enable filtering
	// out most log pages, but dataobj is really intended for very low-level
	// querying.
}

// Entries returns an iterator over [push.Entry] items found in the object for
// the provided stream. EntriesOptions provides basic filtering options. If an
// error is encountered, the iterator returns the error and stops.
func (r *Reader) Entries(ctx context.Context, object string, stream labels.Labels, opts EntriesOptions) result.Seq[push.Entry] {
	// TODO(rfratto): change this remove operation on stream; defer that to a
	// metadata filter.

	// TODO(rfratto): impl
	dec := decoder.BucketDecoder(r.bucket, object).StreamsDecoder()

	return result.Iter(func(yield func(push.Entry) bool) error {
		for res := range r.streams(ctx, object) {
			in, err := res.Value()
			if err != nil {
				return fmt.Errorf("reading streams: %w", err)
			}

			inLabels := logstreamsmd.Labels(in.Identifier)
			if !labels.Equal(inLabels, stream) {
				continue
			}

			for res := range newEntryIterator(dec, in, opts).Iter() {
				entry, err := res.Value()
				if err != nil {
					return fmt.Errorf("iterating entries: %w", err)
				} else if !yield(entry) {
					return nil
				}
			}
		}

		return fmt.Errorf("NYI: Reader.Entries")
	})
}
