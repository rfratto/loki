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
	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
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
func (r *Reader) Streams(ctx context.Context, object string) iter.Seq2[labels.Labels, error] {
	return func(yield func(labels.Labels, error) bool) {
		for stream, err := range r.streams(ctx, object) {
			if err != nil {
				yield(nil, fmt.Errorf("iterating streams: %w", err))
				return
			}

			lbls := streamsmd.Labels(stream.Identifier)
			if !yield(lbls, nil) {
				return
			}
		}
	}
}

func (r *Reader) streams(ctx context.Context, object string) iter.Seq2[*streamsmd.StreamInfo, error] {
	dec := decoder.BucketDecoder(r.bucket, object)

	return func(yield func(*streamsmd.StreamInfo, error) bool) {
		sections, err := dec.Sections(ctx)
		if err != nil {
			yield(nil, fmt.Errorf("reading sections: %w", err))
			return
		}

		for _, sec := range sections {
			if sec.Type != filemd.SECTION_TYPE_STREAMS {
				continue
			}

			streamsDec := dec.StreamsDecoder()
			streams, err := streamsDec.Streams(ctx, sec)
			if err != nil {
				yield(nil, fmt.Errorf("reading streams: %w", err))
				return
			}

			for _, stream := range streams {
				if !yield(stream, nil) {
					return
				}
			}
		}
	}
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
func (r *Reader) Entries(ctx context.Context, object string, stream labels.Labels, opts EntriesOptions) iter.Seq2[push.Entry, error] {
	// TODO(rfratto): impl
	dec := decoder.BucketDecoder(r.bucket, object).StreamsDecoder()

	return func(yield func(push.Entry, error) bool) {
		for in, err := range r.streams(ctx, object) {
			if err != nil {
				yield(push.Entry{}, fmt.Errorf("reading streams: %w", err))
				return
			}

			inLabels := streamsmd.Labels(in.Identifier)
			if !labels.Equal(inLabels, stream) {
				continue
			}

			for entry, err := range newEntryIterator(dec, in, opts).Iter() {
				if err != nil {
					yield(push.Entry{}, fmt.Errorf("iterating entries: %w", err))
					return
				}

				if !yield(entry, nil) {
					return
				}
			}
		}

		yield(push.Entry{}, fmt.Errorf("NYI: Reader.Entries"))
	}
}
