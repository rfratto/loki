package scanner

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
)

// Predicates.
type (
	// LabelMatcher is a predicate for a label. LabelMatcher predicates assert
	// that a label named Name exists and its value is set to Value.
	LabelMatcher struct{ Name, Value string }

	// LabelFilter is a predicate for a label. LabelFilter predicates return
	// whether a combination of Name and Value should be kept in the result; a
	// return value of true indicates to keep the row.
	LabelFilter func(name, value string) bool

	// MetadataMatcher is a predicate for metadata. MetadataMatcher predicates
	// assert that a metadata key named Name exists and its value is set to
	// Value.
	MetadataMatcher struct{ Name, Value string }

	// MetadataFilter is a predicate for metadata. MetadataFilter predicates
	// return whether a combination of Name and Value should be kept in the
	// result; a return value of true indicates to keep the row.
	MetadataFilter func(name, value string) bool
)

// Streams is a scanner for stream entries in a data object.
type Streams struct{}

// NewStreams creates a new Streams scanner which reads stream entries from the
// given bucket and object path.
func NewStreams(bucket objstore.Bucket, path string) *Streams

// AddLabelMatcher adds a label matcher to the scanner. [Streams.Search] will
// only emit Streams for which the label matcher predicate passes.
func (s *Streams) AddLabelMatcher(m LabelMatcher)

// AddLabelFilter adds a label filter to the scanner. [Streams.Search] will
// only emit Streams for which the label filter predicate passes. The
// LabelFilter f will be called with the provided key to allow the same
// function to be reused for multiple keys.
func (s *Streams) AddLabelFilter(key string, f LabelFilter)

// Search returns a sequence of Streams that match the given time range.
// Predicates added to Streams are applied in order.
func (s *Streams) Search(ctx context.Context, minTime, maxTime time.Time) result.Seq[Stream]

// A Stream is a stream entry in a data object.
type Stream struct {
	// ID of the stream. The Stream ID is unique only to a data object.
	ID int64

	// Stream labels. All labels are returned, including ones not listed in
	// Predicates.
	Labels labels.Labels
}

// Records is a scanner for log records in a data object.
type Records struct{}

// NewRecords creates a new Records scanner which reads log records from the
// given bucket and object path.
//
// Only the given section ID is searched, and only records from streams in the
// list of stream IDs are returned.
func NewRecords(bucket objstore.Bucket, path string, sectionID int, streamIDs []int64) *Records

// AddMetadataMatcher adds a metadata matcher to the scanner. [Records.Search]
// will only emit Records for which the label matcher predicate passes.
func (r *Records) AddMetadataMatcher(m MetadataMatcher)

// AddMetadataFilter adds a metadata filter to the scanner. [Records.Search]
// will only emit Records for which the metadata filter predicate passes. The
// MetadataFilter f will be called with the provided key to allow the same
// function to be reused for multiple keys.
func (r *Records) AddMetadataFilter(key string, f MetadataFilter)

// Search returns a sequence of [Record]s that match the given time range.
// Predicates added to Records are applied in order.
func (r *Records) Search(ctx context.Context, minTime, maxTime time.Time) result.Seq[Stream]

// A Record is an individual log record in a logs section.
type Record struct {
	StreamID  int64
	Timestamp time.Time
	Metadata  labels.Labels
	Line      string
}
