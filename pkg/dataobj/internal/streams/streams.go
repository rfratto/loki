// Package streams defines types used for the data object streams section. The
// streams section reports the list of streams present in the log file.
package streams

import (
	"fmt"
	"time"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/obj"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
	"github.com/prometheus/prometheus/model/labels"
)

// Streams tracks information about streams in a data object.
type Streams struct {
	streams map[uint64][]*Stream

	// orderedStreams is used for consistently iterating over the list of
	// streams. It contains streams added in append order.
	orderedStreams []*Stream
}

// New creates a new Streams section.
func New() *Streams {
	return &Streams{streams: make(map[uint64][]*Stream)}
}

// Stream is an individual stream within the section.
type Stream struct {
	Labels       labels.Labels
	MinTimestamp time.Time
	MaxTimestamp time.Time
	Rows         int
}

// AddStreamRecord records a stream record within a data object. The provided
// timestamp is used to track the minimum and maximum timestamp of a stream.
// The number of calls to AddStreamRecord is used to track the number of rows
// for a stream.
func (s *Streams) AddStreamRecord(streamLabels labels.Labels, ts time.Time) {
	stream := s.getOrAddStream(streamLabels)
	if stream.MinTimestamp.IsZero() || ts.Before(stream.MinTimestamp) {
		stream.MinTimestamp = ts
	}
	if stream.MaxTimestamp.IsZero() || ts.After(stream.MaxTimestamp) {
		stream.MaxTimestamp = ts
	}
	stream.Rows++
}

func (s *Streams) getOrAddStream(streamLabels labels.Labels) *Stream {
	hash := streamLabels.Hash()
	matches, ok := s.streams[hash]
	if !ok {
		newStream := &Stream{Labels: streamLabels}
		s.streams[hash] = []*Stream{newStream}
		s.orderedStreams = append(s.orderedStreams, newStream)
		return newStream
	}

	for _, stream := range matches {
		if labels.Equal(stream.Labels, streamLabels) {
			return stream
		}
	}

	// If we didn't find a match, add a new stream.
	newStream := &Stream{Labels: streamLabels}
	s.streams[hash] = append(s.streams[hash], newStream)
	s.orderedStreams = append(s.orderedStreams, newStream)
	return newStream
}

// WriteTo writes the list of recorded streams to the provided encoder.
// pageSize and metadataSize controls target sizes for pages and metadata.
// WriteTo can generate multiple sections if the list of streams is too big to
// fit into a single section.
//
// Streams are encoded in the order they were first appended.
func (s *Streams) WriteTo(enc *obj.Encoder, pageSize, metadataSize int) error {
	minTimestampColumn, err := dataset.NewColumn[int64]("", dataset.BufferOptions{
		PageSizeHint: pageSize,
		Encoding:     datasetmd.ENCODING_TYPE_DELTA,
		Compression:  datasetmd.COMPRESSION_TYPE_NONE,
	})
	if err != nil {
		return fmt.Errorf("creating minimum timestamp column: %w", err)
	}

	maxTimestampColumn, err := dataset.NewColumn[int64]("", dataset.BufferOptions{
		PageSizeHint: pageSize,
		Encoding:     datasetmd.ENCODING_TYPE_DELTA,
		Compression:  datasetmd.COMPRESSION_TYPE_NONE,
	})
	if err != nil {
		return fmt.Errorf("creating maximum timestamp column: %w", err)
	}

	recordsCountColumn, err := dataset.NewColumn[int64]("", dataset.BufferOptions{
		PageSizeHint: pageSize,
		Encoding:     datasetmd.ENCODING_TYPE_DELTA,
		Compression:  datasetmd.COMPRESSION_TYPE_NONE,
	})
	if err != nil {
		return fmt.Errorf("creating records count column: %w", err)
	}

	var (
		labelColumns      = []*dataset.Column[string]{}
		labelColumnLookup = map[string]int{} // Name to index
	)

	getLabelColumn := func(name string) (*dataset.Column[string], error) {
		idx, ok := labelColumnLookup[name]
		if ok {
			return labelColumns[idx], nil
		}

		labelColumn, err := dataset.NewColumn[string](name, dataset.BufferOptions{
			PageSizeHint: pageSize,
			Encoding:     datasetmd.ENCODING_TYPE_PLAIN,
			Compression:  datasetmd.COMPRESSION_TYPE_ZSTD,
		})
		if err != nil {
			return nil, err
		}

		labelColumns = append(labelColumns, labelColumn)
		labelColumnLookup[name] = len(labelColumnLookup)
		return labelColumn, nil
	}

	// First, populate all of our columns.
	for i, stream := range s.orderedStreams {
		// Append only fails if the rows are out-of-order, which can't happen here.
		_ = minTimestampColumn.Append(i, stream.MinTimestamp.UnixNano())
		_ = maxTimestampColumn.Append(i, stream.MaxTimestamp.UnixNano())
		_ = recordsCountColumn.Append(i, int64(stream.Rows))

		for _, label := range stream.Labels {
			labelColumn, err := getLabelColumn(label.Name)
			if err != nil {
				return fmt.Errorf("creating label column: %w", err)
			}
			_ = labelColumn.Append(i, label.Value)
		}
	}

	// Finally, populate our data set.
	streamsEnc, err := enc.OpenStreams()
	if err != nil {
		return fmt.Errorf("opening streams section: %w", err)
	}

	{
		minTimestampColumn.Flush()
		columnEnc, _ := streamsEnc.OpenColumn(streamsmd.COLUMN_TYPE_MIN_TIMESTAMP, minTimestampColumn.Info())
		for _, page := range minTimestampColumn.Pages() {
			_ = columnEnc.AppendPage(page)
		}
		_ = columnEnc.Close()
	}

	{
		maxTimestampColumn.Flush()
		columnEnc, _ := streamsEnc.OpenColumn(streamsmd.COLUMN_TYPE_MAX_TIMESTAMP, maxTimestampColumn.Info())
		for _, page := range maxTimestampColumn.Pages() {
			_ = columnEnc.AppendPage(page)
		}
		_ = columnEnc.Close()
	}

	{
		recordsCountColumn.Flush()
		columnEnc, _ := streamsEnc.OpenColumn(streamsmd.COLUMN_TYPE_ROWS, recordsCountColumn.Info())
		for _, page := range recordsCountColumn.Pages() {
			_ = columnEnc.AppendPage(page)
		}
		_ = columnEnc.Close()
	}

	// TODO(rfratto): label columns are when the metadata size for the section
	// can grow too big. However, there's no good way of splitting the section
	// that ensures we don't immediately hit the size limit again.
	//
	// Rather, we'll likely want to support some kind of "aggregated label
	// column" which we combine columns into once columns reach a certain size.
	//
	// However, to support this, we need to return to the original encoding API
	// of buffering to allow us to completely discard sections without writing
	// them to a file.
	for _, labelColumn := range labelColumns {
		// For consistency we'll make sure each label column has the same number of rows as streams.
		labelColumn.Backfill(len(s.orderedStreams))
		labelColumn.Flush()

		columnEnc, _ := streamsEnc.OpenColumn(streamsmd.COLUMN_TYPE_LABEL, labelColumn.Info())
		for _, page := range labelColumn.Pages() {
			_ = columnEnc.AppendPage(page)
		}
		_ = columnEnc.Close()
	}

	if err := streamsEnc.Close(); err != nil {
		return fmt.Errorf("closing streams section: %w", err)
	}
	return enc.Close()
}
