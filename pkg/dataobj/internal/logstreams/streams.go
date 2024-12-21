// Package logstreams holds utilities for constructing data for the data object
// logstreams section in memory, stored in a [Stream]. Use the builder package
// to construct data objects from this data.
package logstreams

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/logstreamsmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

type Stream struct {
	maxPageSize uint64
	id          logstreamsmd.StreamIdentifier

	timestamp *Column[time.Time]
	metadata  map[string]*Column[string]
	logColumn *Column[string]
	rows      int

	minTs *time.Time
	maxTs *time.Time
}

func NewStream(maxPageSize uint64, labels string) (*Stream, error) {
	lbls, err := parser.ParseMetric(labels)
	if err != nil {
		return nil, err
	}

	return &Stream{
		id:        labelsToIdentifier(lbls),
		timestamp: NewTimestampColumn(uint64(maxPageSize)),
		metadata:  make(map[string]*Column[string]),
		logColumn: NewLogColumn(uint64(maxPageSize)),
	}, nil
}

func labelsToIdentifier(lbls labels.Labels) logstreamsmd.StreamIdentifier {
	res := logstreamsmd.StreamIdentifier{
		Labels: make([]*logstreamsmd.StreamIdentifier_Label, 0, len(lbls)),
	}

	for _, lbl := range lbls {
		res.Labels = append(res.Labels, &logstreamsmd.StreamIdentifier_Label{
			Name:  lbl.Name,
			Value: lbl.Value,
		})
	}

	return res
}

// CutHead cuts the head page of all columns.
func (s *Stream) CutHead() {
	s.timestamp.cutPage()
	for _, col := range s.metadata {
		col.cutPage()
	}
	s.logColumn.cutPage()
}

// ID returns the stream's identifier. The returned value must not be modified.
func (s *Stream) ID() logstreamsmd.StreamIdentifier { return s.id }

func (s *Stream) Iter() result.Seq[push.Entry] {
	// Before we iterate, we must backfill all columns to guarantee all columns
	// have the same number of rows.
	s.Backfill()

	return result.Iter(func(yield func(push.Entry) bool) error {
		pullTs, stopTs := result.Pull(s.timestamp.Iter())
		defer stopTs()

		pullColumns := s.metadataPullers()
		defer func() {
			for _, col := range pullColumns {
				col.Stop()
			}
		}()

		pullLog, stopLog := result.Pull(s.logColumn.Iter())
		defer stopLog()

		for {
			var ent push.Entry

			tsRes, ok := pullTs()
			ts, err := tsRes.Value()
			if err != nil {
				return err
			} else if !ok {
				return nil
			}
			ent.Timestamp = ts

			for _, pullMetadata := range pullColumns {
				valRes, ok := pullMetadata.NextValue()
				val, err := valRes.Value()
				if err != nil {
					return err
				} else if !ok {
					return nil
				} else if val == "" {
					continue
				}

				ent.StructuredMetadata = append(ent.StructuredMetadata, push.LabelAdapter{
					Name:  pullMetadata.Key,
					Value: val,
				})
			}

			logRes, ok := pullLog()
			log, err := logRes.Value()
			if err != nil {
				return err
			} else if !ok {
				return nil
			}

			ent.Line = log
			if !yield(ent) {
				return nil
			}
		}
	})
}

// Timestamp returns the timestamp column. The returned value must be treated
// as read-only.
func (s *Stream) Timestamp() *Column[time.Time] { return s.timestamp }

// LogLine returns the log line column. The returned value must be treated as
// read-only.
func (s *Stream) LogLine() *Column[string] { return s.logColumn }

// MetadataNames returns the set of metadata column names in sorted order.
func (s *Stream) MetadataNames() []string {
	res := slices.Collect(maps.Keys(s.metadata))
	slices.Sort(res)
	return res
}

// Metadata returns the metadata column with the provided name. The returned
// value must be treated as read-only. If the column does not exist, Metadata
// returns nil.
func (s *Stream) Metadata(name string) *Column[string] {
	return s.metadata[name]
}

type metadataPuller struct {
	Key       string
	NextValue func() (result.Result[string], bool)
	Stop      func()
}

// metadataPullers returns a sequence of metadataPuller instances for each
// column.
func (s *Stream) metadataPullers() []metadataPuller {
	columnNames := slices.Collect(maps.Keys(s.metadata))

	pullers := make([]metadataPuller, 0, len(columnNames))
	for _, name := range columnNames {
		next, stop := result.Pull(s.metadata[name].Iter())

		puller := metadataPuller{
			Key:       name,
			NextValue: next,
			Stop:      stop,
		}
		pullers = append(pullers, puller)
	}
	return pullers
}

func (s *Stream) Append(ctx context.Context, entries []push.Entry) error {
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return ctx.Err()
		}

		if s.minTs == nil || entry.Timestamp.Before(*s.minTs) {
			s.minTs = &entry.Timestamp
		}
		if s.maxTs == nil || entry.Timestamp.After(*s.maxTs) {
			s.maxTs = &entry.Timestamp
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

func (s *Stream) getOrAddTextColumn(key string) *Column[string] {
	col, ok := s.metadata[key]
	if !ok {
		col = NewMetadataColumn(key, uint64(s.maxPageSize))
		s.metadata[key] = col
	}
	return col
}

// Backfill ensures all optional columns have the same number of rows as other columns.
func (s *Stream) Backfill() {
	for _, col := range s.metadata {
		for col.Count() < s.rows {
			// s.rows is a count, so we need to subtract 1 to get the last row index.
			col.Backfill(s.rows - 1)
		}
	}
}

// UncompressedSize returns the uncompressed size of all columns in the stream.
func (s *Stream) UncompressedSize() int {
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
func (s *Stream) CompressedSize(includeHead bool) int {
	var total int
	total += s.timestamp.CompressedSize(includeHead)
	for _, md := range s.metadata {
		total += md.CompressedSize(includeHead)
	}
	total += s.logColumn.CompressedSize(includeHead)
	return total
}
