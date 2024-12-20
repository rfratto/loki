package streams

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/obj"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
)

// IterStreams iterates over streams in the provided decoder.
func IterStreams(ctx context.Context, dec obj.Decoder) iter.Seq2[Stream, error] {

	return func(yield func(Stream, error) bool) {
		sections, err := dec.Sections(ctx)
		if err != nil {
			yield(Stream{}, err)
			return
		}

		streamsDec := dec.StreamsDecoder()

		for _, section := range sections {
			if section.Type != filemd.SECTION_TYPE_STREAMS {
				continue
			}

			for stream, err := range iterSection(ctx, streamsDec, section) {
				if !yield(stream, err) || err != nil {
					return
				}
			}
		}
	}
}

func iterSection(ctx context.Context, dec obj.StreamsDecoder, section *filemd.SectionInfo) iter.Seq2[Stream, error] {
	return func(yield func(Stream, error) bool) {
		if section.Type != filemd.SECTION_TYPE_STREAMS {
			return
		}

		columns, err := dec.Columns(ctx, section)
		if err != nil {
			yield(Stream{}, err)
			return
		}

		scanner := dataset.NewScanner(streamsmd.ColumnInfos(columns), dec.DatasetDecoder())
		for rowEntry, err := range scanner.Iter(ctx) {
			if err != nil {
				yield(Stream{}, err)
				return
			}

			stream, err := decodeRow(columns, rowEntry)
			if !yield(stream, err) || err != nil {
				return
			}
		}
	}
}

func decodeRow(columns []*streamsmd.ColumnDesc, row []dataset.ScannerEntry) (Stream, error) {
	var stream Stream

	for columnIndex, columnEntry := range row {
		if columnEntry.Value.IsNil() {
			continue
		}

		column := columns[columnIndex]
		switch column.Type {
		case streamsmd.COLUMN_TYPE_MIN_TIMESTAMP:
			if ty := columnEntry.Value.Type(); ty != datasetmd.VALUE_TYPE_INT64 {
				return stream, fmt.Errorf("invalid type %s for %s", ty, column.Type)
			}
			stream.MinTimestamp = time.Unix(0, columnEntry.Value.Int64()).UTC()

		case streamsmd.COLUMN_TYPE_MAX_TIMESTAMP:
			if ty := columnEntry.Value.Type(); ty != datasetmd.VALUE_TYPE_INT64 {
				return stream, fmt.Errorf("invalid type %s for %s", ty, column.Type)
			}
			stream.MaxTimestamp = time.Unix(0, columnEntry.Value.Int64()).UTC()

		case streamsmd.COLUMN_TYPE_LABEL:
			if ty := columnEntry.Value.Type(); ty != datasetmd.VALUE_TYPE_STRING {
				return stream, fmt.Errorf("invalid type %s for %s", ty, column.Type)
			}
			stream.Labels = append(stream.Labels, labels.Label{
				Name:  column.Info.Name,
				Value: columnEntry.Value.String(),
			})

		case streamsmd.COLUMN_TYPE_ROWS:
			if ty := columnEntry.Value.Type(); ty != datasetmd.VALUE_TYPE_INT64 {
				return stream, fmt.Errorf("invalid type %s for %s", ty, column.Type)
			}
			stream.Rows = int(columnEntry.Value.Int64())
		}
	}

	return stream, nil
}
