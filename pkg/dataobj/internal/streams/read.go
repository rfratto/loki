package streams

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/obj"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
)

// IterStreams iterates over streams in the provided decoder.
func IterStreams(ctx context.Context, dec obj.Decoder) result.Seq[Stream] {
	return result.Iter(func(yield func(Stream) bool) error {
		sections, err := dec.Sections(ctx)
		if err != nil {
			return err
		}

		streamsDec := dec.StreamsDecoder()

		for _, section := range sections {
			if section.Type != filemd.SECTION_TYPE_STREAMS {
				continue
			}

			for res := range iterSection(ctx, streamsDec, section) {
				if res.Err() != nil || !yield(res.MustValue()) {
					return res.Err()
				}
			}
		}

		return nil
	})
}

func iterSection(ctx context.Context, dec obj.StreamsDecoder, section *filemd.SectionInfo) result.Seq[Stream] {
	return result.Iter(func(yield func(Stream) bool) error {
		if section.Type != filemd.SECTION_TYPE_STREAMS {
			return nil
		}

		streamsColumns, err := dec.Columns(ctx, section)
		if err != nil {
			return err
		}

		dset, err := obj.StreamsDataset(dec, section)
		if err != nil {
			return err
		}

		columns, err := result.Collect(dset.ListColumns(ctx))
		if err != nil {
			return err
		}

		scanner := dataset.NewScanner(dset, columns)
		for result := range scanner.Iter(ctx) {
			rowEntry, err := result.Value()
			if err != nil {
				return err
			}

			stream, err := decodeRow(streamsColumns, rowEntry)
			if err != nil {
				return err
			} else if !yield(stream) {
				return nil
			}
		}

		return nil
	})
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
