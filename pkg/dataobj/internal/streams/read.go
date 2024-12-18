package streams

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/obj"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
	"github.com/prometheus/prometheus/model/labels"
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

			columns, err := streamsDec.Columns(ctx, section)
			if err != nil {
				yield(Stream{}, err)
				return
			}

			type puller2[T page.DataType] struct {
				Next func() (dataset.Entry[T], error, bool)
				Stop func()
			}

			type labelPuller struct {
				LabelName string
				Puller    puller2[string]
			}

			var (
				pullMinTs  puller2[int64]
				pullMaxTs  puller2[int64]
				pullRows   puller2[int64]
				pullLabels []labelPuller
			)

			// Open pull-based iterators for all of our columns.
			for _, column := range columns {
				pages, err := streamsDec.Pages(ctx, column)
				if err != nil {
					yield(Stream{}, fmt.Errorf("getting pages: %w", err))
				}

				switch column.Type {
				case streamsmd.COLUMN_TYPE_MIN_TIMESTAMP:
					values := dataset.IterPages[int64](streamsDec.ReadPages(ctx, pages))
					next, stop := iter.Pull2(values)
					pullMinTs = puller2[int64]{next, stop}

				case streamsmd.COLUMN_TYPE_MAX_TIMESTAMP:
					values := dataset.IterPages[int64](streamsDec.ReadPages(ctx, pages))
					next, stop := iter.Pull2(values)
					pullMaxTs = puller2[int64]{next, stop}

				case streamsmd.COLUMN_TYPE_LABEL:
					values := dataset.IterPages[string](streamsDec.ReadPages(ctx, pages))
					next, stop := iter.Pull2(values)
					pullLabels = append(pullLabels, labelPuller{
						LabelName: column.Info.Name,
						Puller:    puller2[string]{next, stop},
					})

				case streamsmd.COLUMN_TYPE_ROWS:
					values := dataset.IterPages[int64](streamsDec.ReadPages(ctx, pages))
					next, stop := iter.Pull2(values)
					pullRows = puller2[int64]{next, stop}

				default:
					yield(Stream{}, fmt.Errorf("unknown column type %s", column.Type))
					return
				}
			}
			switch {
			case pullMinTs.Next == nil:
				yield(Stream{}, errors.New("missing minimum timestamp column"))
				return
			case pullMaxTs.Next == nil:
				yield(Stream{}, errors.New("missing maximum timestamp column"))
				return
			case pullRows.Next == nil:
				yield(Stream{}, errors.New("missing row count column"))
				return
			case len(pullLabels) == 0:
				yield(Stream{}, errors.New("missing at least one label column"))
				return
			}
			defer func() {
				pullMinTs.Stop()
				pullMaxTs.Stop()
				pullRows.Stop()
				for _, lp := range pullLabels {
					lp.Puller.Stop()
				}
			}()

			// Finally, iterate over all rows until we're out of entries.
			for {
				minTs, err, ok := pullMinTs.Next()
				if err != nil {
					yield(Stream{}, fmt.Errorf("pulling min ts: %w", err))
					return
				} else if !ok {
					return
				}

				maxTs, err, ok := pullMaxTs.Next()
				if err != nil {
					yield(Stream{}, fmt.Errorf("pulling max ts: %w", err))
					return
				} else if !ok {
					return
				}

				rowCount, err, ok := pullRows.Next()
				if err != nil {
					yield(Stream{}, fmt.Errorf("pulling row count: %w", err))
					return
				} else if !ok {
					return
				}

				var labelSet labels.Labels
				for _, lp := range pullLabels {
					labelValue, err, ok := lp.Puller.Next()
					if err != nil {
						yield(Stream{}, fmt.Errorf("pulling label %q: %w", lp.LabelName, err))
						return
					} else if !ok {
						// Column exhausted; this is OK since we can treat other rows are
						// NULL.
						continue
					}

					if val := labelValue.Value; val != "" {
						labelSet = append(labelSet, labels.Label{Name: lp.LabelName, Value: val})
					}
				}

				stream := Stream{
					Labels:       labelSet,
					MinTimestamp: time.Unix(0, minTs.Value).UTC(),
					MaxTimestamp: time.Unix(0, maxTs.Value).UTC(),
					Rows:         int(rowCount.Value),
				}
				if !yield(stream, nil) {
					return
				}
			}
		}
	}
}
