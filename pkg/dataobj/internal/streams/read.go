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

		streamPuller, err := newStreamPuller(ctx, dec, columns)
		if err != nil {
			yield(Stream{}, err)
			return
		}
		defer streamPuller.Stop()

		for {
			stream, err, ok := streamPuller.Next()
			if !ok || !yield(stream, nil) || err != nil {
				return
			}
		}
	}
}

type streamPuller struct {
	pullMinTimestamp pullIter[int64]
	pullMaxTimestamp pullIter[int64]
	pullRowCount     pullIter[int64]
	pullLabels       []labelPuller
}

type pullIter[T page.DataType] struct {
	Next func() (dataset.Entry[T], error, bool)
	Stop func()
}

type labelPuller struct {
	pullIter[string]
	LabelName string
}

// newStreamPuller loads pages from all of the provided columns and iterates
// over the streams.
func newStreamPuller(ctx context.Context, dec obj.StreamsDecoder, columns []*streamsmd.ColumnDesc) (sp *streamPuller, err error) {
	sp = &streamPuller{}
	defer func() {
		if err != nil {
			sp.Stop()
		}
	}()

	for _, column := range columns {
		pages, err := dec.Pages(ctx, column)
		if err != nil {
			return nil, fmt.Errorf("getting pages: %w", err)
		}

		switch column.Type {
		case streamsmd.COLUMN_TYPE_MIN_TIMESTAMP:
			values := dataset.IterPages[int64](dec.ReadPages(ctx, pages))
			next, stop := iter.Pull2(values)
			sp.pullMinTimestamp = pullIter[int64]{next, stop}

		case streamsmd.COLUMN_TYPE_MAX_TIMESTAMP:
			values := dataset.IterPages[int64](dec.ReadPages(ctx, pages))
			next, stop := iter.Pull2(values)
			sp.pullMaxTimestamp = pullIter[int64]{next, stop}

		case streamsmd.COLUMN_TYPE_LABEL:
			values := dataset.IterPages[string](dec.ReadPages(ctx, pages))
			next, stop := iter.Pull2(values)
			sp.pullLabels = append(sp.pullLabels, labelPuller{
				pullIter:  pullIter[string]{next, stop},
				LabelName: column.Info.Name,
			})

		case streamsmd.COLUMN_TYPE_ROWS:
			values := dataset.IterPages[int64](dec.ReadPages(ctx, pages))
			next, stop := iter.Pull2(values)
			sp.pullRowCount = pullIter[int64]{next, stop}

		default:
			return nil, fmt.Errorf("unknown column type %s", column.Type)
		}
	}
	switch {
	case sp.pullMaxTimestamp.Next == nil:
		return nil, errors.New("missing minimum timestamp column")
	case sp.pullMaxTimestamp.Next == nil:
		return nil, errors.New("missing maximum timestamp column")
	case sp.pullRowCount.Next == nil:
		return nil, errors.New("missing row count column")
	case len(sp.pullLabels) == 0:
		return nil, errors.New("missing at least one label column")
	}

	return sp, nil
}

func (p *streamPuller) Next() (Stream, error, bool) {
	minTs, err, ok := p.pullMinTimestamp.Next()
	if err != nil {
		return Stream{}, fmt.Errorf("pulling min ts: %w", err), true
	} else if !ok {
		return Stream{}, nil, false
	}

	maxTs, err, ok := p.pullMaxTimestamp.Next()
	if err != nil {
		return Stream{}, fmt.Errorf("pulling max ts: %w", err), true
	} else if !ok {
		return Stream{}, nil, false
	}

	rowCount, err, ok := p.pullRowCount.Next()
	if err != nil {
		return Stream{}, fmt.Errorf("pulling row count: %w", err), true
	} else if !ok {
		return Stream{}, nil, false
	}

	var labelSet labels.Labels
	for _, lp := range p.pullLabels {
		labelValue, err, ok := lp.Next()
		if err != nil {
			return Stream{}, fmt.Errorf("pulling label %q: %w", lp.LabelName, err), true
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
	return stream, nil, true
}

func (p *streamPuller) Stop() {
	callIfPresent := func(stop func()) {
		if stop != nil {
			stop()
		}
	}

	callIfPresent(p.pullMinTimestamp.Stop)
	callIfPresent(p.pullMaxTimestamp.Stop)
	callIfPresent(p.pullRowCount.Stop)
	for _, lp := range p.pullLabels {
		callIfPresent(lp.Stop)
	}
}
