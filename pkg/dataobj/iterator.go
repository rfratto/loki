package dataobj

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/decoder"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/logstreams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/logstreamsmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
)

type entryIterator struct {
	dec    decoder.StreamsDecoder
	stream *logstreamsmd.StreamInfo
	opts   EntriesOptions
}

func newEntryIterator(dec decoder.StreamsDecoder, stream *logstreamsmd.StreamInfo, opts EntriesOptions) *entryIterator {
	return &entryIterator{
		dec:    dec,
		stream: stream,
		opts:   opts,
	}
}

func (it *entryIterator) Iter() result.Seq[push.Entry] {
	// TODO(rfratto): entryIterator needs to do a few things:
	//
	// 1. Pull the set of columns that are going to be needed based on opts.
	//
	// 2. For each column to read, pull the set of pages.
	//
	// 3. Create lazy iterators for each column and convert them into pull-based
	//    iterators.
	//
	// 4. Pull values from all the columns and combine them into a push.Entry.
	//
	// This should be enough for a basic implementation and get some tests
	// passing. Once sorted timestamps are guaranteed, we'll want a few extra
	// changes:
	//
	// * Detect the starting row number and filter out pages that don't contain
	//   those rows; this will also require passing additional info to lazy
	//   iterators so they know which records to skip within the loaded pages.

	return result.Iter(func(yield func(push.Entry) bool) error {
		return fmt.Errorf("entryIterator: NYI")
	})
}

func lazyTimeIterator(ctx context.Context, dec decoder.StreamsDecoder, col *logstreamsmd.ColumnInfo, pages []*logstreamsmd.PageInfo) result.Seq[time.Time] {
	return result.Iter(func(yield func(time.Time) bool) error {
		for res := range dec.ReadPages(ctx, pages) {
			pageData, err := res.Value()
			if err != nil {
				return err
			}

			for res := range logstreams.IterTimePage(col, pageData) {
				if res.Err() != nil || !yield(res.MustValue()) {
					return res.Err()
				}
			}
		}

		return nil
	})
}

func lazyTextIterator(ctx context.Context, dec decoder.StreamsDecoder, col *logstreamsmd.ColumnInfo, pages []*logstreamsmd.PageInfo) result.Seq[time.Time] {
	return result.Iter(func(yield func(time.Time) bool) error {
		for res := range dec.ReadPages(ctx, pages) {
			pageData, err := res.Value()
			if err != nil {
				return err
			}

			for res := range logstreams.IterTimePage(col, pageData) {
				if res.Err() != nil || !yield(res.MustValue()) {
					return res.Err()
				}
			}
		}

		return nil
	})
}
