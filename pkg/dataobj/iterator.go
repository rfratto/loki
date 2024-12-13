package dataobj

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/decoder"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/logstreamsmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
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

func (it *entryIterator) Iter() iter.Seq2[push.Entry, error] {
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

	return func(yield func(push.Entry, error) bool) {
		yield(push.Entry{}, fmt.Errorf("entryIterator: NYI"))
	}
}

func lazyTimeIterator(ctx context.Context, dec decoder.StreamsDecoder, col *logstreamsmd.ColumnInfo, pages []*logstreamsmd.PageInfo) iter.Seq2[time.Time, error] {
	return func(yield func(time.Time, error) bool) {
		for pageData, err := range dec.ReadPages(ctx, pages) {
			if err != nil {
				yield(time.Time{}, err)
				return
			}

			for ts, err := range streams.IterTimePage(col, pageData) {
				if !yield(ts, err) {
					return
				}
				if err != nil {
					return
				}
			}
		}
	}
}

func lazyTextIterator(ctx context.Context, dec decoder.StreamsDecoder, col *logstreamsmd.ColumnInfo, pages []*logstreamsmd.PageInfo) iter.Seq2[time.Time, error] {
	return func(yield func(time.Time, error) bool) {
		for pageData, err := range dec.ReadPages(ctx, pages) {
			if err != nil {
				yield(time.Time{}, err)
				return
			}

			for ts, err := range streams.IterTimePage(col, pageData) {
				if !yield(ts, err) {
					return
				}
				if err != nil {
					return
				}
			}
		}
	}
}
