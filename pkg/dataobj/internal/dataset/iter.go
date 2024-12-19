package dataset

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/bitmap"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"golang.org/x/net/context"
)

// columnIter iterates over entries in a column by lazily loading pages on
// demand.
type columnIter struct {
	column     *datasetmd.ColumnInfo
	pages      []*datasetmd.PageInfo
	rowOffsets []int
	getter     PageGetter

	row int
}

func newColumnIter(column *datasetmd.ColumnInfo, pages []*datasetmd.PageInfo, getter PageGetter) *columnIter {
	_, offsets := getRowOffsets(pages)

	return &columnIter{
		column:     column,
		pages:      pages,
		rowOffsets: offsets,
		getter:     getter,
	}
}

// Seek sets the row number for the next row to read. If Seek is called
// mid-iteration, the provided row number will be the next row emitted.
func (it *columnIter) Seek(row int) {
	it.row = row
}

// Iter returns an interator over entries in the column. Pages are lazily
// loaded as needed. [columnIter.Seek] may be called mid-iteration to skip
// pages or rows.
//
// An Iter may be re-consumed by calling it.Seek(0).
func (it *columnIter) Iter(ctx context.Context) iter.Seq2[ScannerEntry, error] {
	return func(yield func(ScannerEntry, error) bool) {
		var (
			curPage     *datasetmd.PageInfo
			curPageData PageData
		)

	NextPage:
		for {
			// Find the page it.row is in.
			nextPage, pageStart, _ := it.pageForRow(it.row)
			if nextPage == nil { // No more pages.
				return
			} else if nextPage != curPage { // Lazily load page data.
				data, err := it.getter.ReadPages(ctx, []*datasetmd.PageInfo{nextPage})
				if err != nil {
					yield(ScannerEntry{}, err)
					return
				} else if len(data) != 1 {
					yield(ScannerEntry{}, errors.New("page count mismatch"))
					return
				}

				curPage = nextPage
				curPageData = data[0]
			}

			startRow := it.row

			for ent, err := range iterPage(pageStart, it.column, curPage, curPageData) {
				if err != nil {
					yield(ent, err)
					return
				}

				// Update our row offset to start at the next row.
				it.row = ent.Row + 1

				// Emit the current row if it's after our starting row, otherwise we're
				// trying to do a partial read through a page.
				if ent.Row >= startRow && !yield(ent, err) {
					return
				}

				// If a call to yield updated it.row to be before the current row,
				// we've seeked backwards; start over so we iterate this page from the
				// start.
				if it.row <= ent.Row {
					continue NextPage
				}

				// Update our start row if we've read far enough.
				startRow = max(startRow, it.row)
			}
		}
	}
}

func iterPage(startRow int, column *datasetmd.ColumnInfo, pageInfo *datasetmd.PageInfo, data PageData) iter.Seq2[ScannerEntry, error] {
	p := RawPage(column, pageInfo, data)

	return func(yield func(ScannerEntry, error) bool) {
		presenceReader, valuesReader, err := p.Reader()
		if err != nil {
			yield(ScannerEntry{}, fmt.Errorf("opening page for reading: %w", err))
			return
		}
		defer presenceReader.Close()
		defer valuesReader.Close()

		presenceDec := bitmap.NewDecoder(bufio.NewReader(presenceReader))
		valuesDec := newDecoder(bufio.NewReader(valuesReader), column.ValueType, p.Encoding)
		if valuesDec == nil {
			yield(ScannerEntry{}, fmt.Errorf("no decoder available: %w", err))
			return
		}

		row := startRow
		for {
			entry := ScannerEntry{Row: row}

			present, err := presenceDec.Decode()
			if errors.Is(err, io.EOF) {
				return
			} else if err != nil {
				yield(entry, fmt.Errorf("decoding presence: %w", err))
				return
			} else if present.Type() != datasetmd.VALUE_TYPE_UINT64 {
				yield(entry, fmt.Errorf("invalid presence type: %v", present.Type()))
				return
			}

			if present.Uint64() == 1 {
				value, err := valuesDec.Decode()
				if err != nil {
					yield(entry, fmt.Errorf("decoding value: %w", err))
				}
				entry.Value = value
			}

			if !yield(entry, nil) {
				return
			}
			row++
		}
	}
}

// pageForRow returns the page that contains the provided row number. If row is
// out of bounds of all pages, pageForRow returns nil.
func (it *columnIter) pageForRow(row int) (pi *datasetmd.PageInfo, startRow, endRow int) {
	for i, page := range it.pages {
		pageStart := it.rowOffsets[i]
		pageEnd := pageStart + int(page.RowsCount) - 1

		if row >= pageStart && row <= pageEnd {
			return page, pageStart, pageEnd
		}
	}

	return nil, -1, -1
}

// preloadPages preloads a set of pages in bulk across the following iterators.
// Pages for which the iterators have yet to fetch for their upcoming row will
// be retrieved in a batch.
func preloadPages(ctx context.Context, it ...*columnIter) error {
	// TODO(rfratto): impl (and use!) this will require moving some additional
	// state out of the Iter() loop and into the columnIter itself.
	panic("NYI")
}
