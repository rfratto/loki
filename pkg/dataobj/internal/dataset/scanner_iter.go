package dataset

import (
	"iter"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"golang.org/x/net/context"
)

// columnIter iterates over entries in a column, lazily loading pages on
// demand.
type columnIter struct {
	column     Column
	pages      []Page
	rowOffsets []int
	getter     Dataset

	row int
}

func newColumnIter(column Column, pages []Page, dataset Dataset) *columnIter {
	_, offsets := getRowOffsets(pages)

	return &columnIter{
		column:     column,
		pages:      pages,
		rowOffsets: offsets,
		getter:     dataset,
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
			curPage     Page
			curPageData page.Data
		)

	NextPage:
		for {
			// Find the page it.row is in.
			nextPage, pageStart, _ := it.pageForRow(it.row)
			if nextPage == nil { // No more pages.
				return
			} else if nextPage != curPage { // Lazily load page data.
				data, err := nextPage.Data(ctx)
				if err != nil {
					yield(ScannerEntry{}, err)
					return
				}

				curPage = nextPage
				curPageData = data
			}

			startRow := it.row

			for ent, err := range iterPage(pageStart, curPage, curPageData) {
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

func iterPage(startRow int, p Page, data page.Data) iter.Seq2[ScannerEntry, error] {
	return func(yield func(ScannerEntry, error) bool) {
		row := startRow

		for val, err := range page.Iter(page.Raw(p.Info(), data)) {
			if err != nil {
				yield(ScannerEntry{}, err)
				return
			}

			entry := ScannerEntry{Row: row, Value: val}
			if !yield(entry, nil) {
				return
			}
			row++
		}
	}
}

// pageForRow returns the page that contains the provided row number. If row is
// out of bounds of all pages, pageForRow returns nil.
func (it *columnIter) pageForRow(row int) (p Page, startRow, endRow int) {
	for i, page := range it.pages {
		pageStart := it.rowOffsets[i]
		pageEnd := pageStart + int(page.Info().RowCount) - 1

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
