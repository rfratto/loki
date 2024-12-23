package dataset

import (
	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
	"golang.org/x/net/context"
)

// lazyColumnIter iterates over pages, lazily loading page data on demand.
type lazyColumnIter struct {
	pages      []Page
	rowOffsets []int
	getter     Dataset

	row int
}

func newLazyColumnIter(columnPages []Page, dataset Dataset) *lazyColumnIter {
	_, offsets := getRowOffsets(columnPages)

	return &lazyColumnIter{
		pages:      columnPages,
		rowOffsets: offsets,
		getter:     dataset,
	}
}

// Seek sets the row number for the next row to read. If Seek is called
// mid-iteration, the provided row number will be the next row emitted.
func (it *lazyColumnIter) Seek(row int) {
	it.row = row
}

// Iter returns an interator over entries in the column. Pages are lazily
// loaded as needed. [columnIter.Seek] may be called mid-iteration to skip
// pages or rows.
//
// An Iter may be re-consumed by calling it.Seek(0).
func (it *lazyColumnIter) Iter(ctx context.Context) result.Seq[ScannerEntry] {
	return result.Iter(func(yield func(ScannerEntry) bool) error {
		var (
			curPage     Page
			curPageData page.Data
		)

	NextPage:
		for {
			// Find the page it.row is in.
			nextPage, pageStart, _ := it.pageForRow(it.row)
			if nextPage == nil { // No more pages.
				return nil
			} else if nextPage != curPage { // Lazily load page data.
				data, err := nextPage.Data(ctx)
				if err != nil {
					return err
				}
				curPage = nextPage
				curPageData = data
			}

			startRow := it.row

			for res := range iterPage(pageStart, curPage, curPageData) {
				ent, err := res.Value()
				if err != nil {
					return err
				}

				// Update our row offset to start at the next row.
				it.row = ent.Row + 1

				// Emit the current row if it's after our starting row, otherwise we're
				// trying to do a partial read through a page.
				if ent.Row >= startRow && !yield(ent) {
					return nil
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
	})
}

func iterPage(startRow int, p Page, data page.Data) result.Seq[ScannerEntry] {
	return result.Iter(func(yield func(ScannerEntry) bool) error {
		row := startRow

		for res := range page.Iter(page.Raw(p.Info(), data)) {
			val, err := res.Value()
			if err != nil {
				return err
			}
			entry := ScannerEntry{Row: row, Value: val}
			if !yield(entry) {
				return nil
			}
			row++
		}

		return nil
	})
}

// pageForRow returns the page that contains the provided row number. If row is
// out of bounds of all pages, pageForRow returns nil.
func (it *lazyColumnIter) pageForRow(row int) (p Page, startRow, endRow int) {
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
func preloadPages(ctx context.Context, it ...*lazyColumnIter) error {
	// TODO(rfratto): impl (and use!) this will require moving some additional
	// state out of the Iter() loop and into the columnIter itself.
	panic("NYI")
}
