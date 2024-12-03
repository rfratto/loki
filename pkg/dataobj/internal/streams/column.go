package streams

import (
	"bufio"
	"fmt"
	"iter"
)

// Column holds a Column of data for a given RowType. Records are accumulated
// in memory and then flushed to a [page] once the [headPage] is full.
type Column[RowType any] struct {
	// columnRows tracks the number of rows in the column, not including the head
	// page. It's updated when a page is cut.
	columnRows int
	headRows   int // headRows tracks the number of rows in the head page.

	pages    []Page
	pageIter func(s scanner, rows int) iter.Seq2[RowType, error]
	curPage  headPage[RowType]
}

// Append a new record into the column. If the current [headPage] is full,
// Append cuts the page and then appends the record into the new page.
func (c *Column[RowType]) Append(row int, value RowType) {
	// TODO(rfratto): should Append automatically handle backfilling so we don't
	// have to pass the row down to the headPage?

	// We give two attempts to append the data to the page; if the current page
	// is full, we cut it and append to the reset page.
	//
	// Appending to the reset page should never fail, as it'll allow its first
	// record to be oversized. If it fails, there's a bug.
	for range 2 {
		// Translate the column row index to the head page row index. This must be
		// done in the loop since the number of pages may change.
		headRow := c.headRow(row)

		if c.curPage.Append(headRow, value) {
			c.updateHeadRowCount(headRow)
			return
		}
		c.cutPage()
	}

	panic("column.Append: failed to append text to fresh page")
}

// headRow gets the head page row number from the column row number.
func (c *Column[RowType]) headRow(columnRow int) int {
	return columnRow - c.columnRows
}

// updateHeadRowCount updates the head page's row count if the provided head
// row is greater than the current row count.
func (c *Column[RowType]) updateHeadRowCount(headRow int) {
	rows := headRow + 1 // Row is zero-indexed so the count of rows is row+1.

	// Out-of-order row writes are not allowed and panic elsewhere, but to be
	// safe we check here.
	if rows > c.headRows {
		c.headRows = rows
	}
}

func (c *Column[RowType]) cutPage() {
	page, err := c.curPage.Flush()
	if err != nil {
		panic(fmt.Sprintf("failed to flush page: %v", err))
	}
	c.columnRows += page.RowCount
	c.headRows = 0 // Reset the head page row count.
	c.pages = append(c.pages, page)
}

// Iter returns an iterator over all data in the column. Iteration stops until
// all rows have been returned or upon encountering an error.
func (c *Column[RowType]) Iter() iter.Seq2[RowType, error] {
	return func(yield func(RowType, error) bool) {
		// Iterate over cut pages.
		for _, page := range c.pages {
			if !c.iterPage(page, yield) {
				return
			}
		}

		headPageIter(c.curPage, c.pageIter)(yield)
	}
}

func (c *Column[RowType]) iterPage(p Page, yield func(RowType, error) bool) bool {
	var zero RowType

	r, err := p.Reader()
	if err != nil {
		yield(zero, err)
		return false
	}
	defer r.Close()

	// TODO(rfratto): pool?
	br := bufio.NewReaderSize(r, 4096)

	for s, err := range c.pageIter(br, int(p.RowCount)) {
		if err != nil {
			yield(zero, err)
			return false
		} else if !yield(s, nil) {
			return false
		}
	}

	return true
}

// Backfill appends empty entries to the column up to the provided row number.
// For example, if the column is empty, and Backfill(5) is invoked, five empty
// entries will be appended to the column.
//
// Backfill does nothing if the column already contains the provided number of
// rows.
func (c *Column[RowType]) Backfill(row int) {
	// We give two attempts to append the data to the page; if the current page
	// is full, we cut it and append to the reset page.
	//
	// Appending to the reset page should never fail, as it'll allow its first
	// record to be oversized. If it fails, there's a bug.
	for range 2 {
		if c.curPage.Backfill(row) {
			return
		}
		c.cutPage()
	}

	panic("column.Backfill: failed to backfill to fresh page")
}

// Count returns the number of rows in the column.
func (c *Column[RowType]) Count() int {
	count := 0
	for _, p := range c.pages {
		count += p.RowCount
	}
	if c.curPage != nil {
		_, memRows := c.curPage.Data()
		count += memRows
	}
	return count
}

// UncompressedSize returns the total size of the column in bytes before
// compression, including the data currently in the head page.
func (c *Column[RowType]) UncompressedSize() int {
	var total int
	for _, p := range c.pages {
		total += p.UncompressedSize
	}
	total += headPageSize(c.curPage)
	return total
}

// CompressedSize returns the total size of the column in bytes after
// compression. If a page has no compression, its compressed size is the same
// as its uncompressed size.
//
// If includeHead is true, the current uncompressed data in the head page is
// included in the result.
func (c *Column[RowType]) CompressedSize(includeHead bool) int {
	var total int
	for _, p := range c.pages {
		total += p.CompressedSize
	}
	if includeHead {
		total += headPageSize(c.curPage)
	}
	return total
}

// headPage accumulates RowType records in memory for a [column].
type headPage[RowType any] interface {
	// Append a new RowType record into the headPage at the provided zero-indexed
	// row number. Row numbers are scoped to the headPage and not the column.
	//
	// Append returns true if the data was appended; false if the page was full.
	Append(row int, value RowType) bool

	// Backfill appends empty entries to the headPage up to (and including) the
	// provided zero-indexed row number. For example, if there is one row in the
	// head page (index 0), and Backfill(5) is invoked, five empty entries will
	// be appended to the head page (index 1, 2, 3, 4, 5). Row numbers are scoped
	// to the headPage and not the headPage and not the column.
	//
	// Backfill does nothing if the column already contains the provided number
	// of rows.
	Backfill(row int) bool

	// Data returns the current data in the headPage along with the number of
	// rows. The returned buf must not be modified.
	Data() (buf []byte, rows int)

	// Flush returns a page from the headPage, then resets the headPage for new
	// data. After Flush is called, row numbers start back at 0.
	Flush() (Page, error)
}

// headPageSize returns the size of a headPage in bytes.
func headPageSize[RowType any](p headPage[RowType]) int {
	buf, _ := p.Data()
	return len(buf)
}

func headPageIter[RowType any](
	p headPage[RowType],
	iter func(s scanner, rows int) iter.Seq2[RowType, error],
) iter.Seq2[RowType, error] {

	curBytes, curRows := p.Data()
	br := byteReader{buf: curBytes}
	return iter(&br, curRows)
}
