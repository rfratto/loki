package dataobj

import (
	"bufio"
	"fmt"
	"iter"
)

// TODO(rfratto): replace textColumn, timeColumn with column. Create
// page_time.go and page_text.go.
//
// See other TODOs in this file; some details are changing that should be
// handled (such as having memPage-scoped row numbers and column-scoped row
// numbers).

// column holds a column of data for a given RowType. Records are accumulated
// in memory and then flushed to a [page] once the [memPage] is full.
type column[RowType any] struct {
	pages    []page
	pageIter func(s scanner, rows int) iter.Seq2[RowType, error]
	curPage  memPage[RowType]
}

// Append a new record into the column. If the current [memPage] is full,
// Append cuts the page and then appends the record into the new page.
func (c *column[RowType]) Append(row int, value RowType) {
	// TODO(rfratto): translate column row index to memPage row index to avoid
	// having column state leak into memPages.

	// TODO(rfratto): should Append automatically handle backfilling so we don't
	// have to pass the row down to the memPage?

	// We give two attempts to append the data to the page; if the current page
	// is full, we cut it and append to the reset page.
	//
	// Appending to the reset page should never fail, as it'll allow its first
	// record to be oversized. If it fails, there's a bug.
	for range 2 {
		if c.curPage.Append(row, value) {
			return
		}
		c.cutPage()
	}

	panic("column.Append: failed to append text to fresh page")
}

func (c *column[RowType]) cutPage() {
	page, err := c.curPage.Flush()
	if err != nil {
		panic(fmt.Sprintf("failed to flush page: %v", err))
	}
	c.pages = append(c.pages, page)
}

// Iter returns an iterator over all data in the column. Iteration stops until
// all rows have been returned or upon encountering an error.
func (c *column[RowType]) Iter() iter.Seq2[RowType, error] {
	return func(yield func(RowType, error) bool) {
		// Iterate over cut pages.
		for _, page := range c.pages {
			if !c.iterPage(page, yield) {
				return
			}
		}

		curBytes, curRows := c.curPage.Data()
		br := byteReader{buf: curBytes}
		c.pageIter(&br, curRows)(yield)
	}
}

func (c *column[RowType]) iterPage(p page, yield func(RowType, error) bool) bool {
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
func (c *column[RowType]) Backfill(row int) {
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
func (c *column[RowType]) Count() int {
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

// memPage accumulates RowType records in memory for a [column].
type memPage[RowType any] interface {
	// Append a new RowType record into the memPage at the provided zero-indexed
	// row number. Row numbers are scoped to the memPage and not the column.
	//
	// Append returns true if the data was appended; false if the page was full.
	Append(row int, value RowType) bool

	// Flush returns a page from the memPage, then resets the memPage for new
	// data. After Flush is called, row numbers start back at 0.
	Flush() (page, error)

	// Data returns the current data in the memPage along with the number of
	// rows. The returned buf must not be modified.
	Data() (buf []byte, rows int)

	// Backfill appends empty entries to the memPage up to the provided
	// zero-indexed row number. For example, if the memPage is at row 1, and
	// Backfill(5) is invoked, four empty entries will be appended to the column.
	// Row numbers are scoped to the memPage and not the column.
	//
	// Backfill does nothing if the column already contains the provided number
	// of rows.
	Backfill(row int) bool
}
