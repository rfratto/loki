package dataset

// A Buffer accumulates RowType records in memory for a [Column].
type Buffer[RowType any] interface {
	// Append a new RowType record into the Buffer at the provided zero-indexed
	// row number. Row numbers are scoped to the Buffer and not the column.
	//
	// Append returns true if the data was appended; false if the page was full.
	Append(row int, value RowType) bool

	// Backfill appends empty entries to the Buffer up to (and including) the
	// provided zero-indexed row number. For example, if there is one row in the
	// buffer (index 0), and Backfill(5) is invoked, five empty entries will be
	// appended to the head page (index 1, 2, 3, 4, 5). Row numbers are scoped to
	// the headPage and not the headPage and not the column.
	//
	// Backfill does nothing if the column already contains the provided number
	// of rows.
	Backfill(row int) bool

	// Data returns the current data in the Buffer along with the number of rows.
	// The returned buf must not be modified.
	Data() (buf []byte, rows int)

	// Flush returns a page from the Buffer, then resets the Buffer for new data.
	// After Flush is called, row numbers start back at 0.
	Flush() (Page, error)
}

// BufferRows returns the number of rows in the Buffer.
func BufferRows[RowType any](b Buffer[RowType]) int {
	_, rows := b.Data()
	return rows
}
