package dataset

// TODO(rfratto): If we switch to a system similar to definition levels in
// Parquet, Backfill no longer makes sense (nor does passing the row number to
// Append).
//
// It really depends how much work we want to make Buffer responsible for. If
// Buffer is still responsible for NULLs (but now puts them in definition
// levels), then some form of the interface below is still useful but might
// benefit from some tweaking.

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

	// EstimatedSize returns the estimated size of the Buffer in bytes.
	// EstimatedSize should return a value that accounts for both encoding and
	// compression. The true size of the page after flushing may be larger or
	// smaller than this value.
	EstimatedSize() int

	// Rows returns the number of rows in the Buffer.
	Rows() int

	// Flush returns a page from the Buffer, then resets the Buffer for new data.
	// After Flush is called, row numbers start back at 0.
	Flush() (Page, error)
}

// TODO(rfratto): We can get a little bit clever with implementing
// EstimatedSize to take advantage of streaming to a compression writer.
//
// If we track the number of bytes that have been written to our underlying
// buffer (post-encoding, post-compression), we can also track the number of
// bytes that have yet to be written (and scale it down by the average
// compression ratio for that algorithm).
//
// This approach would allow us to get pretty close to the target page size,
// but it's unlikely to be perfect, since the worst-case compression ratio (for
// completely random data) can be slightly lower than 1. We can account for
// this as best as possible by using the lower bound of what we would normally
// expect the ratio to be.
//
// This works best when we use compression algorithms with a block size lower
// than our page size. Zstd defaults to a block size of 128KB, and Snappy
// defaults to 64KB. Targeting a page size of 1.5MB means that we'll get a
// pretty good estimate.
