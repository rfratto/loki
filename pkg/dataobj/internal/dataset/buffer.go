package dataset

import "github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"

// TODO(rfratto): It might not make sense for Buffer to be an interface; the
// implementations so far are extremely similar, with the only difference being
// which encoder they immediately pass data to.

// A Buffer accumulates RowType records in memory for a [Column].
type Buffer[RowType any] interface {
	// Append a new RowType record into the Buffer.
	//
	// Append returns true if the data was appended; false if the page was full.
	Append(value RowType) bool

	// AppendNull appends a NULL value to the Buffer. AppendNull returns true if
	// the NULL was appended; false if the page was full.
	AppendNull() bool

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

// BufferOptions configures common settings for a [Buffer].
type BufferOptions struct {
	// PageSizeHint hints the size to target for each page. Buffers try to fill
	// pages as close to this size as possible after accounting for both encoding
	// and compression, but the actual size may be slightly larger or smaller.
	PageSizeHint int

	// Compression is the compression algorithm to use.
	Compression datasetmd.CompressionType
}
