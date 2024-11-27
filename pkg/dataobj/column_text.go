package dataobj

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"unsafe"
)

var textColumn struct {
	maxPageSizeBytes int

	pages []textPage
}

// textPage is an individual page containing text data. Calls to textPage are
// not goroutine safe; the caller must synchronize access.
type textPage struct {
	// maxPageSizeBytes is the maximum size of encoded data this page may contain
	// pre-compression.
	maxPageSizeBytes int

	// Index of the very first row in this page, across all pages. Rows are
	// zero-indexed.
	firstRow int

	rows int
	buf  []byte
}

// Iter returns an iterator over the text in the page. Iteration stops upon
// encountering an error.
func (p *textPage) Iter() iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		br := byteReader{buf: p.buf}

		// We iterate over the number of expected rows in the page (rather than
		// freely fetching entries until EOF) to catch potential encoding errors.
		for i := 0; i < p.rows; i++ {
			size, err := binary.ReadUvarint(&br)
			if errors.Is(err, io.EOF) {
				// TODO(rfratto): log; unexpected early EOF
				return
			} else if err != nil {
				yield("", err)
				return
			}

			textBytes, read := br.Next(int(size))
			if read != int(size) {
				// TODO(rfratto): log; unexpected early EOF
				return
			}

			// We make a copy of the text here to ensure the garbage collector can
			// free p.buf once we're done reading the pages.
			if !yield(string(textBytes), nil) {
				return
			}
		}
	}
}

// Append appends text to the page, using run-length encoding. The row argument
// specifies the column-wide row number of the text being appended. Missing
// rows will be backfilled with empty entries.
//
// Append returns true if text was appended; false if the page was full.
//
// Append panics if called with an out-of-order row number.
func (p *textPage) Append(row int, text string) bool {
	pageRow := p.firstRow + p.rows
	if row < pageRow {
		panic(fmt.Sprintf("textPage.Append: out-of-order row %d (expected >= %d)", row, pageRow))
	}

	// Backfill missing rows.
	for row > pageRow {
		// TODO(rfratto): encoding missing rows as [0 null_count] to pack more
		// nulls into a single page.
		if len(p.buf) > 0 && len(p.buf)+1 > p.maxPageSizeBytes {
			return false
		}

		// Uvarint 0 encodes as 0.
		p.buf = append(p.buf, 0)
		p.rows++
		pageRow++
	}

	// Append text, if it would fit.
	lenBuf, lenBufRelease := getUvarint(uint64(len(text)))
	defer func() { lenBufRelease(&lenBuf) }()

	writeSize := len(lenBuf) + len(text)
	if len(p.buf) > 0 && len(p.buf)+writeSize > p.maxPageSizeBytes {
		return false
	}

	p.buf = append(p.buf, lenBuf...)
	p.buf = append(p.buf, unsafe.Slice(unsafe.StringData(text), len(text))...)
	p.rows++
	return true
}
