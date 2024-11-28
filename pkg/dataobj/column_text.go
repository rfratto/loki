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
//
// textPage is encoded with plain encoding. Non-empty strings are encoded first
// with the uvarint length of the string, followed by the bytes of the string.
// A sequence of empty strings are encoded with a uvarint length of 0, followed
// by the count of empty strings.
//
// This encoding style allows for efficient packing of NULLS, where up to 2^64
// - 1 NULLS can be packed into 11 bytes (one for the 0 length, another for the
// 64-bit uvarint count).
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
		for range p.rows {
			size, err := binary.ReadUvarint(&br)
			if errors.Is(err, io.EOF) {
				// TODO(rfratto): log; unexpected early EOF
				return
			} else if err != nil {
				yield("", err)
				return
			}

			switch size {
			case 0: // Sequence of NULLs
				nullCount, err := binary.ReadUvarint(&br)
				if errors.Is(err, io.EOF) {
					// TODO(rfratto): log; unexpected early EOF
					return
				} else if err != nil {
					yield("", err)
					return
				}

				// Yield nullCount NULLs.
				for range nullCount {
					if !yield("", nil) {
						return
					}
				}

			default: // String
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
}

// Append appends text to the page. The row argument specifies the column-wide
// row number (zero-indexed) of the text being appended. Missing rows will be
// backfilled with empty entries.
//
// Append returns true if text was appended; false if the page was full.
//
// Append panics if called with an out-of-order row number.
func (p *textPage) Append(row int, text string) bool {
	pageRow := p.firstRow + p.rows
	fmt.Println("Row", row, "PageRow", pageRow)

	if row < pageRow {
		panic(fmt.Sprintf("textPage.Append: out-of-order row %d (expected >= %d)", row, pageRow))
	} else if row > pageRow {
		// Backfill row - pageRow rows.
		//
		// As a special case, NULLs are encoded as [0 null_count] to pack more
		// nulls into a single page.
		countBuf, countBufRelease := getUvarint(uint64(row - pageRow))
		defer func() { countBufRelease(&countBuf) }()

		writeSize := 1 /* for the 0 len*/ + len(countBuf)
		if len(p.buf) > 0 && len(p.buf)+writeSize > p.maxPageSizeBytes {
			return false
		}

		p.buf = append(p.buf, 0)           // 0 len
		p.buf = append(p.buf, countBuf...) // Count of nulls
		p.rows += row - pageRow
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
