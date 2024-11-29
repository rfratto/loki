package dataobj

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"unsafe"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

type textColumn struct {
	maxPageSizeBytes int

	pages   []page
	curPage *memTextPage
}

func (c *textColumn) Append(row int, text string) {
	if c.curPage == nil {
		// We use compression on the individual pages, so we'll want to increase
		// their size to account for expected compression ratio.
		targetSize := targetCompressedPageSize(c.maxPageSizeBytes, streamsmd.COMPRESSION_GZIP)
		c.curPage = &memTextPage{
			maxPageSizeBytes: targetSize,
			buf:              make([]byte, 0, targetSize),
		}
	}

	// We give two attempts to append the data to the page; if the current page
	// is full, we cut it and append to the reset page.
	//
	// Appending to the reset page should never fail, as it'll allow its first
	// record to be oversized. If it fails, there's a bug.
	for range 2 {
		if c.curPage.Append(row, text) {
			return
		}
		c.cutPage()
	}

	panic("textColumn.Append: failed to append text to fresh page")
}

func (c *textColumn) cutPage() {
	if c.curPage == nil {
		return
	}

	buf, crc32, err := compressData(c.curPage.buf, streamsmd.COMPRESSION_GZIP)
	if err != nil {
		panic(fmt.Sprintf("failed to compress text page: %v", err))
	}

	c.pages = append(c.pages, page{
		UncompressedSize: len(c.curPage.buf),
		CompressedSize:   len(buf),
		CRC32:            crc32,
		RowCount:         c.curPage.count,
		Compression:      streamsmd.COMPRESSION_GZIP,
		Encoding:         streamsmd.ENCODING_PLAIN,
		Data:             buf,
	})

	// Reset the current page for new data.
	c.curPage.firstRow += c.curPage.count
	c.curPage.count = 0
	c.curPage.buf = c.curPage.buf[:0]
}

func (c *textColumn) Iter() iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		// Iterate over cut pages.
		for _, page := range c.pages {
			if !c.iterPage(page, yield) {
				return
			}
		}

		// Iterate over the in-memory page.
		c.curPage.Iter()(yield)
	}
}

func (c *textColumn) iterPage(p page, yield func(string, error) bool) bool {
	r, err := p.Reader()
	if err != nil {
		yield("", err)
		return false
	}
	defer r.Close()

	// TODO(rfratto): pool?
	br := bufio.NewReaderSize(r, 4096)

	for s, err := range textPageIter(br, int(p.RowCount)) {
		if err != nil {
			yield("", err)
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
func (c *textColumn) Backfill(row int) {
	if c.curPage == nil {
		// We use compression on the individual pages, so we'll want to increase
		// their size to account for expected compression ratio.
		targetSize := targetCompressedPageSize(c.maxPageSizeBytes, streamsmd.COMPRESSION_GZIP)
		c.curPage = &memTextPage{
			maxPageSizeBytes: targetSize,
			firstRow:         0,
			count:            0,
			buf:              make([]byte, 0, targetSize),
		}
	}

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

	panic("textColumn.Backfill: failed to backfill to fresh page")
}

func (c *textColumn) Count() int {
	count := 0
	for _, p := range c.pages {
		count += p.RowCount
	}
	if c.curPage != nil {
		count += c.curPage.count
	}
	return count
}

// memTextPage is an individual page containing text data. Calls to memTextPage are
// not goroutine safe; the caller must synchronize access.
//
// memTextPage is encoded with plain encoding. Non-empty strings are encoded first
// with the uvarint length of the string, followed by the bytes of the string.
// A sequence of empty strings are encoded with a uvarint length of 0, followed
// by the count of empty strings.
//
// This encoding style allows for efficient packing of NULLS, where up to 2^64
// - 1 NULLS can be packed into 11 bytes (one for the 0 length, another for the
// 64-bit uvarint count).
type memTextPage struct {
	// maxPageSizeBytes is the maximum size of encoded data this page may contain
	// pre-compression.
	maxPageSizeBytes int

	// Index of the very first row in this page, across all pages. Rows are
	// zero-indexed.
	firstRow int

	count int // Number of rows currently in the page.
	buf   []byte
}

// Iter returns an iterator over the text in the page. Iteration stops upon
// encountering an error.
func (p *memTextPage) Iter() iter.Seq2[string, error] {
	br := byteReader{buf: p.buf}
	return textPageIter(&br, p.count)
}

// Append appends text to the page. The row argument specifies the column-wide
// row number (zero-indexed) of the text being appended. Missing rows will be
// backfilled with empty entries.
//
// Append returns true if text was appended; false if the page was full.
//
// Append panics if called with an out-of-order row number.
func (p *memTextPage) Append(row int, text string) bool {
	pageRow := p.firstRow + p.count

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
		p.count += row - pageRow
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
	p.count++
	return true
}

// Backfill appends empty entries to the page up to and including the provided
// row number.
func (p *memTextPage) Backfill(row int) bool {
	pageRow := p.firstRow + p.count
	if row <= pageRow {
		return true
	}

	// Backfill row - pageRow + 1 rows.
	//
	// As a special case, NULLs are encoded as [0 null_count] to pack more
	// nulls into a single page.
	countBuf, countBufRelease := getUvarint(uint64(row - pageRow + 1))
	defer func() { countBufRelease(&countBuf) }()

	writeSize := 1 /* for the 0 len*/ + len(countBuf)
	if len(p.buf) > 0 && len(p.buf)+writeSize > p.maxPageSizeBytes {
		return false
	}

	p.buf = append(p.buf, 0)           // 0 len
	p.buf = append(p.buf, countBuf...) // Count of nulls
	p.count += row - pageRow + 1
	return true
}

type textPageReader struct{ s scanner }

// textPageIter returns an iterator for the provided rows count over text page
// data read from s.
func textPageIter(s scanner, rows int) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		// We iterate over the number of expected rows in the page (rather than
		// freely fetching entries until EOF) to catch potential encoding errors.
		for range rows {
			size, err := binary.ReadUvarint(s)
			if errors.Is(err, io.EOF) {
				// TODO(rfratto): log; unexpected early EOF
				return
			} else if err != nil {
				yield("", err)
				return
			}

			switch size {
			case 0: // Sequence of NULLs
				nullCount, err := binary.ReadUvarint(s)
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
				textBytes, err := s.Peek(int(size))
				if errors.Is(err, io.EOF) {
					// TODO(rfratto): log; unexpected early EOF
					yield("", err)
					return
				}

				// We make a copy of the text here to ensure the garbage collector can
				// free p.buf once we're done reading the pages.
				if !yield(string(textBytes), nil) {
					return
				}

				s.Discard(len(textBytes))
			}
		}
	}
}
