package streams

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"
	"unsafe"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/scanner"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

// IterTextPage returns an iterator over a page of values in a text-based
// column. IterTextPage yields an error if the column does not contain text or
// the page could not be read.
func IterTextPage(col *streamsmd.ColumnInfo, page Page) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if !isTextColumn(col) {
			yield("", fmt.Errorf("column type %s does not contain text data", col.Type))
			return
		}

		rc, err := page.Reader()
		if err != nil {
			yield("", fmt.Errorf("opening page reader: %w", err))
			return
		}
		defer rc.Close()

		if page.Encoding != streamsmd.ENCODING_PLAIN {
			yield("", fmt.Errorf("unsupported encoding %s for text column", page.Encoding))
			return
		}
		textPageIter(bufio.NewReader(rc), page.RowCount)(yield)
	}
}

func isTextColumn(col *streamsmd.ColumnInfo) bool {
	return col.Type == streamsmd.COLUMN_TYPE_LOG_LINE || col.Type == streamsmd.COLUMN_TYPE_METADATA
}

func NewMetadataColumn(name string, maxPageSizeBytes uint64) *Column[string] {
	// text pages are compressed with GZIP, so we'll want to account for expected
	// compression ratio when sizing the pages.
	targetSize := targetCompressedPageSize(maxPageSizeBytes, streamsmd.COMPRESSION_GZIP)

	return &Column[string]{
		name:        name,
		ty:          streamsmd.COLUMN_TYPE_METADATA,
		compression: streamsmd.COMPRESSION_GZIP,

		pageIter: textPageIter,
		curPage: &headTextPage{
			maxPageSizeBytes: targetSize,

			buf: make([]byte, 0, targetSize),
		},
	}
}

// NewLogColumn creates a new column for storing log lines.
func NewLogColumn(maxPageSizeBytes uint64) *Column[string] {
	// text pages are compressed with GZIP, so we'll want to account for expected
	// compression ratio when sizing the pages.
	targetSize := targetCompressedPageSize(maxPageSizeBytes, streamsmd.COMPRESSION_GZIP)

	return &Column[string]{
		ty:          streamsmd.COLUMN_TYPE_LOG_LINE,
		compression: streamsmd.COMPRESSION_GZIP,

		pageIter: textPageIter,
		curPage: &headTextPage{
			maxPageSizeBytes: targetSize,

			buf: make([]byte, 0, targetSize),
		},
	}
}

// headTextPage is a head page containing text data.
//
// headTextPage is encoded with plain encoding. Non-empty strings are encoded
// first with the uvarint length of the string, followed by the bytes of the
// string. A sequence of empty strings are encoded with a uvarint length of 0,
// followed by the count of empty strings.
//
// Packing multiple empty strings into a single record allows for encoding up
// to 2^64 - 1 empty strings into 11 bytes.
type headTextPage struct {
	// maxPageSizeBytes is the maximum size of encoded data this page may contain
	// pre-compression.
	maxPageSizeBytes uint64

	rows int // Number of rows currently in the page.
	buf  []byte
}

var _ headPage[string] = (*headTextPage)(nil)

// Append appends text to the page. The row argument specifies the page-scoped
// row number (zero-indexed) of the text being appended. Missing rows will be
// backfilled with empty entries.
//
// Append returns true if text was appended; false if the page was full.
//
// Append panics if called with an out-of-order row number.
func (p *headTextPage) Append(row int, text string) bool {
	if row < p.rows {
		panic(fmt.Sprintf("headTextPage.Append: out-of-order row %d (expected >= %d)", row, p.rows))
	} else if row > p.rows {
		// Backfill rows up to row. Backfill is inclusive so we subtract 1 to allow
		// us to write a new entry for row.
		if !p.Backfill(row - 1) {
			return false
		}
	} else if text == "" {
		// Treat an empty string as a backfill up to row.
		return p.Backfill(row)
	}

	// Append text, if it would fit.
	lenBuf, lenBufRelease := getUvarint(uint64(len(text)))
	defer func() { lenBufRelease(&lenBuf) }()

	writeSize := len(lenBuf) + len(text)
	if len(p.buf) > 0 && uint64(len(p.buf)+writeSize) > p.maxPageSizeBytes {
		return false
	}

	p.buf = append(p.buf, lenBuf...)
	p.buf = append(p.buf, unsafe.Slice(unsafe.StringData(text), len(text))...)
	p.rows = row + 1
	return true
}

// Backfill appends empty entries to the page up to and including the provided
// row number.
func (p *headTextPage) Backfill(row int) bool {
	if row < p.rows {
		return true
	}

	// Row numbers are zero-indexed so we need to add one to get the count of
	// rows.
	backfillCount := row - p.rows + 1

	// Nulls are encoded as [0 null_count] to pack more nulls into a single page.
	countBuf, countBufRelease := getUvarint(uint64(backfillCount))
	defer func() { countBufRelease(&countBuf) }()

	writeSize := 1 /* for the 0 len*/ + len(countBuf)
	if len(p.buf) > 0 && uint64(len(p.buf)+writeSize) > p.maxPageSizeBytes {
		return false
	}

	p.buf = append(p.buf, 0)           // 0 len
	p.buf = append(p.buf, countBuf...) // Count of nulls
	p.rows = row + 1
	return true
}

// Data returns the current data in the page along with the number of rows. The
// returned buf must not be modified.
func (p *headTextPage) Data() ([]byte, int) {
	return p.buf, p.rows
}

// Flush returns a page from p, then resets p for new data.
func (p *headTextPage) Flush() (Page, error) {
	buf, crc32, err := compressData(p.buf, streamsmd.COMPRESSION_GZIP)
	if err != nil {
		return Page{}, fmt.Errorf("compressing text page: %w", err)
	}

	newPage := Page{
		UncompressedSize: len(p.buf),
		CompressedSize:   len(buf),
		CRC32:            crc32,
		RowCount:         p.rows,
		Compression:      streamsmd.COMPRESSION_GZIP,
		Encoding:         streamsmd.ENCODING_PLAIN,
		Data:             buf,
	}

	// Reset the current page for new data.
	p.rows = 0
	p.buf = p.buf[:0]
	return newPage, nil
}

// textPageIter returns an iterator for the provided rows count over text page
// data read from s.
func textPageIter(s scanner.Scanner, rows int) iter.Seq2[string, error] {
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
				var sb strings.Builder
				sb.Grow(int(size))
				if _, err := io.Copy(&sb, io.LimitReader(s, int64(size))); err != nil {
					yield("", fmt.Errorf("read text: %w", err))
					return
				}
				if !yield(sb.String(), nil) {
					return
				}
			}
		}
	}
}
