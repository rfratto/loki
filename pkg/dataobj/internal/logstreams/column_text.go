package logstreams

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"unsafe"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/logstreamsmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
)

const (
	// We use Zstd for compression text columns to balance between compression
	// ratio and speed. Zstd slightly outperforms gzip on average, while having
	// near-comparable read and write performance to snappy.
	//
	// https://github.com/facebook/zstd#benchmarks
	textColumnCompression = logstreamsmd.COMPRESSION_TYPE_ZSTD
)

// IterTextPage returns an iterator over a page of values in a text-based
// column. IterTextPage yields an error if the column does not contain text or
// the page could not be read.
func IterTextPage(col *logstreamsmd.ColumnInfo, page Page) result.Seq[string] {
	return result.Iter(func(yield func(string) bool) error {
		if !isTextColumn(col) {
			return fmt.Errorf("column type %s does not contain text data", col.Type)
		}

		rc, err := page.Reader()
		if err != nil {
			return fmt.Errorf("opening page reader: %w", err)
		}
		defer rc.Close()

		if page.Encoding != logstreamsmd.ENCODING_TYPE_PLAIN {
			return fmt.Errorf("unsupported encoding %s for text column", page.Encoding)
		}

		for res := range textPageIter(bufio.NewReader(rc), page.RowCount) {
			text, err := res.Value()
			if err != nil {
				return err
			} else if !yield(text) {
				return nil
			}
		}

		return nil
	})
}

func isTextColumn(col *logstreamsmd.ColumnInfo) bool {
	return col.Type == logstreamsmd.COLUMN_TYPE_LOG_LINE || col.Type == logstreamsmd.COLUMN_TYPE_METADATA
}

func NewMetadataColumn(name string, maxPageSizeBytes uint64) *Column[string] {
	// text pages are compressed, so we'll want to account for expected
	// compression ratio when sizing the pages.
	targetSize := targetCompressedPageSize(maxPageSizeBytes, textColumnCompression)

	return &Column[string]{
		name:        name,
		ty:          logstreamsmd.COLUMN_TYPE_METADATA,
		compression: textColumnCompression,

		pageIter: textPageIter,
		curPage: &headTextPage{
			maxPageSizeBytes: targetSize,

			buf: make([]byte, 0, targetSize),
		},
	}
}

// NewLogColumn creates a new column for storing log lines.
func NewLogColumn(maxPageSizeBytes uint64) *Column[string] {
	// text pages are compressed, so we'll want to account for expected
	// compression ratio when sizing the pages.
	targetSize := targetCompressedPageSize(maxPageSizeBytes, textColumnCompression)

	return &Column[string]{
		ty:          logstreamsmd.COLUMN_TYPE_LOG_LINE,
		compression: textColumnCompression,

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
		//
		// TODO(rfratto): This may be a bit inefficient if multiple calls to Append
		// append empty strings. We may see this happen more frequently after
		// sorting a stream by timestamp, as backfilled rows may be split up.
		//
		// We may wish to add support for NULLS (zero values) at the [Column] level
		// and have it flush a sequence of NULLS via a Backfill call once it
		// receives as non-NULL value or upon calling [Column.cutPage].
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
	if p.rows == 0 {
		return Page{}, errors.New("headTextPage.Flush: page is empty")
	}

	buf, crc32, err := compressData(p.buf, textColumnCompression)
	if err != nil {
		return Page{}, fmt.Errorf("compressing text page: %w", err)
	}

	newPage := Page{
		UncompressedSize: len(p.buf),
		CompressedSize:   len(buf),
		CRC32:            crc32,
		RowCount:         p.rows,
		Compression:      textColumnCompression,
		Encoding:         logstreamsmd.ENCODING_TYPE_PLAIN,
		Data:             buf,
	}

	// Reset the current page for new data.
	p.rows = 0
	p.buf = p.buf[:0]
	return newPage, nil
}

// textPageIter returns an iterator for the provided rows count over text page
// data read from s.
func textPageIter(s encoding.Reader, rows int) result.Seq[string] {
	return result.Iter(func(yield func(string) bool) error {
		// We iterate over the number of expected rows in the page (rather than
		// freely fetching entries until EOF) to catch potential encoding errors.
		for range rows {
			size, err := binary.ReadUvarint(s)
			if errors.Is(err, io.EOF) {
				// TODO(rfratto): log; unexpected early EOF
				return nil
			} else if err != nil {
				return err
			}

			switch size {
			case 0: // Sequence of NULLs
				nullCount, err := binary.ReadUvarint(s)
				if errors.Is(err, io.EOF) {
					// TODO(rfratto): log; unexpected early EOF
					return nil
				} else if err != nil {
					return err
				}

				// Yield nullCount NULLs.
				for range nullCount {
					if !yield("") {
						return nil
					}
				}

			default: // String
				var sb strings.Builder
				sb.Grow(int(size))
				if _, err := io.Copy(&sb, io.LimitReader(s, int64(size))); err != nil {
					return fmt.Errorf("read text: %w", err)
				} else if !yield(sb.String()) {
					return nil
				}
			}
		}

		return nil
	})
}
