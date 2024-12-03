package streams

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

// NewTimeColumn creates a new column for storing timestamps.
func NewTimeColumn(maxPageSizeBytes uint64) *Column[time.Time] {
	return &Column[time.Time]{
		pageIter: timePageIter,
		curPage: &headTimePage{
			maxPageSizeBytes: maxPageSizeBytes,

			buf: make([]byte, 0, maxPageSizeBytes),
		},
	}
}

// headTimePage is a head page containing time data.
//
// Timestamps are encoded using nanosecond-precision delta encoding. The first
// timestamp is encoded as the full UnixNano value; subsequent timestamps are
// encoded as the delta between the previous timestamp and the current
// timestamp.
//
// Empty rows are encoded as a zero delta.
type headTimePage struct {
	// maxPageSizeBytes is the maximum size of encoded data this page may
	// contain.
	maxPageSizeBytes uint64

	rows int // Number of rows currently in the page.
	buf  []byte

	lastTS int64
}

var _ headPage[time.Time] = (*headTimePage)(nil)

// Append appends a timestamp to the page. Thw row argument specifies the
// page-scoped row number (zero-indexed) of the timestamp being appended.
// Missing rows will be backfilled with a delta of zero.
//
// Append returns true if data was appended; false if the page was full.
//
// Append panics if called with an out-of-order row number.
func (p *headTimePage) Append(row int, ts time.Time) bool {
	if row < p.rows {
		panic(fmt.Sprintf("headTimePage.Append: out-of-order row %d (expected >= %d)", row, p.rows))
	} else if row > p.rows {
		// Backfill rows up to row. Backfill is inclusive so we subtract 1 to allow
		// us to write a new entry for row.
		if !p.Backfill(row - 1) {
			return false
		}
	}

	var (
		rawValue    = ts.UnixNano()
		encodeValue = ts.UnixNano()
	)
	if p.rows > 0 {
		// Delta-encode subsequent timestamps after the first entry.
		encodeValue = rawValue - p.lastTS
	}

	buf, bufRelease := getVarint(encodeValue)
	defer func() { bufRelease(&buf) }()

	if len(p.buf) > 0 && uint64(len(buf)+len(p.buf)) > p.maxPageSizeBytes {
		return false
	}

	p.buf = append(p.buf, buf...)
	p.lastTS = rawValue
	p.rows = row + 1
	return true
}

// Backfill appends zero deltas to the page up to and including the provided
// row number.
func (p *headTimePage) Backfill(row int) bool {
	if row < p.rows {
		return true
	}

	lastTS := time.Unix(0, p.lastTS).UTC()

	for backfillRow := p.rows; backfillRow <= row; backfillRow++ {
		// Re-append the last TS to the page; the delta will be zero.
		if !p.Append(backfillRow, lastTS) {
			return false
		}
	}

	return true
}

// Data returns the current data in the page along with the number of rows. The
// returned buf must not be modified.
func (p *headTimePage) Data() ([]byte, int) {
	return p.buf, p.rows
}

// Flush returns a page from p, then resets p for new data.
func (p *headTimePage) Flush() (Page, error) {
	// No compression is used for timestamps; it's unlikely that compression can
	// make delta encoding more efficient.
	buf, crc32, err := compressData(p.buf, streamsmd.COMPRESSION_NONE)
	if err != nil {
		return Page{}, fmt.Errorf("compressing text page: %w", err)
	}

	newPage := Page{
		UncompressedSize: len(p.buf),
		CompressedSize:   len(buf),
		CRC32:            crc32,
		RowCount:         p.rows,
		Compression:      streamsmd.COMPRESSION_NONE,
		Encoding:         streamsmd.ENCODING_DELTA,
		Data:             buf,
	}

	// Reset the current page for new data.
	p.rows = 0
	p.buf = p.buf[:0]
	p.lastTS = 0
	return newPage, nil
}

// timePageIter returns an iterator that reads delta-encoded timestamps from a
// scanner.
func timePageIter(s scanner, rows int) iter.Seq2[time.Time, error] {
	return func(yield func(time.Time, error) bool) {
		if rows == 0 {
			return
		}

		first, err := binary.ReadVarint(s)
		if errors.Is(err, io.EOF) {
			return
		} else if err != nil {
			yield(time.Time{}, err)
			return
		}

		ts := time.Unix(0, first).UTC()
		if !yield(ts, nil) {
			return
		}

		for range rows - 1 {
			delta, err := binary.ReadVarint(s)
			if errors.Is(err, io.EOF) {
				return
			} else if err != nil {
				yield(time.Time{}, err)
				return
			}

			ts = ts.Add(time.Duration(delta))
			if !yield(ts, nil) {
				return
			}
		}
	}
}
