package dataobj

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"
	"time"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

var varintPool = sync.Pool{
	New: func() any {
		buf := make([]byte, binary.MaxVarintLen64)
		return &buf
	},
}

var errPageFull = errors.New("page full")

// timeColumn buffers timestamp pages in memory.
type timeColumn struct {
	maxPageSizeBytes int

	mut   sync.RWMutex
	pages []page

	curPage *memTimePage
}

// Append appends a timestamp to the column.
func (c *timeColumn) Append(ts time.Time) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.curPage == nil {
		c.curPage = &memTimePage{maxPageSizeBytes: c.maxPageSizeBytes}
	}

	// We give two attempts to append the data to the page; if the current page
	// is full, we cut it and append to the reset page.
	//
	// Appending to the reset page should never fail, as it'll allow its first
	// record to be oversized. If it fails, there's a bug.
	for range 2 {
		if c.curPage.Append(ts) {
			return
		}

		c.cutPage()
	}

	panic("textColumn.Append: failed to append text to fresh page")
}

func (c *timeColumn) cutPage() {
	if c.curPage == nil {
		return
	}

	buf, crc32, err := compressData(c.curPage.buf, streamsmd.COMPRESSION_NONE)
	if err != nil {
		panic(fmt.Sprintf("failed to compress text page: %v", err))
	}

	c.pages = append(c.pages, page{
		UncompressedSize: len(c.curPage.buf),
		CompressedSize:   len(buf),
		CRC32:            crc32,
		RowCount:         c.curPage.count,
		Compression:      streamsmd.COMPRESSION_NONE,
		Encoding:         streamsmd.ENCODING_PLAIN,
		Data:             buf,
	})

	// Reset the current page for new data.
	c.curPage.count = 0
	c.curPage.buf = c.curPage.buf[:0]
}

// Iter returns an iterator over the timestamps in the column. A read lock is
// held during iteration.
func (c *timeColumn) Iter() iter.Seq2[time.Time, error] {
	return func(yield func(time.Time, error) bool) {
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

func (c *timeColumn) iterPage(p page, yield func(time.Time, error) bool) bool {
	r, err := p.Reader()
	if err != nil {
		yield(time.Time{}, err)
		return false
	}
	defer r.Close()

	// TODO(rfratto): pool?
	br := bufio.NewReaderSize(r, 4096)

	for s, err := range timePageIter(br, int(p.RowCount)) {
		if err != nil {
			yield(s, err)
			return false
		} else if !yield(s, nil) {
			return false
		}
	}

	return true
}

// UncompressedSize returns the uncompressed size of the column.
func (c *timeColumn) UncompressedSize() int {
	c.mut.RLock()
	defer c.mut.RUnlock()

	var totalSz int
	for _, p := range c.pages {
		totalSz += p.UncompressedSize
	}
	if c.curPage != nil {
		totalSz += c.curPage.UncompressedSize()
	}
	return totalSz
}

// Count returns the number of timestamps in the column.
func (c *timeColumn) Count() int {
	c.mut.RLock()
	defer c.mut.RUnlock()

	var total int
	for _, p := range c.pages {
		total += p.RowCount
	}
	if c.curPage != nil {
		total += c.curPage.Count()
	}
	return total
}

// memTimePage is an individual timestamp page. Calls to memTimePage are
// not goroutine safe; the caller must synchronize access.
type memTimePage struct {
	lastTS int64
	count  int
	buf    []byte

	maxPageSizeBytes int
}

// Iter returns an iterator over the timestamps in the page. Iteration stops
// upon encountering an error.
func (p *memTimePage) Iter() iter.Seq2[time.Time, error] {
	br := byteReader{buf: p.buf}
	return timePageIter(&br, p.count)
}

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

// Append appends a timestamp to the page. Returns true if data was appended;
// false if the page was full.
func (p *memTimePage) Append(ts time.Time) bool {
	var (
		rawValue    = ts.UnixNano()
		encodeValue = ts.UnixNano()
	)
	if p.count > 0 {
		// Delta-encode subsequent timestamps after the first entry.
		encodeValue = rawValue - p.lastTS
	}

	buf, bufRelease := getVarint(encodeValue)
	defer func() { bufRelease(&buf) }()

	if len(p.buf) > 0 && len(buf)+len(p.buf) > p.maxPageSizeBytes {
		return false
	}

	p.buf = append(p.buf, buf...)
	p.lastTS = rawValue
	p.count++
	return true
}

// UncompressedSize is the size of the page in bytes.
func (p *memTimePage) UncompressedSize() int {
	return len(p.buf)
}

// Count returns the number of timestamps in the page.
func (p *memTimePage) Count() int {
	return p.count
}

// getUvarint returns a buffer containing the uvarint encoding of val. Call
// release after using buf to return it to the pool.
func getUvarint(val uint64) (buf []byte, release func(*[]byte)) {
	bufPtr := varintPool.Get().(*[]byte)
	buf = (*bufPtr)[:0]
	buf = binary.AppendUvarint(buf, val)

	// Our release function accepts a pointer to the buf to return to the pool to
	// avoid having &buf escape to the heap; this reduces allocations by 1 per
	// op.
	return buf, func(b *[]byte) { varintPool.Put(b) }
}

// getVarint returns a buffer containing the varint encoding of val. Call
// release after using buf to return it to the pool.
func getVarint(val int64) (buf []byte, release func(*[]byte)) {
	bufPtr := varintPool.Get().(*[]byte)
	buf = (*bufPtr)[:0]
	buf = binary.AppendVarint(buf, val)

	// Our release function accepts a pointer to the buf to return to the pool to
	// avoid having &buf escape to the heap; this reduces allocations by 1 per
	// op.
	return buf, func(b *[]byte) { varintPool.Put(b) }
}
