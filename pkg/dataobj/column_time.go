package dataobj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"sync"
	"time"
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
	pages []timePage
}

// Append appends a timestamp to the column.
func (c *timeColumn) Append(ts time.Time) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if len(c.pages) == 0 {
		c.pages = append(c.pages, timePage{maxPageSizeBytes: c.maxPageSizeBytes})
	}

	for {
		if !c.pages[len(c.pages)-1].Append(ts) {
			c.pages = append(c.pages, timePage{maxPageSizeBytes: c.maxPageSizeBytes})
			continue
		}

		return
	}
}

// Iter returns an iterator over the timestamps in the column. A read lock is
// held during iteration.
func (c *timeColumn) Iter() iter.Seq2[time.Time, error] {
	return func(yield func(time.Time, error) bool) {
		c.mut.RLock()
		defer c.mut.RUnlock()

		for _, p := range c.pages {
			for ts, err := range p.Iter() {
				if err != nil {
					yield(ts, err)
					return
				} else if !yield(ts, nil) {
					return
				}
			}
		}
	}
}

// UncompressedSize returns the uncompressed size of the column.
func (c *timeColumn) UncompressedSize() int {
	c.mut.RLock()
	defer c.mut.RUnlock()

	var totalSz int
	for _, p := range c.pages {
		totalSz += p.UncompressedSize()
	}
	return totalSz
}

// Count returns the number of timestamps in the column.
func (c *timeColumn) Count() int {
	c.mut.RLock()
	defer c.mut.RUnlock()

	var total int
	for _, p := range c.pages {
		total += p.Count()
	}
	return total
}

// timePage is an individual timestamp page. Calls to timePage are
// not goroutine safe; the caller must synchronize access.
type timePage struct {
	lastTS int64
	count  int
	buf    []byte

	maxPageSizeBytes int
}

// Iter returns an iterator over the timestamps in the page. Iteration stops
// upon encountering an error.
func (p *timePage) Iter() iter.Seq2[time.Time, error] {
	return func(yield func(time.Time, error) bool) {
		reader := bytes.NewReader(p.buf)
		first, err := binary.ReadVarint(reader)
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

		// TODO(rfratto): change to looping over expected rows; then error if we
		// EOF early or error if we haven't EOF'd when we should.
		for {
			delta, err := binary.ReadVarint(reader)
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
func (p *timePage) Append(ts time.Time) bool {
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
func (p *timePage) UncompressedSize() int {
	return len(p.buf)
}

// Count returns the number of timestamps in the page.
func (p *timePage) Count() int {
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
