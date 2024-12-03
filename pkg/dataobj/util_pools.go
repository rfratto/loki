package dataobj

import (
	"encoding/binary"
	"sync"
)

var varintPool = sync.Pool{
	New: func() any {
		buf := make([]byte, binary.MaxVarintLen64)
		return &buf
	},
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
