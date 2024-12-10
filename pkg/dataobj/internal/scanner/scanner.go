package scanner

import (
	"bufio"
	"io"
)

// Scanner is an interface for efficiently reading bytes from a stream. It
// extends [io.Reader] and [io.ByteReader] with methods to get a sequence of
// bytes without allocations.
//
// Scanner is implemented by [bufio.Reader] and [Slice].
type Scanner interface {
	io.Reader
	io.ByteReader

	// Discard skips the next n bytes, returning the number of bytes discarded.
	// If Discard skips fewer than n bytes, it also returns an error.
	Discard(n int) (discarded int, err error)

	// Peek returns the next n bytes without advancing the scanner. The bytes
	// stop being valid at the nextread or discard call. If Peek returns fewer
	// than n bytes, it also returns an error explaining why the read is short.
	// The error is [bufio.ErrBufferFull] if scanner is buffered and n is larger
	// than the scanner's buffer size.
	Peek(n int) ([]byte, error)
}

var _ Scanner = (*bufio.Reader)(nil)
