package dataobj

import (
	"bufio"
	"io"
)

// scanner is an interface for reading bytes from a stream. It extends
// [io.Reader] and [io.ByteReader] with methods to get a set of bytes at once.
//
// scanner is implemented by [bufio.Reader] and [byteReader].
type scanner interface {
	io.Reader
	io.ByteReader

	// Discard skips the next n bytes, returning the number of bytes discarded.
	// If Discard skips fewer than n bytes, it also returns an error.
	Discard(n int) (discarded int, err error)

	// Peek returns the next n bytes without advancing the scanner. The bytes
	// stop being valid at the next read or discard call. If Peek returns fewer
	// than n bytes, it also returns an error explaining why the read is short.
	// The error is [bufio.ErrBufferFull] if scanner is buffered and n is larger
	// than the scanner's buffer size.
	Peek(n int) ([]byte, error)
}

var (
	_ scanner = (*bufio.Reader)(nil)
)

// byteReader allows for reading bytes from a slice. Unlike [bytes.NewBuffer],
// it does not take ownership over the byte slice.
type byteReader struct {
	buf []byte
	off int
}

var (
	_ scanner = (*byteReader)(nil)
)

func (br *byteReader) ReadByte() (byte, error) {
	if br.off >= len(br.buf) {
		return 0, io.EOF
	}

	b := br.buf[br.off]
	br.off++
	return b, nil
}

func (br *byteReader) Read(p []byte) (int, error) {
	n := copy(p, br.buf[br.off:])
	br.off += n
	if n == len(p) {
		return n, nil
	}

	if br.off >= len(br.buf) {
		return n, io.EOF
	}
	return n, nil
}

// Discard skips the next n bytes, returning the number of bytes discarded.
func (br *byteReader) Discard(n int) (int, error) {
	if br.off+n > len(br.buf) {
		n = len(br.buf) - br.off
	}
	br.off += n
	return n, nil
}

// Peek returns up to the next n bytes without advancing the scanner. The bytes
// stop being valid at the next read or discard call.
func (br *byteReader) Peek(n int) ([]byte, error) {
	var err error
	if br.off+n > len(br.buf) {
		n = len(br.buf) - br.off
		err = io.EOF
	}
	return br.buf[br.off : br.off+n], err
}
