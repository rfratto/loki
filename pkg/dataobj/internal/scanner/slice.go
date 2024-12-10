package scanner

import "io"

// Slice allows for scanning bytes from a slice. Unlike [bytes.NewBuffer], it
// does not take ownership over the byte slice.
type Slice struct {
	buf []byte
	off int
}

// FromSlice creates a new Slice.
func FromSlice(buf []byte) *Slice {
	return &Slice{buf: buf}
}

var _ Scanner = (*Slice)(nil)

// ReadByte reads a single byte from s.
func (s *Slice) ReadByte() (byte, error) {
	if s.off >= len(s.buf) {
		return 0, io.EOF
	}

	b := s.buf[s.off]
	s.off++
	return b, nil
}

// Read reads up to len(p) bytes into p. It returns the number of bytes read
// and [io.EOF] if fewer bytes were read than requested.
func (s *Slice) Read(p []byte) (int, error) {
	n := copy(p, s.buf[s.off:])
	s.off += n
	if n == len(p) {
		return n, nil
	}

	if s.off >= len(s.buf) {
		return n, io.EOF
	}
	return n, nil
}

// Discard skips the next n bytes, returning the number of bytes discarded.
func (s *Slice) Discard(n int) (int, error) {
	if s.off+n > len(s.buf) {
		n = len(s.buf) - s.off
	}
	s.off += n
	return n, nil
}

// Peek returns up to the next n bytes without advancing s. The bytes stop
// being valid at the next read or discard call.
func (s *Slice) Peek(n int) ([]byte, error) {
	var err error
	if s.off+n > len(s.buf) {
		n = len(s.buf) - s.off
		err = io.EOF
	}
	return s.buf[s.off : s.off+n], err
}
