package dataobj

import "io"

// byteReader allows for reading bytes from a slice. Unlike [bytes.NewBuffer],
// it does not take ownership over the byte slice.
type byteReader struct {
	buf []byte
	off int
}

var (
	_ io.Reader     = (*byteReader)(nil)
	_ io.ByteReader = (*byteReader)(nil)
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

// Next retrieves up to the next n bytes from the buffer, returning the slice
// and the number of bytes read.
func (br *byteReader) Next(n int) ([]byte, int) {
	if br.off >= len(br.buf) {
		return nil, 0
	}

	end := br.off + n
	if end > len(br.buf) {
		end = len(br.buf)
	}

	b := br.buf[br.off:end]
	br.off = end
	return b, len(b)
}
