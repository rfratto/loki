package scanner

import (
	"io"
)

// TODO(rfratto): Previously [Scanner] had more methods, but they were removed,
// and now this package is very light. Should we move the Scanner definition
// somewhere else?

// Scanner is an interface for efficiently reading bytes from a stream.
type Scanner interface {
	io.Reader
	io.ByteReader
}
