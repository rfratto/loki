// Package obj provides utilities for encoding and decoding data objects.
package obj

// TODO(rfratto): document object structure in package comment.

var (
	magic = []byte("THOR")
)

const (
	fileFormatVersion    = 0x1
	streamsFormatVersion = 0x1
)
