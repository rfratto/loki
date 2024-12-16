package rle

import (
	"errors"
	"fmt"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
)

var errBufferFull = errors.New("buffer full")
var errWidthChanged = errors.New("width changed")

type bitpackBuffer struct {
	maxBufferSize int

	width int

	sets int    // Number of encoded sets. Each set has 8 elements.
	data []byte // Total amount of data.
}

func newBitpackBuffer() *bitpackBuffer {
	return &bitpackBuffer{
		maxBufferSize: 4096, // 4KiB
	}
}

// AppendSet appends a bitpacked set of 8 elements to the buffer. AppendSet
// fails if the buffer is too large or if the width changed.
func (b *bitpackBuffer) AppendSet(width int, data []byte) error {
	if width < 1 || width > 64 {
		return fmt.Errorf("invalid width: %d", width)
	}

	// Error conditions
	switch {
	case len(b.data)+len(data) > b.maxBufferSize && b.sets > 0:
		return errBufferFull
	case b.width != width && b.sets > 0:
		return errWidthChanged
	}

	b.sets++
	b.width = width
	b.data = append(b.data, data...)
	return nil
}

// Flush flushes buffered data to the writer and resets state for more writes.
func (b *bitpackBuffer) Flush(w encoding.Writer) error {
	if b.sets == 0 {
		return nil
	}

	// The header of the bitpacked sequence encodes:
	//
	// * The number of sets
	// * The width of elements in the sets (1-64)
	// * A flag indicating the header type
	//
	// To encode the width in 6 bits, we encode width-1. That reserves the bottom
	// 7 bits for metadata, and the remaining 57 bits for the number of sets.
	const maxSets = 1<<57 - 1

	// Validate constraints for safety.
	switch {
	case b.width < 1 || b.width > 64:
		return fmt.Errorf("invalid width: %d", b.width)
	case b.sets > maxSets:
		// This shouldn't ever happen; 2^57-1 sets, in the best case (width of 1),
		// would require our buffer to be 144PB.
		return fmt.Errorf("too many sets: %d", b.sets)
	}

	// Width can be between 1 and 64. To pack it into 6 bits, we subtract 1 from
	// the value.
	header := (uint64(b.sets) << 7) | (uint64(b.width-1) << 1) | 1
	if err := encoding.WriteUvarint(w, header); err != nil {
		return err
	}

	if n, err := w.Write(b.data); err != nil {
		return err
	} else if n != len(b.data) {
		return fmt.Errorf("short write: %d != %d", n, len(b.data))
	}

	b.width = 0
	b.sets = 0
	b.data = b.data[:0]
	return nil
}

// Reset resets the buffer to its initial state without flushing.
func (b *bitpackBuffer) Reset() {
	b.width = 0
	b.sets = 0
	b.data = b.data[:0]
}
