// Package plain provides an API for encoding and decoding plaintext values.
package plain

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
)

// Read decodes a string from r.
func Read(r encoding.Reader) (string, error) {
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		return "", err
	}

	dst := make([]byte, int(sz))
	if _, err := r.Read(dst); err != nil {
		return "", err
	} else if len(dst) != int(sz) {
		return "", fmt.Errorf("short read; expected %d bytes, got %d", sz, len(dst))
	}

	return string(dst), nil
}

// Write encodes the string s to the w.
func Write(w encoding.Writer, s string) error {
	if err := encoding.WriteUvarint(w, uint64(len(s))); err != nil {
		return err
	}

	// This saves a few allocations by avoiding a copy of the string.
	// Implementations of io.Writer are not supposed to modifiy the slice passed
	// to Write, so this is generally safe.
	_, err := w.Write(unsafe.Slice(unsafe.StringData(s), len(s)))
	return err
}
