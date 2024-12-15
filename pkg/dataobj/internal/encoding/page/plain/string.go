package plain

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
)

// StringEncoder encodes strings to an [encoding.Writer].
type StringEncoder struct {
	w encoding.Writer
}

var _ page.Encoder[string] = (*StringEncoder)(nil)

// NewStringEncoder creates a StringEncoder that writes encoded strings to w.
func NewStringEncoder(w encoding.Writer) *StringEncoder {
	return &StringEncoder{w: w}
}

// Encode encodes a string.
func (enc *StringEncoder) Encode(v string) error {
	if err := encoding.WriteUvarint(enc.w, uint64(len(v))); err != nil {
		return err
	}

	// This saves a few allocations by avoiding a copy of the string.
	// Implementations of io.Writer are not supposed to modifiy the slice passed
	// to Write, so this is generally safe.
	_, err := enc.w.Write(unsafe.Slice(unsafe.StringData(v), len(v)))
	return err
}

// Flush implements [page.Encoder]. It is a no-op for StringEncoder.
func (enc *StringEncoder) Flush() error {
	return nil
}

// Reset implements [page.Encoder]. It resets the encoder to write to w.
func (enc *StringEncoder) Reset(w encoding.Writer) {
	enc.w = w
}

// StringDecoder decodes strings from an [encoding.Reader].
type StringDecoder struct {
	r encoding.Reader
}

var _ page.Decoder[string] = (*StringDecoder)(nil)

// NewStringDecoder creates a StringDecoder that reads encoded strings from r.
func NewStringDecoder(r encoding.Reader) *StringDecoder {
	return &StringDecoder{r: r}
}

// Decode decodes a string.
func (dec *StringDecoder) Decode() (string, error) {
	sz, err := binary.ReadUvarint(dec.r)
	if err != nil {
		return "", err
	}

	dst := make([]byte, int(sz))
	if _, err := dec.r.Read(dst); err != nil {
		return "", err
	} else if len(dst) != int(sz) {
		return "", fmt.Errorf("short read; expected %d bytes, got %d", sz, len(dst))
	}

	return string(dst), nil
}

// Reset implements [page.Decoder]. It resets the decoder to read from r.
func (dec *StringDecoder) Reset(r encoding.Reader) {
	dec.r = r
}
