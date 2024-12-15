// Package delta provides an API for encoding and decoding delta-encoded
// numbers.
package delta

import (
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
)

// The Encoder encodes delta-encoded int64s. Values are encoded as varint, with
// each subsequent value being the delta from the previous value.
type Encoder struct {
	w    encoding.Writer
	prev int64
}

var _ page.Encoder[int64] = (*Encoder)(nil)

// NewEncoder creates an Encoder that writes encoded numbers to w.
func NewEncoder(w encoding.Writer) *Encoder {
	var enc Encoder
	enc.Reset(w)
	return &enc
}

// Encode encodes a new value.
func (enc *Encoder) Encode(v int64) error {
	delta := v - enc.prev
	enc.prev = v
	return encoding.WriteVarint(enc.w, delta)
}

// Flush implements [page.Encoder]. It is a no-op for Encoder.
func (enc *Encoder) Flush() error {
	return nil
}

// Reset resets the encoder to its initial state.
func (enc *Encoder) Reset(w encoding.Writer) {
	enc.prev = 0
	enc.w = w
}

// The Decoder decodes delta-encoded numbers. Values are decoded as varint, with
// each subsequent value being the delta from the previous value.
type Decoder struct {
	r    encoding.Reader
	prev int64
}

var _ page.Decoder[int64] = (*Decoder)(nil)

// NewDecoder creates a Decoder that reads encoded numbers from r.
func NewDecoder(r encoding.Reader) *Decoder {
	var dec Decoder
	dec.Reset(r)
	return &dec
}

// Decode decodes the next value.
func (dec *Decoder) Decode() (int64, error) {
	delta, err := encoding.ReadVarint(dec.r)
	if err != nil {
		return 0, err
	}

	dec.prev += delta
	return dec.prev, nil
}

// Reset resets the decoder to its initial state.
func (dec *Decoder) Reset(r encoding.Reader) {
	dec.prev = 0
	dec.r = r
}
