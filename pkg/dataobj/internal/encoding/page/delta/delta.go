// Package delta provides an API for encoding and decoding delta-encoded
// numbers.
package delta

import (
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
)

// The Encoder encodes delta-encoded numbers. Values are encoded as varint,
// with each subsequent value being the delta from the previous value.
type Encoder struct {
	w    encoding.Writer
	prev int64
}

// NewEncoder creates an Encoder that writes encoded numbers to w.
func NewEncoder(w encoding.Writer) *Encoder {
	return &Encoder{w: w}
}

// Encode encodes a new value.
func (enc *Encoder) Encode(v int64) error {
	delta := v - enc.prev
	enc.prev = v
	return encoding.WriteVarint(enc.w, delta)
}

// Reset resets the encoder to its initial state.
func (enc *Encoder) Reset() {
	enc.prev = 0
}

// The Decoder decodes delta-encoded numbers. Values are decoded as varint, with
// each subsequent value being the delta from the previous value.
type Decoder struct {
	r    encoding.Reader
	prev int64
}

// NewDecoder creates a Decoder that reads encoded numbers from r.
func NewDecoder(r encoding.Reader) *Decoder {
	return &Decoder{r: r}
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
