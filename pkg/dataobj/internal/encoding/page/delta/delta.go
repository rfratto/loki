// Package delta provides an API for encoding and decoding delta-encoded
// numbers.
package delta

import (
	"fmt"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

func init() {
	// Register the encoding to the page package so instances of it can be
	// dynamically created.
	page.RegisterValueEncoding(
		datasetmd.VALUE_TYPE_INT64,
		datasetmd.ENCODING_TYPE_DELTA,
		func(w encoding.Writer) page.ValueEncoder { return NewEncoder(w) },
		func(r encoding.Reader) page.ValueDecoder { return NewDecoder(r) },
	)
}

// The Encoder encodes delta-encoded int64s. Values are encoded as varint, with
// each subsequent value being the delta from the previous value.
type Encoder struct {
	w    encoding.Writer
	prev int64
}

var _ page.ValueEncoder = (*Encoder)(nil)

// NewEncoder creates an Encoder that writes encoded numbers to w.
func NewEncoder(w encoding.Writer) *Encoder {
	var enc Encoder
	enc.Reset(w)
	return &enc
}

// ValueType returns [datasetmd.VALUE_TYPE_INT64].
func (enc *Encoder) ValueType() datasetmd.ValueType {
	return datasetmd.VALUE_TYPE_INT64
}

// EncodingType returns [datasetmd.ENCODING_TYPE_DELTA].
func (enc *Encoder) EncodingType() datasetmd.EncodingType {
	return datasetmd.ENCODING_TYPE_DELTA
}

// Encode encodes a new value.
func (enc *Encoder) Encode(v page.Value) error {
	if v.Type() != datasetmd.VALUE_TYPE_INT64 {
		return fmt.Errorf("delta: invalid value type %v", v.Type())
	}
	iv := v.Int64()

	delta := iv - enc.prev
	enc.prev = iv
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

var _ page.ValueDecoder = (*Decoder)(nil)

// NewDecoder creates a Decoder that reads encoded numbers from r.
func NewDecoder(r encoding.Reader) *Decoder {
	var dec Decoder
	dec.Reset(r)
	return &dec
}

// ValueType returns [datasetmd.VALUE_TYPE_INT64].
func (dec *Decoder) ValueType() datasetmd.ValueType {
	return datasetmd.VALUE_TYPE_INT64
}

// Type returns [datasetmd.ENCODING_TYPE_DELTA].
func (dec *Decoder) EncodingType() datasetmd.EncodingType {
	return datasetmd.ENCODING_TYPE_DELTA
}

// Decode decodes the next value.
func (dec *Decoder) Decode() (page.Value, error) {
	delta, err := encoding.ReadVarint(dec.r)
	if err != nil {
		return page.Int64Value(dec.prev), err
	}

	dec.prev += delta
	return page.Int64Value(dec.prev), nil
}

// Reset resets the decoder to its initial state.
func (dec *Decoder) Reset(r encoding.Reader) {
	dec.prev = 0
	dec.r = r
}
