package plain

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

func init() {
	// Register the encoding to the page package so instances of it can be
	// dynamically created.
	page.RegisterValueEncoding(
		datasetmd.VALUE_TYPE_STRING,
		datasetmd.ENCODING_TYPE_PLAIN,
		func(w encoding.Writer) page.ValueEncoder { return NewStringEncoder(w) },
		func(r encoding.Reader) page.ValueDecoder { return NewStringDecoder(r) },
	)
}

// StringEncoder encodes strings to an [encoding.Writer].
type StringEncoder struct {
	w encoding.Writer
}

var _ page.ValueEncoder = (*StringEncoder)(nil)

// NewStringEncoder creates a StringEncoder that writes encoded strings to w.
func NewStringEncoder(w encoding.Writer) *StringEncoder {
	return &StringEncoder{w: w}
}

// ValueType returns [datasetmd.VALUE_TYPE_STRING].
func (enc *StringEncoder) ValueType() datasetmd.ValueType {
	return datasetmd.VALUE_TYPE_STRING
}

// EncodingType returns [datasetmd.ENCODING_TYPE_PLAIN].
func (enc *StringEncoder) EncodingType() datasetmd.EncodingType {
	return datasetmd.ENCODING_TYPE_PLAIN
}

// Encode encodes a string.
func (enc *StringEncoder) Encode(v page.Value) error {
	if v.Type() != datasetmd.VALUE_TYPE_STRING {
		return fmt.Errorf("plain: invalid value type %v", v.Type())
	}
	sv := v.String()

	if err := encoding.WriteUvarint(enc.w, uint64(len(sv))); err != nil {
		return err
	}

	// This saves a few allocations by avoiding a copy of the string.
	// Implementations of io.Writer are not supposed to modifiy the slice passed
	// to Write, so this is generally safe.
	_, err := enc.w.Write(unsafe.Slice(unsafe.StringData(sv), len(sv)))
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

var _ page.ValueDecoder = (*StringDecoder)(nil)

// NewStringDecoder creates a StringDecoder that reads encoded strings from r.
func NewStringDecoder(r encoding.Reader) *StringDecoder {
	return &StringDecoder{r: r}
}

// ValueType returns [datasetmd.VALUE_TYPE_STRING].
func (dec *StringDecoder) ValueType() datasetmd.ValueType {
	return datasetmd.VALUE_TYPE_STRING
}

// EncodingType returns [datasetmd.ENCODING_TYPE_PLAIN].
func (dec *StringDecoder) EncodingType() datasetmd.EncodingType {
	return datasetmd.ENCODING_TYPE_PLAIN
}

// Decode decodes a string.
func (dec *StringDecoder) Decode() (page.Value, error) {
	sz, err := binary.ReadUvarint(dec.r)
	if err != nil {
		return page.StringValue(""), err
	}

	dst := make([]byte, int(sz))
	if _, err := dec.r.Read(dst); err != nil {
		return page.StringValue(""), err
	} else if len(dst) != int(sz) {
		return page.StringValue(""), fmt.Errorf("short read; expected %d bytes, got %d", sz, len(dst))
	}

	return page.StringValue(string(dst)), nil
}

// Reset implements [page.Decoder]. It resets the decoder to read from r.
func (dec *StringDecoder) Reset(r encoding.Reader) {
	dec.r = r
}
