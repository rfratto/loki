// Package page defines interfaces shared by other packages that encode values
// into dataset pages.
package page

import (
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

// TODO(rfratto): remove DataType, MetadataValueType, Encoder, and Decoder in
// favor of new ValueEncoder and ValueDecoder.
//
// Consider moving dataset.Page and related utilities to here, with column
// being left in dataset.

// DataType is a constraint that permits only data types that can be used in a
// page.
type DataType interface {
	// Normally type constraints would use the term ~T to allow matching any type
	// whose underlying type is T (~int64, etc). However, we want to explicitly
	// only include types for which encoders are defined.

	int64 | uint64 | string
}

// MetadataValueType returns the metadata value type for the given data type.
func MetadataValueType[T DataType]() datasetmd.ValueType {
	var zero T
	switch any(zero).(type) {
	case int64:
		return datasetmd.VALUE_TYPE_INT64
	case uint64:
		return datasetmd.VALUE_TYPE_UINT64
	case string:
		return datasetmd.VALUE_TYPE_STRING
	}
	panic("MetadataValueType: unrecognized type")
}

// An Encoder encodes values of type T, writing them to an underlying
// [encoding.Writer].
type Encoder[T DataType] interface {
	// Type returns the encoding type supported by the Encoder.
	Type() datasetmd.EncodingType

	// Encode encodes a value. Encode returns an error if encoded data cannot be
	// written to the underlying Writer.
	Encode(v T) error

	// Flush encodes any buffered data and immediately writes it to the
	// underlying Writer. Flush returns an error if the underlying Writer returns
	// an error.
	Flush() error

	// Reset discards any state without flushing and switches the Encoder to
	// write to w. This permits reusing a Writer rather than allocating a new
	// one.
	Reset(w encoding.Writer)
}

// A Decoder decodes values of type T from an underlying [encoding.Reader].
type Decoder[T DataType] interface {
	// Type returns the encoding type supported by the Encoder.
	Type() datasetmd.EncodingType

	// Decode decodes a value. Decode returns an error if the underlying Reader
	// returns an error or if the data cannot be decoded.
	Decode() (T, error)

	// Reset discards any state and switches the Decoder to read from r. This
	// permits reusing a Reader rather than allocating a new one.
	Reset(r encoding.Reader)
}

// A ValueEncoder encodes sequences of [Value], writing them to an underlying
// [encoding.Writer].
type ValueEncoder interface {
	// ValueType returns the value type supported by the ValueEncoder.
	ValueType() datasetmd.ValueType

	// EncodingType returns the encoding type supported by the ValueEncoder.
	EncodingType() datasetmd.EncodingType

	// Encode encodes a [Value]. Encode returns an error if writing to the
	// underlying encoding.Writer fails, or if value is an unsupported type.
	Encode(value Value) error

	// Flush encodes any buffered data and immediately writes it to the
	// underlying encoding.Writer. Flush returns an error if the underlying
	// encoding.Writer returns an error.
	Flush() error

	// Reset discards any state without flushing and switches the ValueEncoder to
	// write to w. This permits reusing a Writer rather than allocating a new
	// one.
	Reset(w encoding.Writer)
}

// A ValueDecoder decodes sequences of [Value] from an underlying
// [encoding.Reader].
type ValueDecoder interface {
	// ValueType returns the value type supported by the ValueDecoder.
	ValueType() datasetmd.ValueType

	// EncodingType returns the encoding type supported by the ValueDecoder.
	EncodingType() datasetmd.EncodingType

	// Decode decodes a [Value]. Decode returns an error if reading from the
	// underlying Reader fails or if the data cannot be decoded.
	Decode() (Value, error)

	// Reset discards any state and switches the ValueDecoder to read from r. This
	// permits reusing a Reader rather than allocating a new one.
	Reset(r encoding.Reader)
}
