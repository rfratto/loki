// Package page defines interfaces shared by other packages that encode values
// into dataset pages.
package page

import (
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

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
