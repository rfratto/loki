// Package page defines interfaces shared by other packages that encode values
// into dataset pages.
package page

import (
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

// TODO(rfratto): Consider moving dataset.Page and related utilities to here,
// with column being left in dataset.

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
