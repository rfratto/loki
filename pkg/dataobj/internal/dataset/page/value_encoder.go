package page

import (
	"fmt"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

// A ValueEncoder encodes sequences of [Value], writing them to an underlying
// [encoding.Writer]. Packages implementing encoding types must cal
// [RegisterValueEncoding] to register a ValueEncoder and ValueDecoder.
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
// [encoding.Reader]. Packages implementing encoding types must call
// [RegisterValueEncoding] to register a ValueEncoder and ValueDecoder.
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

type (
	registryKey struct {
		Value    datasetmd.ValueType
		Encoding datasetmd.EncodingType
	}

	registryValue struct {
		NewEncoder func(encoding.Writer) ValueEncoder
		NewDecoder func(encoding.Reader) ValueDecoder
	}
)

var registry = map[registryKey]registryValue{}

// RegisterValueEncoding registers a ValueEncoder and ValueDecoder for a
// specified valueType and encodingType tuple. If another encoding has been
// registered for the same tuple, RegisterValueEncoding panics.
//
// RegisterValueEncoding should be called in an init method of packages
// implementing encodings. To ensure the init functions are invoked, add
// encoding packages to the imports in
// [github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/all].
func RegisterValueEncoding(
	valueType datasetmd.ValueType,
	encodingType datasetmd.EncodingType,
	newEncoder func(encoding.Writer) ValueEncoder,
	newDecoder func(encoding.Reader) ValueDecoder,
) {
	key := registryKey{
		Value:    valueType,
		Encoding: encodingType,
	}
	if _, exist := registry[key]; exist {
		panic(fmt.Sprintf("page: RegisterEncoding already called for %s/%s", valueType, encodingType))
	}

	registry[key] = registryValue{
		NewEncoder: newEncoder,
		NewDecoder: newDecoder,
	}
}

// NewValueEncoder creates a new ValueEncoder for the specified valueType and
// encodingType. If no encoding is registered for the specified combination of
// valueType and encodingType, NewValueEncoder returns nil and false.
func NewValueEncoder(valueType datasetmd.ValueType, encodingType datasetmd.EncodingType, w encoding.Writer) (ValueEncoder, bool) {
	key := registryKey{
		Value:    valueType,
		Encoding: encodingType,
	}
	value, exist := registry[key]
	if !exist {
		return nil, false
	}
	return value.NewEncoder(w), true
}

// NewValueDecoder creates a new ValueDecoder for the specified valueType and
// encodingType. If no encoding is registered for the specified combination of
// valueType and encodingType, NewValueDecoder returns nil and false.
func NewValueDecoder(valueType datasetmd.ValueType, encodingType datasetmd.EncodingType, r encoding.Reader) (ValueDecoder, bool) {
	key := registryKey{
		Value:    valueType,
		Encoding: encodingType,
	}
	value, exist := registry[key]
	if !exist {
		return nil, false
	}
	return value.NewDecoder(r), true
}
