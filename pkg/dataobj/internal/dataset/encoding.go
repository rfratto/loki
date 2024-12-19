package dataset

import (
	"fmt"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/bitmap"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/delta"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/plain"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

// TODO(rfratto): This is convoluted, and weird to track in the dataset
// package. We should move to some kind of encoder and decoder registration for
// packages in encoding/page.

type encodingKey struct {
	value    datasetmd.ValueType
	encoding datasetmd.EncodingType
}

// Mappings from an encoding key (value type + encoding type) to a function
// that creates the appropriate encoder/decoder. Mappings which are unsupported
// are nil.
//
// All possible combinations must be listed here to prevent hard-to-catch bugs.
// Automated testing ensures that all combinations are accounted for.
var (
	encoders = map[encodingKey]func(w encoding.Writer) page.ValueEncoder{
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_BITMAP}:      nil,

		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_DELTA}:       func(w encoding.Writer) page.ValueEncoder { return delta.NewEncoder(w) },
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_BITMAP}:      nil,

		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_BITMAP}:      func(w encoding.Writer) page.ValueEncoder { return bitmap.NewEncoder(w) },

		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_PLAIN}:       func(w encoding.Writer) page.ValueEncoder { return plain.NewStringEncoder(w) },
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_BITMAP}:      nil,
	}

	decoders = map[encodingKey]func(r encoding.Reader) page.ValueDecoder{
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_BITMAP}:      nil,

		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_DELTA}:       func(r encoding.Reader) page.ValueDecoder { return delta.NewDecoder(r) },
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_BITMAP}:      nil,

		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_BITMAP}:      func(r encoding.Reader) page.ValueDecoder { return bitmap.NewDecoder(r) },

		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_PLAIN}:       func(r encoding.Reader) page.ValueDecoder { return plain.NewStringDecoder(r) },
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_BITMAP}:      nil,
	}
)

// newEncoder creates an encoder for the given encoding type. If no encoder
// exists for encodingType and T, newEncoder returns nil.
func newEncoder(w encoding.Writer, valueType datasetmd.ValueType, encodingType datasetmd.EncodingType) page.ValueEncoder {
	f, ok := encoders[encodingKey{valueType, encodingType}]
	if !ok {
		panic(fmt.Sprintf("unrecognized encoding pair %s/%s", valueType, encodingType))
	} else if f == nil {
		return nil
	}
	return f(w)
}

// newDecoder creates a decoder for the given encoding type. If no decoder
// exists for encodingType and T, newDecoder returns nil.
func newDecoder(r encoding.Reader, valueType datasetmd.ValueType, encodingType datasetmd.EncodingType) page.ValueDecoder {
	f, ok := decoders[encodingKey{valueType, encodingType}]
	if !ok {
		panic(fmt.Sprintf("unrecognized decoding pair %s/%s", valueType, encodingType))
	} else if f == nil {
		return nil
	}
	return f(r)
}
