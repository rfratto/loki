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

// TODO(rfratto): This is convoluted.
//
// My intent, originally, was to abstract away knowledge of encoding
// implementations away from callers. In particular, this simplifies the API of
// a page reader, which can now function without asking the caller to provide a
// constructor to create decoders.
//
// However, it might not be worth it: because we use generics, the caller
// already needs to know some fixed information about the page.
//
// Once we have fleshed out the code on the read path a bit more, it'll be more
// obvious how valuable this is and whether there's a simplified architecture.

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
//
// TODO(rfratto): is this the simpliest way of doing this? It reads horribly.
var (
	encoders = map[encodingKey]func(w encoding.Writer) any{
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_BITMAP}:      nil,

		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_DELTA}:       func(w encoding.Writer) any { return delta.NewEncoder(w) },
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_BITMAP}:      nil,

		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_BITMAP}:      func(w encoding.Writer) any { return bitmap.NewEncoder(w) },

		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_PLAIN}:       func(w encoding.Writer) any { return plain.NewStringEncoder(w) },
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_BITMAP}:      nil,
	}

	decoders = map[encodingKey]func(r encoding.Reader) any{
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_UNSPECIFIED, datasetmd.ENCODING_TYPE_BITMAP}:      nil,

		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_DELTA}:       func(r encoding.Reader) any { return delta.NewDecoder(r) },
		{datasetmd.VALUE_TYPE_INT64, datasetmd.ENCODING_TYPE_BITMAP}:      nil,

		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_PLAIN}:       nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_BITMAP}:      func(r encoding.Reader) any { return bitmap.NewDecoder(r) },

		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_UNSPECIFIED}: nil,
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_PLAIN}:       func(r encoding.Reader) any { return plain.NewStringDecoder(r) },
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_DELTA}:       nil,
		{datasetmd.VALUE_TYPE_STRING, datasetmd.ENCODING_TYPE_BITMAP}:      nil,
	}
)

// newEncoder creates an encoder for the given encoding type. If no encoder
// exists for encodingType and T, newEncoder returns nil.
func newEncoder[T page.DataType](w encoding.Writer, encodingType datasetmd.EncodingType) page.Encoder[T] {
	valueType := page.MetadataValueType[T]()

	f, ok := encoders[encodingKey{valueType, encodingType}]
	if !ok {
		panic(fmt.Sprintf("unrecognized encoding pair %s/%s", valueType, encodingType))
	}

	enc, ok := f(w).(page.Encoder[T])
	if !ok {
		panic(fmt.Sprintf("encoder type mismatch for %s/%s", valueType, encodingType))
	}
	return enc
}

// newDecoder creates a decoder for the given encoding type. If no decoder
// exists for encodingType and T, newDecoder returns nil.
func newDecoder[T page.DataType](r encoding.Reader, encodingType datasetmd.EncodingType) page.Decoder[T] {
	valueType := page.MetadataValueType[T]()

	f, ok := decoders[encodingKey{valueType, encodingType}]
	if !ok {
		panic(fmt.Sprintf("unrecognized decoding pair %s/%s", valueType, encodingType))
	}

	dec, ok := f(r).(page.Decoder[T])
	if !ok {
		panic(fmt.Sprintf("decoder type mismatch for %s/%s", valueType, encodingType))
	}
	return dec
}
