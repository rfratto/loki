package dataset

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type encodingFormat interface{ Type() datasetmd.EncodingType }

// Test_encodings ensures that all encoders and decoders are correctly registered.
//
// It uses protobuf reflection to look up all possible values for enums and
// then checks the following:
//
//  1. All combinations of value types and encoding types are registered, even
//     if they are nil (indicating unsupported).
//
//  2. The returned encoder and decoder types match the input encoding type.
func Test_encodings(t *testing.T) {
	valueTypes, err := getEnumValues(datasetmd.VALUE_TYPE_UNSPECIFIED)
	require.NoError(t, err)

	encodingTypes, err := getEnumValues(datasetmd.ENCODING_TYPE_UNSPECIFIED)
	require.NoError(t, err)

	for _, valueType := range valueTypes {
		for _, encodingType := range encodingTypes {
			key := encodingKey{
				value:    datasetmd.ValueType(valueType),
				encoding: datasetmd.EncodingType(encodingType),
			}
			newEncoder, ok := encoders[key]
			assert.True(t, ok, fmt.Sprintf("missing encoder entry for %s/%s", key.value, key.encoding))
			if newEncoder != nil {
				enc := newEncoder(encoding.Discard).(encodingFormat)
				assert.Equal(t, key.encoding, enc.Type(), "encoder type mismatch for %s/%s: got %s", key.value, key.encoding, enc.Type())
			}

			newDecoder, ok := decoders[key]
			assert.True(t, ok, fmt.Sprintf("missing decoder entry for %s/%s", key.value, key.encoding))
			if newDecoder != nil {
				dec := newDecoder(bytes.NewReader(nil)).(encodingFormat)
				assert.Equal(t, key.encoding, dec.Type())
				assert.Equal(t, key.encoding, dec.Type(), "decoder type mismatch for %s/%s: got %s", key.value, key.encoding, dec.Type())
			}
		}
	}
}

type enum interface{ EnumDescriptor() ([]byte, []int) }

func getEnumValues(e enum) ([]int, error) {
	// NOTE(rfratto): this is awful, but it's the best we can currently do with
	// gogo protobuf. The newer protobuf libraries from Google have ways to
	// reflect over a type without needing to manually decompress and unmarshal
	// the descriptor.

	descBytesCompressed, indices := e.EnumDescriptor()
	if len(indices) != 1 {
		return nil, fmt.Errorf("expected exactly one index, got %d", len(indices))
	}
	index := indices[0]

	gr, err := gzip.NewReader(bytes.NewReader(descBytesCompressed))
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	descBytes, err := io.ReadAll(gr)
	if err != nil {
		return nil, err
	}

	var desc descriptor.FileDescriptorProto
	if err := proto.Unmarshal(descBytes, &desc); err != nil {
		return nil, err
	} else if index >= len(desc.EnumType) {
		return nil, fmt.Errorf("index %d out of bounds", index)
	}

	res := make([]int, 0, len(desc.EnumType[index].Value))
	for _, v := range desc.EnumType[index].Value {
		res = append(res, int(v.GetNumber()))
	}
	return res, nil
}
