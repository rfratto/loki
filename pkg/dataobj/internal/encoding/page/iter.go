package page

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

// Iter iterates over all values in the provided Page. If a decoding error
// occurs during iteration, the iterator emits the error and stops.
//
// Callers must ensure that all encodings are in the import graph for Iter to
// be able to successfully decode values; otherwise, Iter will emit an error
// saying a decoder cannot be found.
func Iter(p Page) iter.Seq2[Value, error] {
	return func(yield func(Value, error) bool) {
		presenceReader, valuesReader, err := p.Reader()
		if err != nil {
			yield(Value{}, fmt.Errorf("opening page for reading: %w", err))
			return
		}
		defer presenceReader.Close()
		defer valuesReader.Close()

		presenceDec, ok := NewValueDecoder(datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_BITMAP, bufio.NewReader(presenceReader))
		if !ok {
			yield(Value{}, fmt.Errorf("no decoder available for %s/%s", datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_BITMAP))
			return
		}
		valuesDec, ok := NewValueDecoder(p.Value, p.Encoding, bufio.NewReader(valuesReader))
		if !ok {
			yield(Value{}, fmt.Errorf("no decoder available for %s/%s", p.Value, p.Encoding))
			return
		}

		for {
			var value Value

			present, err := presenceDec.Decode()
			if errors.Is(err, io.EOF) {
				return
			} else if err != nil {
				yield(Value{}, fmt.Errorf("decoding presence bitmap: %w", err))
				return
			} else if present.Type() != datasetmd.VALUE_TYPE_UINT64 {
				yield(Value{}, fmt.Errorf("invalid presence type: %v", present.Type()))
				return
			}

			if present.Uint64() == 1 { // Present; decode the value.
				value, err = valuesDec.Decode()
				if err != nil {
					yield(Value{}, fmt.Errorf("decoding value: %w", err))
					return
				}
			}

			if !yield(value, nil) {
				return
			}
		}
	}
}
