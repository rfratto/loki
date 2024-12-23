package page

import (
	"bufio"
	"errors"
	"fmt"
	"io"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
)

// Iter iterates over all values in the provided Page. If a decoding error
// occurs during iteration, the iterator emits the error and stops.
//
// Callers must ensure that all encodings are in the import graph for Iter to
// be able to successfully decode values; otherwise, Iter will emit an error
// saying a decoder cannot be found.
func Iter(p *Page) result.Seq[Value] {
	return result.Iter(func(yield func(Value) bool) error {
		presenceReader, valuesReader, err := p.Reader()
		if err != nil {
			return fmt.Errorf("opening page for reading: %w", err)
		}
		defer presenceReader.Close()
		defer valuesReader.Close()

		presenceDec, ok := NewValueDecoder(datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_BITMAP, bufio.NewReader(presenceReader))
		if !ok {
			return fmt.Errorf("no decoder available for %s/%s", datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_BITMAP)
		}
		valuesDec, ok := NewValueDecoder(p.Value, p.Encoding, bufio.NewReader(valuesReader))
		if !ok {
			return fmt.Errorf("no decoder available for %s/%s", p.Value, p.Encoding)
		}

		for {
			var value Value

			present, err := presenceDec.Decode()
			if errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return fmt.Errorf("decoding presence bitmap: %w", err)
			} else if present.Type() != datasetmd.VALUE_TYPE_UINT64 {
				return fmt.Errorf("invalid presence type: %v", present.Type())
			}

			if present.Uint64() == 1 { // Present; decode the value.
				value, err = valuesDec.Decode()
				if err != nil {
					return fmt.Errorf("decoding value: %w", err)
				}
			}

			if !yield(value) {
				return nil
			}
		}
	})
}
