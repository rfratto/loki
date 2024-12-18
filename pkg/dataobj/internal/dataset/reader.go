package dataset

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/bitmap"
)

// IterPages iterates over all values in the provided Pages. The value type in
// the pages must be equivalent to the Go type provided. If a decoding error is
// encountered in any page, the iterator emits the error and stops.
func IterPages[Value page.DataType](pages ...Page) iter.Seq2[Entry[Value], error] {
	return func(yield func(Entry[Value], error) bool) {
		var row int

		for _, p := range pages {
			for ent, err := range iterPageFromRow[Value](row, p) {
				if !yield(ent, err) {
					return
				}
				if err != nil {
					return
				}
				row++
			}
		}
	}
}

// IterPage iterates over all values in the provided Page. The value type in
// the page must be equivalent to the Go Value type provided. If a decoding
// error is encountered, the iterator emits the error and stops.
func IterPage[Value page.DataType](p Page) iter.Seq2[Entry[Value], error] {
	return iterPageFromRow[Value](0, p)
}

func iterPageFromRow[Value page.DataType](row int, p Page) iter.Seq2[Entry[Value], error] {
	return func(yield func(Entry[Value], error) bool) {
		valueType := page.MetadataValueType[Value]()
		if valueType != p.Value {
			err := fmt.Errorf("value type mismatch: expected %s, got %s", valueType, p.Value)
			yield(Entry[Value]{}, err)
			return
		}

		presenceReader, valuesReader, err := p.Reader()
		if err != nil {
			yield(Entry[Value]{}, fmt.Errorf("opening page for reading: %w", err))
			return
		}
		defer presenceReader.Close()
		defer valuesReader.Close()

		var (
			presenceDec = bitmap.NewDecoder(bufio.NewReader(presenceReader))
			valuesDec   = newDecoder[Value](bufio.NewReader(valuesReader), p.Encoding)
		)
		if valuesDec == nil {
			err := fmt.Errorf("no decoder available for %s/%s", valueType, p.Encoding)
			yield(Entry[Value]{}, err)
			return
		}

		for {
			var value Value

			present, err := presenceDec.Decode()
			if errors.Is(err, io.EOF) {
				return
			} else if err != nil {
				yield(Entry[Value]{}, fmt.Errorf("decoding presence bitmap: %w", err))
				return
			}

			if present == 1 { // Present; decode the value.
				value, err = valuesDec.Decode()
				if err != nil {
					yield(Entry[Value]{}, fmt.Errorf("decoding value: %w", err))
					return
				}
			}

			if !yield(Entry[Value]{Row: row, Value: value}, nil) {
				return
			}
			row++
		}
	}
}

// Entry is an entry in a page, returned by [IterPage].
type Entry[Value page.DataType] struct {
	Row   int   // Row number of the entry.
	Value Value // Value of the entry.
}
