package dataset

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/bitmap"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

// IterPages iterates over all values from the provided iterator of pages. The
// value type in pages must match the Value type parameter. If a decoding error
// is encountered in any page, the iterator emits the error and stops.
//
// Row numbers emitted by IterPagesSlice will be relative to the pages iterated
// over, and not the entire column.
func IterPages(pageIter iter.Seq2[Page, error]) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		var row int

		for p, err := range pageIter {
			if err != nil {
				yield(Entry{}, err)
				return
			}

			for ent, err := range IterPage(row, p) {
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

// IterPagesSlice iterates over all values in the provided Pages. The value
// type in the pages must be equivalent to the Go type provided. If a decoding
// error is encountered in any page, the iterator emits the error and stops.
//
// Row numbers emitted by IterPagesSlice will be relative to the set of pages
// provided, not the column.
func IterPagesSlice(pages ...Page) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		var row int

		for _, p := range pages {
			for ent, err := range IterPage(row, p) {
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
//
// The row numbers emitted start at rowOffset up to rowOffset+p.RowCount.
func IterPage(rowOffset int, p Page) iter.Seq2[Entry, error] {
	row := rowOffset

	return func(yield func(Entry, error) bool) {
		presenceReader, valuesReader, err := p.Reader()
		if err != nil {
			yield(Entry{}, fmt.Errorf("opening page for reading: %w", err))
			return
		}
		defer presenceReader.Close()
		defer valuesReader.Close()

		var (
			presenceDec = bitmap.NewDecoder(bufio.NewReader(presenceReader))
			valuesDec   = newDecoder(bufio.NewReader(valuesReader), p.Value, p.Encoding)
		)
		if valuesDec == nil {
			err := fmt.Errorf("no decoder available for %s/%s", p.Value, p.Encoding)
			yield(Entry{}, err)
			return
		}

		for {
			var value page.Value

			present, err := presenceDec.Decode()
			if errors.Is(err, io.EOF) {
				return
			} else if err != nil {
				yield(Entry{}, fmt.Errorf("decoding presence bitmap: %w", err))
				return
			} else if present.Type() != datasetmd.VALUE_TYPE_UINT64 {
				yield(Entry{}, fmt.Errorf("invalid presence type: %v", present.Type()))
				return
			}

			if present.Uint64() == 1 { // Present; decode the value.
				value, err = valuesDec.Decode()
				if err != nil {
					yield(Entry{}, fmt.Errorf("decoding value: %w", err))
					return
				}
			}

			if !yield(Entry{Row: row, Value: value}, nil) {
				return
			}
			row++
		}
	}
}

// Entry is an entry in a page, returned by [IterPage].
type Entry struct {
	Row   int        // Row number of the entry.
	Value page.Value // Value of the entry.
}
