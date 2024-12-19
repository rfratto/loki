package dataset

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"math"
	"slices"
	"unsafe"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

// A Scanner enables scanning over a set of rows in a dataset. Scanners lazily
// load columns based on what is being queried and any filters added to the
// Scanner.
type Scanner struct {
	totalRows int
	columns   []*datasetmd.ColumnInfo
	getter    PageGetter

	columnIndex  map[*datasetmd.ColumnInfo]int // Looks up original index of a column.
	pageFilters  map[*datasetmd.ColumnInfo]PageFilter
	entryFilters map[*datasetmd.ColumnInfo]EntryFilter
}

// NewScanner creates a new Scanner which scans over entries in the provided
// columns, lazily downloading pages as needed from getter.
func NewScanner(columns []*datasetmd.ColumnInfo, getter PageGetter) *Scanner {
	columnIndex := make(map[*datasetmd.ColumnInfo]int, len(columns))
	for i, column := range columns {
		columnIndex[column] = i
	}

	totalRows := math.MinInt64
	for _, column := range columns {
		totalRows = max(totalRows, int(column.RowsCount))
	}

	return &Scanner{
		totalRows: totalRows,
		columns:   columns,
		getter:    getter,

		columnIndex:  columnIndex,
		pageFilters:  make(map[*datasetmd.ColumnInfo]PageFilter),
		entryFilters: make(map[*datasetmd.ColumnInfo]EntryFilter),
	}
}

// AddPageFilter allows filtering out entire pages within columns. If a page is
// filtered out, the Scanner will not download the page and will skip over any
// rows across all columns for which the provided include function returns
// false.
//
// The order of calls to include is not guaranteed.
//
// The most common use of a page filter is to analyze its statistics to filter
// out based on min/max value ranges.
//
// AddPageFilter returns an error if the listed column was not provided to
// NewScanner.
func (*Scanner) AddPageFilter(column *datasetmd.ColumnInfo, include PageFilter) error { panic("NYI") }

// AddEntryFilter allows for filtering out entries within pages. If an entry is
// filtered out, the Scanner will skip the row. If reading the remainder of the
// entry would have loaded a new page, loading that page is skipped.
//
// The order of calls to include is not guaranteed.
//
// AddEntryFilter returns an error if the listed column was not provided to
// NewScanner.
func (*Scanner) AddEntryFilter(column *datasetmd.ColumnInfo, include EntryFilter) error { panic("NYI") }

// Iter iterates over entries in the Scanner's columns.
//
// To do wide-sweeping filtering of rows, page filters added to the Scanner are
// processed first.
//
// Iteration begins after page filters have been processed. Pages with entry
// filters are loaded first, and entry filters are processed. If the entry has
// not been filtered out by any entry filter, the remainder of the row is
// constructed, lazily loading pages as needed.
//
// For each row not filtered out, Iter emits a slice of ScannerEntry matching
// the order of columns passed to NewScanner. Callers must not retain this
// slice.
//
// If a decoding error is encountered during iteration, Iter emits an error and
// stops.
func (s *Scanner) Iter(ctx context.Context) iter.Seq2[[]ScannerEntry, error] {
	type columnData struct {
		Pages      []*datasetmd.PageInfo
		RowOffsets []int
		TotalRows  int
	}

	type pullIter struct {
		Iter *columnIter
		Next func() (ScannerEntry, error, bool)
		Stop func()
	}

	return func(yield func([]ScannerEntry, error) bool) {
		var (
			columnDataSet = map[*datasetmd.ColumnInfo]*columnData{}
			columnIters   = map[*datasetmd.ColumnInfo]pullIter{}
			startRow      = 0
		)
		defer func() {
			for _, it := range columnIters {
				it.Stop()
			}
		}()

		getColumnData := func(ci *datasetmd.ColumnInfo) (*columnData, error) {
			if p, ok := columnDataSet[ci]; ok {
				return p, nil
			}
			pi, err := s.getter.ColumnPages(ctx, ci)
			if err != nil {
				return nil, err
			}

			totalRows, offsets := getRowOffsets(pi)
			cd := &columnData{
				Pages:      pi,
				RowOffsets: offsets,
				TotalRows:  totalRows,
			}
			columnDataSet[ci] = cd
			return cd, nil
		}

		getColumnIter := func(ci *datasetmd.ColumnInfo) (pullIter, error) {
			if pi, ok := columnIters[ci]; ok {
				return pi, nil
			}

			cd, err := getColumnData(ci)
			if err != nil {
				return pullIter{}, err
			}

			it := newColumnIter(ci, cd.Pages, s.getter)
			next, stop := iter.Pull2(it.Iter(ctx))
			columnIters[ci] = pullIter{
				Iter: it,
				Next: next,
				Stop: stop,
			}
			return columnIters[ci], nil
		}

		// First, we need to process page filters.
		for column, pageFilter := range s.pageFilters {
			cd, err := getColumnData(column)
			if err != nil {
				yield(nil, err)
				return
			}

			for i, page := range cd.Pages {
				pageStartRow := cd.RowOffsets[i]
				pageEndRow := pageStartRow + int(page.RowsCount) - 1

				// If this page ends before our starting row, it's already being
				// filtered out.
				if pageEndRow < startRow {
					continue
				}

				if !pageFilter(column, page) {
					// If a page is filtered out we want to start on first row of the
					// next page, which is going to be pageEnd+1.
					startRow = pageEndRow + 1
				}
			}
		}

		// Populate our columnDataSet cache with the columns in entry filters. This
		// will allow us to sort columns by page population below.
		for column := range s.entryFilters {
			_, err := getColumnData(column)
			if err != nil {
				yield(nil, err)
				return
			}
		}

		// Now that we know what row we're starting on, we can start iterating over
		// entries.
		//
		// We want to lazily load pages for each column only as they are needed. To
		// minimize the number of page loads, we first iterate over columns with
		// entry filters, prefering columns that have less pages (implying more
		// rows per page).
		orderedColumns := slices.Clone(s.columns)
		slices.SortStableFunc(orderedColumns, func(a, b *datasetmd.ColumnInfo) int {
			var (
				_, aHasEntryFilter = s.entryFilters[a]
				_, bHasEntryFilter = s.entryFilters[b]
			)

			// -1 when a < b, 0 when a == b, +1 when a > b
			switch {
			case aHasEntryFilter && !bHasEntryFilter:
				return -1 // a < b
			case !aHasEntryFilter && bHasEntryFilter:
				return 1 // a > b
			case aHasEntryFilter && bHasEntryFilter: // Data with less pages goes first
				aData, _ := getColumnData(a)
				bData, _ := getColumnData(b)
				return cmp.Compare(len(aData.Pages), len(bData.Pages))
			default:
				return 0
			}
		})

	NextRow:
		for row := startRow; row < s.totalRows; row++ {
			rowEntries := make([]ScannerEntry, len(s.columns)) // Each index corresponds to the index in s.columns.

			for _, column := range orderedColumns {
				columnIndex := s.columnIndex[column] // Index to fill in rowEntries

				// Lazily get the column iter.
				columnIter, err := getColumnIter(column)
				if err != nil {
					yield(nil, err)
					return
				}

				// Make sure our iter is the on the row we're trying to read. If it's
				// currently on the same row, this does nothing.
				columnIter.Iter.Seek(row)

				ent, err, ok := columnIter.Next()
				if !ok {
					continue
				} else if err != nil {
					yield(nil, err)
					return
				}

				rowEntries[columnIndex].Row = ent.Row
				rowEntries[columnIndex].Value = ent.Value

				// If any of our filters for this column filter the column out, we can
				// skip the entire row.
				for _, filter := range s.entryFilters {
					if !filter(column, ent) {
						continue NextRow
					}
				}
			}

			if !yield(rowEntries, nil) {
				return
			}
		}
	}
}

func getRowOffsets(pages []*datasetmd.PageInfo) (totalRows int, offsets []int) {
	offsets = make([]int, len(pages))
	for i, page := range pages {
		// The row offset for a page is the total number of rows up to that page.
		offsets[i] = totalRows
		totalRows += int(page.RowsCount)
	}
	return totalRows, offsets
}

// PageGetter retrieves page metadata for a column and the data of pages
// itself.
type PageGetter interface {
	// ColumnPages returns the set of available pages for the given column.
	ColumnPages(ctx context.Context, column *datasetmd.ColumnInfo) ([]*datasetmd.PageInfo, error)

	// ReadPages retrieves PageData for the provided pages. The order of the
	// returned set of PageData must match the input set.
	ReadPages(ctx context.Context, pages []*datasetmd.PageInfo) ([]PageData, error)
}

// Filters.
type (
	// PageFilter is a function which filters out pages based on a column and
	// page tuple. PageFilter should return true to keep the page, false to
	// discard it.
	PageFilter func(column *datasetmd.ColumnInfo, page *datasetmd.PageInfo) bool

	// EntryFilter is a function which filters out individual entries in a
	// column. EntryFilter should return true to keep the entry, false to discard
	// it.
	EntryFilter func(column *datasetmd.ColumnInfo, entry ScannerEntry) bool
)

// ScannerEntry represents an individual entry in a scanned column.
type ScannerEntry struct {
	Row   int          // Column-wide row number of the scanned entry.
	Value ScannerValue // Scanned value.
}

// ScannerValue represents a scanned value in a column.
type ScannerValue struct {
	// The internal representation of ScannerValue is based on log/slog, which
	// designed its Value type to avoid allocations.
	//
	// Our subset of usage avoids allocations entirely.

	_ [0]func() // Disable equality checking

	// num holds the value for numeric types in the [page.DataType] type
	// constraint and the string length for string types.
	num uint64

	// If any is of type datasetmd.ValueType, then the value is in num as
	// described above.
	//
	// If any is of type *byte, then the ScannerValue is
	// datasetmd.VALUE_TYPE_STRING and the string value consists of the length in
	// num and the pointer in any.
	any any
}

func int64Value(value int64) ScannerValue {
	return ScannerValue{num: uint64(value), any: datasetmd.VALUE_TYPE_INT64}
}

func uint64Value(value uint64) ScannerValue {
	return ScannerValue{num: value, any: datasetmd.VALUE_TYPE_UINT64}
}

func stringValue(value string) ScannerValue {
	return ScannerValue{num: uint64(len(value)), any: (*byte)(unsafe.StringData(value))}
}

// Type return's the dataset type of sv.
func (sv ScannerValue) Type() datasetmd.ValueType {
	if sv.IsNil() {
		return datasetmd.VALUE_TYPE_UNSPECIFIED
	}

	switch v := sv.any.(type) {
	case datasetmd.ValueType:
		return v
	case *byte:
		return datasetmd.VALUE_TYPE_STRING
	default:
		panic(fmt.Sprintf("ScannerValue has unexpected type %T", v))
	}
}

// IsNil returns true if sv is nil.
func (sv ScannerValue) IsNil() bool {
	return sv.any == nil
}

// Int64 returns sv's value as an int64. It panics if sv is not a signed
// integer.
func (sv ScannerValue) Int64() int64 {
	if expect, actual := datasetmd.VALUE_TYPE_INT64, sv.Type(); expect != actual {
		panic(fmt.Sprintf("ScannerValue kind is %s, not %s", actual, expect))
	}
	return int64(sv.num)
}

// Uint64 returns sv's value as a uint64. It panics if sv is not a signed
// integer.
func (sv ScannerValue) Uint64() uint64 {
	if expect, actual := datasetmd.VALUE_TYPE_UINT64, sv.Type(); expect != actual {
		panic(fmt.Sprintf("ScannerValue kind is %s, not %s", actual, expect))
	}
	return sv.num
}

// String returns sv's value as a string. Because of Go's String method
// convention, if sv is not a string, String returns a string of the form
// "VALUE_TYPE_T" where T is the appropriate type.
func (sv ScannerValue) String() string {
	if bp, ok := sv.any.(*byte); ok {
		return unsafe.String(bp, sv.num)
	}
	return sv.Type().String()
}
