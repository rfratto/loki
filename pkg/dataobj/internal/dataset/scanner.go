package dataset

import (
	"cmp"
	"context"
	"errors"
	"math"
	"slices"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/column"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
)

// TODO(rfratto): way more scanner tests.
//
// TODO(rfratto): support a presence column to avoid loading columns
// unnecessarily. The presence column would be passed to NewScanner separately
// and it wouldn't emit entries with it.
// A Scanner enables scanning over a set of rows in a dataset. Scanners lazily
// load columns based on what is being queried and any filters added to the
// Scanner.
type Scanner struct {
	totalRows int
	columns   []Column
	dataset   Dataset

	columnIndex  map[Column]int // Looks up original index of a column.
	pageFilters  map[Column]PageFilter
	entryFilters map[Column]EntryFilter
}

// Filters.
type (
	// PageFilter is a function which filters out pages based on a column and
	// page tuple. PageFilter should return true to keep the page, false to
	// discard it.
	PageFilter func(column *column.Info, page *page.Info) bool

	// EntryFilter is a function which filters out individual entries in a
	// column. EntryFilter should return true to keep the entry, false to discard
	// it.
	EntryFilter func(column *column.Info, entry ScannerEntry) bool
)

// ScannerEntry represents an individual entry in a scanned column.
type ScannerEntry struct {
	Row   int        // Column-wide row number of the scanned entry.
	Value page.Value // Scanned value.
}

// NewScanner creates a new Scanner which scans over entries in the provided
// columns, lazily downloading pages as needed from getter.
func NewScanner(dataset Dataset, columns []Column) *Scanner {
	columnIndex := make(map[Column]int, len(columns))
	for i, column := range columns {
		columnIndex[column] = i
	}

	totalRows := math.MinInt64
	for _, column := range columns {
		totalRows = max(totalRows, int(column.Info().RowsCount))
	}

	return &Scanner{
		totalRows: totalRows,
		columns:   columns,
		dataset:   dataset,

		columnIndex:  columnIndex,
		pageFilters:  make(map[Column]PageFilter),
		entryFilters: make(map[Column]EntryFilter),
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
func (s *Scanner) AddPageFilter(column Column, include PageFilter) error {
	if _, hasColumn := s.columnIndex[column]; !hasColumn {
		return errors.New("column not in scanner")
	}
	s.pageFilters[column] = include
	return nil
}

// AddEntryFilter allows for filtering out entries within pages. If an entry is
// filtered out, the Scanner will skip the row. If reading the remainder of the
// entry would have loaded a new page, loading that page is skipped.
//
// The order of calls to include is not guaranteed.
//
// AddEntryFilter returns an error if the listed column was not provided to
// NewScanner.
func (s *Scanner) AddEntryFilter(column Column, include EntryFilter) error {
	if _, hasColumn := s.columnIndex[column]; !hasColumn {
		return errors.New("column not in scanner")
	}
	s.entryFilters[column] = include
	return nil
}

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
func (s *Scanner) Iter(ctx context.Context) result.Seq[[]ScannerEntry] {
	type columnData struct {
		Info       *column.Info
		Pages      []Page
		PageInfos  []*page.Info
		RowOffsets []int
		TotalRows  int
	}

	type pullIter struct {
		Iter *lazyColumnIter
		Next func() (result.Result[ScannerEntry], bool)
		Stop func()
	}

	return result.Iter(func(yield func([]ScannerEntry) bool) error {
		var (
			columnDataSet = map[Column]*columnData{}
			columnIters   = map[Column]pullIter{}
			startRow      = 0
		)
		defer func() {
			for _, it := range columnIters {
				it.Stop()
			}
		}()

		getColumnData := func(c Column) (*columnData, error) {
			if p, ok := columnDataSet[c]; ok {
				return p, nil
			}

			pages, err := result.Collect(c.Pages(ctx))
			if err != nil {
				return nil, err
			}

			pageInfos := make([]*page.Info, len(pages))
			for i, p := range pages {
				pageInfos[i] = p.Info()
			}

			totalRows, offsets := getRowOffsets(pages)
			cd := &columnData{
				Info:       c.Info(),
				Pages:      pages,
				PageInfos:  pageInfos,
				RowOffsets: offsets,
				TotalRows:  totalRows,
			}
			columnDataSet[c] = cd
			return cd, nil
		}

		getColumnIter := func(c Column) (pullIter, error) {
			if pi, ok := columnIters[c]; ok {
				return pi, nil
			}

			cd, err := getColumnData(c)
			if err != nil {
				return pullIter{}, err
			}

			it := newLazyColumnIter(cd.Pages, s.dataset)
			next, stop := result.Pull(it.Iter(ctx))
			columnIters[c] = pullIter{
				Iter: it,
				Next: next,
				Stop: stop,
			}
			return columnIters[c], nil
		}

		// First, we need to process page filters.
		for column, pageFilter := range s.pageFilters {
			cd, err := getColumnData(column)
			if err != nil {
				return err
			}

			for i, page := range cd.PageInfos {
				pageStartRow := cd.RowOffsets[i]
				pageEndRow := pageStartRow + int(page.RowCount) - 1

				// If this page ends before our starting row, it's already being
				// filtered out.
				if pageEndRow < startRow {
					continue
				}

				if !pageFilter(cd.Info, page) {
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
				return err
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
		slices.SortStableFunc(orderedColumns, func(a, b Column) int {
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
			// TODO(rfratto): it's a bit weird for the row to be repeated in every
			// scanner entry emited. Maybe we want a RowEntry type instead?
			rowEntries := make([]ScannerEntry, len(s.columns)) // Each index corresponds to the index in s.columns.

			for _, column := range orderedColumns {
				columnIndex := s.columnIndex[column] // Index to fill in rowEntries

				// Lazily get the column iter.
				//
				// TODO(rfratto): if we're done using entry filters for the row, we
				// might want to consider batching page gets across all remaining
				// columns (if pages need to be retrieved). That way we pull multiple
				// pages at once and minimize GET calls.
				columnIter, err := getColumnIter(column)
				if err != nil {
					return err
				}

				// Make sure our iter is the on the row we're trying to read. If it's
				// currently on the same row, this does nothing.
				columnIter.Iter.Seek(row)

				res, ok := columnIter.Next()
				ent, err := res.Value()
				if !ok {
					continue
				} else if err != nil {
					return err
				}

				rowEntries[columnIndex].Row = ent.Row
				rowEntries[columnIndex].Value = ent.Value

				// If any of our filters for this column filter the column out, we can
				// skip the entire row.
				for _, filter := range s.entryFilters {
					if !filter(column.Info(), ent) {
						continue NextRow
					}
				}
			}

			if !yield(rowEntries) {
				return nil
			}
		}

		return nil
	})
}

func getRowOffsets(pages []Page) (totalRows int, offsets []int) {
	offsets = make([]int, len(pages))
	for i, page := range pages {
		// The row offset for a page is the total number of rows up to that page.
		offsets[i] = totalRows
		totalRows += int(page.Info().RowCount)
	}
	return totalRows, offsets
}
