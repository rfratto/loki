package dataset

import (
	"context"
	"fmt"
	"slices"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/column"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
)

// Sort creates a new Dataset with rows sorted by the given columns in
// ascending order. The order of columns in the new Dataset will be the same as
// the original dataset.
//
// If columns is empty or if the columns contain no data, Sort will return the
// original dataset.
func Sort(ctx context.Context, dataset Dataset, columns []Column, pageSizeHint int) (Dataset, error) {
	if len(columns) == 0 {
		return dataset, nil
	}

	// First, we need to read all the values from columns.
	type sortData struct {
		Row    int          // Original row of the sorted data.
		Values []page.Value // Values to sort the row by.
	}
	var sortDataList []sortData

	scanner := NewScanner(dataset, columns)
	for ent := range scanner.Iter(ctx) {
		row, err := ent.Value()
		if err != nil {
			return nil, fmt.Errorf("iterating over existing dataset: %w", err)
		} else if len(row) != len(columns) {
			return nil, fmt.Errorf("row length mismatch: expected %d, got %d", len(columns), len(row))
		}

		values := make([]page.Value, len(row))
		for i, v := range row {
			values[i] = v.Value
		}
		sortDataList = append(sortDataList, sortData{
			Row:    row[0].Row,
			Values: values,
		})
	}

	if len(sortDataList) == 0 {
		return dataset, nil
	}

	// Sort all of our rows by the columns we specified.
	slices.SortStableFunc(sortDataList, func(a sortData, b sortData) int {
		for i, aVal := range a.Values {
			if i >= len(b.Values) {
				break
			}
			bVal := b.Values[i]

			// The moment a<b or a>b for any column, we can return the result.
			// Otherwise, the rows are equal (in terms of the sorting columns).
			switch page.CompareValues(aVal, bVal) {
			case -1:
				return -1
			case 1:
				return 1
			}
		}

		return 0
	})

	// Now, we create a new set of columns. We fill them with rows matching the order of sortDataList.
	allColumns, err := result.Collect(dataset.ListColumns(ctx))
	if err != nil {
		return nil, fmt.Errorf("listing columns: %w", err)
	}

	newColumns := make([]*column.Builder, 0, len(allColumns))
	for _, origColumn := range allColumns {
		origInfo := origColumn.Info()
		pages, err := result.Collect(origColumn.Pages(ctx))
		if err != nil {
			return nil, fmt.Errorf("getting pages: %w", err)
		} else if len(pages) == 0 {
			return nil, fmt.Errorf("unexpected column with no pages")
		}

		newColumn, err := column.NewBuilder(origInfo.Name, page.BuilderOptions{
			PageSizeHint: pageSizeHint,
			Value:        origInfo.Type,
			Encoding:     pages[0].Info().Encoding,
			Compression:  origInfo.Compression,
		})
		if err != nil {
			return nil, fmt.Errorf("creating new column: %w", err)
		}

		// The new column becomes populated with sorted data by iterating over
		// values in the original column in sorted order. To minimize the memory
		// impact of this, we only load in values from one page at a time.
		//
		// Loading in the entire column would have a larger memory overhead than
		// sortDataList, as that only contains columns to sort on, which are
		// typically much smaller in size.
		//
		// Loading in the entire page is more memory-efficient, but comes at the
		// cost of a lot of page loads for very unsorted data.
		var (
			curPage       Page         // Current page for iteration.
			curPageValues []page.Value // Values for the current page.
		)
		for i, sortData := range sortDataList {
			// To avoid downloading the page for each row, we cache the most recent
			// page and its values. This way, we only need to read a page when our
			// sorted row changes pages.
			nextPage, rowInPage := pageForRow(pages, sortData.Row)
			if nextPage == nil {
				return nil, fmt.Errorf("row %d not found in column", sortData.Row)
			} else if nextPage != curPage {
				curPage = nextPage

				data, err := curPage.Data(ctx)
				if err != nil {
					return nil, fmt.Errorf("getting page data: %w", err)
				}

				rawPage := page.Raw(curPage.Info(), data)
				curPageValues, err = result.Collect(page.Iter(rawPage))
				if err != nil {
					return nil, fmt.Errorf("reading page: %w", err)
				}
			}

			if err := newColumn.Append(i, curPageValues[rowInPage]); err != nil {
				return nil, fmt.Errorf("appending value: %w", err)
			}
		}

		newColumn.Flush()
		newColumns = append(newColumns, newColumn)
	}

	return FromBuilders(newColumns), nil
}

// pageForRow returns the page that contains the provided column row number
// along with the relative row number for that page.
func pageForRow(pages []Page, row int) (Page, int) {
	startRow := 0

	for _, page := range pages {
		info := page.Info()

		if row >= startRow && row < startRow+info.RowCount {
			return page, row - startRow
		}

		startRow += info.RowCount
	}

	return nil, -1
}
