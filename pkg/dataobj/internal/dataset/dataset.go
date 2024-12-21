// Package dataset contains utilities for constructing page-oriented columnar
// data.
package dataset

import (
	"context"
	"fmt"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
)

// A Dataset holds a collection of columns, each of which holds a seuqence of
// [page.Value] entries.
//
// Dataset is read-only; callers must not modify any of the values returned by
// methods in Dataset.
type Dataset interface {
	// ListColumns returns the set of columns in the Dataset.
	ListColumns(ctx context.Context) ([]Column, error)

	// ListPages retrieves a set of pages given a list of columns.
	// Implementations of Dataset may use ListPages to optimize for batch
	// reading. The order of [Page]s in the returned slice matches the order of
	// the columns argument.
	ListPages(ctx context.Context, columns []Column) ([]Pages, error)

	// ReadPages returns the set of page data for the specified pages.
	// Implementations of Dataset may use ReadPages to optimize for batch
	// reading. The order of [page.Data] in the returned slice matches the order
	// of the pages argument.
	ReadPages(ctx context.Context, pages []Page) ([]page.Data, error)
}

// A Column is a sequence of values within a dataset. Columns are split up
// across one or more [Page]s to limit the amount of memory needed to read a
// portion of the column at a time.
type Column interface {
	// Info returns the metadata for the Column.
	Info() *ColumnInfo

	// Pages returns the set of ordered pages in the column.
	Pages(ctx context.Context) (Pages, error)
}

// Pages is a set of [Page]s.
type Pages []Page

// A Page is a sequence of values within a [Column].
type Page interface {
	// Info returns the metadata for the Page.
	Info() *page.Info

	// Data returns the data for the Page.
	Data(ctx context.Context) (page.Data, error)
}

// FromBuilders returns an in-memory [Dataset] from the given list of
// [ColumnBuilder]s.
func FromBuilders(builders []*ColumnBuilder) Dataset {
	return &buildersDataset{builders: builders}
}

type buildersDataset struct {
	builders []*ColumnBuilder
}

func (d *buildersDataset) ListColumns(ctx context.Context) ([]Column, error) {
	var columns = make([]Column, len(d.builders))
	for i, b := range d.builders {
		columns[i] = column{builder: b}
	}
	return columns, nil
}

func (d *buildersDataset) ListPages(ctx context.Context, columns []Column) ([]Pages, error) {
	pagesList := make([]Pages, len(columns))

	for i, c := range columns {
		c, ok := c.(column)
		if !ok {
			return nil, fmt.Errorf("unrecognized column %v", c)
		}
		pages, err := c.Pages(ctx)
		if err != nil {
			return nil, err
		}
		pagesList[i] = pages
	}

	return pagesList, nil
}

func (d *buildersDataset) ReadPages(ctx context.Context, pages []Page) ([]page.Data, error) {
	dataList := make([]page.Data, len(pages))

	for i, page := range pages {
		p, ok := page.(*columnPage)
		if !ok {
			return nil, fmt.Errorf("unrecognized page %v", p)
		}
		dataList[i] = p.page.Data
	}

	return dataList, nil
}

type column struct {
	builder *ColumnBuilder
}

func (c column) Info() *ColumnInfo { return c.builder.Info() }

func (c column) Pages(ctx context.Context) (Pages, error) {
	var pages = make([]Page, len(c.builder.Pages()))
	for i, p := range c.builder.Pages() {
		pages[i] = &columnPage{page: p}
	}
	return pages, nil
}

type columnPage struct {
	page *page.Page
}

func (p *columnPage) Info() *page.Info                            { return p.page.Info() }
func (p *columnPage) Data(ctx context.Context) (page.Data, error) { return p.page.Data, nil }
