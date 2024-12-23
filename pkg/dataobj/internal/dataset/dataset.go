// Package dataset contains utilities for constructing page-oriented columnar
// data.
package dataset

import (
	"context"
	"fmt"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
)

// TODO(rfratto): Having datatset.Column but page.Page is getting weird.
//
// 1. Move encoding/page to dataset/page and create a daataset/column with
//    column-wide utilities. (DONE)
//
// 2. Update page.Page to page.Memory, and then move the Page interface to the
//    page package. Also move Column to column.Column.
//
// 3. Move the contents of the encoding package to streamio for "streaming
//    i/o". streamio.Reader, streamio.Writer, streamio.WriteUvarint, etc.
//
// 4. Relocate encoding/obj to encoding.
//
// 5. Update other packages to operate on dataset.Dataset where appropriate
//    (though it looks like most things have been updated by now).

// A Dataset holds a collection of columns, each of which holds a seuqence of
// [page.Value] entries.
//
// Dataset is read-only; callers must not modify any of the values returned by
// methods in Dataset.
type Dataset interface {
	// ListColumns returns the set of columns in the Dataset. The return order of
	// ListColumns must be consistent.
	ListColumns(ctx context.Context) result.Seq[Column]

	// ListPages retrieves a set of pages given a list of columns.
	// Implementations of Dataset may use ListPages to optimize for batch
	// reading. The order of [Page]s in the returned slice matches the order of
	// the columns argument.
	ListPages(ctx context.Context, columns []Column) result.Seq[Pages]

	// ReadPages returns the set of page data for the specified pages.
	// Implementations of Dataset may use ReadPages to optimize for batch
	// reading. The order of [page.Data] in the returned slice matches the order
	// of the pages argument.
	ReadPages(ctx context.Context, pages []Page) result.Seq[page.Data]
}

// A Column is a sequence of values within a dataset. Columns are split up
// across one or more [Page]s to limit the amount of memory needed to read a
// portion of the column at a time.
type Column interface {
	// Info returns the metadata for the Column.
	Info() *ColumnInfo

	// Pages returns the set of ordered pages in the column.
	Pages(ctx context.Context) result.Seq[Page]
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

func (d *buildersDataset) ListColumns(ctx context.Context) result.Seq[Column] {
	return result.Iter(func(yield func(Column) bool) error {
		for _, b := range d.builders {
			if !yield(column{builder: b}) {
				return nil
			}
		}
		return nil
	})
}

func (d *buildersDataset) ListPages(ctx context.Context, columns []Column) result.Seq[Pages] {
	return result.Iter(func(yield func(Pages) bool) error {
		for _, c := range columns {
			c, ok := c.(column)
			if !ok {
				return fmt.Errorf("unrecognized column %v", c)
			}

			pages, err := result.Collect(c.Pages(ctx))
			if err != nil {
				return err
			} else if !yield(pages) {
				return nil
			}
		}

		return nil
	})
}

func (d *buildersDataset) ReadPages(ctx context.Context, pages []Page) result.Seq[page.Data] {
	return result.Iter(func(yield func(page.Data) bool) error {
		for _, p := range pages {
			p, ok := p.(*columnPage)
			if !ok {
				return fmt.Errorf("unrecognized page %v", p)
			} else if !yield(p.page.Data) {
				return nil
			}
		}

		return nil
	})
}

type column struct {
	builder *ColumnBuilder
}

func (c column) Info() *ColumnInfo {
	// Columns may change over time so we don't cache the result of
	// c.builder.Info().
	return c.builder.Info()
}

func (c column) Pages(ctx context.Context) result.Seq[Page] {
	return result.Iter(func(yield func(Page) bool) error {
		for _, p := range c.builder.Pages() {
			if !yield(&columnPage{info: p.Info(), page: p}) {
				return nil
			}
		}

		return nil
	})
}

type columnPage struct {
	info *page.Info
	page *page.Page
}

func (p *columnPage) Info() *page.Info                            { return p.info }
func (p *columnPage) Data(ctx context.Context) (page.Data, error) { return p.page.Data, nil }
