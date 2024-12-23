// Package column defines a dataset column, a collection pof pages.
package column

import (
	"fmt"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"

	_ "github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page/all" // Register all page encodings.
)

// A Builder builds a sequence of [page.Value] entries of a common type into a
// column. Values are accumulated in a buffer and then flushed to a [Page] once
// the amount of values exceeds a configurable size.
type Builder struct {
	name string
	opts page.BuilderOptions

	rows int // Total number of rows in the column.

	pages   []*page.Page
	builder *page.Builder
}

// NewBuilder creates a new Builder from the optional name and the provided
// options. NewBuilder returns an error if the options are invalid.
func NewBuilder(name string, opts page.BuilderOptions) (*Builder, error) {
	builder, err := page.NewBuilder(opts)
	if err != nil {
		return nil, fmt.Errorf("creating buffer: %w", err)
	}

	return &Builder{
		name: name,
		opts: opts,

		builder: builder,
	}, nil
}

// Append adds a new value to the Builder with the given row value. If the row
// number is more than the current number of rows in the Builder, nulls are
// added up to the new row.
//
// Append returns an error if the row number is out of order.
func (c *Builder) Append(row int, value page.Value) error {
	if row < c.rows {
		return fmt.Errorf("row %d is older than current row %d", row, c.rows)
	}

	// We give two attempts to append the data to the buffer; if the buffer is
	// full, we cut a page and then append again to the newly reset buffer.
	//
	// The second iteration should never fail, as the buffer will always be
	// empty.
	for range 2 {
		if c.append(row, value) {
			c.rows = row + 1
			return nil
		}

		c.Flush()
	}

	panic("column.Append: failed to append value to fresh buffer")
}

// Backfill adds NULLs into Builder up to the provided row number. If values
// exist up to the row number, Backfill does nothing.
func (c *Builder) Backfill(row int) {
	// We give two attempts to append the data to the buffer; if the buffer is
	// full, we cut a page and then append again to the newly reset buffer.
	//
	// The second iteration should never fail, as the buffer will always be
	// empty.
	for range 2 {
		if c.backfill(row) {
			return
		}
		c.Flush()
	}

	panic("column.Backfill: failed to append value to fresh buffer")
}

func (c *Builder) backfill(row int) bool {
	for row > c.rows {
		if !c.builder.AppendNull() {
			return false
		}
		c.rows++
	}

	return true
}

func (c *Builder) append(row int, value page.Value) bool {
	for row > c.rows {
		if !c.builder.AppendNull() {
			return false
		}
		c.rows++
	}

	return c.builder.Append(value)
}

// Flush flushes the current buffer and stores it as a [page.Page]. Flush does
// nothing if there are no rows in the buffer.
func (c *Builder) Flush() {
	if c.builder.Rows() == 0 {
		return
	}

	page, err := c.builder.Flush()
	if err != nil {
		panic(fmt.Sprintf("failed to flush page: %s", err))
	}
	c.pages = append(c.pages, page)
}

// Pages returns a read-only slice of Pages in the Column. The caller must not
// modify this slice.
//
// Any unflushed data is not included in the returned slice of Pages. To make
// sure all data is available, call [Builder.Flush] before calling Pages.
func (c *Builder) Pages() []*page.Page {
	return c.pages
}

// Info describes a column.
type Info struct {
	Name string              // Name of the column, if any.
	Type datasetmd.ValueType // Type of values in the column.

	RowsCount        int // Total number of rows in the column.
	CompressedSize   int // Total size of all pages in the column after compression.
	UncompressedSize int // Total size of all pages in the column before compression.

	Compression datasetmd.CompressionType // Compression used for the column.

	Statistics *datasetmd.Statistics // Optional statistics for the column.
}

// Info returns [Info] for the column being built.
//
// Any unflushed data is not included in calculated info. To make sure all data
// is available, call [Builder.Flush] before calling Pages.
//
// By default, ColumnInfo.Statistics is nil. Callers must manually compute
// statistics for columns if needed.
func (c *Builder) Info() *Info {
	info := Info{
		Name: c.name,
		Type: c.opts.Value,

		Compression: c.opts.Compression,
	}

	// TODO(rfratto): Should we compute column-wide statistics if they're
	// available in pages?
	//
	// That would potentially work for min/max values, but not for count
	// distinct, unless we had a way to pass sketches around.

	for _, page := range c.pages {
		info.RowsCount += page.RowCount
		info.CompressedSize += page.CompressedSize
		info.UncompressedSize += page.UncompressedSize
	}

	return &info
}
