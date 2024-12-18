package dataset

import (
	"fmt"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
)

// A Column holds a sequence of entries of a given Value type. Values are
// accumulated in a buffer and then flushed to a [Page] once the amount of
// values exceeds a configurable size.
type Column[Value page.DataType] struct {
	name string
	opts BufferOptions

	rows int // Total number of rows in the column.

	pages []Page
	buf   *Buffer[Value]
}

// NewColumn creates a new Column from the optional name and the provided
// options. NewColumn returns an error if the options are invalid.
func NewColumn[Value page.DataType](name string, opts BufferOptions) (*Column[Value], error) {
	buf, err := NewBuffer[Value](opts)
	if err != nil {
		return nil, fmt.Errorf("creating buffer: %w", err)
	}

	return &Column[Value]{
		name: name,
		opts: opts,

		buf: buf,
	}, nil
}

// Append adds a new value to the Column with the given row value. If the row
// number is more than the current number of rows in the Column, nulls are
// added up to the new row.
//
// Append returns an error if the row number is out of order.
func (c *Column[Value]) Append(row int, value Value) error {
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

func (c *Column[Value]) append(row int, value Value) bool {
	for row > c.rows {
		if !c.buf.AppendNull() {
			return false
		}
		c.rows++
	}

	return c.buf.Append(value)
}

// Flush flushes the current buffer and stores it as a [Page]. Flush does
// nothing if there are no rows in the buffer.
func (c *Column[Value]) Flush() {
	if c.buf.Rows() == 0 {
		return
	}

	page, err := c.buf.Flush()
	if err != nil {
		panic(fmt.Sprintf("failed to flush page: %s", err))
	}
	c.pages = append(c.pages, page)
}

// Pages returns a read-only slice of Pages in the Column. The caller must not
// modify this slice.
//
// Any unflushed data is not included in the returned slice of Pages. To make
// sure all data is available, call [Column.Flush] before calling Pages.
func (c *Column[Value]) Pages() []Page {
	return c.pages
}
