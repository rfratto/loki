// Package decoder provides an API for decoding data objects.
package decoder

import (
	"io"
	"iter"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

// TODO(rfratto): At the moment this is being designed just for testing the
// encoder. In the future, we'll want to introduce the ability for batching and
// likely change the interface given to Open to pass context.Context instances
// in.

// Object is an opened data object.
type Object struct{}

// Open a data object from the provided reader. Open returns an error if the
// data object is malformed.
func Open(r io.ReadSeeker, size int) *Object

// Validate checks the magic of the data object.
func (obj *Object) Validate() error

// Sections retrieves the set of sections in the object.
func (obj *Object) Sections() ([]filemd.Section, error)

// Streams retrieves the set of streams from the provided streams section.
func (obj *Object) Streams(sec filemd.Section) ([]streamsmd.Stream, error)

// Columns retrieves the set of columns from the provided stream.
func (obj *Object) Columns(stream streamsmd.Stream) ([]streamsmd.ColumnsMetadata, error)

// Pages returns an iterator over pages from the provided column.
func (obj *Object) Pages(col streamsmd.ColumnsMetadata) iter.Seq2[streams.Page, error]
