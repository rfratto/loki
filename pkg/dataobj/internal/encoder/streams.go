package encoder

import (
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

// Streams is an instance of the streams section within a data object.
type Streams struct {
	metadataSize int
	obj          *Object
}

// OpenStream opens a new Stream. OpenStream fails if there is currently
// another open stream or if the Streams section has been closed.
//
// If opening a new stream would exceed the maximum metadata size for Streams,
// OpenStream returns an error.
func (s *Streams) OpenStream(id streamsmd.StreamIdentifier) (*Stream, error)

// Close the streams section, writing it to the data object. After Close is
// called, the streams section can no longer be modified.
func (s *Streams) Close() error

// Discard closes the streams section without writing it to the object. After
// Discard is called, the streams section can no longer be modified.
func (s *Streams) Discard() error

// Stream is an instance of a stream within a data object.
type Stream struct {
	metadataSize int
	streams      *Streams
}

// OpenColumn opens a new column in the stream. OpenColumn fails if there is
// currently another open column or if the Stream has been closed.
//
// If opening a new column would exceed the maximum metadata size for Stream,
// OpenColumn returns an error.
func (s *Stream) OpenColumn() (*Column, error)

// Close the stream, writing it to the data object. After Close is called, there
// can be no further modifications to the stream.
func (s *Stream) Close() error

// Discard closes the stream without writing it to the object. After Discard
// is called, there can be no further modifications to the stream.
func (s *Stream) Discard() error

// Column is an instance of a column within a stream.
type Column struct {
	metadataSize int
	stream       *Stream
}

// AppendPage appends a new page to the column. AppendPage fails if the column
// has been closed.
//
// If appending a new page would exceed the maximum metadata size for Column,
// AppendPage returns an error.
func (c *Column) AppendPage(page streams.Page) error

// Close the column, writing it to the stream. After Close is called, there can
// be no further modifications to the column.
func (c *Column) Close() error

// Discard closes the column without writing it to the stream. After Discard is
// called, there can be no further modifications to the column.
func (c *Column) Discard() error
