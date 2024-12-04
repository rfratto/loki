package encoder

import (
	"encoding/binary"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

const (
	streamsFormatVersion uint8 = 1
)

// Streams is an instance of the streams section within a data object.
type Streams struct {
	parent *Object

	metadataSize int // Max metadata size.
	offset       int // Byte offset in the file where the Streams section begins.

	closed bool // closed specifies whether the Streams section has been closed.
	inuse  bool // inuse specifies whether a Stream is currently open.

	data    []byte
	streams []streamsmd.Stream
}

// OpenStream opens a new Stream. OpenStream fails if there is currently
// another open stream or if the Streams section has been closed.
//
// If opening a new stream would exceed the maximum metadata size for Streams,
// OpenStream returns an error.
func (s *Streams) OpenStream(id streamsmd.StreamIdentifier) (*Stream, error) {
	if s.closed {
		return nil, ErrClosed
	} else if s.inuse {
		return nil, ErrElementExist
	}

	// The streamsmd.Stream entry starts incomplete; we can't know the size of
	// the stream until all columns have been added. While the caller could pass
	// a size from all in-memory data, that data may not necessarily by written
	// to the encoder.
	//
	// We allow the caller to pass information about its size upwards.
	//
	// TODO(rfratto): is calling s.buildMetadata for each stream too expensive
	// here? The size increases linearly with the number of streams. We may want
	// to replace it with a constant-size estimate and use the max size as a soft
	// limit.
	s.streams = append(s.streams, streamsmd.Stream{Identifier: &id})
	if md, err := s.buildMetadata(); err != nil {
		return nil, err
	} else if len(md) > s.metadataSize {
		s.streams = s.streams[:len(s.streams)-1]
		return nil, ErrMetadataSize
	}

	s.inuse = true
	return &Stream{
		parent: s,

		metadataSize: s.metadataSize,
		offset:       s.offset + len(s.data),
	}, nil
}

// buildMetadata builds the set of []streamsmd.Stream to be written as the
// metadata section. buildMetadata does not directly check for size limits to
// avoid unexpected errors from mismatches between the estimated metadata size
// and the effective size.
func (s *Streams) buildMetadata() ([]byte, error) {
	var buf []byte

	buf = binary.AppendUvarint(buf, uint64(streamsFormatVersion))
	buf = binary.AppendUvarint(buf, uint64(len(s.streams)))
	for _, stream := range s.streams {
		streamBytes, err := proto.Marshal(&stream)
		if err != nil {
			return nil, EncodingError{err}
		}

		buf = binary.AppendUvarint(buf, uint64(len(streamBytes)))
		buf = append(buf, streamBytes...)
	}

	return buf, nil
}

// Close the streams section, writing it to the data object. After Close is
// called, the streams section can no longer be modified.
func (s *Streams) Close() error {
	if s.closed {
		return ErrClosed
	} else if s.inuse {
		return ErrElementExist
	}
	s.closed = true

	metadata, err := s.buildMetadata()
	if err != nil {
		return err
	}

	err = s.parent.append(s.data, metadata)
	s.data = nil
	s.streams = nil
	return err
}

// Discard closes the streams section without writing it to the object. After
// Discard is called, the streams section can no longer be modified.
func (s *Streams) Discard() error {
	if s.closed {
		return ErrClosed
	} else if s.inuse {
		return ErrElementExist
	}
	s.closed = true

	err := s.parent.append(nil, nil) // Notify parent of discard.
	s.data = nil
	return err
}

// append adds data and metadata to s. append must only be called from child
// elements on Close and Discard. Discard calls must pass empty byte slices to
// denote no data or metadata.
func (s *Streams) append(data, metadata []byte) error {
	if s.closed {
		return ErrClosed
	} else if !s.inuse {
		return errElementNoExist
	}
	s.inuse = false

	if len(data) == 0 && len(metadata) == 0 {
		// Stream was discarded; pop it.
		s.streams = s.streams[:len(s.streams)-1]
		return nil
	}

	// Update the stream entry with the offset and size of the metadata.
	s.streams[len(s.streams)-1].MetadataOffset = uint32(s.offset + len(s.data) + len(data))
	s.streams[len(s.streams)-1].MetadataSize = uint32(len(metadata))

	s.data = append(s.data, data...)
	s.data = append(s.data, metadata...)
	return nil
}

// Stream is an instance of a stream within a data object.
type Stream struct {
	parent *Streams

	metadataSize int // Max metadata size.
	offset       int // Byte offset in the file where the Stream begins.

	closed bool // closed specifies whether the Stream has been closed.
	inuse  bool // inuse specifies whether a Column is currently open.

	data    []byte
	columns []streamsmd.Column
}

// OpenColumn opens a new column in the stream. OpenColumn fails if there is
// currently another open column or if the Stream has been closed.
//
// If opening a new column would exceed the maximum metadata size for Stream,
// OpenColumn returns an error.
func (s *Stream) OpenColumn(column streams.ColumnInfo) (*Column, error) {
	if s.closed {
		return nil, ErrClosed
	} else if s.inuse {
		return nil, ErrElementExist
	}

	columnOffset := s.offset + len(s.data)

	// Add the new column and check to see if the metadata has exceeded the size
	// limit.
	//
	// TODO(rfratto): is calling s.buildMetadata for each column too expensive
	// here? The size increases linearly with the number of columns. We may want
	// to replace it with a constant-size estimate and use the max size as a soft
	// limit.
	//
	// TODO(rfratto): we shouldn't allow the caller to provide the size; they
	// might not append every page held in memory.
	s.columns = append(s.columns, streamsmd.Column{
		Name:             column.Name,
		Type:             column.Type,
		RowsCount:        uint32(column.RowsCount),
		Compression:      column.CompressionType,
		UncompressedSize: uint32(column.UncompressedSize),
		CompressedSize:   uint32(column.CompressedSize),
		Statistics:       column.Statistics,
	})
	if md, err := s.buildMetadata(); err != nil {
		return nil, err
	} else if len(md) > s.metadataSize {
		s.columns = s.columns[:len(s.columns)-1]
		return nil, ErrMetadataSize
	}

	s.inuse = true
	return &Column{
		parent: s,

		metadataSize: s.metadataSize,
		offset:       columnOffset,
	}, nil
}

// buildMetadata builds the set of ColumnsMetadata for the stream.
// buildMetadata does not check for size limits to avoid unexpected errors from
// mismatches between the estimated metadata size and the effective size.
func (s *Stream) buildMetadata() ([]byte, error) {
	var buf []byte

	buf = binary.AppendUvarint(buf, uint64(len(s.columns)))
	for _, col := range s.columns {
		columnBytes, err := proto.Marshal(&col)
		if err != nil {
			return nil, EncodingError{err}
		}

		buf = binary.AppendUvarint(buf, uint64(len(columnBytes)))
		buf = append(buf, columnBytes...)
	}

	return buf, nil
}

// Close the stream, writing it to the data object. After Close is called, there
// can be no further modifications to the stream.
func (s *Stream) Close() error {
	if s.closed {
		return ErrClosed
	} else if s.inuse {
		return ErrElementExist
	}
	s.closed = true

	metadata, err := s.buildMetadata()
	if err != nil {
		return err
	}

	s.updateParentMetadata()

	err = s.parent.append(s.data, metadata)
	s.data = nil
	s.columns = nil
	return err
}

// updateParentMetadata propagates information about the stream upwards.
func (s *Stream) updateParentMetadata() {
	// TODO(rfratto): This feels gross to do; is there a cleaner way (which is
	// still short and simple) for children elements to pass information to their
	// parents?

	ent := s.parent.streams[len(s.parent.streams)-1]
	for _, col := range s.columns {
		ent.CompressedSize += col.CompressedSize
		ent.UncompressedSize += col.UncompressedSize
	}
	s.parent.streams[len(s.parent.streams)-1] = ent
}

// Discard closes the stream without writing it to the object. After Discard
// is called, there can be no further modifications to the stream.
func (s *Stream) Discard() error {
	if s.closed {
		return ErrClosed
	} else if s.inuse {
		return ErrElementExist
	}
	s.closed = true

	err := s.parent.append(nil, nil) // Notify parent of discard.
	s.data = nil
	return err
}

// append adds data and metadata to s. append must only be called from
// child elements on Close and Discard. Discard calls must pass empty byte
// slices to denote no data or metadata.
func (s *Stream) append(data, metadata []byte) error {
	if s.closed {
		return ErrClosed
	} else if !s.inuse {
		return errElementNoExist
	}
	s.inuse = false

	if len(data) == 0 && len(metadata) == 0 {
		// Column was discarded; pop the metadata.
		s.columns = s.columns[:len(s.columns)-1]
		return nil
	}

	// Update the column entry with the offset and size of the metadata.
	s.columns[len(s.columns)-1].MetadataOffset = uint32(s.offset + len(s.data) + len(data))
	s.columns[len(s.columns)-1].MetadataSize = uint32(len(metadata))

	s.data = append(s.data, data...)
	s.data = append(s.data, metadata...)
	return nil
}

// Column is an instance of a column within a stream.
type Column struct {
	// metadataSize and offset aren't used in Column but are kept for consistency
	// with the other element types.

	parent *Stream

	metadataSize int // Max metadata size.
	offset       int // Byte offset in the file where the Column begins.

	closed bool // closed specifies whether the Column has been closed.

	data  []byte
	pages []streamsmd.Page
}

// AppendPage appends a new page to the column. AppendPage fails if the column
// has been closed.
//
// If appending a new page would exceed the maximum metadata size for Column,
// AppendPage returns an error.
func (c *Column) AppendPage(page streams.Page) error {
	if c.closed {
		return ErrClosed
	}

	// TODO(rfratto): is calling c.buildMetadata for each page too expensive
	// here? The size increases linearly with the number of streams. We may want
	// to replace it with a constant-size estimate and use the max size as a soft
	// limit.
	c.pages = append(c.pages, streamsmd.Page{
		UncompressedSize: uint32(page.UncompressedSize),
		CompressedSize:   uint32(page.CompressedSize),
		Crc32:            page.CRC32,
		RowsCount:        uint32(page.RowCount),
		Compression:      page.Compression,
		Encoding:         page.Encoding,
		Statistics:       page.Stats,

		DataOffset: uint32(c.offset + len(c.data)),
		DataSize:   uint32(len(page.Data)),
	})
	if md, err := c.buildMetadata(); err != nil {
		return err
	} else if len(md) > c.metadataSize {
		return ErrMetadataSize
	}

	c.data = append(c.data, page.Data...)
	return nil
}

// buildMetadata builds the set of []Page to be written as the metadata for the
// Column.
func (c *Column) buildMetadata() ([]byte, error) {
	var buf []byte

	buf = binary.AppendUvarint(buf, uint64(len(c.pages)))
	for _, page := range c.pages {
		pageBytes, err := proto.Marshal(&page)
		if err != nil {
			return nil, EncodingError{err}
		}

		buf = binary.AppendUvarint(buf, uint64(len(pageBytes)))
		buf = append(buf, pageBytes...)
	}

	return buf, nil
}

// Close the column, writing it to the stream. After Close is called, there can
// be no further modifications to the column.
func (c *Column) Close() error {
	if c.closed {
		return ErrClosed
	}
	c.closed = true

	metadata, err := c.buildMetadata()
	if err != nil {
		return err
	}

	err = c.parent.append(c.data, metadata)
	c.data = nil
	c.pages = nil
	return err
}

// Discard closes the column without writing it to the stream. After Discard is
// called, there can be no further modifications to the column.
func (c *Column) Discard() error {
	if c.closed {
		return ErrClosed
	}
	c.closed = true

	err := c.parent.append(nil, nil) // Notify parent of discard.
	c.data = nil
	return err
}
