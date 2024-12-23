package obj

import (
	"math"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
)

// StreamsEncoder encodes an individual streams section in a data object.
type StreamsEncoder struct {
	parent *Encoder
	w      *countingWriter

	closed bool // closed specifies whether the StreamsEncoder is closed.

	columns   []*streamsmd.ColumnDesc // closed columns.
	curColumn *streamsmd.ColumnDesc   // curColumn is the currently open column.
}

// OpenColumn opens a new column in the streams section. OpenColumn fails if
// there is another open column, or if the StreamsEncoder is closed.
func (enc *StreamsEncoder) OpenColumn(columnType streamsmd.ColumnType, info *dataset.ColumnInfo) (*StreamsColumnEncoder, error) {
	if enc.curColumn != nil {
		return nil, ErrElementExist
	} else if enc.closed {
		return nil, ErrClosed
	}

	// MetadataOffset and MetadataSize is not available until the column provides
	// it.
	//
	// We temporarily set these fields to the maximum values so they are
	// accounted for in the MetadataSize estimate.
	enc.curColumn = &streamsmd.ColumnDesc{
		Type: columnType,
		Info: &datasetmd.ColumnInfo{
			Name:             info.Name,
			ValueType:        info.Type,
			RowsCount:        uint32(info.RowsCount),
			Compression:      info.Compression,
			UncompressedSize: uint32(info.UncompressedSize),
			CompressedSize:   uint32(info.CompressedSize),
			Statistics:       info.Statistics,

			MetadataOffset: math.MaxUint32,
			MetadataSize:   math.MaxUint32,
		},
	}

	return &StreamsColumnEncoder{
		parent: enc,
		w:      enc.w,
	}, nil
}

// MetadataSize returns an estimate of the current metadataSize. MetadataSize
// includes an estimate for the currently open element.
func (enc *StreamsEncoder) MetadataSize() int {
	temp := &countingWriter{w: encoding.Discard}
	_ = enc.writeMetadata(temp)
	return int(temp.total)
}

func (enc *StreamsEncoder) writeMetadata(w encoding.Writer) error {
	md, err := proto.Marshal(&streamsmd.Metadata{Columns: enc.allColumns()})
	if err != nil {
		return EncodingError{err}
	}

	if err := encoding.WriteUvarint(w, streamsFormatVersion); err != nil {
		return EncodingError{err}
	} else if err := encoding.WriteUvarint(w, uint64(len(md))); err != nil {
		return EncodingError{err}
	} else if _, err := w.Write(md); err != nil {
		return err
	}

	return nil
}

func (enc *StreamsEncoder) allColumns() []*streamsmd.ColumnDesc {
	columns := enc.columns[:len(enc.columns):cap(enc.columns)]
	if enc.curColumn != nil {
		columns = append(columns, enc.curColumn)
	}
	return columns
}

// Close closes the section, flushing all data to the data object. After Close
// is called, the streams section can no longer be modified.
//
// Close fails if there is currently an open element.
func (enc *StreamsEncoder) Close() error {
	if enc.closed {
		return ErrClosed
	} else if enc.curColumn != nil {
		return ErrElementExist
	}

	if len(enc.columns) == 0 {
		// No data was written; discard.
		enc.closed = true
		return enc.parent.onChildDiscard()
	}

	metadataStart := enc.w.total
	if err := enc.writeMetadata(enc.w); err != nil {
		return err
	}
	metadataEnd := enc.w.total

	enc.columns = nil
	enc.closed = true
	return enc.parent.onChildClose(metadataStart, metadataEnd-metadataStart)
}

// onChildClose is called from a child element when it closes. onChildClose
// must only be called from the open child element.
//
// Children are responsible for tracking the offset and size of their metadata.
func (enc *StreamsEncoder) onChildClose(metadataOffset, metadataSize int64) error {
	if enc.curColumn == nil {
		return errElementNoExist
	}

	enc.curColumn.Info.MetadataOffset = uint32(metadataOffset)
	enc.curColumn.Info.MetadataSize = uint32(metadataSize)

	enc.columns = append(enc.columns, enc.curColumn)
	enc.curColumn = nil
	return nil
}

// onChildDiscard is called from a child element when it is discarded. Children
// encoders must take care to only call onChildDiscard if no data was written
// to enc.w.
func (enc *StreamsEncoder) onChildDiscard() error {
	if enc.curColumn == nil {
		return errElementNoExist
	}
	enc.curColumn = nil
	return nil
}

// StreamsColumnEncoder encodes an individual column in the streams section.
type StreamsColumnEncoder struct {
	parent *StreamsEncoder
	w      *countingWriter

	closed bool // closed specifies whether the StreamsEncoder is closed.

	pages []*streamsmd.PageDesc // appended pages.
}

// AppendPage appends a new page to the column. AppendPage fails if the column
// has been closed.
func (enc *StreamsColumnEncoder) AppendPage(page *page.Page) error {
	if enc.closed {
		return ErrClosed
	}

	// It's possible that the caller provided an invalid value for
	// UncompressedSize and CompressedSize, but those values aren't used in
	// encoding or decoding so we don't check it here.
	enc.pages = append(enc.pages, &streamsmd.PageDesc{
		Info: &datasetmd.PageInfo{
			UncompressedSize: uint32(page.UncompressedSize),
			CompressedSize:   uint32(page.CompressedSize),
			Crc32:            page.CRC32,
			RowsCount:        uint32(page.RowCount),
			Compression:      page.Compression,
			Encoding:         page.Encoding,
			Statistics:       page.Stats,

			DataOffset: uint32(enc.w.total),
			DataSize:   uint32(len(page.Data)),
		},
	})

	_, err := enc.w.Write(page.Data)
	return err
}

// MetadataSize returns an estimate of the current metadataSize.
func (enc *StreamsColumnEncoder) MetadataSize() int {
	temp := &countingWriter{w: encoding.Discard}
	_ = enc.writeMetadata(temp)
	return int(temp.total)
}

func (enc *StreamsColumnEncoder) writeMetadata(w encoding.Writer) error {
	md, err := proto.Marshal(&streamsmd.ColumnMetadata{Pages: enc.pages})
	if err != nil {
		return EncodingError{err}
	}

	if err := encoding.WriteUvarint(w, uint64(len(md))); err != nil {
		return EncodingError{err}
	} else if _, err := w.Write(md); err != nil {
		return err
	}

	return nil
}

// Close closes the column, flushing all data to the data object. After Close
// is called, the column can no longer be modified.
func (enc *StreamsColumnEncoder) Close() error {
	if enc.closed {
		return ErrClosed
	}

	if len(enc.pages) == 0 {
		// No data was written; discard.
		enc.closed = true
		return enc.parent.onChildDiscard()
	}

	metadataStart := enc.w.total
	if err := enc.writeMetadata(enc.w); err != nil {
		return err
	}
	metadataEnd := enc.w.total

	enc.pages = nil
	enc.closed = true
	return enc.parent.onChildClose(metadataStart, metadataEnd-metadataStart)
}
