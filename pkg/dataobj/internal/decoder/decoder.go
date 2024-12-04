// Package decoder provides an API for decoding data objects.
package decoder

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"iter"

	"github.com/gogo/protobuf/proto"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

// TODO(rfratto): At the moment this is being designed just for testing the
// encoder. In the future, we'll want to introduce the ability for batching and
// likely change the interface given to Open to pass context.Context instances
// in.

// Object is an opened data object.
type Object struct {
	r io.ReadSeeker
}

// Open a data object from the provided reader. Open returns an error if the
// data object is malformed.
func Open(r io.ReadSeeker) *Object {
	return &Object{r: r}
}

// Validate checks the magic of the data object.
func (obj *Object) Validate() error {
	_, err := obj.r.Seek(-4, io.SeekEnd)
	if err != nil {
		return err
	}

	var magic [4]byte
	_, err = obj.r.Read(magic[:])
	if err != nil {
		return fmt.Errorf("could not read magic: %w", err)
	}
	if string(magic[:]) != "THOR" {
		return fmt.Errorf("invalid magic: %s", magic)
	}
	return nil
}

// Sections retrieves the set of sections in the object.
func (obj *Object) Sections() ([]filemd.Section, error) {
	var metadataSize uint32
	if _, err := obj.r.Seek(-8, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek to file metadata size: %w", err)
	} else if err := binary.Read(obj.r, binary.LittleEndian, &metadataSize); err != nil {
		return nil, fmt.Errorf("read file metadata size: %w", err)
	}

	// TODO(rfratto): do we really need this byte slice? Can't we read it
	// directly from obj.r?
	fileMetadata := make([]byte, metadataSize)
	if _, err := obj.r.Seek(-int64(metadataSize)-8, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek to file metadata: %w", err)
	} else if readSz, err := obj.r.Read(fileMetadata); err != nil {
		return nil, fmt.Errorf("read file metadata: %w", err)
	} else if uint32(readSz) != metadataSize {
		return nil, fmt.Errorf("read file metadata: short read")
	}

	fileMetadataReader := bytes.NewReader(fileMetadata)

	fileFormatVersion, err := binary.ReadUvarint(fileMetadataReader)
	if err != nil {
		return nil, fmt.Errorf("read file format version: %w", err)
	} else if fileFormatVersion != 1 {
		return nil, fmt.Errorf("unsupported file format version: %d", fileFormatVersion)
	}

	var sections []filemd.Section
	sectionCount, err := binary.ReadUvarint(fileMetadataReader)
	if err != nil {
		return nil, fmt.Errorf("read section count: %w", err)
	}
	for range sectionCount {
		sectionSize, err := binary.ReadUvarint(fileMetadataReader)
		if err != nil {
			return nil, fmt.Errorf("read section size: %w", err)
		}

		sectionBytes := make([]byte, sectionSize)
		if _, err := fileMetadataReader.Read(sectionBytes); err != nil {
			return nil, fmt.Errorf("read section: %w", err)
		}

		var section filemd.Section
		if proto.Unmarshal(sectionBytes, &section) != nil {
			return nil, fmt.Errorf("unmarshal section: %w", err)
		}
		sections = append(sections, section)
	}

	return sections, nil
}

// Streams retrieves the set of streams from the provided streams section.
func (obj *Object) Streams(sec filemd.Section) ([]streamsmd.Stream, error) {
	if sec.Type != filemd.SECTION_TYPE_STREAMS {
		return nil, fmt.Errorf("section is type %s, not streams", sec.Type)
	}

	if _, err := obj.r.Seek(int64(sec.MetadataOffset), io.SeekStart); err != nil {
		return nil, err
	}

	// TODO(rfratto): do we really need this byte slice? Can't we read it
	// directly from obj.r?
	metadataBytes := make([]byte, sec.MetadataSize)
	if sz, err := obj.r.Read(metadataBytes); err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	} else if uint32(sz) != sec.MetadataSize {
		return nil, fmt.Errorf("read metadata: short read")
	}

	metadataReader := bytes.NewReader(metadataBytes)
	formatVersion, err := binary.ReadUvarint(metadataReader)
	if err != nil {
		return nil, fmt.Errorf("read format version: %w", err)
	} else if formatVersion != 1 {
		return nil, fmt.Errorf("unsupported format version: %d", formatVersion)
	}

	var streams []streamsmd.Stream
	streamCount, err := binary.ReadUvarint(metadataReader)
	if err != nil {
		return nil, fmt.Errorf("read stream count: %w", err)
	}
	for range streamCount {
		streamSize, err := binary.ReadUvarint(metadataReader)
		if err != nil {
			return nil, fmt.Errorf("read stream size: %w", err)
		}

		streamBytes := make([]byte, streamSize)
		if _, err := metadataReader.Read(streamBytes); err != nil {
			return nil, fmt.Errorf("read stream: %w", err)
		}

		var stream streamsmd.Stream
		if proto.Unmarshal(streamBytes, &stream) != nil {
			return nil, fmt.Errorf("unmarshal stream: %w", err)
		}
		streams = append(streams, stream)
	}

	return streams, nil
}

// Columns retrieves the set of columns from the provided stream.
func (obj *Object) Columns(stream streamsmd.Stream) ([]streamsmd.ColumnsMetadata, error) {
	if _, err := obj.r.Seek(int64(stream.ColumnsMetadataOffset), io.SeekStart); err != nil {
		return nil, err
	}

	// TODO(rfratto): do we really need this byte slice? Can't we read it
	// directly from obj.r?
	columnsBytes := make([]byte, stream.ColumnsMetadataSize)
	if sz, err := obj.r.Read(columnsBytes); err != nil {
		return nil, fmt.Errorf("read columns metadata: %w", err)
	} else if uint32(sz) != stream.ColumnsMetadataSize {
		return nil, fmt.Errorf("read columns metadata: short read")
	}

	columnsReader := bytes.NewReader(columnsBytes)

	var columns []streamsmd.ColumnsMetadata
	columnCount, err := binary.ReadUvarint(columnsReader)
	if err != nil {
		return nil, fmt.Errorf("read column count: %w", err)
	}
	for range columnCount {
		columnSize, err := binary.ReadUvarint(columnsReader)
		if err != nil {
			return nil, fmt.Errorf("read column size: %w", err)
		}

		columnBytes := make([]byte, columnSize)
		if _, err := columnsReader.Read(columnBytes); err != nil {
			return nil, fmt.Errorf("read column: %w", err)
		}

		var column streamsmd.ColumnsMetadata
		if proto.Unmarshal(columnBytes, &column) != nil {
			return nil, fmt.Errorf("unmarshal column: %w", err)
		}

		columns = append(columns, column)
	}

	return columns, nil
}

// Pages returns an iterator over pages from the provided column.
func (obj *Object) Pages(col streamsmd.ColumnsMetadata) iter.Seq2[streams.Page, error] {
	return func(yield func(streams.Page, error) bool) {
		if _, err := obj.r.Seek(int64(col.Page0HeaderOffset), io.SeekStart); err != nil {
			yield(streams.Page{}, err)
			return
		}

		// TODO(rfratto): We currently can't decode pages; we don't know when the
		// set of pages ends. The fastest way to fix this is to add some kind of
		// ending marker (such as header size 0).
		//
		// However, since we're planning on moving the set of pages to a metadata
		// section, it probably makes sense to do that instead. That would give us
		// the exact number of pages and their sizes.
	}
}
