package decoder

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

// ReadSeekerObject is an opened data object.
type ReadSeekerObject struct {
	r io.ReadSeeker
}

// OpenReadSeeker a data object from the provided reader. OpenReadSeeker returns an error if the
// data object is malformed.
func OpenReadSeeker(r io.ReadSeeker) *ReadSeekerObject {
	return &ReadSeekerObject{r: r}
}

// Validate checks the magic of the data object.
func (obj *ReadSeekerObject) Validate() error {
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
func (obj *ReadSeekerObject) Sections() ([]filemd.Section, error) {
	var metadataSize uint32
	if _, err := obj.r.Seek(-8, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek to file metadata size: %w", err)
	} else if err := binary.Read(obj.r, binary.LittleEndian, &metadataSize); err != nil {
		return nil, fmt.Errorf("read file metadata size: %w", err)
	}

	if _, err := obj.r.Seek(-int64(metadataSize)-8, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek to file metadata: %w", err)
	}
	r := bufio.NewReader(io.LimitReader(obj.r, int64(metadataSize)))
	return scanFileMetadata(r)
}

// Streams retrieves the set of streams from the provided streams section.
func (obj *ReadSeekerObject) Streams(sec filemd.Section) ([]streamsmd.Stream, error) {
	if sec.Type != filemd.SECTION_TYPE_STREAMS {
		return nil, fmt.Errorf("section is type %s, not streams", sec.Type)
	}

	if _, err := obj.r.Seek(int64(sec.MetadataOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to streams section metadata: %w", err)
	}
	r := bufio.NewReader(io.LimitReader(obj.r, int64(sec.MetadataSize)))
	return scanStreamsMetadata(r)
}

// Columns retrieves the set of columns from the provided stream.
func (obj *ReadSeekerObject) Columns(stream streamsmd.Stream) ([]streamsmd.Column, error) {
	if _, err := obj.r.Seek(int64(stream.MetadataOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to stream metadata: %w", err)
	}
	r := bufio.NewReader(io.LimitReader(obj.r, int64(stream.MetadataSize)))
	return scanStreamMetadata(r)
}

// Pages returns an iterator over pages from the provided column.
func (obj *ReadSeekerObject) Pages(col streamsmd.Column) ([]streamsmd.Page, error) {
	if _, err := obj.r.Seek(int64(col.MetadataOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to column metadata: %w", err)
	}
	r := bufio.NewReader(io.LimitReader(obj.r, int64(col.MetadataSize)))
	return scanColumnMetadata(r)
}

func (obj *ReadSeekerObject) Page(page streamsmd.Page) (streams.Page, error) {
	if _, err := obj.r.Seek(int64(page.DataOffset), io.SeekStart); err != nil {
		return streams.Page{}, err
	}

	data := make([]byte, page.DataSize)
	if sz, err := obj.r.Read(data); err != nil {
		return streams.Page{}, fmt.Errorf("read page data: %w", err)
	} else if uint32(sz) != page.DataSize {
		return streams.Page{}, fmt.Errorf("read page data: short read")
	}

	return streams.Page{
		UncompressedSize: int(page.UncompressedSize),
		CompressedSize:   int(page.CompressedSize),
		CRC32:            uint32(page.Crc32),
		RowCount:         int(page.RowsCount),

		Compression: page.Compression,
		Encoding:    page.Encoding,
		Stats:       page.Statistics,

		Data: data,
	}, nil
}
