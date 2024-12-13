package decoder

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"iter"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/logstreams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/logstreamsmd"
)

type readSeekerDecoder struct {
	r io.ReadSeeker
}

// ReadSeekerDecoder a data object from the provided [io.ReadSeeker].
func ReadSeekerDecoder(r io.ReadSeeker) Decoder {
	return &readSeekerDecoder{r: r}
}

// Sections retrieves the set of sections in the object.
func (dec *readSeekerDecoder) Sections(_ context.Context) ([]*filemd.SectionInfo, error) {
	var metadataSize uint32
	if _, err := dec.r.Seek(-8, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek to file metadata size: %w", err)
	} else if err := binary.Read(dec.r, binary.LittleEndian, &metadataSize); err != nil {
		return nil, fmt.Errorf("read file metadata size: %w", err)
	}

	if _, err := dec.r.Seek(-int64(metadataSize)-8, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek to file metadata: %w", err)
	}
	r := bufio.NewReader(io.LimitReader(dec.r, int64(metadataSize)))

	md, err := scanFileMetadata(r)
	if err != nil {
		return nil, err
	}
	return md.Sections, nil
}

// StreamsDecoder returns a StreamsDecoder.
func (dec *readSeekerDecoder) StreamsDecoder() StreamsDecoder {
	return &readSeekerStreamsDecoder{r: dec.r}
}

type readSeekerStreamsDecoder struct {
	r io.ReadSeeker
}

// Streams retrieves the set of streams from the provided streams section.
func (dec *readSeekerStreamsDecoder) Streams(_ context.Context, sec *filemd.SectionInfo) ([]*logstreamsmd.StreamInfo, error) {
	if sec.Type != filemd.SECTION_TYPE_LOG_STREAMS {
		return nil, fmt.Errorf("section is type %s, not streams", sec.Type)
	}

	if _, err := dec.r.Seek(int64(sec.MetadataOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to streams section metadata: %w", err)
	}
	r := bufio.NewReader(io.LimitReader(dec.r, int64(sec.MetadataSize)))

	md, err := scanStreamsMetadata(r)
	if err != nil {
		return nil, err
	}
	return md.Streams, nil
}

// Columns retrieves the set of columns from the provided stream.
func (dec *readSeekerStreamsDecoder) Columns(_ context.Context, stream *logstreamsmd.StreamInfo) ([]*logstreamsmd.ColumnInfo, error) {
	if _, err := dec.r.Seek(int64(stream.MetadataOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to stream metadata: %w", err)
	}
	r := bufio.NewReader(io.LimitReader(dec.r, int64(stream.MetadataSize)))

	md, err := scanStreamMetadata(r)
	if err != nil {
		return nil, err
	}
	return md.Columns, nil
}

// Pages returns an iterator over pages from the provided column.
func (dec *readSeekerStreamsDecoder) Pages(_ context.Context, col *logstreamsmd.ColumnInfo) ([]*logstreamsmd.PageInfo, error) {
	if _, err := dec.r.Seek(int64(col.MetadataOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to column metadata: %w", err)
	}
	r := bufio.NewReader(io.LimitReader(dec.r, int64(col.MetadataSize)))

	md, err := scanColumnMetadata(r)
	if err != nil {
		return nil, err
	}
	return md.Pages, nil
}

func (dec *readSeekerStreamsDecoder) ReadPages(_ context.Context, pages []*logstreamsmd.PageInfo) iter.Seq2[logstreams.Page, error] {
	readPage := func(page *logstreamsmd.PageInfo) (logstreams.Page, error) {
		if _, err := dec.r.Seek(int64(page.DataOffset), io.SeekStart); err != nil {
			return logstreams.Page{}, err
		}

		data := make([]byte, page.DataSize)
		if sz, err := dec.r.Read(data); err != nil {
			return logstreams.Page{}, fmt.Errorf("read page data: %w", err)
		} else if uint32(sz) != page.DataSize {
			return logstreams.Page{}, fmt.Errorf("read page data: short read")
		}

		return logstreams.Page{
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

	return func(yield func(logstreams.Page, error) bool) {
		for _, pageHeader := range pages {
			page, err := readPage(pageHeader)
			if !yield(page, err) {
				return
			}

			if err != nil {
				return
			}
		}
	}
}
