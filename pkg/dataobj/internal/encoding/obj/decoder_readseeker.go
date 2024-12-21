package obj

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"iter"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
)

type readSeekerDecoder struct {
	r io.ReadSeeker
}

// ReadSeekerDecoder decodes a data object from the provided [io.ReadSeeker].
func ReadSeekerDecoder(r io.ReadSeeker) Decoder {
	return &readSeekerDecoder{r: r}
}

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

	md, err := decodeFileMetadata(r)
	if err != nil {
		return nil, err
	}
	return md.Sections, nil
}

func (dec *readSeekerDecoder) StreamsDecoder() StreamsDecoder {
	return &readSeekerStreamsDecoder{r: dec.r}
}

type readSeekerStreamsDecoder struct {
	r io.ReadSeeker
}

func (dec *readSeekerStreamsDecoder) Columns(_ context.Context, section *filemd.SectionInfo) ([]*streamsmd.ColumnDesc, error) {
	if section.Type != filemd.SECTION_TYPE_STREAMS {
		return nil, fmt.Errorf("section is type %s, not streams", section.Type)
	}

	if _, err := dec.r.Seek(int64(section.MetadataOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to stream metadata: %w", err)
	}
	r := bufio.NewReader(io.LimitReader(dec.r, int64(section.MetadataSize)))

	md, err := decodeStreamsMetadata(r)
	if err != nil {
		return nil, err
	}
	return md.Columns, nil
}

func (dec *readSeekerStreamsDecoder) Pages(_ context.Context, column *streamsmd.ColumnDesc) ([]*ColumnPageDesc, error) {
	if _, err := dec.r.Seek(int64(column.Info.MetadataOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to stream metadata: %w", err)
	}
	r := bufio.NewReader(io.LimitReader(dec.r, int64(column.Info.MetadataSize)))

	md, err := decodeStreamsColumnMetadata(r)
	if err != nil {
		return nil, err
	}

	res := make([]*ColumnPageDesc, 0, len(md.Pages))
	for _, page := range md.Pages {
		res = append(res, &ColumnPageDesc{
			Column: column,
			Page:   page,
		})
	}
	return res, nil
}

func (dec *readSeekerStreamsDecoder) ReadPages(_ context.Context, pages []*ColumnPageDesc) iter.Seq2[*page.Page, error] {
	readPage := func(column *streamsmd.ColumnDesc, pageDesc *streamsmd.PageDesc) (*page.Page, error) {
		if _, err := dec.r.Seek(int64(pageDesc.Info.DataOffset), io.SeekStart); err != nil {
			return nil, err
		}
		data := make([]byte, pageDesc.Info.DataSize)
		if _, err := io.ReadFull(dec.r, data); err != nil {
			return nil, fmt.Errorf("read page data: %w", err)
		}

		info := &page.Info{
			UncompressedSize: int(pageDesc.Info.UncompressedSize),
			CompressedSize:   int(pageDesc.Info.CompressedSize),
			CRC32:            pageDesc.Info.Crc32,
			RowCount:         int(pageDesc.Info.RowsCount),

			Value:       column.Info.ValueType,
			Compression: pageDesc.Info.Compression,
			Encoding:    pageDesc.Info.Encoding,
			Stats:       pageDesc.Info.Statistics,
		}
		return page.Raw(info, data), nil
	}

	return func(yield func(*page.Page, error) bool) {
		for _, pageDesc := range pages {
			page, err := readPage(pageDesc.Column, pageDesc.Page)
			if !yield(page, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}
}
