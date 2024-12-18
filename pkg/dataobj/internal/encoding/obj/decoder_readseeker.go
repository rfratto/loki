package obj

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"iter"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
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

	md, err := scanFileMetadata(r)
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

	md, err := scanStreamsMetadata(r)
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

	md, err := scanStreamsColumnMetadata(r)
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

func (dec *readSeekerStreamsDecoder) ReadPages(_ context.Context, pages []*ColumnPageDesc) iter.Seq2[dataset.Page, error] {
	readPage := func(column *streamsmd.ColumnDesc, page *streamsmd.PageDesc) (dataset.Page, error) {
		if _, err := dec.r.Seek(int64(page.Info.DataOffset), io.SeekStart); err != nil {
			return dataset.Page{}, err
		}

		data := make([]byte, page.Info.DataSize)
		if _, err := io.ReadFull(dec.r, data); err != nil {
			return dataset.Page{}, fmt.Errorf("read page data: %w", err)
		}

		return dataset.Page{
			UncompressedSize: int(page.Info.UncompressedSize),
			CompressedSize:   int(page.Info.CompressedSize),
			CRC32:            page.Info.Crc32,
			RowCount:         int(page.Info.RowsCount),

			Value:       column.Info.ValueType,
			Compression: page.Info.Compression,
			Encoding:    page.Info.Encoding,
			Stats:       page.Info.Statistics,

			Data: data,
		}, nil
	}

	return func(yield func(dataset.Page, error) bool) {
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
