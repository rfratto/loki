// Package dataset contains utilities for constructing page-oriented columnar
// data.
package dataset

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/klauspost/compress/zstd"
)

var (
	checksumTable = crc32.MakeTable(crc32.Castagnoli)
)

// PageData holds compressed and encoded page data.
type PageData []byte

// A Page is a single unit of data within a column. It is encoded with a
// specific format and is optionally compressed.
type Page struct {
	UncompressedSize int    // UncompressedSize is the size of the page before compression.
	CompressedSize   int    // CompressedSize is the size of the page after compression.
	CRC32            uint32 // CRC32 is the CRC32 checksum of the compressed page.
	RowCount         int    // RowCount is the number of rows in the page.

	Value       datasetmd.ValueType       // Value type of the page.
	Compression datasetmd.CompressionType // Compression used for the compressed page.
	Encoding    datasetmd.EncodingType    // Encoding used for the decompressed page.
	Stats       *datasetmd.Statistics     // Optional statistics for the page.

	Data PageData // Data for the page.
}

// RawPage converts metadata and raw data into a Page. The arguments are not
// validated to be correct.
func RawPage(column *datasetmd.ColumnInfo, page *datasetmd.PageInfo, data PageData) Page {
	return Page{
		UncompressedSize: int(page.UncompressedSize),
		CompressedSize:   int(page.CompressedSize),
		CRC32:            uint32(page.Crc32),
		RowCount:         int(page.RowsCount),

		Value:       column.ValueType,
		Compression: page.Compression,
		Encoding:    page.Encoding,
		Stats:       page.Statistics,

		Data: data,
	}
}

// Reader returns a reader for decompressed page data. Reader returns an error
// if the CRC32 fails to validate.
func (p *Page) Reader() (presence io.ReadCloser, values io.ReadCloser, err error) {
	if actual := crc32.Checksum(p.Data, checksumTable); p.CRC32 != actual {
		return nil, nil, fmt.Errorf("invalid crc32 checksum %x, expected %x", actual, p.CRC32)
	}

	// The first thing written to the page is the presence bitmap size.
	bitmapSize, n := binary.Uvarint(p.Data)
	if n <= 0 {
		return nil, nil, fmt.Errorf("reading presence bitmap size: %w", err)
	}

	bitmapReader := bytes.NewReader(p.Data[n : n+int(bitmapSize)])
	dataReader := bytes.NewReader(p.Data[n+int(bitmapSize):])

	// The rest of the page is the values.
	switch p.Compression {
	case datasetmd.COMPRESSION_TYPE_UNSPECIFIED, datasetmd.COMPRESSION_TYPE_NONE:
		return io.NopCloser(bitmapReader), io.NopCloser(dataReader), nil

	case datasetmd.COMPRESSION_TYPE_SNAPPY:
		sr := snappy.NewReader(dataReader)
		return io.NopCloser(bitmapReader), io.NopCloser(sr), nil

	case datasetmd.COMPRESSION_TYPE_ZSTD:
		zr, err := zstd.NewReader(dataReader)
		if err != nil {
			return nil, nil, fmt.Errorf("creating zstd reader: %w", err)
		}
		return io.NopCloser(bitmapReader), newZstdReader(zr), nil
	}

	panic(fmt.Sprintf("Unexpected compression type %s", p.Compression.String()))
}
