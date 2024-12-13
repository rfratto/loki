// Package dataset contains utilities for constructing page-oriented columnar
// data.
package dataset

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/datasetmd"
	"github.com/klauspost/compress/zstd"
)

var (
	checksumTable = crc32.MakeTable(crc32.Castagnoli)
)

// A Page is a single unit of data within a column. It is encoded with a
// specific format and is optionally compressed.
type Page struct {
	UncompressedSize int    // UncompressedSize is the size of the page before compression.
	CompressedSize   int    // CompressedSize is the size of the page after compression.
	CRC32            uint32 // CRC32 is the CRC32 checksum of the compressed page.
	RowCount         int    // RowCount is the number of rows in the page.

	Compression datasetmd.CompressionType // Compression used for the compressed page.
	Encoding    datasetmd.EncodingType    // Encoding used for the decompressed page.
	Stats       *datasetmd.Statistics     // Optional statistics for the page.

	Data []byte // Data is the compressed and encoded page data.
}

// Reader returns a reader for decompressed page data. Reader returns an error
// if the CRC32 fails to validate.
func (p *Page) Reader() (io.ReadCloser, error) {
	if actual := crc32.Checksum(p.Data, checksumTable); p.CRC32 != actual {
		return nil, fmt.Errorf("invalid crc32 checksum %x, expected %x", actual, p.CRC32)
	}

	switch p.Compression {
	case datasetmd.COMPRESSION_TYPE_UNSPECIFIED, datasetmd.COMPRESSION_TYPE_NONE:
		return io.NopCloser(bytes.NewReader(p.Data)), nil

	case datasetmd.COMPRESSION_TYPE_SNAPPY:
		sr := snappy.NewReader(bytes.NewReader(p.Data))
		return io.NopCloser(sr), nil

	case datasetmd.COMPRESSION_TYPE_ZSTD:
		zr, err := zstd.NewReader(bytes.NewReader(p.Data))
		if err != nil {
			return nil, fmt.Errorf("creating zstd reader: %w", err)
		}
		return newZstdReader(zr), nil
	}

	panic(fmt.Sprintf("Unexpected compression type %s", p.Compression.String()))
}
