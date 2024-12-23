// Package page defines interfaces shared by other packages that encode values
// into dataset pages.
//
// See [github.com/grafana/loki/v3/pkg/dataobj/internal/dataset] for the code
// to construct pages.
package page

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

// Data holds compressed and encoded page data.
type Data []byte

// A Page is a collection of [Value] entries of a common type. Use [Builder] to
// construct pages.
type Page struct {
	UncompressedSize int    // UncompressedSize is the size of the page before compression.
	CompressedSize   int    // CompressedSize is the size of the page after compression.
	CRC32            uint32 // CRC32 is the CRC32 checksum of the compressed page.
	RowCount         int    // RowCount is the number of rows in the page.

	Value       datasetmd.ValueType       // Value type of valueData in Data.
	Compression datasetmd.CompressionType // Compression used for valueData in Data.
	Encoding    datasetmd.EncodingType    // Encoding used for valueData in Data.
	Stats       *datasetmd.Statistics     // Optional statistics for the page.

	// Data for the page. Page data is formatted as:
	//
	//  [uvarint(bitmapSize)][presenceBitmap][valueData]
	//
	// The presenceBitmap is always written uncompressed, and formatted as a
	// sequence of bitmap-encoded uint64 values, where values describe which rows
	// are present (1) or nil (0).
	//
	// valueData is the compressed annd encoded set of non-NULL values, whose
	// type, compression, and encoding are specified by the Value, Compression,
	// and Encoding fields.
	Data Data
}

// Raw creates a Page from the given info and data. The arguments are not
// validated to be correct.
func Raw(info *Info, data Data) *Page {
	return &Page{
		UncompressedSize: int(info.UncompressedSize),
		CompressedSize:   int(info.CompressedSize),
		CRC32:            uint32(info.CRC32),
		RowCount:         int(info.RowCount),

		Value:       info.Value,
		Compression: info.Compression,
		Encoding:    info.Encoding,
		Stats:       info.Stats,

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

// Info describes the content of a [Page].
type Info struct {
	UncompressedSize int    // UncompressedSize is the size of the page before compression.
	CompressedSize   int    // CompressedSize is the size of the page after compression.
	CRC32            uint32 // CRC32 is the CRC32 checksum of the compressed page.
	RowCount         int    // RowCount is the number of rows in the page.

	Value       datasetmd.ValueType       // Value type of valueData in Data.
	Compression datasetmd.CompressionType // Compression used for valueData in Data.
	Encoding    datasetmd.EncodingType    // Encoding used for valueData in Data.
	Stats       *datasetmd.Statistics     // Optional statistics for the page.
}

// Info returns the [Info] for the Page.
func (p *Page) Info() *Info {
	return &Info{
		UncompressedSize: p.UncompressedSize,
		CompressedSize:   p.CompressedSize,
		CRC32:            p.CRC32,
		RowCount:         p.RowCount,

		Value:       p.Value,
		Compression: p.Compression,
		Encoding:    p.Encoding,
		Stats:       p.Stats,
	}
}

// zstdReader implements [io.ReadCloser] for a [zstd.Decoder].
type zstdReader struct{ *zstd.Decoder }

// newZstdReader returns a new [io.ReadCloser] for a [zstd.Decoder].
func newZstdReader(dec *zstd.Decoder) io.ReadCloser {
	return &zstdReader{Decoder: dec}
}

// Close implements [io.Closer].
func (r *zstdReader) Close() error {
	r.Decoder.Close()
	return nil
}
