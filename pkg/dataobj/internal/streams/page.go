package streams

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

var (
	checksumTable = crc32.MakeTable(crc32.Castagnoli)
)

// A Page is a single unit of data within a column. It is encoded and
// optionally compressed.
type Page struct {
	UncompressedSize int    // UncompressedSize is the size of the page before compression.
	CompressedSize   int    // CompressedSize is the size of the page after compression.
	CRC32            uint32 // CRC32 is the CRC32 checksum of the compressed page.
	RowCount         int    // RowCount is the number of rows in the page.

	Compression streamsmd.CompressionType // Compression used for the compressed page.
	Encoding    streamsmd.EncodingType    // Encoding used for the decompressed page.
	Stats       *streamsmd.Statistics     // Optional statistics for the page.

	Data []byte // Data is the compressed and encoded page data.
}

// Reader returns a reader for decompressed page data. Reader returns an error
// if the CRC32 fails to validate.
func (p *Page) Reader() (io.ReadCloser, error) {
	if actual := crc32.Checksum(p.Data, checksumTable); p.CRC32 != actual {
		return nil, fmt.Errorf("invalid crc32 checksum %x, expected %x", actual, p.CRC32)
	}

	switch p.Compression {
	case streamsmd.COMPRESSION_UNSPECIFIED, streamsmd.COMPRESSION_NONE:
		return io.NopCloser(bytes.NewReader(p.Data)), nil

	case streamsmd.COMPRESSION_GZIP:
		gr, err := gzip.NewReader(bytes.NewReader(p.Data))
		if err != nil {
			return nil, fmt.Errorf("creating gzip reader: %w", err)
		}
		return gr, nil
	}

	panic(fmt.Sprintf("Unexpected compression type %s", p.Compression.String()))
}

// compressData compresses buf using the provided compression algorithm and
// returns the compressed data along with the crc32 checksum of the compressed
// data.
func compressData(buf []byte, compression streamsmd.CompressionType) ([]byte, uint32, error) {
	switch compression {
	case streamsmd.COMPRESSION_UNSPECIFIED, streamsmd.COMPRESSION_NONE:
		return bytes.Clone(buf), crc32.Checksum(buf, checksumTable), nil

	case streamsmd.COMPRESSION_GZIP:
		compressedBuf := bytes.NewBuffer(nil)
		gw := gzip.NewWriter(compressedBuf)

		_, err := io.Copy(gw, bytes.NewReader(buf))
		if err != nil {
			return nil, 0, fmt.Errorf("compressing data: %w", err)
		} else if err := gw.Close(); err != nil {
			return nil, 0, fmt.Errorf("closing gzip writer: %w", err)
		}

		return compressedBuf.Bytes(), crc32.Checksum(compressedBuf.Bytes(), checksumTable), nil
	}

	panic(fmt.Sprintf("Unexpected compression type %s", compression.String()))
}

// targetCompressedPageSize returns what size a page should be when accounting
// for compression, increasing uncompressedSize by the average compression
// ratio.
//
// If compression is streamsmd.COMPRESSION_NONE, uncompressedSize is returned
// unmodified.
func targetCompressedPageSize(uncompressedSize uint64, compression streamsmd.CompressionType) uint64 {
	const (
		// averageCompressionRatioGzip is the average compression ratio of Gzip.
		//
		// Gzip typically has a 2:1 to 3:1 compression ratio; we pick the average
		// between 2:1 (0.5) and 3:1 (0.33).
		averageCompressionRatioGzip = 0.415
	)

	switch compression {
	case streamsmd.COMPRESSION_UNSPECIFIED, streamsmd.COMPRESSION_NONE:
		return uncompressedSize
	case streamsmd.COMPRESSION_GZIP:
		return uint64(float64(uncompressedSize) / averageCompressionRatioGzip)
	}

	panic(fmt.Sprintf("Unexpected compression type %s", compression.String()))
}
