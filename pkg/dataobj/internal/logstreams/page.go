package logstreams

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/logstreamsmd"
	"github.com/klauspost/compress/zstd"
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

	Compression logstreamsmd.CompressionType // Compression used for the compressed page.
	Encoding    logstreamsmd.EncodingType    // Encoding used for the decompressed page.
	Stats       *logstreamsmd.Statistics     // Optional statistics for the page.

	Data []byte // Data is the compressed and encoded page data.
}

// Reader returns a reader for decompressed page data. Reader returns an error
// if the CRC32 fails to validate.
func (p *Page) Reader() (io.ReadCloser, error) {
	if actual := crc32.Checksum(p.Data, checksumTable); p.CRC32 != actual {
		return nil, fmt.Errorf("invalid crc32 checksum %x, expected %x", actual, p.CRC32)
	}

	switch p.Compression {
	case logstreamsmd.COMPRESSION_TYPE_UNSPECIFIED, logstreamsmd.COMPRESSION_TYPE_NONE:
		return io.NopCloser(bytes.NewReader(p.Data)), nil

	case logstreamsmd.COMPRESSION_TYPE_SNAPPY:
		sr := snappy.NewReader(bytes.NewReader(p.Data))
		return io.NopCloser(sr), nil

	case logstreamsmd.COMPRESSION_TYPE_ZSTD:
		zr, err := zstd.NewReader(bytes.NewReader(p.Data))
		if err != nil {
			return nil, fmt.Errorf("creating zstd reader: %w", err)
		}
		return newZstdReader(zr), nil
	}

	panic(fmt.Sprintf("Unexpected compression type %s", p.Compression.String()))
}

// compressData compresses buf using the provided compression algorithm and
// returns the compressed data along with the crc32 checksum of the compressed
// data.
func compressData(buf []byte, compression logstreamsmd.CompressionType) ([]byte, uint32, error) {
	switch compression {
	case logstreamsmd.COMPRESSION_TYPE_UNSPECIFIED, logstreamsmd.COMPRESSION_TYPE_NONE:
		return bytes.Clone(buf), crc32.Checksum(buf, checksumTable), nil

	case logstreamsmd.COMPRESSION_TYPE_SNAPPY:
		compressedBuf := bytes.NewBuffer(nil)
		sw := snappy.NewBufferedWriter(compressedBuf)

		_, err := io.Copy(sw, bytes.NewReader(buf))
		if err != nil {
			return nil, 0, fmt.Errorf("compressing data: %w", err)
		} else if err := sw.Close(); err != nil {
			return nil, 0, fmt.Errorf("closing zstd writer: %w", err)
		}
		return compressedBuf.Bytes(), crc32.Checksum(compressedBuf.Bytes(), checksumTable), nil

	case logstreamsmd.COMPRESSION_TYPE_ZSTD:
		// Facebook's benchmarks place the best compression of Zstd as nearly twice
		// as good as Snappy, while being only very slightly slower.
		//
		//   Compression | Ratio | Write    | Read
		//   ----------- | ----- | -------- | ---------
		//   Zstd (Best) | 2.887 | 510 MB/s | 1580 MB/s
		//   Snappy      | 2.073 | 530 MB/s | 1660 MB/s
		compressedBuf := bytes.NewBuffer(nil)
		zw, err := zstd.NewWriter(compressedBuf, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
		if err != nil {
			return nil, 0, fmt.Errorf("creating zstd writer: %w", err)
		}

		_, err = io.Copy(zw, bytes.NewReader(buf))
		if err != nil {
			return nil, 0, fmt.Errorf("compressing data: %w", err)
		} else if err := zw.Close(); err != nil {
			return nil, 0, fmt.Errorf("closing zstd writer: %w", err)
		}
		return compressedBuf.Bytes(), crc32.Checksum(compressedBuf.Bytes(), checksumTable), nil
	}

	panic(fmt.Sprintf("Unexpected compression type %s", compression.String()))
}

// targetCompressedPageSize returns what size a page should be when accounting
// for compression, increasing uncompressedSize by the average compression
// ratio.
//
// If compression is logstreamsmd.COMPRESSION_NONE, uncompressedSize is returned
// unmodified.
func targetCompressedPageSize(uncompressedSize uint64, compression logstreamsmd.CompressionType) uint64 {
	const (
		// averageCompressionRatioSnappy is the average compression ratio of Snappy.
		//
		// Snappy typically has a 2:1 compression ratio for general purpose data.
		averageCompressionRatioSnappy = 0.5

		// averageCompressionRatioZstd is the average compression ratio of Zstd.
		//
		// As we encode with the best compression level, we expect a higher
		// compression ratio than the default level, around 2.887:1.
		averageCompressionRatioZstd = 0.346
	)

	switch compression {
	case logstreamsmd.COMPRESSION_TYPE_UNSPECIFIED, logstreamsmd.COMPRESSION_TYPE_NONE:
		return uncompressedSize
	case logstreamsmd.COMPRESSION_TYPE_SNAPPY:
		return uint64(float64(uncompressedSize) / averageCompressionRatioSnappy)
	case logstreamsmd.COMPRESSION_TYPE_ZSTD:
		return uint64(float64(uncompressedSize) / averageCompressionRatioZstd)
	}

	panic(fmt.Sprintf("Unexpected compression type %s", compression.String()))
}
