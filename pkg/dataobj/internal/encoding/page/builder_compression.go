package page

import (
	"bufio"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

// A compressWriter compresses data written to it and forwards compressed data
// to an underlying buffer.
type compressWriter struct {
	// To be able to implement [io.ByteWriter], we always write directly to buf,
	// which then flushes to w once it's full.

	w   io.WriteCloser // Compressing writer.
	buf *bufio.Writer  // Buffered writer in front of w to be able to do WriteByte.

	compression       datasetmd.CompressionType // Compression type used.
	uncompressedBytes int                       // Number of uncompressed bytes written.
}

var _ encoding.Writer = (*compressWriter)(nil)

func newCompressWriter(w io.Writer, ty datasetmd.CompressionType) *compressWriter {
	c := compressWriter{compression: ty}
	c.Reset(w)
	return &c
}

// Write writes p to the underlying buffer.
func (c *compressWriter) Write(p []byte) (n int, err error) {
	n, err = c.buf.Write(p)
	c.uncompressedBytes += n
	return
}

// WriteByte writes a single byte to the underlying buffer.
func (c *compressWriter) WriteByte(b byte) error {
	if err := c.buf.WriteByte(b); err != nil {
		return fmt.Errorf("writing byte: %w", err)
	}
	c.uncompressedBytes++
	return nil
}

// Flush flushes all pending data to the underlying writer.
func (c *compressWriter) Flush() error {
	// First flush our buffer.
	if err := c.buf.Flush(); err != nil {
		return fmt.Errorf("flushing buffer: %w", err)
	}

	// Then flush c.w if it implements flush, which it may not if we're not using
	// compression.
	if f, ok := c.w.(interface{ Flush() error }); ok {
		if err := f.Flush(); err != nil {
			return fmt.Errorf("flushing writer: %w", err)
		}
	}

	return nil
}

// Reset discards the writer's state and switches the compresser to write to w.
// This permits reusing a Writer rather than allocating a new one.
func (c *compressWriter) Reset(writer io.Writer) {
	if r, ok := c.w.(interface{ Reset(io.Writer) }); ok {
		r.Reset(writer)
	} else {
		var compressedWriter io.WriteCloser

		switch c.compression {
		case datasetmd.COMPRESSION_TYPE_UNSPECIFIED, datasetmd.COMPRESSION_TYPE_NONE:
			compressedWriter = nopCloseWriter{w: writer}

		case datasetmd.COMPRESSION_TYPE_SNAPPY:
			compressedWriter = snappy.NewBufferedWriter(writer)

		case datasetmd.COMPRESSION_TYPE_ZSTD:
			zw, err := zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
			if err != nil {
				panic(fmt.Sprintf("creating zstd writer: %v", err))
			}
			compressedWriter = zw

		default:
			panic(fmt.Sprintf("Unexpected compression type %s", c.compression.String()))
		}

		c.w = compressedWriter
	}

	if c.buf != nil {
		c.buf.Reset(c.w)
	} else {
		c.buf = bufio.NewWriter(c.w)
	}
	c.uncompressedBytes = 0
}

// UncompressedSize returns the total number of bytes written to the writer.
func (c *compressWriter) UncompressedSize() int {
	return c.uncompressedBytes
}

// CompressedSize returns an estimate of the total number of bytes after
// compression.
func (c *compressWriter) CompressedSize() int {
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

	switch c.compression {
	case datasetmd.COMPRESSION_TYPE_UNSPECIFIED, datasetmd.COMPRESSION_TYPE_NONE:
		return int(c.uncompressedBytes)
	case datasetmd.COMPRESSION_TYPE_SNAPPY:
		return int(float64(c.uncompressedBytes) * averageCompressionRatioSnappy)
	case datasetmd.COMPRESSION_TYPE_ZSTD:
		return int(float64(c.uncompressedBytes) * averageCompressionRatioZstd)
	}

	panic(fmt.Sprintf("Unexpected compression type %s", c.compression.String()))
}

// Close closes the underlying writer.
func (c *compressWriter) Close() error {
	if err := c.Flush(); err != nil {
		return err
	}
	return c.w.Close()
}

type nopCloseWriter struct{ w io.Writer }

func (w nopCloseWriter) Write(p []byte) (n int, err error) { return w.w.Write(p) }
func (w nopCloseWriter) Close() error                      { return nil }
