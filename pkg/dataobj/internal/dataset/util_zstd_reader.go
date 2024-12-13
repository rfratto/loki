package dataset

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

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
