package dataobj

import (
	"fmt"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

// targetCompressedPageSize returns what size a page should be when accounting
// for compression, increasing uncompressedSize by the average compression
// ratio.
//
// If compression is streamsmd.COMPRESSION_NONE, uncompressedSize is returned
// unmodified.
func targetCompressedPageSize(uncompressedSize int, compression streamsmd.CompressionType) int {
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
		return int(float64(uncompressedSize) / averageCompressionRatioGzip)
	}

	panic(fmt.Sprintf("Unexpected compression type %s", compression.String()))
}
