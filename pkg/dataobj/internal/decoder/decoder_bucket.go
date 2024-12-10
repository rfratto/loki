package decoder

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"iter"

	"github.com/thanos-io/objstore"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

type bucketDecoder struct {
	bucket objstore.BucketReader
	path   string
}

// BucketDecoder returns a new Decoder which reads the data object from the
// provided path within the specified bucket.
func BucketDecoder(bucket objstore.BucketReader, path string) Decoder {
	return &bucketDecoder{
		bucket: bucket,
		path:   path,
	}
}

func (bd *bucketDecoder) Sections(ctx context.Context) (res []filemd.Section, err error) {
	tailer, err := bd.tailer(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading tailer: %w", err)
	}

	rc, err := bd.bucket.GetRange(ctx, bd.path, int64(tailer.FileSize-tailer.MetadataSize-8), int64(tailer.MetadataSize))
	if err != nil {
		return nil, fmt.Errorf("getting file metadata: %w", err)
	}
	defer rc.Close()
	return scanFileMetadata(bufio.NewReader(rc))
}

type tailer struct {
	MetadataSize uint32
	FileSize     uint32
}

func (bd *bucketDecoder) tailer(ctx context.Context) (tailer, error) {
	attrs, err := bd.bucket.Attributes(ctx, bd.path)
	if err != nil {
		return tailer{}, fmt.Errorf("reading attributes: %w", err)
	}

	// Read the last 8 bytes of the object to get the metadata size and magic.
	rc, err := bd.bucket.GetRange(ctx, bd.path, attrs.Size-8, 8)
	if err != nil {
		return tailer{}, fmt.Errorf("getting file tailer: %w", err)
	}
	defer rc.Close()

	metadataSize, err := scanTailer(bufio.NewReaderSize(rc, 8))
	if err != nil {
		return tailer{}, fmt.Errorf("scanning tailer: %w", err)
	}

	return tailer{
		MetadataSize: metadataSize,
		FileSize:     uint32(attrs.Size),
	}, nil
}

func (bd *bucketDecoder) StreamsDecoder(sec filemd.Section) (StreamsDecoder, error) {
	if sec.Type != filemd.SECTION_TYPE_STREAMS {
		return nil, fmt.Errorf("unsupported section type: %s", sec.Type)
	}

	return &bucketStreamsDecoder{
		bucket:  bd.bucket,
		path:    bd.path,
		section: sec,
	}, nil
}

type bucketStreamsDecoder struct {
	bucket  objstore.BucketReader
	path    string
	section filemd.Section
}

func (bd *bucketStreamsDecoder) Streams(ctx context.Context) ([]streamsmd.Stream, error) {
	rc, err := bd.bucket.GetRange(ctx, bd.path, int64(bd.section.MetadataOffset), int64(bd.section.MetadataSize))
	if err != nil {
		return nil, fmt.Errorf("reading streams section metadata: %w", err)
	}
	defer rc.Close()
	return scanStreamsMetadata(bufio.NewReader(rc))
}

func (bd *bucketStreamsDecoder) Columns(ctx context.Context, stream streamsmd.Stream) ([]streamsmd.Column, error) {
	rc, err := bd.bucket.GetRange(ctx, bd.path, int64(stream.MetadataOffset), int64(stream.MetadataSize))
	if err != nil {
		return nil, fmt.Errorf("reading stream metadata: %w", err)
	}
	defer rc.Close()
	return scanStreamMetadata(bufio.NewReader(rc))
}

func (bd *bucketStreamsDecoder) Pages(ctx context.Context, col streamsmd.Column) ([]streamsmd.Page, error) {
	rc, err := bd.bucket.GetRange(ctx, bd.path, int64(col.MetadataOffset), int64(col.MetadataSize))
	if err != nil {
		return nil, fmt.Errorf("reading column metadata: %w", err)
	}
	defer rc.Close()
	return scanColumnMetadata(bufio.NewReader(rc))
}

func (bd *bucketStreamsDecoder) ReadPages(ctx context.Context, pages []streamsmd.Page) iter.Seq2[streams.Page, error] {
	readPage := func(ctx context.Context, path string, page streamsmd.Page) (streams.Page, error) {
		rc, err := bd.bucket.GetRange(ctx, path, int64(page.DataOffset), int64(page.DataSize))
		if err != nil {
			return streams.Page{}, err
		}
		defer rc.Close()

		pageData, err := io.ReadAll(rc)
		if err != nil {
			return streams.Page{}, fmt.Errorf("reading page data: %w", err)
		}

		return streams.Page{
			UncompressedSize: int(page.UncompressedSize),
			CompressedSize:   int(page.CompressedSize),
			CRC32:            uint32(page.Crc32),
			RowCount:         int(page.RowsCount),

			Compression: page.Compression,
			Encoding:    page.Encoding,
			Stats:       page.Statistics,

			Data: pageData,
		}, nil
	}

	return func(yield func(streams.Page, error) bool) {
		// TODO(rfratto): this could be optimized by getting multiple pages at
		// once; pages that are right next to one another can be read in a single
		// pass, but we can also tolerate some amount of unused data in between to
		// minimize reads.
		for _, pageHeader := range pages {
			page, err := readPage(ctx, bd.path, pageHeader)
			if !yield(page, err) {
				return
			}

			if err != nil {
				return
			}
		}
	}
}
