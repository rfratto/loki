package decoder

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"iter"

	"github.com/thanos-io/objstore"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/logstreams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/logstreamsmd"
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

func (bd *bucketDecoder) Sections(ctx context.Context) (res []*filemd.SectionInfo, err error) {
	tailer, err := bd.tailer(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading tailer: %w", err)
	}

	rc, err := bd.bucket.GetRange(ctx, bd.path, int64(tailer.FileSize-tailer.MetadataSize-8), int64(tailer.MetadataSize))
	if err != nil {
		return nil, fmt.Errorf("getting file metadata: %w", err)
	}
	defer rc.Close()

	md, err := scanFileMetadata(bufio.NewReader(rc))
	if err != nil {
		return nil, err
	}
	return md.Sections, nil
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

func (bd *bucketDecoder) StreamsDecoder() StreamsDecoder {
	return &bucketStreamsDecoder{
		bucket: bd.bucket,
		path:   bd.path,
	}
}

type bucketStreamsDecoder struct {
	bucket objstore.BucketReader
	path   string
}

func (bd *bucketStreamsDecoder) Streams(ctx context.Context, sec *filemd.SectionInfo) ([]*logstreamsmd.StreamInfo, error) {
	if sec.Type != filemd.SECTION_TYPE_LOG_STREAMS {
		return nil, fmt.Errorf("unsupported section type: %s", sec.Type)
	}

	rc, err := bd.bucket.GetRange(ctx, bd.path, int64(sec.MetadataOffset), int64(sec.MetadataSize))
	if err != nil {
		return nil, fmt.Errorf("reading streams section metadata: %w", err)
	}
	defer rc.Close()

	md, err := scanStreamsMetadata(bufio.NewReader(rc))
	if err != nil {
		return nil, err
	}
	return md.Streams, nil
}

func (bd *bucketStreamsDecoder) Columns(ctx context.Context, stream *logstreamsmd.StreamInfo) ([]*logstreamsmd.ColumnInfo, error) {
	rc, err := bd.bucket.GetRange(ctx, bd.path, int64(stream.MetadataOffset), int64(stream.MetadataSize))
	if err != nil {
		return nil, fmt.Errorf("reading stream metadata: %w", err)
	}
	defer rc.Close()

	md, err := scanStreamMetadata(bufio.NewReader(rc))
	if err != nil {
		return nil, err
	}
	return md.Columns, nil
}

func (bd *bucketStreamsDecoder) Pages(ctx context.Context, col *logstreamsmd.ColumnInfo) ([]*logstreamsmd.PageInfo, error) {
	rc, err := bd.bucket.GetRange(ctx, bd.path, int64(col.MetadataOffset), int64(col.MetadataSize))
	if err != nil {
		return nil, fmt.Errorf("reading column metadata: %w", err)
	}
	defer rc.Close()

	md, err := scanColumnMetadata(bufio.NewReader(rc))
	if err != nil {
		return nil, err
	}
	return md.Pages, nil
}

func (bd *bucketStreamsDecoder) ReadPages(ctx context.Context, pages []*logstreamsmd.PageInfo) iter.Seq2[logstreams.Page, error] {
	readPage := func(ctx context.Context, path string, page *logstreamsmd.PageInfo) (logstreams.Page, error) {
		rc, err := bd.bucket.GetRange(ctx, path, int64(page.DataOffset), int64(page.DataSize))
		if err != nil {
			return logstreams.Page{}, err
		}
		defer rc.Close()

		pageData, err := io.ReadAll(rc)
		if err != nil {
			return logstreams.Page{}, fmt.Errorf("reading page data: %w", err)
		}

		return logstreams.Page{
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

	return func(yield func(logstreams.Page, error) bool) {
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
