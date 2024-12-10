package decoder

import (
	"bufio"
	"context"
	"fmt"
	"iter"

	"github.com/thanos-io/objstore"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

// BucketDecoder returns a new Decoder which reads the data object from the
// provided path within the specified bucket.
func BucketDecoder(bucket objstore.BucketReader, path string) Decoder {
	return &bucketDecoder{
		bucket: bucket,
		path:   path,
	}
}

type bucketDecoder struct {
	bucket objstore.BucketReader
	path   string
}

func (bd *bucketDecoder) Sections(ctx context.Context) (res []filemd.Section, err error) {
	attrs, err := bd.bucket.Attributes(ctx, bd.path)
	if err != nil {
		return nil, fmt.Errorf("reading attributes: %w", err)
	}

	// Read the last 8 bytes of the object to get the metadata size and magic.
	rc, err := bd.bucket.GetRange(ctx, bd.path, attrs.Size-8, 8)
	if err != nil {
		return nil, fmt.Errorf("getting file tailer: %w", err)
	}
	defer rc.Close()

	metadataSize, err := scanTailer(bufio.NewReaderSize(rc, 8))
	if err != nil {
		return nil, fmt.Errorf("reading file tailer: %w", err)
	}

	rc, err = bd.bucket.GetRange(ctx, bd.path, attrs.Size-int64(metadataSize)-8, int64(metadataSize))
	if err != nil {
		return nil, fmt.Errorf("getting file metadata: %w", err)
	}
	defer rc.Close()

	return scanFileMetadata(bufio.NewReader(rc))
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
	// TODO(rfratto): impl
	return nil, fmt.Errorf("NYI")
}

func (bd *bucketStreamsDecoder) Columns(ctx context.Context, stream streamsmd.Stream) ([]streamsmd.Column, error) {
	// TODO(rfratto): impl
	return nil, fmt.Errorf("NYI")
}

func (bd *bucketStreamsDecoder) Pages(ctx context.Context, col streamsmd.Column) ([]streamsmd.Page, error) {
	// TODO(rfratto): impl
	return nil, fmt.Errorf("NYI")
}

func (bd *bucketStreamsDecoder) ReadPages(ctx context.Context, pages []streamsmd.Page) iter.Seq2[streams.Page, error] {
	return func(yield func(streams.Page, error) bool) {
		// TODO(rfratto): impl
		yield(streams.Page{}, fmt.Errorf("NYI"))
	}

}
