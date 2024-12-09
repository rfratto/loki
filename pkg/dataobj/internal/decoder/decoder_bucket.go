package decoder

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"iter"

	"github.com/gogo/protobuf/proto"
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

	metadataSize, err := readTailer(rc)
	if err != nil {
		return nil, fmt.Errorf("reading file tailer: %w", err)
	}

	rc, err = bd.bucket.GetRange(ctx, bd.path, attrs.Size-int64(metadataSize)-8, int64(metadataSize))
	if err != nil {
		return nil, fmt.Errorf("getting file metadata: %w", err)
	}
	defer rc.Close()

	return readSections(bufio.NewReader(rc))
}

type reader interface {
	io.Reader
	io.ByteReader
}

// readTailer reads the last 8 bytes of the file to retrieve the metadata size
// and the magic value.
func readTailer(r io.Reader) (metadataSize uint32, err error) {
	if err := binary.Read(r, binary.LittleEndian, &metadataSize); err != nil {
		return 0, fmt.Errorf("reading metadata size: %w", err)
	}

	var magic [4]byte
	if _, err := r.Read(magic[:]); err != nil {
		return metadataSize, fmt.Errorf("reading magic: %w", err)
	} else if string(magic[:]) != "THOR" {
		return metadataSize, fmt.Errorf("invalid magic: %x", magic)
	}

	return
}

func readSections(r reader) ([]filemd.Section, error) {
	fileFormatVersion, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("read file format version: %w", err)
	} else if fileFormatVersion != 1 { // TODO(rfratto): replace with constant
		return nil, fmt.Errorf("unsupported file format version: %d", fileFormatVersion)
	}

	var sections []filemd.Section
	sectionCount, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("read section count: %w", err)
	}
	for range sectionCount {
		sectionSize, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read section size: %w", err)
		}

		sectionBytes := make([]byte, sectionSize)
		if _, err := r.Read(sectionBytes); err != nil {
			return nil, fmt.Errorf("read section: %w", err)
		}

		var section filemd.Section
		if proto.Unmarshal(sectionBytes, &section) != nil {
			return nil, fmt.Errorf("unmarshal section: %w", err)
		}
		sections = append(sections, section)
	}

	return sections, nil
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
