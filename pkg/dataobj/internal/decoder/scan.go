package decoder

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/logstreamsmd"
)

// scanTailer scans the tailer of the file to retrieve the metadata size and
// the magic value.
func scanTailer(r encoding.Reader) (metadataSize uint32, err error) {
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

// scanFileMetadata scans file metadata from r.
func scanFileMetadata(r encoding.Reader) (*filemd.Metadata, error) {
	fileFormatVersion, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("read file format version: %w", err)
	} else if fileFormatVersion != 1 {
		return nil, fmt.Errorf("unsupported file format version: %d", fileFormatVersion)
	}

	var metadata filemd.Metadata
	if err := scanProto(r, &metadata); err != nil {
		return nil, fmt.Errorf("stream metadata: %w", err)
	}
	return &metadata, nil
}

// scanStreamsMetadata scans streams section metadata from r.
func scanStreamsMetadata(r encoding.Reader) (*logstreamsmd.Metadata, error) {
	formatVersion, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("read format version: %w", err)
	} else if formatVersion != 1 {
		return nil, fmt.Errorf("unsupported streams format version: %d", formatVersion)
	}

	var metadata logstreamsmd.Metadata
	if err := scanProto(r, &metadata); err != nil {
		return nil, fmt.Errorf("stream metadata: %w", err)
	}
	return &metadata, nil
}

// scanStreamMetadata scans a stream's metadata from r.
func scanStreamMetadata(r encoding.Reader) (*logstreamsmd.StreamMetadata, error) {
	var metadata logstreamsmd.StreamMetadata
	if err := scanProto(r, &metadata); err != nil {
		return nil, fmt.Errorf("stream metadata: %w", err)
	}
	return &metadata, nil
}

// scanColumnMetadata scans a column's metadata from r.
func scanColumnMetadata(r encoding.Reader) (*logstreamsmd.ColumnMetadata, error) {
	var metadata logstreamsmd.ColumnMetadata
	if err := scanProto(r, &metadata); err != nil {
		return nil, fmt.Errorf("column metadata: %w", err)
	}
	return &metadata, nil
}

// scanProto scans a proto message from r and stores it in pb. Proto messages
// are expected to be encoded with their size, followed by the proto bytes.
func scanProto(r encoding.Reader, pb proto.Message) error {
	// TODO(rfratto): Should we use a pool for the protoBytes buffer here?

	size, err := binary.ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("read size: %w", err)
	}
	protoBytes := make([]byte, size)
	if _, err := io.ReadFull(r, protoBytes); err != nil {
		return fmt.Errorf("read message: %w", err)
	}
	if err := proto.Unmarshal(protoBytes, pb); err != nil {
		return fmt.Errorf("unmarshal message: %w", err)
	}
	return nil
}
