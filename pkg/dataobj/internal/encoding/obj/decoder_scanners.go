package obj

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
)

// scan* methods shared by Decoder implementations.

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
	fileFormatVersion, err := encoding.ReadUvarint(r)
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
func scanStreamsMetadata(r encoding.Reader) (*streamsmd.Metadata, error) {
	formatVersion, err := encoding.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("read streams format version: %w", err)
	} else if formatVersion != streamsFormatVersion {
		return nil, fmt.Errorf("unsupported streams format version %d", formatVersion)
	}

	var metadata streamsmd.Metadata
	if err := scanProto(r, &metadata); err != nil {
		return nil, fmt.Errorf("streams metadata: %w", err)
	}
	return &metadata, nil
}

// scanStreamsColumnMetadata scans a column's metadata from r.
func scanStreamsColumnMetadata(r encoding.Reader) (*streamsmd.ColumnMetadata, error) {
	var metadata streamsmd.ColumnMetadata
	if err := scanProto(r, &metadata); err != nil {
		return nil, fmt.Errorf("streams column metadata: %w", err)
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
