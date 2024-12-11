package decoder

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/scanner"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

// scanTailer scans the tailer of the file to retrieve the metadata size and
// the magic value.
func scanTailer(s scanner.Scanner) (metadataSize uint32, err error) {
	if err := binary.Read(s, binary.LittleEndian, &metadataSize); err != nil {
		return 0, fmt.Errorf("reading metadata size: %w", err)
	}

	var magic [4]byte
	if _, err := s.Read(magic[:]); err != nil {
		return metadataSize, fmt.Errorf("reading magic: %w", err)
	} else if string(magic[:]) != "THOR" {
		return metadataSize, fmt.Errorf("invalid magic: %x", magic)
	}

	return
}

// scanFileMetadata scans file metadata from s.
func scanFileMetadata(s scanner.Scanner) (*filemd.Metadata, error) {
	fileFormatVersion, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read file format version: %w", err)
	} else if fileFormatVersion != 1 {
		return nil, fmt.Errorf("unsupported file format version: %d", fileFormatVersion)
	}

	var metadata filemd.Metadata
	if err := scanProto(s, &metadata); err != nil {
		return nil, fmt.Errorf("stream metadata: %w", err)
	}
	return &metadata, nil
}

// scanStreamsMetadata scans streams section metadata from s.
func scanStreamsMetadata(s scanner.Scanner) (*streamsmd.Metadata, error) {
	formatVersion, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read format version: %w", err)
	} else if formatVersion != 1 {
		return nil, fmt.Errorf("unsupported streams format version: %d", formatVersion)
	}

	var metadata streamsmd.Metadata
	if err := scanProto(s, &metadata); err != nil {
		return nil, fmt.Errorf("stream metadata: %w", err)
	}
	return &metadata, nil
}

// scanStreamMetadata scans a stream's metadata from s.
func scanStreamMetadata(s scanner.Scanner) (*streamsmd.StreamMetadata, error) {
	var metadata streamsmd.StreamMetadata
	if err := scanProto(s, &metadata); err != nil {
		return nil, fmt.Errorf("stream metadata: %w", err)
	}
	return &metadata, nil
}

// scanColumnMetadata scans a column's metadata from s.
func scanColumnMetadata(s scanner.Scanner) (*streamsmd.ColumnMetadata, error) {
	var metadata streamsmd.ColumnMetadata
	if err := scanProto(s, &metadata); err != nil {
		return nil, fmt.Errorf("column metadata: %w", err)
	}
	return &metadata, nil
}

// scanProto scans a proto message from s and stores it in pb. Proto messages
// are expected to be encoded with their size, followed by the proto bytes.
func scanProto(s scanner.Scanner, pb proto.Message) error {
	// TODO(rfratto): Should we use a pool for the protoBytes buffer here?

	size, err := binary.ReadUvarint(s)
	if err != nil {
		return fmt.Errorf("read size: %w", err)
	}
	protoBytes := make([]byte, size)
	if _, err := io.ReadFull(s, protoBytes); err != nil {
		return fmt.Errorf("read message: %w", err)
	}
	if err := proto.Unmarshal(protoBytes, pb); err != nil {
		return fmt.Errorf("unmarshal message: %w", err)
	}
	return nil
}
