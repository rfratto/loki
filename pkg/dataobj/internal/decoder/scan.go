package decoder

import (
	"encoding/binary"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/scanner"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

// scanTailer decodes the tailer of the file to retrieve the metadata size and
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

// scanFileMetadata decodes a set of Sections from s.
func scanFileMetadata(s scanner.Scanner) (filemd.Metadata, error) {
	fileFormatVersion, err := binary.ReadUvarint(s)
	if err != nil {
		return filemd.Metadata{}, fmt.Errorf("read file format version: %w", err)
	} else if fileFormatVersion != 1 {
		return filemd.Metadata{}, fmt.Errorf("unsupported file format version: %d", fileFormatVersion)
	}

	metadataSize, err := binary.ReadUvarint(s)
	if err != nil {
		return filemd.Metadata{}, fmt.Errorf("read metadata size: %w", err)
	}

	metadataBytes, err := s.Peek(int(metadataSize))
	if err != nil {
		return filemd.Metadata{}, fmt.Errorf("read file metadata: %w", err)
	} else if len(metadataBytes) != int(metadataSize) {
		return filemd.Metadata{}, fmt.Errorf("read file metadata: short read")
	}
	defer s.Discard(int(metadataSize))

	var metadata filemd.Metadata
	if err := proto.Unmarshal(metadataBytes, &metadata); err != nil {
		return filemd.Metadata{}, fmt.Errorf("unmarshal file metadata: %w", err)
	}
	return metadata, nil
}

// scanStreamsMetadata decodes a set of Streams from s.
func scanStreamsMetadata(s scanner.Scanner) (*streamsmd.Metadata, error) {
	formatVersion, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read format version: %w", err)
	} else if formatVersion != 1 {
		return nil, fmt.Errorf("unsupported streams format version: %d", formatVersion)
	}

	metadataSize, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read metadata size: %w", err)
	}

	metadataBytes, err := s.Peek(int(metadataSize))
	if err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	} else if len(metadataBytes) != int(metadataSize) {
		return nil, fmt.Errorf("read metadata: short read")
	}
	defer s.Discard(int(metadataSize))

	var metadata streamsmd.Metadata
	if err := proto.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return &metadata, nil
}

// scanStreamMetadata decodes a set of Columns from s.
func scanStreamMetadata(s scanner.Scanner) (*streamsmd.StreamMetadata, error) {
	metadataSize, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read metadata size: %w", err)
	}

	metadataBytes, err := s.Peek(int(metadataSize))
	if err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	} else if len(metadataBytes) != int(metadataSize) {
		return nil, fmt.Errorf("read metadata: short read")
	}
	defer s.Discard(int(metadataSize))

	var metadata streamsmd.StreamMetadata
	if err := proto.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return &metadata, nil
}

// scanColumnMetadata decodes a set of Pages from s.
func scanColumnMetadata(s scanner.Scanner) (*streamsmd.ColumnMetadata, error) {
	metadataSize, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read metadata size: %w", err)
	}

	metadataBytes, err := s.Peek(int(metadataSize))
	if err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	} else if len(metadataBytes) != int(metadataSize) {
		return nil, fmt.Errorf("read metadata: short read")
	}
	defer s.Discard(int(metadataSize))

	var metadata streamsmd.ColumnMetadata
	if err := proto.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return &metadata, nil
}
