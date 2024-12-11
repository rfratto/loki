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
func scanFileMetadata(s scanner.Scanner) ([]filemd.Section, error) {
	fileFormatVersion, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read file format version: %w", err)
	} else if fileFormatVersion != 1 {
		return nil, fmt.Errorf("unsupported file format version: %d", fileFormatVersion)
	}

	var sections []filemd.Section
	sectionCount, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read section count: %w", err)
	}
	for range sectionCount {
		section, err := scanSection(s)
		if err != nil {
			return nil, err
		}
		sections = append(sections, section)
	}

	return sections, nil
}

// scanSection decodes an individual section from s.
func scanSection(s scanner.Scanner) (filemd.Section, error) {
	sectionSize, err := binary.ReadUvarint(s)
	if err != nil {
		return filemd.Section{}, fmt.Errorf("read section size: %w", err)
	}

	sectionBytes, err := s.Peek(int(sectionSize))
	if err != nil {
		return filemd.Section{}, fmt.Errorf("read section: %w", err)
	} else if len(sectionBytes) != int(sectionSize) {
		return filemd.Section{}, fmt.Errorf("read section: short read")
	}
	defer s.Discard(int(sectionSize))

	var section filemd.Section
	if err := proto.Unmarshal(sectionBytes, &section); err != nil {
		return filemd.Section{}, fmt.Errorf("unmarshal section: %w", err)
	}
	return section, nil
}

// scanStreamsMetadata decodes a set of Streams from s.
func scanStreamsMetadata(s scanner.Scanner) ([]streamsmd.Stream, error) {
	formatVersion, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read format version: %w", err)
	} else if formatVersion != 1 {
		return nil, fmt.Errorf("unsupported streams format version: %d", formatVersion)
	}

	var streams []streamsmd.Stream
	streamCount, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read stream count: %w", err)
	}
	for range streamCount {
		stream, err := scanStream(s)
		if err != nil {
			return nil, err
		}
		streams = append(streams, stream)
	}

	return streams, nil
}

// scanStream decodes an individual stream from s.
func scanStream(s scanner.Scanner) (streamsmd.Stream, error) {
	streamSize, err := binary.ReadUvarint(s)
	if err != nil {
		return streamsmd.Stream{}, fmt.Errorf("read stream size: %w", err)
	}

	streamBytes, err := s.Peek(int(streamSize))
	if err != nil {
		return streamsmd.Stream{}, fmt.Errorf("read stream: %w", err)
	} else if len(streamBytes) != int(streamSize) {
		return streamsmd.Stream{}, fmt.Errorf("read stream: short read")
	}
	defer s.Discard(int(streamSize))

	var stream streamsmd.Stream
	if err := proto.Unmarshal(streamBytes, &stream); err != nil {
		return streamsmd.Stream{}, fmt.Errorf("unmarshal stream: %w", err)
	}
	return stream, nil
}

// scanStreamMetadata decodes a set of Columns from s.
func scanStreamMetadata(s scanner.Scanner) ([]streamsmd.Column, error) {
	columnCount, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read column count: %w", err)
	}

	var columns []streamsmd.Column
	for range columnCount {
		column, err := scanColumn(s)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	return columns, nil
}

// scanColumn decodes an individual column from s.
func scanColumn(s scanner.Scanner) (streamsmd.Column, error) {
	columnSize, err := binary.ReadUvarint(s)
	if err != nil {
		return streamsmd.Column{}, fmt.Errorf("read column size: %w", err)
	}

	columnBytes, err := s.Peek(int(columnSize))
	if err != nil {
		return streamsmd.Column{}, fmt.Errorf("read column: %w", err)
	} else if len(columnBytes) != int(columnSize) {
		return streamsmd.Column{}, fmt.Errorf("read column: short read")
	}
	defer s.Discard(int(columnSize))

	var column streamsmd.Column
	if err := proto.Unmarshal(columnBytes, &column); err != nil {
		return streamsmd.Column{}, fmt.Errorf("unmarshal column: %w", err)
	}
	return column, nil
}

// scanColumnMetadata decodes a set of Pages from s.
func scanColumnMetadata(s scanner.Scanner) ([]streamsmd.Page, error) {
	pageCount, err := binary.ReadUvarint(s)
	if err != nil {
		return nil, fmt.Errorf("read page count: %w", err)
	}

	var pages []streamsmd.Page
	for range pageCount {
		page, err := scanPage(s)
		if err != nil {
			return nil, err
		}
		pages = append(pages, page)
	}

	return pages, nil
}

// scanPage decodes an individual page from s.
func scanPage(s scanner.Scanner) (streamsmd.Page, error) {
	pageSize, err := binary.ReadUvarint(s)
	if err != nil {
		return streamsmd.Page{}, fmt.Errorf("read page size: %w", err)
	}

	pageBytes, err := s.Peek(int(pageSize))
	if err != nil {
		return streamsmd.Page{}, fmt.Errorf("read page: %w", err)
	} else if len(pageBytes) != int(pageSize) {
		return streamsmd.Page{}, fmt.Errorf("read page: short read")
	}
	defer s.Discard(int(pageSize))

	var page streamsmd.Page
	if err := proto.Unmarshal(pageBytes, &page); err != nil {
		return streamsmd.Page{}, fmt.Errorf("unmarshal page: %w", err)
	}
	return page, nil
}
