// Package encoder provides an API for encoding data objects.
package encoder

import (
	"encoding/binary"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
)

const (
	fileFormatVersion = 1
)

var magic = []byte("THOR")

// TODO(rfratto): This package currently works, but there's a few things to consider:
//
// 1. There's a few extra allocations that can be avoided; we should consider
//    using a buffer pool for various elements.
//
// 2. There's some cases where information is propagated back up to the parent
//    in gross ways (see updateParentMetadata). A way of doing this without
//    needing children elements to be aware of internal parent state would be
//    preferred.
//
// 3. We'll likely want to combine this package with the decoder package into a
//    single "encoding" package. See the comment in decoder for more thoughts.

// Object is a data object. Data objects are hierarchical, split into distinct
// sections that contain their own hierarchy. Only one element of the hierarchy
// can be open at a given time.
type Object struct {
	header []byte
	data   []byte
	inuse  bool // inuse specifies whether an element is currently open.

	// sections is the accumulating list of sections in the data object. The last
	// element is partially filled while inuse is true.
	sections []*filemd.SectionInfo
}

// New creates a new Object where metadata is limited to the specified size.
func New() *Object {
	// Loki never reads the header, but we include it anyway so that:
	//
	// 1. Tools can verify the file type by looking at the first 4 bytes rather
	//    than seeking to the end and reading the last four.
	//
	// 2. We can always interpret any data offset of 0 as invalid, since 0 is
	//    where the magic header is.
	return &Object{
		header: magic,
	}
}

// OpenStreams opens a streams section in the object. OpenStreams fails if
// there is another open streams section.
//
// If opening a new streams section would exceed the maximum metadata size for
// Object, OpenStreams returns an error.
func (o *Object) OpenStreams() (*Streams, error) {
	if o.inuse {
		return nil, ErrElementExist
	}

	// The filemd.SectionInfo entry starts incomplete; we can't know the offset
	// of the metadata until all data has been written.
	o.sections = append(o.sections, &filemd.SectionInfo{Type: filemd.SECTION_TYPE_LOG_STREAMS})

	o.inuse = true
	return &Streams{
		parent: o,

		offset: len(o.header) + len(o.data),
	}, nil
}

// MetadataSize returns an estimate of the current metadata size.
func (o *Object) MetadataSize() int {
	md, _ := o.buildMetadata()
	return len(md)
}

// buildMetadata builds the set of []filemd.Section to be written as the file
// metadata. buildMetadata does not directly check for size limits to avoid
// unexpected errors from mismatches between the estimated metadata size and
// the effective size.
func (o *Object) buildMetadata() ([]byte, error) {
	var buf []byte

	md, err := proto.Marshal(&filemd.Metadata{
		Sections: o.sections,
	})
	if err != nil {
		return nil, EncodingError{err}
	}

	buf = binary.AppendUvarint(buf, uint64(fileFormatVersion))
	buf = binary.AppendUvarint(buf, uint64(len(md)))
	buf = append(buf, md...)
	return buf, nil
}

// WriteTo serializes the object to the provided writer. WriteTo returns the
// number of bytes written and any error that occurred during writing. WriteTo
// fails if there is currently an open section.
func (o *Object) WriteTo(w io.Writer) (int64, error) {
	cw := &countingWriter{w: w}

	md, err := o.buildMetadata()
	if err != nil {
		return cw.total, err
	}

	// The overall structure is:
	//
	// header:
	//   [magic]
	// body:
	//   [data]
	//   [metadata]
	// tailer:
	//   [file metadata size (32 bits)]
	//   [magic]
	//
	// The file metadata *must not* use varint since we need the last 8 bytes of
	// the file to consistently retrieve the tailer.
	if _, err := cw.Write(o.header); err != nil {
		return cw.total, err
	} else if _, err := cw.Write(o.data); err != nil {
		return cw.total, err
	} else if _, err := cw.Write(md); err != nil {
		return cw.total, err
	} else if err := binary.Write(cw, binary.LittleEndian, uint32(len(md))); err != nil {
		return cw.total, err
	} else if _, err := cw.Write(magic); err != nil {
		return cw.total, err
	}

	return cw.total, nil
}

type countingWriter struct {
	w     io.Writer
	total int64
}

func (c *countingWriter) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)
	c.total += int64(n)
	return
}

// append adds data and metadata to the Object. append must only be called from
// child elements on Close and Discard. Discard calls must pass empty byte
// slices to denote no data or metadata.
func (o *Object) append(data, metadata []byte) error {
	if !o.inuse {
		return errElementNoExist
	}
	o.inuse = false

	if len(data) == 0 && len(metadata) == 0 {
		o.sections = o.sections[:len(o.sections)-1]
		return nil
	}

	// Update the section entry with the offset and size of the metadata.
	o.sections[len(o.sections)-1].MetadataOffset = uint32(len(o.header) + len(o.data) + len(data))
	o.sections[len(o.sections)-1].MetadataSize = uint32(len(metadata))

	o.data = append(o.data, data...)
	o.data = append(o.data, metadata...)
	return nil
}
