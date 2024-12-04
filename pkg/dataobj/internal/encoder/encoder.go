// Package encoder provides an API for encoding data objects.
package encoder

import (
	"encoding/binary"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
)

const (
	fileFormatVersion = 1
)

var magic = []byte("THOR")

// TODO(rfratto): The implementation of this package is going to require some
// thought.
//
// It seems that the best way to do it is for each element to buffer as much of
// its data in memory as it can (to allow for discarding), and only when it is
// closed will it internally append its data to its parent.
//
// There will need to be some global offset tracking; Column.AppendPage will
// need to know the Object-scoped offset where it would write its data so it
// can put the appropriate offsets in the metadata.
//
// Since only one element can be open at a time, it should be fairly easy to
// pass that state around; the offset can propagate down from Object to any
// element. When an element closes itself and passes its data to its parent,
// the parent can update its offset based on the size of the data it received.
//
// Since elements have an effective "lock" on the section of the object being
// written to (and they accumulate data), there's no need for elements to
// manually propagate the offset upwards; this happens as a side effect of them
// passing their data to their parent.
//
// There may be some cases where an element needs to propagate an offset back
// to the parent. For example, Streams.Close will need to inform Object of
// where the section metadata was written; something the parent does not have
// access to. However, one way this could be avoided would be by passing the
// data and metadata to the parent separetely, either by two different
// functions or two different arguments. This would allow the parent to
// immediately know the offset of the metadata and use it.

// Object is a data object. Data objects are hierarchical, split into distinct
// sections that contain their own hierarchy. Only one element of the hierarchy
// can be open at a given time.
type Object struct {
	metadataSize int
	data         []byte
	inuse        bool // inuse specifies whether an element is currently open.

	// sections is the accumulating list of sections in the data object. The last
	// element is partially filled while inuse is true.
	sections []filemd.Section
}

// New creates a new Object where metadata is limited to the specified size.
func New(maxMetadataSize int) *Object {
	return &Object{
		metadataSize: maxMetadataSize,
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

	// The filemd.Section entry starts incomplete; we can't know the offset of
	// the metadata until all data has been written.
	//
	// Calling o.buildMetadata is less expensive compared to other elements as
	// there will be significantly less sections in the file overall.
	o.sections = append(o.sections, filemd.Section{Type: filemd.SECTION_TYPE_STREAMS})
	if md, err := o.buildMetadata(); err != nil {
		return nil, err
	} else if len(md) > o.metadataSize {
		o.sections = o.sections[:len(o.sections)-1]
		return nil, ErrMetadataSize
	}

	o.inuse = true
	return &Streams{
		parent: o,

		metadataSize: o.metadataSize,
		offset:       len(o.data),
	}, nil
}

// buildMetadata builds the set of []filemd.Section to be written as the file
// metadata. buildMetadata does not directly check for size limits to avoid
// unexpected errors from mismatches between the estimated metadata size and
// the effective size.
func (o *Object) buildMetadata() ([]byte, error) {
	var buf []byte

	buf = binary.AppendUvarint(buf, uint64(fileFormatVersion))
	buf = binary.AppendUvarint(buf, uint64(len(o.sections)))
	for _, section := range o.sections {
		sectionBytes, err := proto.Marshal(&section)
		if err != nil {
			return nil, EncodingError{err}
		}

		buf = binary.AppendUvarint(buf, uint64(len(sectionBytes)))
		buf = append(buf, sectionBytes...)
	}

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
	// body:
	//   [data]
	//   [metadata]
	// tailer:
	//   [file metadata size (32 bits)]
	//   [magic]
	//
	// The file metadata *must not* use varint since we need the last 8 bytes of
	// the file to consistently retrieve the tailer.
	if _, err := cw.Write(o.data); err != nil {
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
	o.sections[len(o.sections)-1].MetadataOffset = uint32(len(o.data) + len(data))
	o.sections[len(o.sections)-1].MetadataSize = uint32(len(metadata))

	o.data = append(o.data, data...)
	o.data = append(o.data, metadata...)
	return nil
}
