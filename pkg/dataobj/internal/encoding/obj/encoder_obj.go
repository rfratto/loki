package obj

import (
	"encoding/binary"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
)

// Encoder encodes a data object. Data objects are hierarchical, split into
// distinct sections that contain their own hierarchy.
//
// To support hierarchical encoding, a set of Open* methods are provided to
// open a child element. Only one element of the hierarchy can be open at a
// given time; call Close on the open element to close it.
type Encoder struct {
	w *countingWriter

	closed bool // closed specifies whether the encoder has been closed.

	// sections is the accumulating list of sections in the data object.
	sections   []*filemd.SectionInfo
	curSection *filemd.SectionInfo // curSection is the currently open section.
}

// NewEncoder creates a new Encoder. Encoded data is written to w as child
// elements are closed. To finish encoding, call Close on the returned Encoder.
//
// NewEncoder fails if writing the magic header fails.
func NewEncoder(w encoding.Writer) (*Encoder, error) {
	enc := &Encoder{w: &countingWriter{w: w}}
	if err := enc.writeHeader(); err != nil {
		return nil, err
	}
	return enc, nil
}

func (enc *Encoder) writeHeader() error {
	// Immediately write the header.
	if _, err := enc.w.Write(magic); err != nil {
		return fmt.Errorf("writing magic header: %w", err)
	}

	return nil
}

// OpenStreams opens a streams section in the object. OpenStreams fails if
// there is another open section.
func (enc *Encoder) OpenStreams() (*StreamsEncoder, error) {
	if enc.curSection != nil {
		return nil, ErrElementExist
	} else if enc.closed {
		return nil, ErrClosed
	}

	enc.curSection = &filemd.SectionInfo{Type: filemd.SECTION_TYPE_STREAMS}

	return &StreamsEncoder{
		parent: enc,
		w:      enc.w,
	}, nil
}

// MetadataSize returns an estimate of the current metadataSize. MetadataSize
// includes an estimate for the currently open element.
func (enc *Encoder) MetadataSize() int {
	temp := &countingWriter{w: encoding.Discard}
	_ = enc.writeMetadata(temp)
	return int(temp.total)
}

func (enc *Encoder) writeMetadata(w encoding.Writer) error {
	md, err := proto.Marshal(&filemd.Metadata{Sections: enc.allSections()})
	if err != nil {
		return EncodingError{err}
	}

	if err := encoding.WriteUvarint(w, fileFormatVersion); err != nil {
		return EncodingError{err}
	} else if err := encoding.WriteUvarint(w, uint64(len(md))); err != nil {
		return EncodingError{err}
	} else if _, err := w.Write(md); err != nil {
		return err
	}

	return nil
}

func (enc *Encoder) allSections() []*filemd.SectionInfo {
	sections := enc.sections[:len(enc.sections):cap(enc.sections)]
	if enc.curSection != nil {
		sections = append(sections, enc.curSection)
	}
	return sections
}

// onChildClose is called from a child element when it closes. onChildClose
// must only be called from the open child element.
//
// Children are responsible for tracking the offset and size of their metadata.
func (enc *Encoder) onChildClose(metadataOffset, metadataSize int64) error {
	if enc.curSection == nil {
		return errElementNoExist
	}

	enc.curSection.MetadataOffset = uint32(metadataOffset)
	enc.curSection.MetadataSize = uint32(metadataSize)

	enc.sections = append(enc.sections, enc.curSection)
	enc.curSection = nil

	return nil
}

// onChildDiscard is called from a child element when it is discarded. Children
// encoders must take care to only call onChildDiscard if no data was written
// to enc.w.
func (enc *Encoder) onChildDiscard() error {
	if enc.curSection == nil {
		return errElementNoExist
	}
	enc.curSection = nil
	return nil
}

// Close closes an Encoder, flushing the rest of its state. Closer fails if
// there is a currently open element.
func (enc *Encoder) Close() error {
	if enc.curSection != nil {
		return ErrElementExist
	}

	metadataStart := enc.w.total
	if err := enc.writeMetadata(enc.w); err != nil {
		return err
	}
	metadataEnd := enc.w.total

	// Write the tailer.
	if err := binary.Write(enc.w, binary.LittleEndian, uint32(metadataEnd-metadataStart)); err != nil {
		return fmt.Errorf("writing tailer: metadata size: %w", err)
	} else if _, err := enc.w.Write(magic); err != nil {
		return fmt.Errorf("writing tailer: magic: %w", err)
	}

	enc.closed = true
	return nil
}

type countingWriter struct {
	w     encoding.Writer
	total int64
}

func (c *countingWriter) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)
	c.total += int64(n)
	return
}

func (c *countingWriter) WriteByte(b byte) error {
	if err := c.w.WriteByte(b); err != nil {
		return err
	}
	c.total++
	return nil
}
