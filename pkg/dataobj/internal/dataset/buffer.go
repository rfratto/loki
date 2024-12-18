package dataset

import (
	"bytes"
	"fmt"
	"hash/crc32"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/bitmap"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

// BufferOptions configures common settings for a [Buffer].
type BufferOptions struct {
	// PageSizeHint hints the size to target for each page. Buffers try to fill
	// pages as close to this size as possible after accounting for both encoding
	// and compression, but the actual size may be slightly larger or smaller.
	PageSizeHint int

	// Encoding is the encoding algorithm to use.
	Encoding datasetmd.EncodingType

	// Compression is the compression algorithm to use.
	Compression datasetmd.CompressionType
}

// Buffer accumulates RowType records in memory for a [Column]. A
// Buffer can be converted into a [Page] by calling [Buffer.Flush].
type Buffer[RowType page.DataType] struct {
	// Each Buffer writes two sets of data.
	//
	// The first set of data is a presence bitmap which tells readers which rows
	// are present. We use 1 to indicate presence and 0 to indicate absence
	// (NULL).
	//
	// The second set of data is the encoded set of non-NULL RowType values. As
	// an optimization, the zero value for RowType is treated as NULL.
	//
	// The two sets of data are accumulated in two separate buffers, with the
	// presence bitmap being written uncompressed and the values being written
	// with the configured compression type (which may be none).
	//
	// To pull this off, we have a few components:
	//
	// * The final buffers where encoded and potentially compressed are written.
	// * The writer performing compression.
	// * The encoders to write each set of data.

	opts BufferOptions

	presenceBuffer *bytes.Buffer // presenceBuffer holds the encoded presence bitmap.
	valuesBuffer   *bytes.Buffer // valuesBuffer holds encoded + compressed values.

	valuesWriter *compresser // Compresses data and writes to valuesBuffer.

	presenceEnc *bitmap.Encoder
	valuesEnc   page.Encoder[RowType]

	rows int // Number of rows appended to the Buffer.
}

type NewEncoderFunc[RowType page.DataType] func(encoding.Writer) page.Encoder[RowType]

// NewBuffer creates a new Buffer that stores a sequence of RowType records.
// NewBuffer returns an error if there is no encoder available for the
// combination of RowType and opts.Encoding.
func NewBuffer[RowType page.DataType](opts BufferOptions) (*Buffer[RowType], error) {
	var (
		presenceBuffer = bytes.NewBuffer(nil)
		valuesBuffer   = bytes.NewBuffer(make([]byte, 0, opts.PageSizeHint))

		valuesWriter = newCompresser(valuesBuffer, opts.Compression)
	)

	valuesEnc := newEncoder[RowType](valuesWriter, opts.Encoding)
	if valuesEnc == nil {
		return nil, fmt.Errorf("no encoder available for %s/%s", page.MetadataValueType[RowType](), opts.Encoding)
	}

	return &Buffer[RowType]{
		opts: opts,

		presenceBuffer: presenceBuffer,
		valuesBuffer:   valuesBuffer,

		valuesWriter: valuesWriter,

		presenceEnc: bitmap.NewEncoder(presenceBuffer),
		valuesEnc:   valuesEnc,
	}, nil
}

// Append appends a new RowType record into the Buffer. Append returns true if
// the data was appended; false if the page was full.
func (buf *Buffer[RowType]) Append(value RowType) bool {
	var zero RowType
	if value == zero {
		return buf.AppendNull()
	}

	// We can't accurately know whether adding value would tip us over the page
	// size: we don't know the current state of the [page.Encoder] and we don't
	// know how much space value will fill after compressing. We also can't
	// easily roll back an Append if it did tip us over the page size.
	//
	// Since page sizes are only hints, we just check to see if the page is
	// already full.
	if buf.EstimatedSize() > buf.opts.PageSizeHint {
		return false
	}

	// The following calls should never fail; they only return errors if the
	// underlying writers fail, which ours won't.
	if err := buf.presenceEnc.Encode(1); err != nil {
		panic(fmt.Sprintf("Buffer.Append: encoding presence bitmap entry: %v", err))
	}
	if err := buf.valuesEnc.Encode(value); err != nil {
		panic(fmt.Sprintf("Buffer.Append: encoding value: %v", err))
	}

	buf.rows++
	return true
}

// AppendNull appends a NULL value to the Buffer. AppendNull returns true if
// the NULL was appended, or false if the Buffer is full.
func (buf *Buffer[RowType]) AppendNull() bool {
	// See comment in Append for why the best we can do is check to see if we're
	// already full.
	if buf.EstimatedSize() > buf.opts.PageSizeHint {
		return false
	}

	if err := buf.presenceEnc.Encode(0); err != nil {
		panic(fmt.Sprintf("Buffer.AppendNull: encoding presence bitmap entry: %v", err))
	}

	buf.rows++
	return true
}

// EstimatedSize returns the estimated size of the Buffer in bytes.
func (buf *Buffer[RowType]) EstimatedSize() int {
	// This estimate doesn't account for the entries in the presence bitmap which
	// haven't been flushed yet. However, any flush to the presence bitmasp is
	// generally 2-11 bytes, with the higher end only being from massive RLE
	// runs.
	return buf.presenceBuffer.Len() + int(buf.valuesWriter.CompressedSize())
}

// Rows returns the number of rows appended to the Buffer.
func (buf *Buffer[RowType]) Rows() int {
	return buf.rows
}

// Flush converts data in the buffer into a [Page], and returns it.
//
// To avoid computing useless Stats, the Stats field of the returned Page is
// unset. If Stats are needed for a Page, callers should compute Stats by
// iterating over values in the returned Page.
func (buf *Buffer[RowType]) Flush() (Page, error) {
	if buf.rows == 0 {
		return Page{}, fmt.Errorf("no data to flush")
	}

	// Before we can build the page we need to finish flushing what's remaining
	// in our buffers.
	if err := buf.valuesWriter.Flush(); err != nil {
		return Page{}, fmt.Errorf("flushing values writer: %w", err)
	} else if err := buf.presenceEnc.Flush(); err != nil {
		return Page{}, fmt.Errorf("flushing presence bitmap: %w", err)
	}

	// The final data of our page is the combination of the presence bitmap and
	// the values. To denote when one ends and the other begins, we prepend the
	// presence bitmap with its uvarint size:
	//
	// <uvarint(presence-buffer-size)> <presence-buffer> <values-buffer>
	var (
		headerSize   = encoding.UvarintSize(uint64(buf.presenceBuffer.Len()))
		presenceSize = buf.presenceBuffer.Len()
		valuesSize   = buf.valuesBuffer.Len()

		finalData = bytes.NewBuffer(make([]byte, 0, headerSize+presenceSize+valuesSize))
	)

	if err := encoding.WriteUvarint(finalData, uint64(buf.presenceBuffer.Len())); err != nil {
		return Page{}, fmt.Errorf("writing presence buffer size: %w", err)
	} else if _, err := buf.presenceBuffer.WriteTo(finalData); err != nil {
		return Page{}, fmt.Errorf("writing presence buffer: %w", err)
	} else if _, err := buf.valuesBuffer.WriteTo(finalData); err != nil {
		return Page{}, fmt.Errorf("writing values: %w", err)
	}

	checksum := crc32.Checksum(finalData.Bytes(), checksumTable)

	page := Page{
		UncompressedSize: headerSize + presenceSize + buf.valuesWriter.UncompressedSize(),
		CompressedSize:   finalData.Len(),
		CRC32:            checksum,
		RowCount:         buf.rows,

		Value:       page.MetadataValueType[RowType](),
		Compression: buf.opts.Compression,
		Encoding:    buf.valuesEnc.Type(),

		Data: finalData.Bytes(),

		// TODO(rfratto): At the moment we don't compute stats because they're not
		// going to be valuable in every scenario: a count-distinct sketch for log
		// lines or timestamps is less useful compared to a count-distinct sketch
		// for metadata values.
		//
		// In the future, we may wish to add more options Buffer to tell it to
		// compute a subset of stats to avoid needing a second iteration over the
		// page to computer them.
		Stats: nil,
	}

	// Reset state before returning.
	buf.reset()
	return page, nil
}

func (buf *Buffer[RowType]) reset() {
	buf.presenceBuffer.Reset()
	buf.valuesBuffer.Reset()
	buf.valuesWriter.Reset(buf.valuesBuffer)
	buf.presenceBuffer.Reset()
	buf.valuesEnc.Reset(buf.valuesWriter)
	buf.rows = 0
}
