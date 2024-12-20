package page

import (
	"bytes"
	"fmt"
	"hash/crc32"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

var (
	checksumTable = crc32.MakeTable(crc32.Castagnoli)
)

// BuilderOptions configures common settings for a [Builder].
type BuilderOptions struct {
	// PageSizeHint hints the size to target for each page. Builders try to fill
	// pages as close to this size as possible after accounting for both encoding
	// and compression, but the actual size may be slightly larger or smaller.
	PageSizeHint int

	// Value is the value type of data to write.
	Value datasetmd.ValueType

	// Encoding is the encoding algorithm to use.
	Encoding datasetmd.EncodingType

	// Compression is the compression algorithm to use.
	Compression datasetmd.CompressionType
}

// Builder accumulates sequences of [Value] in memory until full. A [Page] can
// then be created from a Builder by calling [Builder.Flush].
type Builder struct {
	// Each Builder writes two sets of data.
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

	opts BuilderOptions

	presenceBuffer *bytes.Buffer // presenceBuffer holds the encoded presence bitmap.
	valuesBuffer   *bytes.Buffer // valuesBuffer holds encoded + compressed values.

	valuesWriter *compressWriter // Compresses data and writes to valuesBuffer.

	presenceEnc ValueEncoder
	valuesEnc   ValueEncoder

	rows int // Number of rows appended to the Builder.
}

// NewBuilder creates a new Builder that stores a sequence of values.
// NewBuilder returns an error if there is no encoder available for the
// combination of RowType and opts.Encoding.
func NewBuilder(opts BuilderOptions) (*Builder, error) {
	var (
		presenceBuffer = bytes.NewBuffer(nil)
		valuesBuffer   = bytes.NewBuffer(make([]byte, 0, opts.PageSizeHint))

		valuesWriter = newCompressWriter(valuesBuffer, opts.Compression)
	)

	presenceEnc, ok := NewValueEncoder(datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_BITMAP, presenceBuffer)
	if !ok {
		return nil, fmt.Errorf("presence encoder: no encoder available for %s/%s", datasetmd.VALUE_TYPE_UINT64, datasetmd.ENCODING_TYPE_BITMAP)
	}

	valuesEnc, ok := NewValueEncoder(opts.Value, opts.Encoding, valuesWriter)
	if !ok {
		return nil, fmt.Errorf("value encoder: no encoder available for %s/%s", opts.Value, opts.Encoding)
	}

	return &Builder{
		opts: opts,

		presenceBuffer: presenceBuffer,
		valuesBuffer:   valuesBuffer,

		valuesWriter: valuesWriter,

		presenceEnc: presenceEnc,
		valuesEnc:   valuesEnc,
	}, nil
}

// Append appends a new RowType record into the Builder. Append returns true if
// the data was appended; false if the Builder was full.
func (b *Builder) Append(value Value) bool {
	if value.IsNil() || value.IsZero() {
		return b.AppendNull()
	}

	// We can't accurately know whether adding value would tip us over the page
	// size: we don't know the current state of the [ValueEncoder] and we don't
	// know how much space value will fill after compressing. We also can't
	// easily roll back an Append if it did tip us over the page size.
	//
	// Since page sizes are only hints, we just check to see if the page is
	// already full.
	if b.EstimatedSize() > b.opts.PageSizeHint {
		return false
	}

	// The following calls should never fail; they only return errors if the
	// underlying writers fail, which ours won't.
	if err := b.presenceEnc.Encode(Uint64Value(1)); err != nil {
		panic(fmt.Sprintf("Builder.Append: encoding presence bitmap entry: %v", err))
	}
	if err := b.valuesEnc.Encode(value); err != nil {
		panic(fmt.Sprintf("Builder.Append: encoding value: %v", err))
	}

	b.rows++
	return true
}

// AppendNull appends a NULL value to the Builder. AppendNull returns true if
// the NULL was appended, or false if the Builder is full.
func (b *Builder) AppendNull() bool {
	// See comment in Append for why the best we can do is check to see if we're
	// already full.
	if b.EstimatedSize() > b.opts.PageSizeHint {
		return false
	}

	if err := b.presenceEnc.Encode(Uint64Value(0)); err != nil {
		panic(fmt.Sprintf("Builder.AppendNull: encoding presence bitmap entry: %v", err))
	}

	b.rows++
	return true
}

// EstimatedSize returns the estimated size of the Builder in bytes.
func (b *Builder) EstimatedSize() int {
	// This estimate doesn't account for the entries in the presence bitmap which
	// haven't been flushed yet. However, any flush to the presence bitmasp is
	// generally 2-11 bytes, with the higher end only being from massive RLE
	// runs.
	return b.presenceBuffer.Len() + int(b.valuesWriter.CompressedSize())
}

// Rows returns the number of rows appended to the Builder.
func (b *Builder) Rows() int {
	return b.rows
}

// Flush converts data in the Builder into a [Page], and returns it.
//
// To avoid computing useless Stats, the Stats field of the returned Page is
// unset. If Stats are needed for a Page, callers should compute Stats by
// iterating over values in the returned Page.
func (b *Builder) Flush() (Page, error) {
	if b.rows == 0 {
		return Page{}, fmt.Errorf("no data to flush")
	}

	// Before we can build the page we need to finish flushing what's remaining
	// in our buffers.
	if err := b.valuesWriter.Flush(); err != nil {
		return Page{}, fmt.Errorf("flushing values writer: %w", err)
	} else if err := b.presenceEnc.Flush(); err != nil {
		return Page{}, fmt.Errorf("flushing presence bitmap: %w", err)
	}

	// The final data of our page is the combination of the presence bitmap and
	// the values. To denote when one ends and the other begins, we prepend the
	// presence bitmap with its uvarint size:
	//
	// <uvarint(presence-buffer-size)> <presence-buffer> <values-buffer>
	var (
		headerSize   = encoding.UvarintSize(uint64(b.presenceBuffer.Len()))
		presenceSize = b.presenceBuffer.Len()
		valuesSize   = b.valuesBuffer.Len()

		finalData = bytes.NewBuffer(make([]byte, 0, headerSize+presenceSize+valuesSize))
	)

	if err := encoding.WriteUvarint(finalData, uint64(b.presenceBuffer.Len())); err != nil {
		return Page{}, fmt.Errorf("writing presence buffer size: %w", err)
	} else if _, err := b.presenceBuffer.WriteTo(finalData); err != nil {
		return Page{}, fmt.Errorf("writing presence buffer: %w", err)
	} else if _, err := b.valuesBuffer.WriteTo(finalData); err != nil {
		return Page{}, fmt.Errorf("writing values: %w", err)
	}

	checksum := crc32.Checksum(finalData.Bytes(), checksumTable)

	page := Page{
		UncompressedSize: headerSize + presenceSize + b.valuesWriter.UncompressedSize(),
		CompressedSize:   finalData.Len(),
		CRC32:            checksum,
		RowCount:         b.rows,

		Value:       b.opts.Value,
		Compression: b.opts.Compression,
		Encoding:    b.valuesEnc.EncodingType(),

		Data: finalData.Bytes(),

		// TODO(rfratto): At the moment we don't compute stats because they're not
		// going to be valuable in every scenario: a count-distinct sketch for log
		// lines or timestamps is less useful compared to a count-distinct sketch
		// for metadata values.
		//
		// In the future, we may wish to add more options to Builder to tell it to
		// compute a subset of stats to avoid needing a second iteration over the
		// page to computer them.
		Stats: nil,
	}

	// Reset state before returning.
	b.reset()
	return page, nil
}

func (b *Builder) reset() {
	b.presenceBuffer.Reset()
	b.valuesBuffer.Reset()
	b.valuesWriter.Reset(b.valuesBuffer)
	b.presenceBuffer.Reset()
	b.valuesEnc.Reset(b.valuesWriter)
	b.rows = 0
}
