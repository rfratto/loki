package dataset

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/delta"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/rle"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

type timeBuffer struct {
	opts BufferOptions

	nullsBuffer *bytes.Buffer // Stores rle-encoded NULL bitmask.
	dataBuffer  *bytes.Buffer // Stores encoded + compressed data.

	dataWriter *compresser // Where data is written to.

	nullsEnc *rle.Encoder
	deltaEnc *delta.Encoder

	rows int // Number of rows appended to the timeBuffer.
}

// TimeBuffer creates a [Buffer] that stores a sequence of timestamps.
func TimeBuffer(opts BufferOptions) Buffer[time.Time] {
	var (
		nullsBuffer = bytes.NewBuffer(nil)
		dataBuffer  = bytes.NewBuffer(make([]byte, 0, opts.PageSizeHint))

		dataWriter = newCompresser(dataBuffer, opts.Compression)
	)

	return &timeBuffer{
		opts: opts,

		nullsBuffer: nullsBuffer,
		dataBuffer:  dataBuffer,

		dataWriter: dataWriter,

		nullsEnc: rle.NewEncoder(nullsBuffer, 1),
		deltaEnc: delta.NewEncoder(dataWriter),
	}
}

func (b *timeBuffer) Append(value time.Time) bool {
	// We don't know the state of the delta encoder, so we don't know if adding a
	// value would tip over our page size. Instead, we only check to see if the
	// page is already full.
	if b.EstimatedSize() > b.opts.PageSizeHint {
		return false
	}

	ts := value.UnixNano()

	if err := b.nullsEnc.Encode(1); err != nil {
		panic(fmt.Sprintf("timeBuffer.Append: encoding null bitmask entry: %v", err))
	}
	if err := b.deltaEnc.Encode(ts); err != nil {
		panic(fmt.Sprintf("timeBuffer.Append: encoding value: %v", err))
	}
	b.rows++
	return true
}

func (b *timeBuffer) AppendNull() bool {
	// We don't know the state of the RLE encoder, so we don't know if adding a
	// value would tip us over our page size. Instead, we only check to see if
	// the page is already full.
	if b.EstimatedSize() > b.opts.PageSizeHint {
		return false
	}

	if err := b.nullsEnc.Encode(0); err != nil {
		panic(fmt.Sprintf("timeBuffer.AppendNull: encoding null bitmask entry: %v", err))
	}
	b.rows++
	return true
}

func (b *timeBuffer) EstimatedSize() int {
	// This estimate doesn't account for entries in the NULLs bitmask which
	// haven't been flushed yet, but any flush to the NULLs buffer is generally
	// 2-11 bytes, with higher sizes requiring a massive RLE run.
	return b.nullsBuffer.Len() + int(b.dataWriter.CompressedSize())
}

func (b *timeBuffer) Rows() int {
	return b.rows
}

func (b *timeBuffer) Flush() (Page, error) {
	if b.rows == 0 {
		return Page{}, fmt.Errorf("no data to flush")
	}

	// Before we can build a page, we need to flush what's remaining in our
	// buffers.
	if err := b.dataWriter.Flush(); err != nil {
		return Page{}, fmt.Errorf("flushing data writer: %w", err)
	} else if err := b.nullsEnc.Flush(); err != nil {
		return Page{}, fmt.Errorf("flushing nulls encoder: %w", err)
	}

	// Now, to build a page, we need to combine our buffers; we separate them by
	// prepending the nulls buffer size before the nulls buffer.
	//
	// <varint(nulls-buffer-size)> <nulls-buffer> <data-buffer>
	finalData := bytes.NewBuffer(make([]byte, 0, b.EstimatedSize()))

	if err := encoding.WriteUvarint(finalData, uint64(b.nullsBuffer.Len())); err != nil {
		return Page{}, fmt.Errorf("writing nulls bitmask length: %w", err)
	}
	if _, err := b.nullsBuffer.WriteTo(finalData); err != nil {
		return Page{}, fmt.Errorf("writing nulls bitmask: %w", err)
	}
	if _, err := b.dataBuffer.WriteTo(finalData); err != nil {
		return Page{}, fmt.Errorf("writing data: %w", err)
	}

	checksum := crc32.Checksum(finalData.Bytes(), crc32.IEEETable)

	page := Page{
		UncompressedSize: b.dataWriter.UncompressedSize(),
		CompressedSize:   finalData.Len(),
		CRC32:            checksum,
		RowCount:         b.rows,

		Value:       datasetmd.VALUE_TYPE_TIMESTAMP,
		Compression: b.opts.Compression,
		Encoding:    datasetmd.ENCODING_TYPE_DELTA,

		Data: finalData.Bytes(),
	}

	// Reset our buffers for new data.
	b.nullsBuffer.Reset()
	b.dataBuffer.Reset()
	b.dataWriter.Reset(b.dataBuffer)
	return page, nil
}
