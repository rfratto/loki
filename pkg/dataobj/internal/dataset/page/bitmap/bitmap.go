// Package bitmap encodes and decodes bitmaps of unsigned numbers up to 64 bits
// wide. To use bitmap with signed integers, callers should first encode the
// integers using zig-zag encoding to minimize the number of bits needed for
// negative values.
//
// Data is encoded with a hybrid of run-length encoding and bitpacking. Longer
// sequences of the same value are encoded with run-length encoding, while
// shorter sequences are bitpacked when possible. To avoid padding, bitpacking
// is only used when there are a multiple of 8 values to encode.
//
// Bitpacking is done using a dynamic bit width. The bit width is determined by
// the largest value in the set of 8 values. The bit width can be any value
// from 1 to 64, inclusive.
//
// # Format
//
// Our format is a slight modification of the Parquet format to support longer
// runs and support streaming. The EBNF grammar is as follows:
//
//	bitmap             = run+;
//	run                = bit_packed_run | rle_run;
//	bit_packed_run     = bit_packed_header bit_packed_values;
//	bit_packed_header  = (* uvarint(bit_packed_sets << 7 | bit_width << 1 | 1) *)
//	bit_packed_sets    = (* value between 1 and 2^57-1, inclusive; each set has 8 elements *)
//	bit_width          = (* bit size of element in set; value between 1 and 64, inclusive *)
//	bit_packed_values  = (* least significant bit of each byte to most significant bit of each byte *)
//	rle_run            = rle_header repeated_value;
//	rle_header         = (* uvarint(rle_run_len << 1) *)
//	rle_run_len        = (* value between 1 and 2^63-1, inclusive *)
//	repeated_value     = (* repeated value encoded as uvarint *)
//
// Where this differs from Parquet:
//
//   - We don't use a fixed width for bit-packed values, to allow for stream
//     calls to Encode without knowing the width in advance. Instead, the width
//     is determined when flushing a bit-packed set to an internal buffer.
//
//     To minimize the overhead of encoding the width dynamically, we store
//     each bitpacked set with the smallest amount of bits possible. If two
//     sets have different widths, they are flushed as two different runs.
//
//   - For simplicity, repeated_value is encoded as uvarint rather than
//     flushing the value in its entirety.
//
//   - To facilitate streaming, we don't prepend the length of all bytes
//     written. Callers may choose to prepend the length. Without the length,
//     readers must take caution to not read past the end of the RLE sequence
//     by knowing exactly how many values were encoded.
package bitmap

import (
	"fmt"
	"math/bits"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

func init() {
	// Register the encoding to the page package so instances of it can be
	// dynamically created.
	page.RegisterValueEncoding(
		datasetmd.VALUE_TYPE_UINT64,
		datasetmd.ENCODING_TYPE_BITMAP,
		func(w encoding.Writer) page.ValueEncoder { return NewEncoder(w) },
		func(r encoding.Reader) page.ValueDecoder { return NewDecoder(r) },
	)
}

const maxRunLength uint64 = 1<<63 - 1 // 2^63-1

// An Encoder encodes uint64s to a hybrid run-length encoded format.
type Encoder struct {
	w encoding.Writer

	// Encoder is a basic state machine with three states:
	//
	// READY    The default state; it's ready to start a new run. New values move
	//          to the RLE state.
	//
	// RLE      The encoder is tracking a run of runValue. Active when
	//          runLength>0. If the run ends and runValue<8, the encoder
	//          transitions to the BITPACK state. Otherwise, the encoder flushes
	//          the run and reenters RLE.
	//
	//          If runLength reaches 2^63-1, the encoder will flush the run and
	//          move to READY.
	//
	// BITPACK  The encoder is tracking a set of values to bitpack. Active when
	//          setSize>0. Once the set reaches 8 values, the run is flushed and
	//					the encoder moves to READY.

	runValue  uint64 // Value in the current run.
	runLength uint64 // Length of the current run.

	set     [8]uint64 // Set of bit-packed values.
	setSize byte      // Current number of elements in set.

	buf *bitpackBuffer
}

var _ page.ValueEncoder = (*Encoder)(nil)

// NewEncoder creates an Encoder that writes encoded numbers to w.
func NewEncoder(w encoding.Writer) *Encoder {
	return &Encoder{
		w:   w,
		buf: newBitpackBuffer(),
	}
}

// ValueType returns [datasetmd.VALUE_TYPE_UINT64].
func (enc *Encoder) ValueType() datasetmd.ValueType {
	return datasetmd.VALUE_TYPE_UINT64
}

// EncodingType returns [datasetmd.ENCODING_TYPE_BITMAP].
func (enc *Encoder) EncodingType() datasetmd.EncodingType {
	return datasetmd.ENCODING_TYPE_BITMAP
}

// Encode appends a new value to the encoder. Values are buffered in memory up
// to a maximum of 4KiB.
//
// When flushing, Encode returns an error if writing to the underlying Writer
// fails.
//
// Call [Encoder.Flush] to end the run and flush any remaining values.
func (enc *Encoder) Encode(v page.Value) error {
	if v.Type() != datasetmd.VALUE_TYPE_UINT64 {
		return fmt.Errorf("invalid value type %s", v.Type())
	}
	uv := v.Uint64()

	switch {
	case enc.runLength == 0 && enc.setSize == 0: // Start a new run.
		enc.runValue = uv
		enc.runLength = 1
		return nil

	case enc.runLength > 0 && uv == enc.runValue: // Continue the run.
		enc.runLength++

		// If we hit the maximum run length, we'll flush the run immediately.
		if enc.runLength == maxRunLength {
			return enc.flushRLE()
		}
		return nil

	case enc.runLength > 0 && uv != enc.runValue:
		// The run ended. If the run lasted less than 8 values, we switch to
		// bitpacking. Copy over the old run value and add the new value.
		if enc.runLength < 8 {
			enc.setSize = byte(enc.runLength)
			for i := byte(0); i < enc.setSize; i++ {
				enc.set[i] = enc.runValue
			}

			enc.runLength = 0
			enc.set[enc.setSize] = uv
			enc.setSize++

			if enc.setSize == 8 {
				return enc.flushBitPacked()
			}
			return nil
		}

		// Flush the run.
		//
		// TODO(rfratto): We need to make sure enc.runLength is less than 2^63-1.
		if err := enc.flushRLE(); err != nil {
			return err
		}

		// Start a new run.
		enc.runValue = uv
		enc.runLength = 1
		return nil

	case enc.setSize > 0:
		enc.set[enc.setSize] = uv
		enc.setSize++

		if enc.setSize == 8 {
			return enc.flushBitPacked()
		}
		return nil

	default:
		panic("rle: invalid state")
	}
}

// Flush writes any remaining values to the underlying writer.
func (enc *Encoder) Flush() error {
	// We always flush using RLE. If we were in the middle of bitpacking, we
	// don't have 8 values yet; if we did, they would've been flushed on a call
	// to Encode.
	return enc.flushRLE()
}

// flushBitPacked flushes the current bit-packed set to a buffer for
// accumulating runs. If the buffer is full, we flush the buffer immediately to
// the underlying writer.
func (enc *Encoder) flushBitPacked() error {
	if enc.setSize != 8 {
		panic("rle: flushBitPacked called with less than 8 values")
	}

	// Detect width.
	var width int
	for i := 0; i < int(enc.setSize); i++ {
		if bitLength := bits.Len64(enc.set[i]); bitLength > width {
			width = bitLength
		}
	}

	// Write out the bit-packed values. Bitpacking 8 values of bit width N always
	// requires exactly N bytes.
	//
	// Each value is packed from the least significant bit of each byte to the
	// most significant bit, while still retaining the order of bits from the
	// original value.
	//
	// How to perform this bitpacking can be challenging to reason about. I
	// (rfratto) found it easier when considering how output bits map to the
	// input bits.
	//
	// This means that for width == 3:
	//
	//   index:     0   1   2   3   4   5   6   7
	//   dec value: 0   1   2   3   4   5   6   7
	//   bit value: 000 001 010 011 100 101 110 111
	//   bit label: ABC DEF GHI JKL MNO PQR STU VWX
	//
	//   index:     22111000 54443332 77766655
	//   bit value: 10001000 11000110 11111010
	//   bit label: HIDEFABC RMNOJKLG VWXSTUPQ
	//
	// Formatting it as a table better demonstrates the mapping:
	//
	//   Index Labels Input bit  Output byte   Output bit (for byte)
	//   ----- -----  ---------  -----------   ---------------------
	//   0     ABC     2  1  0   0 0 0         2 1 0
	//   1     DEF     5  4  3   0 0 0         5 4 3
	//   2     GHI     8  7  6   1 0 0         0 7 6
	//   3     JKL    11 10  9   1 1 1         3 2 1
	//   4     MNO    14 13 12   1 1 1         6 5 4
	//   5     PQR    17 16 15   2 2 1         1 0 7
	//   6     STU    20 19 18   2 2 2         4 3 2
	//   7     VWX    23 22 21   2 2 2         7 6 5
	//
	// So, for any given output bit, its value originates from:
	//
	//   * enc.set index:       output_bit/width
	//   * enc.set element bit: output_bit%width
	//
	// If there's a much simpler way to understand and do this packing, I'd love
	// to know.
	buf := make([]byte, 0, width)

	for outputByte := 0; outputByte < width; outputByte++ {
		var b byte

		for i := 0; i < 8; i++ {
			outputBit := outputByte*8 + i
			inputIndex := outputBit / width
			inputBit := outputBit % width

			// Set the bit in the byte.
			if enc.set[inputIndex]&(1<<inputBit) != 0 {
				b |= 1 << i
			}
		}

		buf = append(buf, b)
	}

	// Append the set to our buffer. It can only fail in two scenarios:
	//
	// 1. The width changed.
	// 2. The buffer is full.
	//
	// In either case, we want to flush and try again.
	for range 2 {
		if err := enc.buf.AppendSet(width, buf); err == nil {
			break
		}

		if err := enc.buf.Flush(enc.w); err != nil {
			return err
		}
	}

	enc.setSize = 0
	return nil
}

func (enc *Encoder) flushRLE() error {
	// Flush anything in the bitpack buffer.
	if err := enc.buf.Flush(enc.w); err != nil {
		return err
	}

	switch {
	case enc.runLength > 0:
		if enc.runLength > maxRunLength {
			return fmt.Errorf("run length too large")
		}

		if err := encoding.WriteUvarint(enc.w, enc.runLength<<1); err != nil {
			return err
		}
		if err := encoding.WriteUvarint(enc.w, enc.runValue); err != nil {
			return err
		}

		enc.runLength = 0
		enc.runValue = 0
		return nil

	case enc.setSize > 0:
		// Flush the longest run possible.
		for off := byte(0); off < enc.setSize; {
			var (
				val = enc.set[off]
				run = byte(1)
			)

			for j := off + 1; j < enc.setSize; j++ {
				if enc.set[j] != val {
					break
				}
				run++
			}

			off += run

			// Header.
			if err := encoding.WriteUvarint(enc.w, uint64(run<<1)); err != nil {
				return err
			}

			// Value.
			if err := encoding.WriteUvarint(enc.w, val); err != nil {
				return err
			}
		}

		enc.setSize = 0
		return nil

	default:
		return nil
	}
}

// Reset resets the encoder to write to w.
func (enc *Encoder) Reset(w encoding.Writer) {
	enc.w = w
	enc.runValue = 0
	enc.runLength = 0
	enc.setSize = 0
	enc.buf.Reset()
}

// A Decoder decodes int64s from a hybrid run-length encoded format.
type Decoder struct {
	r encoding.Reader

	// Like [Encoder], Decoder is a basic state machine with four states:
	//
	// READY          The default state; it needs to pull a new run header and
	//                change states.
	//
	// RLE            The decoder is in the middle of a RLE-encoded run. Active
	//                when runLength>0. runLength decreases by one each time
	//                Decode is called, and runValue is returned.
	//
	// BITPACK-READY  The Decoder is ready to read a new bit-packed set. Active
	//                when sets>0 an setSize==0. The Decoder needs to read the
	//                next set, update setSize and decrement sets.
	//
	// BITPACK-SET    The decoder is in the middle of a bitpacked set. Active
	//                when setSize>0. setSize decreases by one each time Decode
	//                is called, and the next bitpacked value in the set is
	//                returned.
	//
	// The decoder will always start in the READY state, and the header it pulls
	// next determines its next state. After fully consuming a run, it reverts
	// back to READY.

	runValue  uint64 // Value of the current run.
	runLength uint64 // Number of values left in current run.

	sets     int    // Number of sets left to read, each of which contains 8 elements.
	setWidth int    // Number of bits to use for each value. Must be no greater than 64.
	setSize  byte   // Number of values left in the current bit-packed set.
	set      []byte // Current set of bit-packed values.
}

var _ page.ValueDecoder = (*Decoder)(nil)

// NewDecoder creates a Decoder that reads encoded numbers from r. The width
// argument specifies the maximum number of bits to use for each value.
// NewDecoder panics if width is greater than 64.
func NewDecoder(r encoding.Reader) *Decoder {
	return &Decoder{r: r}
}

// ValueType returns [datasetmd.VALUE_TYPE_UINT64].
func (dec *Decoder) ValueType() datasetmd.ValueType {
	return datasetmd.VALUE_TYPE_UINT64
}

// EncodingType returns [datasetmd.ENCODING_TYPE_BITMAP].
func (dec *Decoder) EncodingType() datasetmd.EncodingType {
	return datasetmd.ENCODING_TYPE_BITMAP
}

// Decode reads the next value from the decoder.
func (dec *Decoder) Decode() (page.Value, error) {
	// See comment in [Decoder] for the state machine.

NextState:
	switch {
	case dec.runLength == 0 && dec.sets == 0 && dec.setSize == 0: // READY
		if err := dec.readHeader(); err != nil {
			return page.Uint64Value(0), fmt.Errorf("reading header: %w", err)
		}
		goto NextState

	case dec.runLength > 0: // RLE
		dec.runLength--
		return page.Uint64Value(dec.runValue), nil

	case dec.sets > 0 && dec.setSize == 0: // BITPACK-READY
		// BITPACK-READY; load the next bitpack set.
		if err := dec.nextBitpackSet(); err != nil {
			return page.Uint64Value(0), err
		}
		goto NextState

	case dec.setSize > 0: // BITPACK-SET
		elem := 8 - dec.setSize

		var val uint64
		for bit := 0; bit < dec.setWidth; bit++ {
			// Read bit 0 of element index i.
			// element index i is byte i*8/width.

			index := (int(elem)*dec.setWidth + bit) / 8
			offset := (int(elem)*dec.setWidth + bit) % 8
			bitValue := dec.set[index] & (1 << offset) >> offset

			val |= uint64(bitValue) << bit
		}

		dec.setSize--
		return page.Uint64Value(val), nil
	default:
		panic("rle: invalid state")
	}
}

func (dec *Decoder) readHeader() error {
	// Read the next uvarint.
	header, err := encoding.ReadUvarint(dec.r)
	if err != nil {
		return err
	}

	if header&1 == 1 {
		// Start of a bit-packed set.
		dec.sets = int(header >> 7)
		dec.setWidth = int((header>>1)&0x3f) + 1
		dec.setSize = 0 // Sets will be loaded in [Decoder.nextBitpackSet].
		dec.set = make([]byte, dec.setWidth)
	} else {
		// RLE run.
		runLength := header >> 1

		val, err := encoding.ReadUvarint(dec.r)
		if err != nil {
			return err
		}

		dec.runLength = runLength
		dec.runValue = val
	}

	return nil
}

// nextBitpackSet loads the next bitpack set and decrements the sets counter.
func (dec *Decoder) nextBitpackSet() error {
	if dec.sets == 0 {
		return fmt.Errorf("rle: no bit-packed sets remaining")
	}

	// Bit-packed run.
	count, err := dec.r.Read(dec.set)
	if err != nil {
		return err
	} else if count != dec.setWidth {
		return fmt.Errorf("rle: bit-packed run too short")
	}
	dec.setSize = 8 // Always 8 elements in each set.
	dec.sets--
	return nil
}

// Reset resets the decoder to read from r.
func (dec *Decoder) Reset(r encoding.Reader) {
	dec.r = r
	dec.runValue = 0
	dec.runLength = 0
	dec.sets = 0
	dec.setWidth = 0
	dec.setSize = 0
	dec.set = nil
}
