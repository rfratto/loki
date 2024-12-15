// Package rle encodes and decodes a hybrid run-length encoding format for
// numbers up to 64 bits wide.
//
// A set of values is either run-length encoded or bit-packed. Run-length
// encoding is used for runs of 8 or more values. Shorter runs are accumulated
// into a set of 8 values and are bit-packed, combining multiple smaller runs
// together. If encoding ends before accumulating 8 values to bit-pack, the
// remaining values are run-length encoded.
//
// # Format
//
// Our format is a slight modification of the Parquet format to support longer
// runs and more closely match the behaviour of parquet-go. The EBNF grammar is
// as follows:
//
//	rle_hybrid         = run+;
//	run                = bit_packed_run | rle_run;
//	bit_packed_run     = bit_packed_header bit_packed_values;
//	bit_packed_header  = (* uvarint((bit_packed_run_len / 8) << 1) | 1 *)
//	bit_packed_run_len = (* 8 *)
//	bit_packed_values  = (* least significant bit of each byte to most significant bit of each byte *)
//	rle_run            = rle_header repeated_value;
//	rle_header         = (* uvarint(rle_run_len << 1) *)
//	rle_run_len        = (* value between 1 and 2^63-1, inclusive *)
//	repeated_value     = (* repeated value encoded as varint *)
//
// Where this differs from Parquet:
//
//   - In Parquet, bit_packed_header includes the run length of bit-packed values
//     divided by 8 (as each bitpack always holds exactly 8 values at a time).
//     However, as we write our encoders for streaming, we only encode one run of
//     bitpacked values at a time. This means our bit_packed_header is always
//     0b11, at the cost of one extra byte per bitpacked run.
//
//   - For simplicity, repeated_value is encoded as varint rather than flushing
//     the value in its entirety.
//
//   - To facilitate streaming, we don't prepend the length of all bytes
//     written. Callers may choose to prepend the length. Without the length,
//     readers must take caution to not read past the end of the RLE sequence
//     by knowing exactly how many values were encoded.
package rle

import (
	"fmt"
	"math"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
)

const maxRunLength uint64 = 1<<63 - 1 // 2^63-1

// An Encoder encodes int64s to a hybrid run-length encoded format.
type Encoder struct {
	w     encoding.Writer
	width int // Number of bits to use for each value. Must be no greater than 64.

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

	runValue  int64  // Value in the current run.
	runLength uint64 // Length of the current run.

	set     [8]int64 // Set of bit-packed values.
	setSize byte     // Current number of elements in set.
}

var _ page.Encoder[int64] = (*Encoder)(nil)

// NewEncoder creates an Encoder that writes encoded numbers to w. The width
// argument specifies the maximum number of bits to use for each value.
// NewEncoder panics if width is greater than 64.
func NewEncoder(w encoding.Writer, width int) *Encoder {
	if width > 64 {
		panic("rle: width must be no greater than 64")
	}

	return &Encoder{w: w, width: width}
}

// Encode appends a new value to the encoder. Encode returns an error if the
// provided value is too large to fit in the specified width, or if flushing to
// the underlying writer fails.
//
// Runs are accumulated into memory until the run ends. Runs longer than 7
// values are immediately flushed. Shorter runs are converted to bitpacking and
// flushed once there are 8 values.
//
// Call [Encoder.Flush] to end the run and flush any remaining values.
func (enc *Encoder) Encode(v int64) error {
	if max := enc.maxSize(); v > max {
		return fmt.Errorf("value %d is too large for width %d", v, enc.width)
	}

	switch {
	case enc.runLength == 0 && enc.setSize == 0: // Start a new run.
		enc.runValue = v
		enc.runLength = 1
		return nil

	case enc.runLength > 0 && v == enc.runValue: // Continue the run.
		enc.runLength++

		// If we hit the maximum run length, we'll flush the run immediately.
		if enc.runLength == maxRunLength {
			return enc.flushRLE()
		}
		return nil

	case enc.runLength > 0 && v != enc.runValue:
		// The run ended. If the run lasted less than 8 values, we switch to
		// bitpacking. Copy over the old run value and add the new value.
		if enc.runLength < 8 {
			enc.setSize = byte(enc.runLength)
			for i := byte(0); i < enc.setSize; i++ {
				enc.set[i] = enc.runValue
			}

			enc.runLength = 0
			enc.set[enc.setSize] = v
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
		enc.runValue = v
		enc.runLength = 1
		return nil

	case enc.setSize > 0:
		enc.set[enc.setSize] = v
		enc.setSize++

		if enc.setSize == 8 {
			return enc.flushBitPacked()
		}
		return nil

	default:
		panic("rle: invalid state")
	}
}

func (enc *Encoder) maxSize() int64 {
	if enc.width == 64 {
		return math.MaxInt64
	}
	return (1 << enc.width) - 1
}

// Flush writes any remaining values to the underlying writer.
func (enc *Encoder) Flush() error {
	// We always flush using RLE. If we were in the middle of bitpacking, we
	// don't have 8 values yet; if we did, they would've been flushed on a call
	// to Encode.
	return enc.flushRLE()
}

func (enc *Encoder) flushBitPacked() error {
	if enc.setSize != 8 {
		panic("rle: flushBitPacked called with less than 8 values")
	}

	// Write the bit-packed header. (0b11)
	if err := enc.w.WriteByte(1<<1 | 1); err != nil {
		return err
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
	// This means that for enc.width == 3:
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
	for outputByte := 0; outputByte < enc.width; outputByte++ {
		var b byte

		for i := 0; i < 8; i++ {
			outputBit := outputByte*8 + i
			inputIndex := outputBit / enc.width
			inputBit := outputBit % enc.width

			// Set the bit in the byte.
			if enc.set[inputIndex]&(1<<inputBit) != 0 {
				b |= 1 << i
			}
		}

		if err := enc.w.WriteByte(b); err != nil {
			return err
		}
	}

	enc.setSize = 0
	return nil
}

func (enc *Encoder) flushRLE() error {
	switch {
	case enc.runLength > 0:
		if enc.runLength > maxRunLength {
			return fmt.Errorf("run length too large")
		}

		if err := encoding.WriteUvarint(enc.w, uint64(enc.runLength<<1)); err != nil {
			return err
		}
		if err := encoding.WriteVarint(enc.w, enc.runValue); err != nil {
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
			if err := encoding.WriteVarint(enc.w, val); err != nil {
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
}

// A Decoder decodes int64s from a hybrid run-length encoded format.
type Decoder struct {
	r     encoding.Reader
	width int // Number of bits to use for each value. Must be no greater than 64.

	// Like [Encoder], Decoder is a basic state machine with three states:
	//
	// READY    The default state; it needs to pull a new run header and change
	//          states.
	//
	// RLE      The decoder is in the middle of a RLE-encoded run. Active when
	//          runLength>0. runLength decreases by one each time Decode is
	//          called, and runValue is returned.
	//
	// BITPACK  The decoder is in the middle of a bitpacked run. Active when
	//          setSize>0. setSize decreases by one each time Decode is called,
	//          and the next bitpacked value in the set is returned.
	//
	// The decoder will always start in the READY state, and the header it pulls
	// next determines its next state. After fully consuming a run, it reverts
	// back to READY.

	runValue  int64  // Value of the current run.
	runLength uint64 // Number of values left in current run.

	set     []byte // Bit-packed values.
	setSize byte   // Number of values left in the current bit-packed set.
}

var _ page.Decoder[int64] = (*Decoder)(nil)

// NewDecoder creates a Decoder that reads encoded numbers from r. The width
// argument specifies the maximum number of bits to use for each value.
// NewDecoder panics if width is greater than 64.
func NewDecoder(r encoding.Reader, width int) *Decoder {
	if width > 64 {
		panic("rle: width must be no greater than 64")
	}

	return &Decoder{
		r:     r,
		width: width,
		set:   make([]byte, width),
	}
}

// Decode reads the next value from the decoder.
func (dec *Decoder) Decode() (int64, error) {
	// Decoders need quite a bit of state to keep track of where they are.
	//
	// Initially no data is buffered and we need to read the next uvarint. If
	// val&1 == 1, we have a bit-packed run, and we need to read width bytes.
	//
	// Otherwise, if val&1 == 0, we have an RLE run. We need to read the next
	// uvarint to get the value, and check the run length from val>>1.
	//
	// This means we need to keep track of:
	//
	// * Whether we're in a bit-packed run or RLE run.
	// * The current index (0-7) in a bit-packed run.
	// * The run length in an RLE run.
	// * The current index in an RLE run.
	//
	// We should be able to reuse some of these. len and off can be used to tell
	// us how many values are available for reading and how many values we've
	// read so far.

	if dec.runLength == 0 && dec.setSize == 0 {
		// READY; read the next header.
		if err := dec.readHeader(); err != nil {
			return 0, err
		}
	}

	switch {
	case dec.runLength > 0: // RLE run.
		dec.runLength--
		return dec.runValue, nil

	case dec.setSize > 0: // Bit-packed run.
		elem := 8 - dec.setSize

		var val int64
		for bit := 0; bit < dec.width; bit++ {
			// Read bit 0 of element index i.
			// element index i is byte i*8/width.

			index := (int(elem)*dec.width + bit) / 8
			offset := (int(elem)*dec.width + bit) % 8
			bitValue := dec.set[index] & (1 << offset) >> offset

			val |= int64(bitValue) << bit
		}

		dec.setSize--
		return val, nil
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
		// Bit-packed run.
		count, err := dec.r.Read(dec.set)
		if err != nil {
			return err
		} else if count != dec.width {
			return fmt.Errorf("rle: bit-packed run too short")
		}

		dec.setSize = 8 // Always 8 elements in a bit-packed run.
	} else {
		// RLE run.
		runLength := header >> 1

		val, err := encoding.ReadVarint(dec.r)
		if err != nil {
			return err
		}

		dec.runLength = runLength
		dec.runValue = val
	}

	return nil
}

// Reset resets the decoder to read from r.
func (dec *Decoder) Reset(r encoding.Reader) {
	dec.r = r
	dec.runValue = 0
	dec.runLength = 0
	dec.setSize = 0
}
