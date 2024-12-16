package rle_test

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/rle"
	"github.com/stretchr/testify/require"
)

func Fuzz(f *testing.F) {
	f.Add(int64(775972800), 1, 10)
	f.Add(int64(758350800), 8, 25)
	f.Add(int64(1718425412), 32, 50)
	f.Add(int64(1734130411), 64, 75)

	f.Fuzz(func(t *testing.T, seed int64, width int, count int) {
		if width < 1 || width > 64 {
			t.Skip()
		} else if count <= 0 {
			t.Skip()
		}

		rnd := rand.New(rand.NewSource(seed))

		var buf bytes.Buffer

		var (
			enc = rle.NewEncoder(&buf)
			dec = rle.NewDecoder(&buf)
		)

		var numbers []uint64
		for i := 0; i < count; i++ {
			var mask uint64 = math.MaxUint64
			if width < 64 {
				mask = (1 << width) - 1
			}

			v := uint64(rnd.Int63()) & mask
			numbers = append(numbers, v)
			require.NoError(t, enc.Encode(v))
		}
		require.NoError(t, enc.Flush())

		var actual []uint64
		for i := 0; i < count; i++ {
			v, err := dec.Decode()
			require.NoError(t, err)
			actual = append(actual, v)
		}

		require.Equal(t, numbers, actual)
	})
}

func Benchmark_Encoder_Encode(b *testing.B) {
	b.Run("width=1", func(b *testing.B) { benchmark_Encoder_Encode(b, 1) })
	b.Run("width=3", func(b *testing.B) { benchmark_Encoder_Encode(b, 3) })
	b.Run("width=5", func(b *testing.B) { benchmark_Encoder_Encode(b, 5) })
	b.Run("width=8", func(b *testing.B) { benchmark_Encoder_Encode(b, 8) })
	b.Run("width=32", func(b *testing.B) { benchmark_Encoder_Encode(b, 32) })
	b.Run("width=64", func(b *testing.B) { benchmark_Encoder_Encode(b, 64) })
}

func benchmark_Encoder_Encode(b *testing.B, width int) {
	b.Run("variance=none", func(b *testing.B) {
		var cw countingWriter
		enc := rle.NewEncoder(&cw)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = enc.Encode(1)
		}
		_ = enc.Flush()

		b.ReportMetric(float64(cw.n), "encoded_bytes")
	})

	b.Run("variance=alternating", func(b *testing.B) {
		var cw countingWriter
		enc := rle.NewEncoder(&cw)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = enc.Encode(uint64(i % width))
		}
		_ = enc.Flush()

		b.ReportMetric(float64(cw.n), "encoded_bytes")
	})

	b.Run("variance=random", func(b *testing.B) {
		rnd := rand.New(rand.NewSource(0))

		var cw countingWriter
		enc := rle.NewEncoder(&cw)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = enc.Encode(uint64(rnd.Int63()) % uint64(width))
		}
		_ = enc.Flush()

		b.ReportMetric(float64(cw.n), "encoded_bytes")
	})
}

func Benchmark_Encoder_Decode(b *testing.B) {
	b.Run("width=1", func(b *testing.B) { benchmark_Encoder_Decode(b, 1) })
	b.Run("width=3", func(b *testing.B) { benchmark_Encoder_Decode(b, 3) })
	b.Run("width=5", func(b *testing.B) { benchmark_Encoder_Decode(b, 5) })
	b.Run("width=8", func(b *testing.B) { benchmark_Encoder_Decode(b, 8) })
	b.Run("width=32", func(b *testing.B) { benchmark_Encoder_Decode(b, 32) })
	b.Run("width=64", func(b *testing.B) { benchmark_Encoder_Decode(b, 64) })
}

func benchmark_Encoder_Decode(b *testing.B, width int) {
	b.Run("variance=none", func(b *testing.B) {
		var buf bytes.Buffer

		var (
			enc = rle.NewEncoder(&buf)
			dec = rle.NewDecoder(&buf)
		)

		for i := 0; i < b.N; i++ {
			_ = enc.Encode(1)
		}
		_ = enc.Flush()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = dec.Decode()
		}
	})

	b.Run("variance=alternating", func(b *testing.B) {
		var buf bytes.Buffer

		var (
			enc = rle.NewEncoder(&buf)
			dec = rle.NewDecoder(&buf)
		)

		for i := 0; i < b.N; i++ {
			_ = enc.Encode(uint64(i % width))
		}
		_ = enc.Flush()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = dec.Decode()
		}
	})

	b.Run("variance=random", func(b *testing.B) {
		rnd := rand.New(rand.NewSource(0))
		var buf bytes.Buffer

		var (
			enc = rle.NewEncoder(&buf)
			dec = rle.NewDecoder(&buf)
		)

		for i := 0; i < b.N; i++ {
			_ = enc.Encode(uint64(rnd.Int63()) % uint64(width))
		}
		_ = enc.Flush()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = dec.Decode()
		}
	})
}

type countingWriter struct {
	n int64
}

func (w *countingWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	w.n += int64(n)
	return
}

func (w *countingWriter) WriteByte(c byte) error {
	w.n++
	return nil
}

func Test_RLE(t *testing.T) {
	var buf bytes.Buffer

	var (
		enc = rle.NewEncoder(&buf)
		dec = rle.NewDecoder(&buf)
	)

	count := 1500
	for i := 0; i < count; i++ {
		require.NoError(t, enc.Encode(uint64(1)))
	}
	require.NoError(t, enc.Flush())

	t.Logf("Buffer size: %d", buf.Len())

	for range count {
		v, err := dec.Decode()
		require.NoError(t, err)
		require.Equal(t, uint64(1), v)
	}
}

func Test_Bitpacking(t *testing.T) {
	var buf bytes.Buffer

	var (
		enc = rle.NewEncoder(&buf)
		dec = rle.NewDecoder(&buf)
	)

	expect := []uint64{0, 1, 2, 3, 4, 5, 6, 7}
	for _, v := range expect {
		require.NoError(t, enc.Encode(v))
	}
	require.NoError(t, enc.Flush())

	var actual []uint64
	for range len(expect) {
		v, err := dec.Decode()
		require.NoError(t, err)
		actual = append(actual, v)
	}
	require.NoError(t, enc.Flush())

	require.Equal(t, expect, actual)
}

func Test_Bitpacking_Partial(t *testing.T) {
	var buf bytes.Buffer

	var (
		enc = rle.NewEncoder(&buf)
		dec = rle.NewDecoder(&buf)
	)

	expect := []uint64{0, 1, 2, 3, 4}
	for _, v := range expect {
		require.NoError(t, enc.Encode(v))
	}
	require.NoError(t, enc.Flush())

	var actual []uint64
	for range len(expect) {
		v, err := dec.Decode()
		require.NoError(t, err)
		actual = append(actual, v)
	}

	require.Equal(t, expect, actual)
}
