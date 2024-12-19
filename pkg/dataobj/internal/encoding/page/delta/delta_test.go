package delta_test

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/delta"
	"github.com/stretchr/testify/require"
)

func Fuzz(f *testing.F) {
	f.Add(int64(775972800), 10)
	f.Add(int64(758350800), 25)

	f.Fuzz(func(t *testing.T, seed int64, count int) {
		if count <= 0 {
			t.Skip()
		}

		rnd := rand.New(rand.NewSource(seed))

		var buf bytes.Buffer

		var (
			enc = delta.NewEncoder(&buf)
			dec = delta.NewDecoder(&buf)
		)

		var numbers []int64
		for i := 0; i < count; i++ {
			v := rnd.Int63()
			numbers = append(numbers, v)
			require.NoError(t, enc.Encode(page.Int64Value(v)))
		}

		var actual []int64
		for i := 0; i < count; i++ {
			v, err := dec.Decode()
			require.NoError(t, err)
			actual = append(actual, v.Int64())
		}

		require.Equal(t, numbers, actual)
	})
}

func Benchmark_Encoder_Encode(b *testing.B) {
	b.Run("Sequential", func(b *testing.B) {
		enc := delta.NewEncoder(encoding.Discard)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = enc.Encode(page.Int64Value(int64(i)))
		}
	})

	b.Run("Largest delta", func(b *testing.B) {
		enc := delta.NewEncoder(encoding.Discard)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				_ = enc.Encode(page.Int64Value(0))
			} else {
				_ = enc.Encode(page.Int64Value(math.MaxInt64))
			}
		}
	})

	b.Run("Random", func(b *testing.B) {
		rnd := rand.New(rand.NewSource(0))
		enc := delta.NewEncoder(encoding.Discard)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = enc.Encode(page.Int64Value(rnd.Int63()))
		}
	})
}

func Benchmark_Encoder_Decode(b *testing.B) {
	b.Run("Sequential", func(b *testing.B) {
		var buf bytes.Buffer

		var (
			enc = delta.NewEncoder(&buf)
			dec = delta.NewDecoder(&buf)
		)

		for i := 0; i < b.N; i++ {
			_ = enc.Encode(page.Int64Value(int64(i)))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = dec.Decode()
		}
	})

	b.Run("Largest delta", func(b *testing.B) {
		var buf bytes.Buffer

		var (
			enc = delta.NewEncoder(&buf)
			dec = delta.NewDecoder(&buf)
		)

		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				_ = enc.Encode(page.Int64Value(0))
			} else {
				_ = enc.Encode(page.Int64Value(math.MaxInt64))
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = dec.Decode()
		}
	})

	b.Run("Random", func(b *testing.B) {
		rnd := rand.New(rand.NewSource(0))

		var buf bytes.Buffer

		var (
			enc = delta.NewEncoder(&buf)
			dec = delta.NewDecoder(&buf)
		)

		for i := 0; i < b.N; i++ {
			_ = enc.Encode(page.Int64Value(rnd.Int63()))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = dec.Decode()
		}
	})
}

func Test(t *testing.T) {
	numbers := []int64{
		1234,
		543,
		2345,
		1432,
	}

	var buf bytes.Buffer

	var (
		enc = delta.NewEncoder(&buf)
		dec = delta.NewDecoder(&buf)
	)

	for _, num := range numbers {
		require.NoError(t, enc.Encode(page.Int64Value(num)))
	}

	var actual []int64
	for range len(numbers) {
		v, err := dec.Decode()
		require.NoError(t, err)
		actual = append(actual, v.Int64())
	}

	require.Equal(t, numbers, actual)
}
