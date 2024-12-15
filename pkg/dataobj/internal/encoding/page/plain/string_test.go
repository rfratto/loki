package plain_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/plain"
	"github.com/stretchr/testify/require"
)

var testStrings = []string{
	"hello",
	"world",
	"foo",
	"bar",
	"baz",
}

func Benchmark_Append(b *testing.B) {
	enc := plain.NewStringEncoder(encoding.Discard)

	for i := 0; i < b.N; i++ {
		for _, v := range testStrings {
			_ = enc.Encode(v)
		}
	}
}

func Benchmark_Decode(b *testing.B) {
	buf := bytes.NewBuffer(make([]byte, 0, 1024)) // Large enough to avoid reallocations.

	var (
		enc = plain.NewStringEncoder(buf)
		dec = plain.NewStringDecoder(buf)
	)

	for _, v := range testStrings {
		enc.Encode(v)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			var err error
			_, err = dec.Decode()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func Test(t *testing.T) {
	var buf bytes.Buffer

	var (
		enc = plain.NewStringEncoder(&buf)
		dec = plain.NewStringDecoder(&buf)
	)

	for _, v := range testStrings {
		require.NoError(t, enc.Encode(v))
	}

	var out []string

	for {
		str, err := dec.Decode()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(str))
	}

	require.Equal(t, testStrings, out)
}
