package plain_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/encoding/plain"
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
	buf := bytes.NewBuffer(make([]byte, 0, 1024)) // Large enough to avoid reallocations.

	for i := 0; i < b.N; i++ {
		buf.Reset()

		for _, v := range testStrings {
			plain.Write(buf, v)
		}
	}
}

func Benchmark_Decode(b *testing.B) {
	buf := bytes.NewBuffer(make([]byte, 0, 1024)) // Large enough to avoid reallocations.
	for _, v := range testStrings {
		plain.Write(buf, v)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			var err error
			_, err = plain.Read(buf)
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
	for _, v := range testStrings {
		plain.Write(&buf, v)
	}

	var out []string

	for {
		str, err := plain.Read(&buf)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(str))
	}

	require.Equal(t, testStrings, out)
}
