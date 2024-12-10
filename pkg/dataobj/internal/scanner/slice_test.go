package scanner_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/scanner"
)

// Test_Slice_Peek tests that calling [scanner.Slice.Peek] with a size larger
// than the input buffer returns what data is remaining in the buffer along
// with an EOF.
func Test_Slice_Peek(t *testing.T) {
	t.Run("partial read", func(t *testing.T) {
		var (
			input  = []byte("hello world")
			skip   = []byte("hello ")
			expect = []byte("world")
			s      = scanner.FromSlice(input)
		)

		s.Discard(len(skip))

		actual, err := s.Peek(1000)
		require.Equal(t, expect, actual)
		require.Equal(t, err, io.EOF)
	})

	t.Run("empty read", func(t *testing.T) {
		s := scanner.FromSlice([]byte("hello world"))
		s.Discard(1000)

		actual, err := s.Peek(1000)
		require.Empty(t, actual)
		require.Equal(t, err, io.EOF)
	})
}
