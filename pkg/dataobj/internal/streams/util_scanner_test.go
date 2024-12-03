package streams

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test_byteReader_Peek tests that calling [byteReader.Peek] with a size larger
// than the input buffer returns what data is remaining in the buffer along
// with an EOF.
func Test_byteReader_Peek(t *testing.T) {
	t.Run("partial read", func(t *testing.T) {
		var (
			input  = []byte("hello world")
			skip   = []byte("hello ")
			expect = []byte("world")
			br     = byteReader{buf: input}
		)

		br.Discard(len(skip))

		actual, err := br.Peek(1000)
		require.Equal(t, expect, actual)
		require.Equal(t, err, io.EOF)
	})

	t.Run("empty read", func(t *testing.T) {
		br := byteReader{buf: []byte("hello world")}
		br.Discard(1000)

		actual, err := br.Peek(1000)
		require.Empty(t, actual)
		require.Equal(t, err, io.EOF)
	})
}
