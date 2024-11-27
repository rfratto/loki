package dataobj

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_zero_uvarint(t *testing.T) {
	expected := []byte{0}
	actual := binary.AppendUvarint(nil, 0)
	require.Equal(t, expected, actual)
}

func Test_textPage_Iter(t *testing.T) {
	p := textPage{maxPageSizeBytes: 1_500_000}

	input := []struct {
		row  int
		text string
	}{
		{0, "hello"},
		{2, "world"},
		{3, "!"},
	}
	for _, in := range input {
		require.True(t, p.Append(in.row, in.text))
	}

	expect := []string{"hello", "", "world", "!"}

	var actual []string
	for text, err := range p.Iter() {
		require.NoError(t, err)
		actual = append(actual, text)
	}

	require.Equal(t, expect, actual)
}
