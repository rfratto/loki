package dataobj

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_textColumn(t *testing.T) {
	strings := []string{
		"Hello, world!",
		"Goodbye, world!",
		"Hello, again!",
		"Goodbye, again!",
	}

	col := textColumn{maxPageSizeBytes: 10}
	for i, s := range strings {
		col.Append(i, s)
	}

	// Ensure that we cut at least one page to test iteration; since we set our
	// per-page byte limit pretty low we should be covered here.
	require.Greater(t, len(col.pages), 0, "expected at least one page")

	var actual []string
	for text, err := range col.Iter() {
		require.NoError(t, err)
		actual = append(actual, text)
	}
}

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
