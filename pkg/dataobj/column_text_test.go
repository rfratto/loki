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
	p := memTextPage{maxPageSizeBytes: 1_500_000}

	input := []struct {
		row  int
		text string
	}{
		{0, "hello"},
		{2, "world"},
		{5, "!"},
	}
	for _, in := range input {
		require.True(t, p.Append(in.row, in.text))
	}

	require.Equal(t, 6, p.count)

	expect := []string{"hello", "", "world", "", "", "!"}

	var actual []string
	for text, err := range p.Iter() {
		require.NoError(t, err)
		actual = append(actual, text)
	}

	require.Equal(t, expect, actual)
}

func Test_textPage_Backfill(t *testing.T) {
	p := memTextPage{maxPageSizeBytes: 1_500_000}

	require.True(t, p.Append(0, "Hello"))
	require.True(t, p.Backfill(5))

	require.Equal(t, 6, p.count)

	require.True(t, p.Append(6, "Goodbyte"))

	require.Equal(t, 7, p.count)
}
