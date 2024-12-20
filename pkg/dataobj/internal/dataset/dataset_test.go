package dataset_test

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	in := []string{
		"hello, world!",
		"",
		"this is a test of the emergency broadcast system",
		"this is only a test",
		"if this were a real emergency, you would be instructed to panic",
		"but it's not, so don't",
		"",
		"this concludes the test",
		"thank you for your cooperation",
		"goodbye",
	}

	column, err := dataset.NewColumn("", page.BuilderOptions{
		// We want to set page size hint low enough to create more than one page,
		// but high enough that we'll have pages with more than one row.
		PageSizeHint: 10,

		Value:       datasetmd.VALUE_TYPE_STRING,
		Encoding:    datasetmd.ENCODING_TYPE_PLAIN,
		Compression: datasetmd.COMPRESSION_TYPE_ZSTD,
	})
	require.NoError(t, err)

	for i, s := range in {
		require.NoError(t, column.Append(i, page.StringValue(s)))
	}
	column.Flush()
	assert.Greater(t, len(column.Pages()), 1, "expected more than one page")

	t.Log("Iterating over pages:", len(column.Pages()))
	for i, page := range column.Pages() {
		t.Logf("  Page %d: rows=%d, size=%d", i, page.RowCount, page.CompressedSize)
	}

	// To make sure row numbers are emitted correctly (especially across page
	// boundaries), assign directly to the actual slice based on row instead of
	// appending to it.
	//
	// That way, in == actual if and only if the row numbers were correct.
	actual := make([]string, len(in))

	var row int

	for _, p := range column.Pages() {
		for val, err := range page.Iter(p) {
			require.NoError(t, err)

			if !val.IsNil() && val.Type() != datasetmd.VALUE_TYPE_STRING {
				assert.Fail(t, "unexpected value type", "row index %d, expected string, got %s", row, val.Type())
				continue
			}

			if !val.IsNil() {
				actual[row] = val.String()
			}
			row++
		}
	}

	require.Equal(t, in, actual)
}
