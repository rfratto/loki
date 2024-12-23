package dataset_test

import (
	"context"
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	var (
		in     = []int64{1, 5, 3, 2, 9, 6, 8, 4, 7}
		expect = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	)

	column, err := dataset.NewColumnBuilder("", page.BuilderOptions{
		PageSizeHint: 1, // Generate a ton of pages.

		Value:       datasetmd.VALUE_TYPE_INT64,
		Encoding:    datasetmd.ENCODING_TYPE_DELTA,
		Compression: datasetmd.COMPRESSION_TYPE_NONE,
	})
	require.NoError(t, err)

	for i, v := range in {
		require.NoError(t, column.Append(i, page.Int64Value(v)))
	}
	column.Flush()

	dset := dataset.FromBuilders([]*dataset.ColumnBuilder{column})
	dsetColumns, err := result.Collect(dset.ListColumns(context.Background()))
	require.NoError(t, err)

	dset, err = dataset.Sort(context.Background(), dset, []dataset.Column{dsetColumns[0]}, 1024)
	require.NoError(t, err)

	newColumns, err := result.Collect(dset.ListColumns(context.Background()))
	require.NoError(t, err)

	var (
		actual []int64

		scanner = dataset.NewScanner(dset, newColumns)
	)
	for entry := range scanner.Iter(context.Background()) {
		entry, err := entry.Value()
		require.NoError(t, err)

		actual = append(actual, entry[0].Value.Int64())
	}

	require.Equal(t, expect, actual)
}
