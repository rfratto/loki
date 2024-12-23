package obj_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/obj"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
	"github.com/stretchr/testify/require"
)

// Test performs an end-to-end test of the obj package:
//
//  1. Create two columns populated with data.
//  2. Create a data object composed of those two columns.
//  3. Read back the columns and assert it against the original data.
func Test(t *testing.T) {
	opts := page.BuilderOptions{
		PageSizeHint: 10,
		Value:        datasetmd.VALUE_TYPE_STRING,
		Encoding:     datasetmd.ENCODING_TYPE_PLAIN,
		Compression:  datasetmd.COMPRESSION_TYPE_NONE,
	}

	columnFirstName, err := dataset.NewColumnBuilder("first_name", opts)
	require.NoError(t, err)

	columnLastName, err := dataset.NewColumnBuilder("last_name", opts)
	require.NoError(t, err)

	type name struct {
		FirstName string
		LastName  string
	}

	in := []name{
		{"Faith", "Slater"},
		{"Theresa", "Jackson"},
		{"Colin", ""},
		{"Diane", "Black"},
		{"Wendy", "Churchill"},
		{"", "Metcalfe"},
		{"Penelope", ""},
		{"Melanie", "Hart"},
		{"Julian", "Terry"},
		{"Natalie", "Walker"},
		{"Robert", ""},
	}

	// Populate our columns.
	{
		for i, pair := range in {
			if pair.FirstName != "" {
				require.NoError(t, columnFirstName.Append(i, page.StringValue(pair.FirstName)))
			}
			if pair.LastName != "" {
				require.NoError(t, columnLastName.Append(i, page.StringValue(pair.LastName)))
			}
		}

		columnFirstName.Backfill(len(in))
		columnFirstName.Flush()
		columnLastName.Backfill(len(in))
		columnLastName.Flush()
	}

	// Encode data into an object, written to buf.
	var buf bytes.Buffer
	{
		enc, err := obj.NewEncoder(&buf)
		require.NoError(t, err)

		streamsEnc, err := enc.OpenStreams()
		require.NoError(t, err)

		firstNameEnc, err := streamsEnc.OpenColumn(streamsmd.COLUMN_TYPE_LABEL, columnFirstName.Info())
		require.NoError(t, err)
		for _, p := range columnFirstName.Pages() {
			require.NoError(t, firstNameEnc.AppendPage(p))
		}
		require.NoError(t, firstNameEnc.Close())

		lastNameEnc, err := streamsEnc.OpenColumn(streamsmd.COLUMN_TYPE_LABEL, columnLastName.Info())
		require.NoError(t, err)
		for _, p := range columnLastName.Pages() {
			require.NoError(t, lastNameEnc.AppendPage(p))
		}
		require.NoError(t, lastNameEnc.Close())

		require.NoError(t, streamsEnc.Close())
		require.NoError(t, enc.Close())
	}

	// Read back our data.
	actual := make([]name, len(in))
	{
		dec := obj.ReadSeekerDecoder(bytes.NewReader(buf.Bytes()))
		sections, err := dec.Sections(context.TODO())
		require.NoError(t, err)
		require.Len(t, sections, 1)

		streamsDec := dec.StreamsDecoder()
		dset, err := obj.StreamsDataset(streamsDec, sections[0])
		require.NoError(t, err)

		columns, err := result.Collect(dset.ListColumns(context.TODO()))
		require.NoError(t, err)
		require.Len(t, columns, 2)

		scan := dataset.NewScanner(dset, columns)
		for result := range scan.Iter(context.TODO()) {
			rowEntry, err := result.Value()
			require.NoError(t, err)

			for i, colEntry := range rowEntry {
				if colEntry.Value.IsNil() {
					continue
				}

				switch columns[i].Info().Name {
				case "first_name":
					actual[colEntry.Row].FirstName = colEntry.Value.String()
				case "last_name":
					actual[colEntry.Row].LastName = colEntry.Value.String()
				}
			}
		}
	}

	require.Equal(t, in, actual)
}
