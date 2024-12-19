package obj_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/obj"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
	"github.com/stretchr/testify/require"
)

// Test performs an end-to-end test of the obj package:
//
//  1. Create two columns populated with data.
//  2. Create a data object composed of those two columns.
//  3. Read back the columns and assert it against the original data.
func Test(t *testing.T) {
	opts := dataset.BufferOptions{
		PageSizeHint: 10,
		Value:        datasetmd.VALUE_TYPE_STRING,
		Encoding:     datasetmd.ENCODING_TYPE_PLAIN,
		Compression:  datasetmd.COMPRESSION_TYPE_NONE,
	}

	columnFirstName, err := dataset.NewColumn("first_name", opts)
	require.NoError(t, err)

	columnLastName, err := dataset.NewColumn("last_name", opts)
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
		columns, err := streamsDec.Columns(context.TODO(), sections[0])
		require.NoError(t, err)
		require.Len(t, columns, 2)

		for _, column := range columns {
			pages, err := streamsDec.Pages(context.TODO(), column)
			require.NoError(t, err)

			rowOffset := 0

			for p, err := range streamsDec.ReadPages(context.TODO(), pages) {
				require.NoError(t, err)

				for ent, err := range dataset.IterPage(rowOffset, p) {
					require.NoError(t, err)

					if ent.Value.IsNil() {
						continue
					} else if ent.Value.Type() != datasetmd.VALUE_TYPE_STRING {
						require.Fail(t, "unexpected value type", "row index %d, expected string, got %s", ent.Row, ent.Value.Type())
						continue
					}

					switch column.Info.Name {
					case "first_name":
						actual[ent.Row].FirstName = ent.Value.String()
					case "last_name":
						actual[ent.Row].LastName = ent.Value.String()
					}
				}

				rowOffset += p.RowCount
			}
		}
	}

	require.Equal(t, in, actual)
}
