package encoder_test

import (
	"bytes"
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoder"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	// TODO(rfratto): replace this with a test that:
	//
	// 1. Uses the streams package to create pages with real data.
	// 2. Uses the decoder package to read the data back and validate it.

	obj := encoder.New(1_500_000)

	streamsSection, err := obj.OpenStreams()
	require.NoError(t, err)

	stream, err := streamsSection.OpenStream(idFromString(`{env="qa"}`))
	require.NoError(t, err)

	col, err := stream.OpenColumn(streams.ColumnInfo{
		Name:             "foo",
		Type:             streamsmd.COLUMN_TYPE_METADATA,
		RowsCount:        0,
		CompressedSize:   10,
		UncompressedSize: 10,

		CompressionType: streamsmd.COMPRESSION_NONE,

		Statistics: nil,
	})
	require.NoError(t, err)

	err = col.AppendPage(streams.Page{
		UncompressedSize: 5,
		CompressedSize:   5,
		CRC32:            0,

		Compression: streamsmd.COMPRESSION_NONE,
		Encoding:    streamsmd.ENCODING_PLAIN,

		Data: []byte("hello"),
	})
	require.NoError(t, err)

	err = col.AppendPage(streams.Page{
		UncompressedSize: 5,
		CompressedSize:   5,
		CRC32:            0,

		Compression: streamsmd.COMPRESSION_NONE,
		Encoding:    streamsmd.ENCODING_PLAIN,

		Data: []byte("world"),
	})
	require.NoError(t, err)

	require.NoError(t, col.Close())
	require.NoError(t, stream.Close())
	require.NoError(t, streamsSection.Close())

	var buf bytes.Buffer
	sz, err := obj.WriteTo(&buf)
	require.NoError(t, err)
	require.Greater(t, sz, int64(0))
	require.Equal(t, sz, int64(buf.Len()))
}

func idFromString(s string) streamsmd.StreamIdentifier {
	lbls, err := parser.ParseMetric(s)
	if err != nil {
		panic(err)
	}

	var id streamsmd.StreamIdentifier
	for _, lbl := range lbls {
		id.Labels = append(id.Labels, &streamsmd.StreamIdentifier_Label{
			Name:  lbl.Name,
			Value: lbl.Value,
		})
	}
	return id
}
