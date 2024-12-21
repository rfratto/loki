package encoder_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/decoder"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoder"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/logstreams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/logstreamsmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	// TODO(rfratto): replace this with a test that:
	//
	// 1. Uses the streams package to create pages with real data.
	// 2. Uses the decoder package to read the data back and validate it.

	obj := encoder.New()

	streamsSection, err := obj.OpenStreams()
	require.NoError(t, err)

	streamID := idFromString(`{env="qa"}`)
	stream, err := streamsSection.OpenStream(streamID)
	require.NoError(t, err)

	col, err := stream.OpenColumn(logstreams.ColumnInfo{
		Name:             "foo",
		Type:             logstreamsmd.COLUMN_TYPE_METADATA,
		RowsCount:        2,
		CompressedSize:   10,
		UncompressedSize: 10,

		CompressionType: logstreamsmd.COMPRESSION_TYPE_NONE,

		Statistics: nil,
	})
	require.NoError(t, err)

	pages := []logstreams.Page{{
		UncompressedSize: 5,
		CompressedSize:   5,
		CRC32:            0,
		RowCount:         1,

		Compression: logstreamsmd.COMPRESSION_TYPE_NONE,
		Encoding:    logstreamsmd.ENCODING_TYPE_PLAIN,

		Data: []byte("hello"),
	}, {
		UncompressedSize: 5,
		CompressedSize:   5,
		CRC32:            0,
		RowCount:         1,

		Compression: logstreamsmd.COMPRESSION_TYPE_NONE,
		Encoding:    logstreamsmd.ENCODING_TYPE_PLAIN,

		Data: []byte("world"),
	}}
	for _, page := range pages {
		require.NoError(t, col.AppendPage(page))
	}

	require.NoError(t, col.Close())
	require.NoError(t, stream.Close())
	require.NoError(t, streamsSection.Close())

	var buf bytes.Buffer
	sz, err := obj.WriteTo(&buf)
	require.NoError(t, err)
	require.Greater(t, sz, int64(0))
	require.Equal(t, sz, int64(buf.Len()))

	// Additional tests with the decoder.
	{
		dec := decoder.ReadSeekerDecoder(bytes.NewReader(buf.Bytes()))

		sections, err := dec.Sections(context.Background())
		require.NoError(t, err)
		require.Len(t, sections, 1)
		require.Equal(t, sections[0].Type, filemd.SECTION_TYPE_LOG_STREAMS)

		streamsDec := dec.StreamsDecoder()

		streams, err := streamsDec.Streams(context.Background(), sections[0])
		require.NoError(t, err)
		require.Len(t, streams, 1)
		require.True(t, streamID.Equal(streams[0].Identifier), "Expected %v and %v to be equal", streamID, streams[0].Identifier)
		require.Equal(t, streams[0].UncompressedSize, uint32(10))
		require.Equal(t, streams[0].CompressedSize, uint32(10))

		columns, err := streamsDec.Columns(context.Background(), streams[0])
		require.NoError(t, err)
		require.Len(t, columns, 1)
		require.Equal(t, columns[0].Name, "foo")
		require.Equal(t, columns[0].Type, logstreamsmd.COLUMN_TYPE_METADATA)
		require.Equal(t, columns[0].RowsCount, uint32(2))
		require.Equal(t, columns[0].UncompressedSize, uint32(10))
		require.Equal(t, columns[0].CompressedSize, uint32(10))
		require.Equal(t, columns[0].Compression, logstreamsmd.COMPRESSION_TYPE_NONE)
		require.Nil(t, columns[0].Statistics)

		readPages, err := allPages(streamsDec, columns[0])
		require.NoError(t, err)
		require.Equal(t, pages, readPages)
	}
}

func allPages(dec decoder.StreamsDecoder, col *logstreamsmd.ColumnInfo) ([]logstreams.Page, error) {
	headers, err := dec.Pages(context.Background(), col)
	if err != nil {
		return nil, err
	}
	return result.Collect(dec.ReadPages(context.Background(), headers))
}

func idFromString(s string) logstreamsmd.StreamIdentifier {
	lbls, err := parser.ParseMetric(s)
	if err != nil {
		panic(err)
	}

	var id logstreamsmd.StreamIdentifier
	for _, lbl := range lbls {
		id.Labels = append(id.Labels, &logstreamsmd.StreamIdentifier_Label{
			Name:  lbl.Name,
			Value: lbl.Value,
		})
	}
	return id
}
