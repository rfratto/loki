package encoder_test

import (
	"bytes"
	"context"
	"iter"
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/decoder"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoder"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
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

	obj := encoder.New()

	streamsSection, err := obj.OpenStreams()
	require.NoError(t, err)

	streamID := idFromString(`{env="qa"}`)
	stream, err := streamsSection.OpenStream(streamID)
	require.NoError(t, err)

	col, err := stream.OpenColumn(streams.ColumnInfo{
		Name:             "foo",
		Type:             streamsmd.COLUMN_TYPE_METADATA,
		RowsCount:        2,
		CompressedSize:   10,
		UncompressedSize: 10,

		CompressionType: streamsmd.COMPRESSION_TYPE_NONE,

		Statistics: nil,
	})
	require.NoError(t, err)

	pages := []streams.Page{{
		UncompressedSize: 5,
		CompressedSize:   5,
		CRC32:            0,
		RowCount:         1,

		Compression: streamsmd.COMPRESSION_TYPE_NONE,
		Encoding:    streamsmd.ENCODING_PLAIN,

		Data: []byte("hello"),
	}, {
		UncompressedSize: 5,
		CompressedSize:   5,
		CRC32:            0,
		RowCount:         1,

		Compression: streamsmd.COMPRESSION_TYPE_NONE,
		Encoding:    streamsmd.ENCODING_PLAIN,

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
		require.Equal(t, sections[0].Type, filemd.SECTION_TYPE_STREAMS)

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
		require.Equal(t, columns[0].Type, streamsmd.COLUMN_TYPE_METADATA)
		require.Equal(t, columns[0].RowsCount, uint32(2))
		require.Equal(t, columns[0].UncompressedSize, uint32(10))
		require.Equal(t, columns[0].CompressedSize, uint32(10))
		require.Equal(t, columns[0].Compression, streamsmd.COMPRESSION_TYPE_NONE)
		require.Nil(t, columns[0].Statistics)

		readPages, err := allPages(streamsDec, columns[0])
		require.NoError(t, err)
		require.Equal(t, pages, readPages)
	}
}

func allPages(dec decoder.StreamsDecoder, col *streamsmd.ColumnInfo) ([]streams.Page, error) {
	var pages []streams.Page

	headers, err := dec.Pages(context.Background(), col)
	if err != nil {
		return nil, err
	}

	it := dec.ReadPages(context.Background(), headers)
	for page, err := range it {
		if err != nil {
			return nil, err
		}
		pages = append(pages, page)
	}

	return pages, nil
}

func collect[ValueType any](it iter.Seq2[ValueType, error]) ([]ValueType, error) {
	var values []ValueType
	for value, err := range it {
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
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
