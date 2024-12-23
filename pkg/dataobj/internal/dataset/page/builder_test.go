package page_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	_ "github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page/all" // Import encodings

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

func TestBuilder_String(t *testing.T) {
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

	opts := page.BuilderOptions{
		PageSizeHint: 1024,
		Value:        datasetmd.VALUE_TYPE_STRING,
		Compression:  datasetmd.COMPRESSION_TYPE_ZSTD,
		Encoding:     datasetmd.ENCODING_TYPE_PLAIN,
	}
	b, err := page.NewBuilder(opts)
	require.NoError(t, err)

	for _, s := range in {
		require.True(t, b.Append(page.StringValue(s)))
	}

	page, err := b.Flush()
	require.NoError(t, err)
	require.Equal(t, datasetmd.VALUE_TYPE_STRING, page.Value)

	t.Log("Uncompressed size: ", page.UncompressedSize)
	t.Log("Compressed size: ", page.CompressedSize)
}

func TestBuilder_Number(t *testing.T) {
	opts := page.BuilderOptions{
		PageSizeHint: 1_500_000,
		Value:        datasetmd.VALUE_TYPE_INT64,
		Compression:  datasetmd.COMPRESSION_TYPE_NONE,
		Encoding:     datasetmd.ENCODING_TYPE_DELTA,
	}
	buf, err := page.NewBuilder(opts)
	require.NoError(t, err)

	ts := time.Now().UTC()
	for buf.Append(page.Int64Value(ts.UnixNano())) {
		ts = ts.Add(time.Duration(rand.Intn(5000)) * time.Millisecond)
	}

	page, err := buf.Flush()
	require.NoError(t, err)
	require.Equal(t, datasetmd.VALUE_TYPE_INT64, page.Value)
	require.Equal(t, page.UncompressedSize, page.CompressedSize)

	t.Log("Uncompressed size: ", page.UncompressedSize)
	t.Log("Compressed size: ", page.CompressedSize)
	t.Log("Row count: ", page.RowCount)
}
