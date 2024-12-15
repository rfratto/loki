package dataset_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/delta"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/plain"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/stretchr/testify/require"
)

func TestDataBuffer_String(t *testing.T) {
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

	opts := dataset.BufferOptions{
		PageSizeHint: 1024,
		Compression:  datasetmd.COMPRESSION_TYPE_ZSTD,
	}
	b := dataset.NewBuffer(opts, func(w encoding.Writer) page.Encoder[string] {
		return plain.NewStringEncoder(w)
	})

	for _, s := range in {
		require.True(t, b.Append(s))
	}

	page, err := b.Flush()
	require.NoError(t, err)
	require.Equal(t, datasetmd.VALUE_TYPE_STRING, page.Value)

	t.Log("Uncompressed size: ", page.UncompressedSize)
	t.Log("Compressed size: ", page.CompressedSize)
}

func TestDataBuffer_Number(t *testing.T) {
	opts := dataset.BufferOptions{
		PageSizeHint: 1_500_000,
		Compression:  datasetmd.COMPRESSION_TYPE_NONE,
	}
	buf := dataset.NewBuffer(opts, func(w encoding.Writer) page.Encoder[int64] {
		return delta.NewEncoder(w)
	})

	ts := time.Now().UTC()
	for buf.Append(ts.UnixNano()) {
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
