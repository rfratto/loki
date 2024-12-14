package dataset_test

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/stretchr/testify/require"
)

func TestTextBuffer(t *testing.T) {
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

	b := dataset.TextBuffer(dataset.BufferOptions{
		PageSizeHint: 1024,
		Compression:  datasetmd.COMPRESSION_TYPE_ZSTD,
	})

	for _, s := range in {
		require.True(t, b.Append(s))
	}

	page, err := b.Flush()
	require.NoError(t, err)

	t.Log("Uncompressed size: ", page.UncompressedSize)
	t.Log("Compressed size: ", page.CompressedSize)
}
