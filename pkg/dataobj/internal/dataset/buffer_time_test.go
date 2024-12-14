package dataset_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
	"github.com/stretchr/testify/require"
)

func TestTimeBuffer(t *testing.T) {
	buf := dataset.TimeBuffer(dataset.BufferOptions{
		PageSizeHint: 1_500_000,
		Compression:  datasetmd.COMPRESSION_TYPE_NONE,
	})

	ts := time.Now().UTC()

	for buf.Append(ts) {
		ts = ts.Add(time.Duration(rand.Intn(5000)) * time.Millisecond)
	}

	page, err := buf.Flush()
	require.NoError(t, err)

	t.Log("Uncompressed size: ", page.UncompressedSize)
	t.Log("Compressed size: ", page.CompressedSize)
	t.Log("Row count: ", page.RowCount)
}
