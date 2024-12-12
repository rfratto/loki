package streams

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Benchmark_newTimeColumn(b *testing.B) {
	r := rand.New(rand.NewSource(0))
	c := NewTimestampColumn(4_000)

	nextTS := time.Now()

	for i := 0; i < b.N; i++ {
		c.Append(i, nextTS)

		// Add up to 5000ms of jitter to the next time.
		nextTS = nextTS.Add(time.Duration(r.Intn(5000)) * time.Millisecond)
	}

	b.ReportMetric(float64(c.Count()/len(c.pages)), "ts/page")
	b.ReportMetric(float64(len(c.pages)), "pages")
}

func Test_newTimeColumn_Iter(t *testing.T) {
	col := NewTimestampColumn(4_000)

	r := rand.New(rand.NewSource(0))
	nextTS := time.Now().UTC()

	var expect []time.Time

	for len(col.pages) < 3 {
		col.Append(col.columnRows+col.headRows, nextTS)
		expect = append(expect, nextTS)

		nextTS = nextTS.Add(time.Duration(r.Intn(5000)) * time.Millisecond)
	}

	// Read back times.
	var actual []time.Time
	for ts, err := range col.Iter() {
		require.NoError(t, err)
		actual = append(actual, ts)
	}
	require.Equal(t, expect, actual)
}

// Test_headTimePage_packing checks how many ordered timestamps can be stored
// in 1.5MB pages.
func Test_headTimePage_packing(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	c := &headTimePage{
		maxPageSizeBytes: 1_500_000, // 1.5MB
	}

	nextTS := time.Now()

	for c.Append(c.rows, nextTS) {
		// Add up to 5000ms of jitter to the next timestamp.
		nextTS = nextTS.Add(time.Duration(r.Intn(5000)) * time.Millisecond)
	}

	require.Greater(t, c.rows, 0)
	t.Logf("Packed %d timestamps into a single page", c.rows)
}
