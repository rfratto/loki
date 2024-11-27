package dataobj

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Benchmark_timeColumn(b *testing.B) {
	r := rand.New(rand.NewSource(0))
	c := &timeColumn{
		maxPageSizeBytes: 4_000,
	}

	nextTS := time.Now()

	for i := 0; i < b.N; i++ {
		c.Append(nextTS)

		// Add up to 5000ms of jitter to the next time.
		nextTS = nextTS.Add(time.Duration(r.Intn(5000)) * time.Millisecond)
	}

	b.ReportMetric(float64(c.Count()/len(c.pages)), "ts/page")
	b.ReportMetric(float64(len(c.pages)), "pages")
}

func Test_timeColumn_Iter(t *testing.T) {
	col := &timeColumn{maxPageSizeBytes: 4_000}

	r := rand.New(rand.NewSource(0))
	nextTS := time.Now().UTC()

	var expect []time.Time

	for len(col.pages) < 3 {
		col.Append(nextTS)
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

// Test_timePage_packing checks how many ordered timestamps can be stored
// in 1.5MB pages.
func Test_timePage_packing(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	c := &timePage{
		maxPageSizeBytes: 1_500_000, // 1.5MB
	}

	nextTS := time.Now()

	for c.Append(nextTS) {
		// Add up to 5000ms of jitter to the next timestamp.
		nextTS = nextTS.Add(time.Duration(r.Intn(5000)) * time.Millisecond)
	}

	require.Greater(t, c.count, 0)
	t.Logf("Packed %d timestamps into a single page", c.count)
}
