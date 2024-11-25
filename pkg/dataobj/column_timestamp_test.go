package dataobj

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Benchmark_timestampColumn(b *testing.B) {
	r := rand.New(rand.NewSource(0))
	c := &timestampColumn{
		maxPageSizeBytes: 4_000,
	}

	nextTS := time.Now()

	for i := 0; i < b.N; i++ {
		if err := c.Append(nextTS); err != nil {
			require.Fail(b, "unexpected error", err)
		}

		// Add up to 5000ms of jitter to the next timestamp.
		nextTS = nextTS.Add(time.Duration(r.Intn(5000)) * time.Millisecond)
	}

	b.ReportMetric(float64(c.Count()/len(c.pages)), "ts/page")
	b.ReportMetric(float64(len(c.pages)), "pages")
}

func Test_timestampColumn_Iter(t *testing.T) {
	col := &timestampColumn{maxPageSizeBytes: 4_000}

	r := rand.New(rand.NewSource(0))
	nextTS := time.Now().UTC()

	var expect []time.Time

	for len(col.pages) < 3 {
		require.NoError(t, col.Append(nextTS))
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

// Test_timestampPage_packing checks how many ordered timestamps can be stored
// in 1.5MB pages.
func Test_timestampPage_packing(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	c := &timestampPage{
		maxPageSizeBytes: 1_500_000, // 1.5MB
	}

	nextTS := time.Now()

	for {
		err := c.Append(nextTS)
		if errors.Is(err, errPageFull) {
			break
		} else if err != nil {
			require.Fail(t, "unexpected error", err)
		}

		// Add up to 5000ms of jitter to the next timestamp.
		nextTS = nextTS.Add(time.Duration(r.Intn(5000)) * time.Millisecond)
	}

	require.Greater(t, c.count, 0)
	t.Logf("Packed %d timestamps into a single page", c.count)
}
