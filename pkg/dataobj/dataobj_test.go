package dataobj

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/grafana/loki/pkg/push"
	"github.com/stretchr/testify/require"
)

func Test_stream(t *testing.T) {
	builder := NewBuilder(BuilderConfig{
		MaxPageSize:        4096, // 4KB
		MaxMetadataSize:    4096, // 4KB
		MaxObjectSizeBytes: 1_500_000,
	}, nil)
	defer builder.Close(context.Background())

	req := readTestdataEntries(t, "./testdata/logs.txt.gz")
	require.NoError(t, builder.Append(context.Background(), "fake", req))

	// Validate that iterating over each stream in the builder returns the same
	// entries as the input.
	for _, stream := range req.Streams {
		expect := stream.Entries

		builderTenant, ok := builder.tenants["fake"]
		require.True(t, ok, "missing tenant")
		tenantStream, ok := builderTenant.streams[stream.Labels]
		require.True(t, ok, "missing stream")

		// Print stats about the stream. Add 1 to account for in-memory pages.
		t.Logf("Stream %s: %d timestamp pages, %d log pages", stream.Labels, len(tenantStream.timestamp.pages)+1, len(tenantStream.logColumn.pages)+1)

		var actual []push.Entry
		for ent, err := range tenantStream.Iter() {
			require.NoError(t, err)
			actual = append(actual, ent)
		}

		require.Equal(t, expect, actual, "Mismatched entries for stream %s", stream.Labels)
	}
}

func readTestdataEntries(t *testing.T, path string) push.PushRequest {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	gr, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer gr.Close()

	req, err := readEntries(gr)
	require.NoError(t, err)

	return req
}

func readEntries(r io.Reader) (push.PushRequest, error) {
	var (
		streamLookup = make(map[string]int) // Map of labels to index in streams
		streams      []push.Stream
	)

	scan := bufio.NewScanner(r)
	for scan.Scan() {
		// Lines are formatted as:
		// TIMESTAMP {LABELS} LINE
		line := scan.Text()

		var (
			tsStart = 0
			tsEnd   = strings.Index(line, " ") // Timestamp ends at first space

			labelsStart = strings.Index(line, "{")
			labelsEnd   = strings.Index(line, "}")

			lineStart = labelsEnd + 2 // Skip over the closing brace and space
			lineEnd   = len(line)
		)

		switch {
		case tsEnd == -1:
			return push.PushRequest{}, fmt.Errorf("missing timestamp in line")
		case labelsStart == -1 || labelsEnd == -1:
			return push.PushRequest{}, fmt.Errorf("missing labels in line")
		}

		ts, err := time.Parse(time.RFC3339, line[tsStart:tsEnd])
		if err != nil {
			return push.PushRequest{}, fmt.Errorf("parsing timestamp: %w", err)
		}
		labels := line[labelsStart : labelsEnd+1] // +1 to include the closing brace
		line = line[lineStart:lineEnd]

		var streamIdx int
		if idx, ok := streamLookup[labels]; ok {
			streamIdx = idx
		} else {
			streamIdx = len(streams)
			streamLookup[labels] = streamIdx
			streams = append(streams, push.Stream{Labels: labels})
		}

		streams[streamIdx].Entries = append(streams[streamIdx].Entries, push.Entry{
			Timestamp: ts,
			Line:      line,
		})
	}

	return push.PushRequest{Streams: streams}, nil
}
