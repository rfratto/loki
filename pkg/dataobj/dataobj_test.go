package dataobj

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"iter"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/grafana/loki/pkg/push"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func Test(t *testing.T) {
	bucket := objstore.NewInMemBucket()

	builder := NewBuilder(BuilderConfig{
		SHAPrefixSize: 2,

		MaxPageSize:     1_500_000, // 1.5MB
		MaxMetadataSize: 1_500_000, // 1.5MB

		MaxObjectSizeBytes: 0, // Always flush for every append.
	}, bucket)
	defer builder.Close(context.Background())

	req := readTestdataEntries(t, "./testdata/logs.txt.gz")
	require.NoError(t, builder.Append(context.Background(), "fake", req))

	reader := NewReader(bucket)

	objects := slices.Collect(reader.Objects(context.Background(), "fake"))
	require.Len(t, objects, 1)

	// Try to see if the object has all the data we need.
	{
		streams, err := collectFailable(reader.Streams(context.Background(), objects[0]))
		require.NoError(t, err)
		require.Len(t, streams, len(req.Streams))

		for _, stream := range req.Streams {
			lbls, err := parser.ParseMetric(stream.Labels)
			require.NoError(t, err)

			entriesIter := reader.Entries(context.Background(), objects[0], lbls, EntriesOptions{
				GetAllMetadata: true,
				GetLine:        true,
			})
			entries, err := collectFailable(entriesIter)
			require.NoError(t, err)
			require.Equal(t, stream.Entries, entries, "Mismatched entries for stream %s", stream.Labels)
		}
	}
}

// colectFailable collects all values from an iterator, returning an error if
// the iterator encounters an error.
func collectFailable[T any](iter iter.Seq2[T, error]) ([]T, error) {
	var res []T
	for v, err := range iter {
		if err != nil {
			return res, err
		}
		res = append(res, v)
	}
	return res, nil
}

func getObjects(ctx context.Context, bucket objstore.Bucket, dir string) ([]string, error) {
	var objects []string

	err := bucket.Iter(ctx, dir, func(name string) error {
		objects = append(objects, name)
		return nil
	}, objstore.WithRecursiveIter())

	return objects, err
}

func getObject(ctx context.Context, bucket objstore.Bucket, name string) (io.ReadSeeker, error) {
	r, err := bucket.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err = io.Copy(&buf, r); err != nil {
		return nil, err
	}
	return bytes.NewReader(buf.Bytes()), nil
}

func Test_stream(t *testing.T) {
	builder := NewBuilder(BuilderConfig{
		MaxPageSize:        4096, // 4KB
		MaxMetadataSize:    4096, // 4KB
		MaxObjectSizeBytes: 1_500_000,
	}, objstore.NewInMemBucket())
	defer builder.Close(context.Background())

	req := readTestdataEntries(t, "./testdata/logs.txt.gz")
	require.NoError(t, builder.Append(context.Background(), "fake", req))

	var (
		uncompressedSize = builder.uncompressedSize()
		compressedSize   = builder.compressedSize(true)
		compressionRatio = float64(compressedSize) / float64(uncompressedSize)
	)

	t.Logf("Uncompressed size: %d bytes", uncompressedSize)
	t.Logf("Compressed size: %d bytes", compressedSize)
	t.Logf("Compression ratio: %.2f", compressionRatio)

	// Validate that iterating over each stream in the builder returns the same
	// entries as the input.
	for _, stream := range req.Streams {
		expect := stream.Entries

		builderTenant, ok := builder.tenants["fake"]
		require.True(t, ok, "missing tenant")
		tenantStream, ok := builderTenant.streams[stream.Labels]
		require.True(t, ok, "missing stream")

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
