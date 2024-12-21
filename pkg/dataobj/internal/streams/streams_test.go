package streams_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/obj"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	type ent struct {
		Labels labels.Labels
		Time   time.Time
	}

	tt := []ent{
		{labels.FromStrings("cluster", "test", "app", "foo"), time.Unix(10, 0).UTC()},
		{labels.FromStrings("cluster", "test", "app", "bar", "special", "yes"), time.Unix(100, 0).UTC()},
		{labels.FromStrings("cluster", "test", "app", "foo"), time.Unix(15, 0).UTC()},
		{labels.FromStrings("cluster", "test", "app", "foo"), time.Unix(9, 0).UTC()},
	}

	streamsTracker := streams.New()
	for _, tc := range tt {
		streamsTracker.AddStreamRecord(tc.Labels, tc.Time)
	}

	data, err := buildObject(streamsTracker)
	require.NoError(t, err)

	expect := []streams.Stream{
		{
			Labels:       labels.FromStrings("cluster", "test", "app", "foo"),
			MinTimestamp: time.Unix(9, 0).UTC(),
			MaxTimestamp: time.Unix(15, 0).UTC(),
			Rows:         3,
		},
		{
			Labels:       labels.FromStrings("cluster", "test", "app", "bar", "special", "yes"),
			MinTimestamp: time.Unix(100, 0).UTC(),
			MaxTimestamp: time.Unix(100, 0).UTC(),
			Rows:         1,
		},
	}

	dec := obj.ReadSeekerDecoder(bytes.NewReader(data))

	var actual []streams.Stream
	for res := range streams.IterStreams(context.TODO(), dec) {
		s, err := res.Value()
		require.NoError(t, err)
		actual = append(actual, s)
	}

	require.Equal(t, expect, actual)
}

func buildObject(st *streams.Streams) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := obj.NewEncoder(&buf)
	if err != nil {
		return nil, err
	} else if err := st.WriteTo(enc, 1024, 1024); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
