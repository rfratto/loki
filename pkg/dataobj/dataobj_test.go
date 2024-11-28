package dataobj

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/pkg/push"
	"github.com/stretchr/testify/require"
)

func Test_stream_Iter(t *testing.T) {
	in := []push.Entry{
		{
			Timestamp: time.Unix(10, 0).UTC(),
			StructuredMetadata: push.LabelsAdapter{
				{Name: "origin", Value: "earth"},
			},
			Line: "Hello, world!",
		},
		{
			Timestamp: time.Unix(15, 0).UTC(),
			StructuredMetadata: push.LabelsAdapter{
				{Name: "origin", Value: "earth"},
			},
			Line: "Goodbyte, world!",
		},
		{
			Timestamp: time.Unix(20, 0).UTC(),
			Line:      "Help!",
		},
		{
			Timestamp: time.Unix(25, 0).UTC(),
			StructuredMetadata: push.LabelsAdapter{
				{Name: "crisis", Value: "averted"},
			},
			Line: "Nevermind.",
		},
	}

	builder := NewBuilder(BuilderConfig{
		MaxPageSize: 10, // Tiny page size to make sure we generate a few pages.
	}, nil)

	s, err := builder.newStream(`{job="test"}`)
	require.NoError(t, err)
	require.NoError(t, s.Append(context.Background(), in))

	var actual []push.Entry
	for ent, err := range s.Iter() {
		require.NoError(t, err)
		actual = append(actual, ent)
	}

	require.Equal(t, 4, s.rows)
	require.Equal(t, in, actual)

	require.Equal(t, 4, s.timestamp.Count())
	for _, md := range s.metadata {
		require.Equal(t, 4, md.Count())
	}
	require.Equal(t, 4, s.logColumn.Count())

}
