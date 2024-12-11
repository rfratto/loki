// Package decoder provides an API for decoding data objects.
//
// Package decoder is a page-level API for reading data objects.
package decoder

import (
	"context"
	"iter"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streams"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/streamsmd"
)

// TODO(rfratto): This package was hastily through together just for testing
// the encoder. There's a few changes we'll want to make:
//
// 1. Potentially combine the decoder and encoder into a single encoding
//    package; this would centralize the constants (magic, format versions) but
//    make the identifiers slightly more verbose.

// Decoders. To cleanly separate the APIs per section, each section has its own
// Decoder interface which is created by the top-level Decoder interface.
type (
	// Decoder provides an API for decoding a data object.
	Decoder interface {
		// Sections returns the list of sections in a data object.
		Sections(ctx context.Context) ([]*filemd.SectionInfo, error)

		// StreamDecoder returns a StreamDecoder.
		StreamsDecoder() StreamsDecoder
	}

	// StreamsDecoder supports decoding data from a streams section.
	StreamsDecoder interface {
		// Streams returns the set of streams from the StreamsDecoder.
		Streams(ctx context.Context, sec *filemd.SectionInfo) ([]*streamsmd.StreamInfo, error)

		// Columns returns the set of columns within a stream.
		Columns(ctx context.Context, stream *streamsmd.StreamInfo) ([]*streamsmd.ColumnInfo, error)

		// Pages returns the set of pages within a column.
		Pages(ctx context.Context, col *streamsmd.ColumnInfo) ([]*streamsmd.PageInfo, error)

		// ReadPages reads the pages from the column.
		ReadPages(ctx context.Context, pages []*streamsmd.PageInfo) iter.Seq2[streams.Page, error]
	}
)
