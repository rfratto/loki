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
// the encoder. There's a few chagnes we'll want to make:
//
// 1. Potentially combine the decoder and encoder into a single encoding
//    package; this would centralize the constants (magic, format versions) but
//    make the identifiers slightly more verbose.
//
// 2. Consider the CPU/memory overhead of the current implementation. There's a
//    ton of intermediate allocations happening that may be avoidable or
//    cheapened via pooling.

// Decoders. To cleanly separate the APIs per section, each section has its own
// Decoder interface which is created by the top-level Decoder interface.
type (
	// Decoder provides an API for decoding a data object.
	Decoder interface {
		// Sections returns the list of sections in a data object.
		Sections(ctx context.Context) ([]filemd.Section, error)

		// StreamDecoder returns a StreamDecoder for the provided streams section.
		// StreamsDecoder fails if sec is not a streams section.
		StreamsDecoder(sec filemd.Section) (StreamsDecoder, error)
	}

	// StreamsDecoder supports decoding data from a streams section.
	StreamsDecoder interface {
		// Streams returns the set of streams from the StreamsDecoder.
		Streams(ctx context.Context) ([]streamsmd.Stream, error)

		// Columns returns the set of columns within a stream.
		Columns(ctx context.Context, stream streamsmd.Stream) ([]streamsmd.Column, error)

		// Pages returns the set of pages within a column.
		Pages(ctx context.Context, col streamsmd.Column) ([]streamsmd.Page, error)

		// ReadPages reads the pages from the column.
		ReadPages(ctx context.Context, pages []streamsmd.Page) iter.Seq2[streams.Page, error]
	}
)
