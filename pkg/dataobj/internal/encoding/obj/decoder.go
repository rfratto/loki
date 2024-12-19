package obj

import (
	"context"
	"iter"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
)

// Decoders. To cleanly separate the APIs per section, each section has its own
// Decoder interface which is created by the top-level Decoder interface.
//
// Decoders in this package are page-level. Higher-level abstractions are found
// elsewhere.
type (
	// Decoder provides an API for decoding a data object.
	Decoder interface {
		// Sections returns the list of sections in a data object.
		Sections(ctx context.Context) ([]*filemd.SectionInfo, error)

		// StreamsDecoder returns a decoder for streams sections.
		StreamsDecoder() StreamsDecoder
	}

	// StreamsDecoder supports decoding data within a streams section.
	StreamsDecoder interface {
		// DatasetDecoder returns a DatasetDecoder for data in the streams section.
		DatasetDecoder() dataset.PageGetter

		// Columns describes the set of columns in the provided section.
		Columns(ctx context.Context, section *filemd.SectionInfo) ([]*streamsmd.ColumnDesc, error)

		// Pages describes the set of pages within the provided column.
		Pages(ctx context.Context, col *streamsmd.ColumnDesc) ([]*ColumnPageDesc, error)

		// ReadPages reads the provided set of pages, iterating over their data
		// matching the argument order. If an error is encountered while retrieving
		// pages, an error is emitted and iteration stops.
		ReadPages(ctx context.Context, pages []*ColumnPageDesc) iter.Seq2[dataset.Page, error]
	}
)

// ColumnPageDesc is a tuple containing column and page information.
type ColumnPageDesc struct {
	Column *streamsmd.ColumnDesc
	Page   *streamsmd.PageDesc
}
