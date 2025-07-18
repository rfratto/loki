package executor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/streams"
	"github.com/grafana/loki/v3/pkg/engine/internal/datatype"
	"github.com/grafana/loki/v3/pkg/engine/internal/types"
	"github.com/prometheus/prometheus/model/labels"
)

// TODO(rfratto):
//
// There's a bit too much logic baked into the streamInjector, and separating
// it out would be helpful for following the code.
//
// It should be separated out into:
//
// 1. A streamsView which handles the logic for reading streams and providing
//    methods to iterate over the labels of a stream by ID. The streamsView *must*
//    use an arrow.Table internally, since I don't think there's a way to
//    guarantee you read the entire section in a single read, even given a
//    sufficient batch size.
//
// 2. The streamInjector, which only handles the pieces for injection.

var streamInjectorColumnName = "stream_id.int64"

// streamInjector can be used to inject stream labels into an Arrow record.
type streamInjector struct {
	sec *streams.Section // Section containing the stream metadata.
	ids []int64          // Stream IDs to inject into the record.

	initialized   bool
	streams       arrow.Record      // Record of streams, only containing streams for the ids field.
	columns       []*streams.Column // Used columns of the section, including ID.
	idColumnIndex int               // Index of the stream ID column in the streams record.
	idRowMapping  map[int64]int     // Maps stream IDs to their row index in the streams record.
}

// newStreamInjector creates a new streamInjector for the given section which
// will inject stream labels for the given set of streamIDs.
//
// streamInjector will lazily cache streams from the section for injection.
// Only the streams specified in streamIDs are cached. If streamIDs is empty,
// all streams are be cached.
func newStreamInjector(sec *streams.Section, streamIDs []int64) *streamInjector {
	return &streamInjector{sec: sec, ids: streamIDs}
}

// Inject returns a new Arrow record where the stream ID column is replaced
// with a set of stream label columns.
//
// Inject fails if there is no stream ID column in the input record, or if the
// stream ID doesn't exist in the section given to [newStreamInjector].
//
// The returned record must be Release()d by the caller when no longer needed.
func (si *streamInjector) Inject(ctx context.Context, in arrow.Record) (arrow.Record, error) {
	if err := si.init(ctx); err != nil {
		return nil, fmt.Errorf("initializing stream injector: %w", err)
	}

	streamIDIndex, err := si.findStreamID(in)
	if err != nil {
		return nil, fmt.Errorf("finding stream ID column: %w", err)
	}

	streamIDValues, ok := in.Column(streamIDIndex).(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("stream ID column %q must be of type int64, got %s", streamInjectorColumnName, in.Schema().Field(streamIDIndex).Type)
	}

	type labelColumn struct {
		Field   arrow.Field
		Builder *array.StringBuilder
	}

	var (
		labels      = make([]*labelColumn, 0, len(si.columns))
		labelLookup = make(map[string]*labelColumn, len(si.columns))
	)
	defer func() {
		for _, col := range labels {
			col.Builder.Release()
		}
	}()

	getColumn := func(name string) *labelColumn {
		if col, ok := labelLookup[name]; ok {
			return col
		}

		col := &labelColumn{
			Field: arrow.Field{
				Name:     name,
				Type:     arrow.BinaryTypes.String,
				Nullable: true,
				Metadata: datatype.ColumnMetadata(types.ColumnTypeLabel, datatype.Loki.String),
			},
			Builder: array.NewStringBuilder(memory.DefaultAllocator),
		}

		labels = append(labels, col)
		labelLookup[name] = col
		return col
	}

	for i := range streamIDValues.Len() {
		findID := streamIDValues.Value(i)
		if _, exist := si.idRowMapping[findID]; !exist {
			return nil, fmt.Errorf("stream ID %d not found in section", findID)
		}

		for label := range si.iterStream(findID) {
			col := getColumn(label.Name)

			// Backfill any missing NULLs in the column if needed.
			if missing := i - col.Builder.Len(); missing > 0 {
				col.Builder.AppendNulls(missing)
			}
			col.Builder.Append(label.Value)
		}
	}

	// Sort our labels by name for consistent ordering.
	slices.SortFunc(labels, func(a, b *labelColumn) int {
		return cmp.Compare(a.Field.Name, b.Field.Name)
	})

	var (
		// We preallocate enough space for all label fields + original columns
		// minus the stream ID column (which we drop from the final record).

		fields = make([]arrow.Field, 0, len(labels)+int(in.NumCols())-1)
		arrs   = make([]arrow.Array, 0, len(labels)+int(in.NumCols())-1)

		md = in.Schema().Metadata() // Use the same metadata as the input record.
	)

	// Prepare our arrays from the string builders.
	for _, col := range labels {
		// Backfill any final missing NULLs in the StringBuilder if needed.
		if missing := int(in.NumRows()) - col.Builder.Len(); missing > 0 {
			col.Builder.AppendNulls(missing)
		}

		arr := col.Builder.NewArray()
		defer arr.Release()

		fields = append(fields, col.Field)
		arrs = append(arrs, arr)
	}

	// Add all of the original columns except the stream ID column.
	for i := range int(in.NumCols()) {
		if i == streamIDIndex {
			continue
		}

		fields = append(fields, in.Schema().Field(i))
		arrs = append(arrs, in.Column(i))
	}

	schema := arrow.NewSchemaWithEndian(fields, &md, in.Schema().Endianness())
	return array.NewRecord(schema, arrs, in.NumRows()), nil
}

func (si *streamInjector) init(ctx context.Context) error {
	if si.initialized {
		return nil
	}

	info, err := si.readStreams(ctx)
	if err != nil {
		return fmt.Errorf("reading streams: %w", err)
	}
	streams, columns, idIndex := info.Record, info.Columns, info.IDIndex

	idMapping := make(map[int64]int, streams.NumRows())
	idArray, ok := streams.Column(idIndex).(*array.Int64)
	if !ok {
		return fmt.Errorf("stream ID column must be of type int64, got %s", streams.Schema().Field(idIndex).Type)
	}

	for i := range int(idArray.Len()) {
		id := idArray.Value(i)
		idMapping[id] = i
	}

	si.streams = streams
	si.columns = columns
	si.idColumnIndex = idIndex
	si.idRowMapping = idMapping
	si.initialized = true
	return nil
}

type streamsInfo struct {
	Record  arrow.Record
	Columns []*streams.Column // Columns used for reading the streams.
	IDIndex int               // Index of the stream ID column in the Record.
}

// readStreams reads all requested streams from the section, and returns them
// as a record, along with additional information about the record.
func (si *streamInjector) readStreams(ctx context.Context) (info streamsInfo, err error) {
	var findIDs []scalar.Scalar
	for _, id := range si.ids {
		findIDs = append(findIDs, scalar.NewInt64Scalar(id))
	}

	var (
		readColumns []*streams.Column
		predicate   streams.Predicate

		streamIDIndex = -1
	)

	for i, col := range si.sec.Columns() {
		switch col.Type {
		case streams.ColumnTypeStreamID:
			if len(findIDs) > 0 {
				predicate = streams.InPredicate{Column: col, Values: findIDs}
			}
			readColumns = append(readColumns, col)
			streamIDIndex = i

		case streams.ColumnTypeLabel:
			readColumns = append(readColumns, col)
		}
	}

	if streamIDIndex == -1 {
		return streamsInfo{}, errors.New("section does not contain a stream ID column")
	}

	r := streams.NewReader(streams.ReaderOptions{
		Columns:    readColumns,
		Predicates: []streams.Predicate{predicate},
		Allocator:  memory.DefaultAllocator,
	})

	readRows := len(findIDs)
	if predicate == nil {
		readRows = int(si.sec.Columns()[streamIDIndex].Rows())
	}

	rec, err := r.Read(ctx, readRows)

	return streamsInfo{
		Record:  rec,
		Columns: readColumns,
		IDIndex: streamIDIndex,
	}, err
}

func (si *streamInjector) findStreamID(in arrow.Record) (int, error) {
	indices := in.Schema().FieldIndices(streamInjectorColumnName)
	if len(indices) == 0 {
		return -1, fmt.Errorf("stream ID column %q not found in input record", streamInjectorColumnName)
	} else if len(indices) > 1 {
		return -1, fmt.Errorf("multiple stream ID columns found in input record: %v", indices)
	}
	return indices[0], nil
}

func (si *streamInjector) iterStream(id int64) iter.Seq[labels.Label] {
	// Find the actual row where id is stored (in case the section isn't sorted).
	row := si.idRowMapping[id]

	return func(yield func(labels.Label) bool) {
		for colIndex := range int(si.streams.NumCols()) {
			if colIndex == si.idColumnIndex {
				continue // Skip the stream ID column.
			}

			colValues := si.streams.Column(colIndex).(*array.String)

			label := labels.Label{
				// [streams.Reader] guarantees that the order of the columns used for
				// reading match the columns in the returned Arrow record.
				Name:  si.columns[colIndex].Name,
				Value: colValues.Value(row),
			}

			if !yield(label) {
				return
			}
		}
	}
}

// Close releases any resources held by the streamInjector. A streamInjector
// may be reused after calling Close.
func (si *streamInjector) Close() {
	if !si.initialized {
		return
	}

	si.initialized = false
	si.streams.Release()
	si.streams = nil
	clear(si.idRowMapping)
}
