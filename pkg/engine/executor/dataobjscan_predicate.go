package executor

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"

	"github.com/grafana/loki/v3/pkg/dataobj/sections/logs"
	"github.com/grafana/loki/v3/pkg/engine/internal/datatype"
	"github.com/grafana/loki/v3/pkg/engine/internal/types"
	"github.com/grafana/loki/v3/pkg/engine/planner/physical"
)

// buildLogsPredicate builds a [logs.Predicate] from an expr. The columns slice
// determines available columns that can be referenced by the expression.
//
// The returned predicate performs no filtering on stream ID; callers must use
// [logs.AndPredicate]  and manually include filters for stream IDs if desired.
//
// References to columns that do not exist in the columns slice map to a
// [logs.FalsePredicate].
//
// buildLogsPredicate returns an error if:
//
//   - Expressions cannot be represented as a boolean value
//   - An expression is not supported (such as comparing two columns or comparing
//     two literals).
func buildLogsPredicate(expr physical.Expression, columns []*logs.Column) (logs.Predicate, error) {
	switch expr := expr.(type) {
	case physical.UnaryExpression:
		return buildLogsUnaryPredicate(expr, columns)

	case physical.BinaryExpression:
		return buildLogsBinaryPredicate(expr, columns)

	case *physical.LiteralExpr:
		if expr.Literal.Type() == datatype.Loki.Bool {
			// TODO(rfratto): This would add support for statements like
			//
			// SELECT * WHERE true
			//
			// There's no way to write this in LogQL today, so I'm leaving it
			// unsupported for now. Adding support to it would require a
			// TruePredicate to be added to dataset.
			return nil, fmt.Errorf("plain boolean literals are unsupported for logs predicates")
		}

	case *physical.ColumnExpr:
		// TODO(rfratto): This would add support for statements like
		//
		// SELECT * WHERE boolean_column
		//
		// (where boolean_column is a column containing boolean values). No such
		// column type exists now, so I'm leaving it unsupported here for the time
		// being.
		return nil, fmt.Errorf("plain column references (%s) are unsupported for logs predicates", expr)

	default:
		// TODO(rfratto): This doesn't yet cover two potential future cases:
		//
		// * A plain boolean literal
		// * A reference to a column containing boolean values
		return nil, fmt.Errorf("expression %[1]s (type %[1]T) cannot be interpreted as a boolean", expr)
	}

	return nil, fmt.Errorf("expression %[1]s (type %[1]T) cannot be interpreted as a boolean", expr)
}

func buildLogsUnaryPredicate(expr physical.UnaryExpression, columns []*logs.Column) (logs.Predicate, error) {
	unaryExpr, ok := expr.(*physical.UnaryExpr)
	if !ok {
		return nil, fmt.Errorf("expected physical.UnaryExpr, got %[1]T", expr)
	}

	inner, err := buildLogsPredicate(unaryExpr.Left, columns)
	if err != nil {
		return nil, fmt.Errorf("building unary predicate: %w", err)
	}

	switch unaryExpr.Op {
	case types.UnaryOpNot:
		return logs.NotPredicate{Inner: inner}, nil
	}

	return nil, fmt.Errorf("unsupported unary operator %s in logs predicate", unaryExpr.Op)
}

var comparisonBinaryOps = map[types.BinaryOp]struct{}{
	types.BinaryOpEq:              {},
	types.BinaryOpNeq:             {},
	types.BinaryOpGt:              {},
	types.BinaryOpGte:             {},
	types.BinaryOpLt:              {},
	types.BinaryOpLte:             {},
	types.BinaryOpMatchSubstr:     {},
	types.BinaryOpNotMatchSubstr:  {},
	types.BinaryOpMatchRe:         {},
	types.BinaryOpNotMatchRe:      {},
	types.BinaryOpMatchPattern:    {},
	types.BinaryOpNotMatchPattern: {},
}

func buildLogsBinaryPredicate(expr physical.BinaryExpression, columns []*logs.Column) (logs.Predicate, error) {
	binaryExpr, ok := expr.(*physical.BinaryExpr)
	if !ok {
		return nil, fmt.Errorf("expected physical.BinaryExpr, got %[1]T", expr)
	}

	switch binaryExpr.Op {
	case types.BinaryOpAnd:
		left, err := buildLogsPredicate(binaryExpr.Left, columns)
		if err != nil {
			return nil, fmt.Errorf("building left binary predicate: %w", err)
		}
		right, err := buildLogsPredicate(binaryExpr.Right, columns)
		if err != nil {
			return nil, fmt.Errorf("building right binary predicate: %w", err)
		}
		return logs.AndPredicate{Left: left, Right: right}, nil

	case types.BinaryOpOr:
		left, err := buildLogsPredicate(binaryExpr.Left, columns)
		if err != nil {
			return nil, fmt.Errorf("building left binary predicate: %w", err)
		}
		right, err := buildLogsPredicate(binaryExpr.Right, columns)
		if err != nil {
			return nil, fmt.Errorf("building right binary predicate: %w", err)
		}
		return logs.AndPredicate{Left: left, Right: right}, nil
	}

	if _, ok := comparisonBinaryOps[binaryExpr.Op]; ok {
		return buildLogsComparison(binaryExpr, columns)
	}

	return nil, fmt.Errorf("expression %[1]s (type %[1]T) cannot be interpreted as a boolean", expr)
}

func buildLogsComparison(expr *physical.BinaryExpr, columns []*logs.Column) (logs.Predicate, error) {
	// Currently, we only support comparisons where the left-hand side is a
	// [physical.ColumnExpr] and the right-hand side is a [physical.LiteralExpr].
	//
	// Support for other cases could be added in the future:
	//
	// * LHS LiteralExpr, RHS ColumnExpr could be supported by inverting the
	//   operation.
	// * LHS LiteralExpr, RHS LiteralExpr could be supported by evaluating the
	//   expresion immediately.
	// * LHS ColumnExpr, RHS ColumnExpr could be supported in the future, but would need
	//   support down to the dataset level.

	columnRef, leftValid := expr.Left.(*physical.ColumnExpr)
	literalExpr, rightValid := expr.Right.(*physical.LiteralExpr)

	if !leftValid || !rightValid {
		return nil, fmt.Errorf("binary comparisons require the left-hand operation to reference a column (got %T) and the right-hand operation to be a literal (got %T)", expr.Left, expr.Right)
	}

	col, err := findColumn(columnRef.Ref, columns)
	if err != nil {
		return nil, fmt.Errorf("finding column %s: %w", columnRef.Ref, err)
	} else if col == nil {
		// If the column does not exist, we return a FalsePredicate.
		//
		// TODO(rfratto): does returning false here make sense for all operations?
		// Are there cases where it should return true instead?
		return logs.FalsePredicate{}, nil
	}

	s, err := buildDataobjScalar(literalExpr.Literal)
	if err != nil {
		return nil, err
	}

	switch expr.Op {
	case types.BinaryOpEq:
		return logs.EqualPredicate{Column: col, Value: s}, nil
	case types.BinaryOpNeq:
		return logs.NotPredicate{Inner: logs.EqualPredicate{Column: col, Value: s}}, nil

	case types.BinaryOpGt:
		return logs.GreaterThanPredicate{Column: col, Value: s}, nil

	case types.BinaryOpGte:
		return logs.OrPredicate{
			Left:  logs.GreaterThanPredicate{Column: col, Value: s},
			Right: logs.EqualPredicate{Column: col, Value: s},
		}, nil

	case types.BinaryOpLt:
		return logs.LessThanPredicate{Column: col, Value: s}, nil

	case types.BinaryOpLte:
		return logs.OrPredicate{
			Left:  logs.LessThanPredicate{Column: col, Value: s},
			Right: logs.EqualPredicate{Column: col, Value: s},
		}, nil

	case types.BinaryOpMatchSubstr, types.BinaryOpNotMatchSubstr, types.BinaryOpMatchRe, types.BinaryOpNotMatchRe, types.BinaryOpMatchPattern, types.BinaryOpNotMatchPattern:
		return buildLogsMatch(col, expr.Op, s)
	}

	return nil, fmt.Errorf("unsupported binary operator %s in logs predicate", expr.Op)
}

// findColumn finds a column by ref in the slice of columns. If ref is invalid,
// findColumn returns an error. If the column does not exist, findColumn
// returns nil.
func findColumn(ref types.ColumnRef, columns []*logs.Column) (*logs.Column, error) {
	if ref.Type != types.ColumnTypeBuiltin && ref.Type != types.ColumnTypeMetadata {
		return nil, fmt.Errorf("invalid column ref %s, expected builtin or metadata", ref)
	}

	columnMatch := func(ref types.ColumnRef, column *logs.Column) bool {
		switch {
		case ref.Type == types.ColumnTypeBuiltin && ref.Column == types.ColumnNameBuiltinTimestamp:
			return column.Type == logs.ColumnTypeTimestamp
		case ref.Type == types.ColumnTypeBuiltin && ref.Column == types.ColumnNameBuiltinMessage:
			return column.Type == logs.ColumnTypeMessage
		case ref.Type == types.ColumnTypeMetadata:
			return column.Name == ref.Column
		}

		return false
	}

	for _, column := range columns {
		if columnMatch(ref, column) {
			return column, nil
		}
	}

	return nil, nil
}

// buildDataobjScalar builds a dataobj-compatible [scalar.Scalar] from a
// [datatype.Literal].
func buildDataobjScalar(lit datatype.Literal) (scalar.Scalar, error) {
	// [logs.ReaderOptions.Validate] specifies that all scalars must be one of
	// the given types:
	//
	// * [scalar.Null] (of any datatype)
	// * [scalar.Int64]
	// * [scalar.Uint64]
	// * [scalar.Timestamp] (nanosecond precision)
	// * [scalar.Binary]
	//
	// All of our mappings below evaluate to one of the above types.

	switch lit := lit.(type) {
	case *datatype.NullLiteral:
		return scalar.ScalarNull, nil
	case *datatype.IntegerLiteral:
		return scalar.NewInt64Scalar(lit.Value()), nil
	case *datatype.BytesLiteral:
		// [datatype.BytesLiteral] refers to byte sizes, not binary data.
		return scalar.NewInt64Scalar(int64(lit.Value())), nil
	case *datatype.TimestampLiteral:
		ts := arrow.Timestamp(lit.Value())
		tsType := arrow.FixedWidthTypes.Timestamp_ns
		return scalar.NewTimestampScalar(ts, tsType), nil
	case *datatype.StringLiteral:
		buf := memory.NewBufferBytes([]byte(lit.Value()))
		return scalar.NewBinaryScalar(buf, arrow.BinaryTypes.Binary), nil
	}

	return nil, fmt.Errorf("unsupported literal type %T", lit)
}

func buildLogsMatch(col *logs.Column, op types.BinaryOp, value scalar.Scalar) (logs.Predicate, error) {
	// All the match operations require the value to be a string or a binary.
	var find []byte

	switch value := value.(type) {
	case *scalar.Binary:
		find = value.Data()
	case *scalar.String:
		find = value.Data()
	default:
		return nil, fmt.Errorf("unsupported scalar type %T for op %s, expected binary or string", value, op)
	}

	switch op {
	case types.BinaryOpMatchSubstr:
		return logs.FuncPredicate{
			Column: col,
			Keep: func(_ *logs.Column, value scalar.Scalar) bool {
				switch value := value.(type) {
				case *scalar.Binary:
					return bytes.Contains(value.Data(), find)
				case *scalar.String:
					return bytes.Contains(value.Data(), find)
				}

				// Type mismatch?
				return false
			},
		}, nil

	case types.BinaryOpNotMatchSubstr:
		return logs.FuncPredicate{
			Column: col,
			Keep: func(_ *logs.Column, value scalar.Scalar) bool {
				switch value := value.(type) {
				case *scalar.Binary:
					return !bytes.Contains(value.Data(), find)
				case *scalar.String:
					return !bytes.Contains(value.Data(), find)
				}

				// Type mismatch?
				return true
			},
		}, nil

	case types.BinaryOpMatchRe:
		re, err := regexp.Compile(string(find))
		if err != nil {
			return nil, err
		}
		return logs.FuncPredicate{
			Column: col,
			Keep: func(_ *logs.Column, value scalar.Scalar) bool {
				switch value := value.(type) {
				case *scalar.Binary:
					return re.Match(value.Data())
				case *scalar.String:
					return re.Match(value.Data())
				}

				// Type mismatch?
				return false
			},
		}, nil

	case types.BinaryOpNotMatchRe:
		re, err := regexp.Compile(string(find))
		if err != nil {
			return nil, err
		}
		return logs.FuncPredicate{
			Column: col,
			Keep: func(_ *logs.Column, value scalar.Scalar) bool {
				switch value := value.(type) {
				case *scalar.Binary:
					return !re.Match(value.Data())
				case *scalar.String:
					return !re.Match(value.Data())
				}

				// Type mismatch?
				return true
			},
		}, nil
	}

	// NOTE(rfratto): [types.BinaryOpMatchPattern] and [types.BinaryOpNotMatchPattern]
	// are currently unsupported.
	return nil, fmt.Errorf("unrecognized match operation %s", op)
}

// buildLogsRowPredicate builds a [logs.RowPredicate] from an expression.
func buildLogsRowPredicate(expr physical.Expression) (logs.RowPredicate, error) {
	// TODO(rfratto): implement converting expressions into logs predicates.
	//
	// There's a few challenges here:
	//
	// - Expressions do not cleanly map to [logs.RowPredicate]s. For example,
	//   an expression may be simply a column reference, but a logs predicate is
	//   always some expression that can evaluate to true.
	//
	// - Mapping expressions into [dataobj.TimeRangePredicate] is a massive pain;
	//   since TimeRangePredicate specifies both bounds for the time range, we
	//   would need to find and collapse multiple physical.Expressions into a
	//   single TimeRangePredicate.
	//
	// - While [dataobj.MetadataMatcherPredicate] and
	//   [dataobj.LogMessageFilterPredicate] are catch-alls for function-based
	//   predicates, they are row-based and not column-based, so our
	//   expressionEvaluator cannot be used here.
	//
	// Long term, we likely want two things:
	//
	// 1. Use dataset.Reader and dataset.Predicate directly instead of
	//    dataobj.LogsReader.
	//
	// 2. Update dataset.Reader to be vector based instead of row-based.
	//
	// It's not clear if we should resolve the issues with LogsPredicate (or find
	// hacks to make them work in the short term), or skip straight to using
	// dataset.Reader instead.
	//
	// Implementing DataObjScan in the dataobj package would be a clean way to
	// handle all of this, but that would cause cyclic dependencies. I also don't
	// think we should start removing things from internal for this; we can probably
	// find a way to remove the explicit dependency from the dataobj package from
	// the physical planner instead.
	return mapInitiallySupportedRowPredicates(expr)
}

// Support for timestamp and metadata predicates has been implemented.
// TODO(owen-d): this can go away when we use dataset.Reader & dataset.Predicate directly
func mapInitiallySupportedRowPredicates(expr physical.Expression) (logs.RowPredicate, error) {
	return mapRowPredicates(expr)
}

func mapRowPredicates(expr physical.Expression) (logs.RowPredicate, error) {
	switch e := expr.(type) {
	case *physical.BinaryExpr:

		// Special case: both sides of the binary op are again binary expressions (where LHS is expected to be a column expression)
		if e.Left.Type() == physical.ExprTypeBinary && e.Right.Type() == physical.ExprTypeBinary {
			left, err := mapRowPredicates(e.Left)
			if err != nil {
				return nil, err
			}
			right, err := mapRowPredicates(e.Right)
			if err != nil {
				return nil, err
			}
			switch e.Op {
			case types.BinaryOpAnd:
				return logs.AndRowPredicate{
					Left:  left,
					Right: right,
				}, nil
			case types.BinaryOpOr:
				return logs.OrRowPredicate{
					Left:  left,
					Right: right,
				}, nil
			default:
				return nil, fmt.Errorf("unsupported operator in predicate: %s", e.Op)
			}
		}

		if e.Left.Type() != physical.ExprTypeColumn {
			return nil, fmt.Errorf("unsupported predicate, expected column ref on LHS: %s", expr.String())
		}

		left := e.Left.(*physical.ColumnExpr)
		switch left.Ref.Type {
		case types.ColumnTypeBuiltin:
			if left.Ref.Column == types.ColumnNameBuiltinTimestamp {
				return mapTimestampRowPredicate(e)
			}
			if left.Ref.Column == types.ColumnNameBuiltinMessage {
				return mapMessageRowPredicate(e)
			}
			return nil, fmt.Errorf("unsupported builtin column in predicate (only timestamp is supported for now): %s", left.Ref.Column)
		case types.ColumnTypeMetadata:
			return mapMetadataRowPredicate(e)
		default:
			return nil, fmt.Errorf("unsupported column ref type (%T) in predicate: %s", left.Ref, left.Ref.String())
		}
	default:
		return nil, fmt.Errorf("unsupported expression type (%T) in predicate: %s", expr, expr.String())
	}
}

func mapTimestampRowPredicate(expr physical.Expression) (logs.TimeRangeRowPredicate, error) {
	m := newTimestampRowPredicateMapper()
	if err := m.verify(expr); err != nil {
		return logs.TimeRangeRowPredicate{}, err
	}

	if err := m.processExpr(expr); err != nil {
		return logs.TimeRangeRowPredicate{}, err
	}

	// Check for impossible ranges that might have been formed.
	if m.res.StartTime.After(m.res.EndTime) {
		return logs.TimeRangeRowPredicate{},
			fmt.Errorf("impossible time range: start_time (%v) is after end_time (%v)", m.res.StartTime, m.res.EndTime)
	}
	if m.res.StartTime.Equal(m.res.EndTime) && (!m.res.IncludeStart || !m.res.IncludeEnd) {
		return logs.TimeRangeRowPredicate{},
			fmt.Errorf("impossible time range: start_time (%v) equals end_time (%v) but the range is exclusive", m.res.StartTime, m.res.EndTime)
	}

	return m.res, nil
}

func newTimestampRowPredicateMapper() *timestampRowPredicateMapper {
	open := logs.TimeRangeRowPredicate{
		StartTime:    time.Unix(0, math.MinInt64).UTC(),
		EndTime:      time.Unix(0, math.MaxInt64).UTC(),
		IncludeStart: true,
		IncludeEnd:   true,
	}
	return &timestampRowPredicateMapper{
		res: open,
	}
}

type timestampRowPredicateMapper struct {
	res logs.TimeRangeRowPredicate
}

// ensures the LHS is a timestamp column reference
// and the RHS is either a literal or a binary expression
func (m *timestampRowPredicateMapper) verify(expr physical.Expression) error {
	binop, ok := expr.(*physical.BinaryExpr)
	if !ok {
		return fmt.Errorf("unsupported expression type for timestamp predicate: %T, expected *physical.BinaryExpr", expr)
	}

	switch binop.Op {
	case types.BinaryOpEq, types.BinaryOpGt, types.BinaryOpGte, types.BinaryOpLt, types.BinaryOpLte:
		lhs, okLHS := binop.Left.(*physical.ColumnExpr)
		if !okLHS || lhs.Ref.Type != types.ColumnTypeBuiltin || lhs.Ref.Column != types.ColumnNameBuiltinTimestamp {
			return fmt.Errorf("invalid LHS for comparison: expected timestamp column, got %s", binop.Left.String())
		}

		// RHS must be a literal timestamp for simple comparisons.
		rhsLit, okRHS := binop.Right.(*physical.LiteralExpr)
		if !okRHS {
			return fmt.Errorf("invalid RHS for comparison: expected literal timestamp, got %T", binop.Right)
		}
		if rhsLit.ValueType() != datatype.Loki.Timestamp {
			return fmt.Errorf("unsupported literal type for RHS: %s, expected timestamp", rhsLit.ValueType())
		}
		return nil

	case types.BinaryOpAnd:
		if err := m.verify(binop.Left); err != nil {
			return fmt.Errorf("invalid left operand for AND: %w", err)
		}
		if err := m.verify(binop.Right); err != nil {
			return fmt.Errorf("invalid right operand for AND: %w", err)
		}
		return nil

	default:
		return fmt.Errorf("unsupported operator for timestamp predicate: %s", binop.Op)
	}
}

// processExpr recursively processes the expression tree to update the time range.
func (m *timestampRowPredicateMapper) processExpr(expr physical.Expression) error {
	// verify should have already ensured expr is *physical.BinaryExpr
	binExp := expr.(*physical.BinaryExpr)

	switch binExp.Op {
	case types.BinaryOpAnd:
		if err := m.processExpr(binExp.Left); err != nil {
			return err
		}
		return m.processExpr(binExp.Right)
	case types.BinaryOpEq, types.BinaryOpGt, types.BinaryOpGte, types.BinaryOpLt, types.BinaryOpLte:
		// This is a comparison operation, rebound m.res
		// The 'right' here is binExp.Right, which could be a literal or a nested BinaryExpr.
		// rebound will extract the actual literal value.
		return m.rebound(binExp.Op, binExp.Right)
	default:
		// This case should ideally not be reached if verify is correct.
		return fmt.Errorf("unexpected operator in processExpr: %s", binExp.Op)
	}
}

// rebound updates the time range (m.res) based on a single comparison operation.
// The `op` is the comparison operator (e.g., Gt, Lte).
// The `rightExpr` is the RHS of the comparison, which might be a literal
// or a nested binary expression (e.g., `timestamp > (timestamp = X)`).
// This function will traverse `rightExpr` to find the innermost literal value.
func (m *timestampRowPredicateMapper) rebound(op types.BinaryOp, rightExpr physical.Expression) error {
	// `verify` (called by processExpr before this) now ensures that for comparison ops,
	// the original expression's RHS was a literal, or if it was an AND, its constituent parts were.
	// `processExpr` will pass the direct LiteralExpr from a comparison to rebound.
	literalExpr, ok := rightExpr.(*physical.LiteralExpr)
	if !ok {
		// This should not happen if verify and processExpr are correct.
		return fmt.Errorf("internal error: rebound expected LiteralExpr, got %T for: %s", rightExpr, rightExpr.String())
	}

	if literalExpr.ValueType() != datatype.Loki.Timestamp {
		// Also should be caught by verify.
		return fmt.Errorf("internal error: unsupported literal type in rebound: %s, expected timestamp", literalExpr.ValueType())
	}
	v := literalExpr.Literal.(datatype.TimestampLiteral).Value()
	val := time.Unix(0, int64(v)).UTC()

	switch op {
	case types.BinaryOpEq: // ts == val
		m.updateLowerBound(val, true)
		m.updateUpperBound(val, true)
	case types.BinaryOpGt: // ts > val
		m.updateLowerBound(val, false)
	case types.BinaryOpGte: // ts >= val
		m.updateLowerBound(val, true)
	case types.BinaryOpLt: // ts < val
		m.updateUpperBound(val, false)
	case types.BinaryOpLte: // ts <= val
		m.updateUpperBound(val, true)
	default:
		// Should not be reached if processExpr filters operators correctly.
		return fmt.Errorf("unsupported operator in rebound: %s", op)
	}
	return nil
}

// updateLowerBound updates the start of the time range (m.res.StartTime, m.res.IncludeStart).
// `val` is the new potential start time from the condition.
// `includeVal` indicates if this new start time is inclusive (e.g., from '>=' or '==').
func (m *timestampRowPredicateMapper) updateLowerBound(val time.Time, includeVal bool) {
	if val.After(m.res.StartTime) {
		// The new value is strictly greater than the current start, so it becomes the new start.
		m.res.StartTime = val
		m.res.IncludeStart = includeVal
	} else if val.Equal(m.res.StartTime) {
		// The new value is equal to the current start.
		// The range becomes more restrictive if the current start was exclusive and the new one is also exclusive or inclusive.
		// Or if the current was inclusive and the new one is exclusive.
		// Effectively, IncludeStart becomes true only if *both* the existing and new condition allow/imply inclusion.
		// No, this should be: if new condition makes it more restrictive (i.e. current is [T and new is (T ), then new is (T )
		// m.res.IncludeStart = m.res.IncludeStart && includeVal (This is for intersection: [a,b] AND (a,c] -> (a, min(b,c)] )
		// m.res.IncludeStart = m.res.IncludeStart && includeVal
		if !includeVal { // if new bound is exclusive (e.g. from `> val`)
			m.res.IncludeStart = false // existing [val,... or (val,... combined with (> val) becomes (val,...
		}
		// If includeVal is true (e.g. from `>= val`), and m.res.IncludeStart was already true, it stays true.
		// If includeVal is true, and m.res.IncludeStart was false, it stays false ( (val,...) AND [val,...] is (val,...) )
		// This logic is subtle. Let's restate:
		// Current Start: S, Is       (m.res.StartTime, m.res.IncludeStart)
		// New Condition: val, includeVal
		// if val > S: new start is val, includeVal
		// if val == S: new start is val. IncludeStart becomes m.res.IncludeStart AND includeVal.
		//    Example: current (S, ...), new condition S >= S. Combined: (S, ...). So if m.res.IncludeStart=false, new includeVal=true -> false.
		//    Example: current [S, ...), new condition S > S. Combined: (S, ...). So if m.res.IncludeStart=true, new includeVal=false -> false.
		//    This is correct for intersection.
		m.res.IncludeStart = m.res.IncludeStart && includeVal
	}
	// If val is before m.res.StartTime, the current StartTime is already more restrictive, so no change.
}

// updateUpperBound updates the end of the time range (m.res.EndTime, m.res.IncludeEnd).
// `val` is the new potential end time from the condition.
// `includeVal` indicates if this new end time is inclusive (e.g., from '<=' or '==').
func (m *timestampRowPredicateMapper) updateUpperBound(val time.Time, includeVal bool) {
	if val.Before(m.res.EndTime) {
		// The new value is strictly less than the current end, so it becomes the new end.
		m.res.EndTime = val
		m.res.IncludeEnd = includeVal
	} else if val.Equal(m.res.EndTime) {
		// The new value is equal to the current end.
		// Similar to updateLowerBound, the inclusiveness is an AND condition.
		// Example: current (..., S), new condition ts <= S. Combined: (..., S). m.res.IncludeEnd=false, includeVal=true -> false.
		// Example: current (..., S], new condition ts < S. Combined: (..., S). m.res.IncludeEnd=true, includeVal=false -> false.
		m.res.IncludeEnd = m.res.IncludeEnd && includeVal
	}
	// If val is after m.res.EndTime, the current EndTime is already more restrictive, so no change.
}

// mapMetadataRowPredicate converts a physical.Expression into a dataobj.Predicate for metadata filtering.
// It supports MetadataMatcherPredicate for equality checks on metadata fields,
// and can recursively handle AndPredicate, OrPredicate, and NotPredicate.
func mapMetadataRowPredicate(expr physical.Expression) (logs.RowPredicate, error) {
	switch e := expr.(type) {
	case *physical.BinaryExpr:
		switch e.Op {
		case types.BinaryOpEq:
			if e.Left.Type() != physical.ExprTypeColumn {
				return nil, fmt.Errorf("unsupported LHS type (%v) for EQ metadata predicate, expected ColumnExpr", e.Left.Type())
			}
			leftColumn, ok := e.Left.(*physical.ColumnExpr)
			if !ok { // Should not happen due to Type() check but defensive
				return nil, fmt.Errorf("LHS of EQ metadata predicate failed to cast to ColumnExpr")
			}
			if leftColumn.Ref.Type != types.ColumnTypeMetadata {
				return nil, fmt.Errorf("unsupported LHS column type (%v) for EQ metadata predicate, expected ColumnTypeMetadata", leftColumn.Ref.Type)
			}

			if e.Right.Type() != physical.ExprTypeLiteral {
				return nil, fmt.Errorf("unsupported RHS type (%v) for EQ metadata predicate, expected LiteralExpr", e.Right.Type())
			}
			rightLiteral, ok := e.Right.(*physical.LiteralExpr)
			if !ok { // Should not happen
				return nil, fmt.Errorf("RHS of EQ metadata predicate failed to cast to LiteralExpr")
			}
			if rightLiteral.ValueType() != datatype.Loki.String {
				return nil, fmt.Errorf("unsupported RHS literal type (%v) for EQ metadata predicate, expected ValueTypeStr", rightLiteral.ValueType())
			}
			val := rightLiteral.Literal.(datatype.StringLiteral).Value()

			return logs.MetadataMatcherRowPredicate{
				Key:   leftColumn.Ref.Column,
				Value: val,
			}, nil
		case types.BinaryOpAnd:
			leftPredicate, err := mapMetadataRowPredicate(e.Left)
			if err != nil {
				return nil, fmt.Errorf("failed to map left operand of AND: %w", err)
			}
			rightPredicate, err := mapMetadataRowPredicate(e.Right)
			if err != nil {
				return nil, fmt.Errorf("failed to map right operand of AND: %w", err)
			}
			return logs.AndRowPredicate{
				Left:  leftPredicate,
				Right: rightPredicate,
			}, nil
		case types.BinaryOpOr:
			leftPredicate, err := mapMetadataRowPredicate(e.Left)
			if err != nil {
				return nil, fmt.Errorf("failed to map left operand of OR: %w", err)
			}
			rightPredicate, err := mapMetadataRowPredicate(e.Right)
			if err != nil {
				return nil, fmt.Errorf("failed to map right operand of OR: %w", err)
			}
			return logs.OrRowPredicate{
				Left:  leftPredicate,
				Right: rightPredicate,
			}, nil
		default:
			return nil, fmt.Errorf("unsupported binary operator (%s) for metadata predicate, expected EQ, AND, or OR", e.Op)
		}
	case *physical.UnaryExpr:
		if e.Op != types.UnaryOpNot {
			return nil, fmt.Errorf("unsupported unary operator (%s) for metadata predicate, expected NOT", e.Op)
		}
		innerPredicate, err := mapMetadataRowPredicate(e.Left)
		if err != nil {
			return nil, fmt.Errorf("failed to map inner expression of NOT: %w", err)
		}
		return logs.NotRowPredicate{
			Inner: innerPredicate,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported expression type (%T) for metadata predicate, expected BinaryExpr or UnaryExpr", expr)
	}
}

func mapMessageRowPredicate(expr physical.Expression) (logs.RowPredicate, error) {
	switch e := expr.(type) {
	case *physical.BinaryExpr:
		switch e.Op {

		case types.BinaryOpMatchSubstr, types.BinaryOpNotMatchSubstr, types.BinaryOpMatchRe, types.BinaryOpNotMatchRe:
			return matchRow(e)

		case types.BinaryOpMatchPattern, types.BinaryOpNotMatchPattern:
			return nil, fmt.Errorf("unsupported binary operator (%s) for log message predicate", e.Op)

		case types.BinaryOpAnd:
			leftPredicate, err := mapMessageRowPredicate(e.Left)
			if err != nil {
				return nil, fmt.Errorf("failed to map left operand of AND: %w", err)
			}
			rightPredicate, err := mapMessageRowPredicate(e.Right)
			if err != nil {
				return nil, fmt.Errorf("failed to map right operand of AND: %w", err)
			}
			return logs.AndRowPredicate{
				Left:  leftPredicate,
				Right: rightPredicate,
			}, nil

		case types.BinaryOpOr:
			leftPredicate, err := mapMessageRowPredicate(e.Left)
			if err != nil {
				return nil, fmt.Errorf("failed to map left operand of OR: %w", err)
			}
			rightPredicate, err := mapMessageRowPredicate(e.Right)
			if err != nil {
				return nil, fmt.Errorf("failed to map right operand of OR: %w", err)
			}
			return logs.OrRowPredicate{
				Left:  leftPredicate,
				Right: rightPredicate,
			}, nil

		default:
			return nil, fmt.Errorf("unsupported binary operator (%s) for log message predicate, expected MATCH_STR, NOT_MATCH_STR, MATCH_RE, NOT_MATCH_RE, AND, or OR", e.Op)
		}

	case *physical.UnaryExpr:
		if e.Op != types.UnaryOpNot {
			return nil, fmt.Errorf("unsupported unary operator (%s) for log message predicate, expected NOT", e.Op)
		}
		innerPredicate, err := mapMessageRowPredicate(e.Left)
		if err != nil {
			return nil, fmt.Errorf("failed to map inner expression of NOT: %w", err)
		}
		return logs.NotRowPredicate{
			Inner: innerPredicate,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported expression type (%T) for log message predicate, expected BinaryExpr or UnaryExpr", expr)
	}
}

func matchRow(e *physical.BinaryExpr) (logs.RowPredicate, error) {
	val, err := rhsValue(e)
	if err != nil {
		return nil, err
	}

	switch e.Op {

	case types.BinaryOpMatchSubstr:
		return logs.LogMessageFilterRowPredicate{
			Keep: func(line []byte) bool { return bytes.Contains(line, []byte(val)) },
		}, nil

	case types.BinaryOpNotMatchSubstr:
		return logs.LogMessageFilterRowPredicate{
			Keep: func(line []byte) bool { return !bytes.Contains(line, []byte(val)) },
		}, nil

	case types.BinaryOpMatchRe:
		re, err := regexp.Compile(val)
		if err != nil {
			return nil, err
		}
		return logs.LogMessageFilterRowPredicate{
			Keep: func(line []byte) bool { return re.Match(line) },
		}, nil

	case types.BinaryOpNotMatchRe:
		re, err := regexp.Compile(val)
		if err != nil {
			return nil, err
		}
		return logs.LogMessageFilterRowPredicate{
			Keep: func(line []byte) bool { return !re.Match(line) },
		}, nil

	default:
		return nil, fmt.Errorf("unsupported binary operator (%s) for log message predicate", e.Op)
	}
}

func rhsValue(e *physical.BinaryExpr) (string, error) {
	op := e.Op.String()

	if e.Left.Type() != physical.ExprTypeColumn {
		return "", fmt.Errorf("unsupported LHS type (%v) for %s message predicate, expected ColumnExpr", e.Left.Type(), op)
	}
	leftColumn, ok := e.Left.(*physical.ColumnExpr)
	if !ok { // Should not happen due to Type() check but defensive
		return "", fmt.Errorf("LHS of %s message predicate failed to cast to ColumnExpr", op)
	}
	if leftColumn.Ref.Type != types.ColumnTypeBuiltin {
		return "", fmt.Errorf("unsupported LHS column type (%v) for %s message predicate, expected ColumnTypeBuiltin", leftColumn.Ref.Type, op)
	}

	if e.Right.Type() != physical.ExprTypeLiteral {
		return "", fmt.Errorf("unsupported RHS type (%v) for %s message predicate, expected LiteralExpr", e.Right.Type(), op)
	}
	rightLiteral, ok := e.Right.(*physical.LiteralExpr)
	if !ok { // Should not happen
		return "", fmt.Errorf("RHS of %s message predicate failed to cast to LiteralExpr", op)
	}
	if rightLiteral.ValueType() != datatype.Loki.String {
		return "", fmt.Errorf("unsupported RHS literal type (%v) for %s message predicate, expected ValueTypeStr", rightLiteral.ValueType(), op)
	}

	return rightLiteral.Literal.(datatype.StringLiteral).Value(), nil
}
