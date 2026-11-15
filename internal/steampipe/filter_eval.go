// Re-applying pushed filters to plugin rows.
//
// DuckDB removes a predicate it pushes into a table function from its own
// plan, so the worker must return exactly the rows that satisfy it. Plugins
// use quals to narrow their API calls but do not promise exact results, so
// every pushed predicate is evaluated here:
//
//   - comparisons, IN, IS [NOT] NULL and AND/OR trees of them are evaluated
//     in Go, for every column type (TIMESTAMPTZ and JSON included);
//   - any other expression (LIKE, prefix, ...) goes to the SDK's embedded
//     DuckDB evaluator, over a staging batch laid out like the bind schema.

package steampipe

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// tri is SQL three-valued logic.
type tri int8

const (
	triFalse tri = iota
	triTrue
	triNull
)

func triAnd(a, b tri) tri {
	if a == triFalse || b == triFalse {
		return triFalse
	}
	if a == triNull || b == triNull {
		return triNull
	}
	return triTrue
}

func triOr(a, b tri) tri {
	if a == triTrue || b == triTrue {
		return triTrue
	}
	if a == triNull || b == triNull {
		return triNull
	}
	return triFalse
}

// errNotNative marks a filter the Go evaluator does not handle.
var errNotNative = fmt.Errorf("filter shape not evaluated natively")

// rowEvaluator evaluates typed filters over a batch with the bind layout.
type rowEvaluator struct {
	batch arrow.RecordBatch
	index map[string]int
}

func newRowEvaluator(batch arrow.RecordBatch) *rowEvaluator {
	idx := map[string]int{}
	for i, f := range batch.Schema().Fields() {
		idx[f.Name] = i
	}
	return &rowEvaluator{batch: batch, index: idx}
}

func (e *rowEvaluator) column(name string) (arrow.Array, error) {
	i, ok := e.index[name]
	if !ok {
		return nil, fmt.Errorf("filter on unknown column %q", name)
	}
	return e.batch.Column(i), nil
}

// native reports whether every node of f is a shape eval handles.
func native(f vgi.Filter) bool {
	switch x := f.(type) {
	case *vgi.ConstantFilter, *vgi.InFilter, *vgi.IsNullFilter, *vgi.IsNotNullFilter:
		return true
	case *vgi.AndFilter:
		for _, c := range x.Children {
			if !native(c) {
				return false
			}
		}
		return true
	case *vgi.OrFilter:
		for _, c := range x.Children {
			if !native(c) {
				return false
			}
		}
		return true
	}
	return false
}

func (e *rowEvaluator) eval(f vgi.Filter, row int) (tri, error) {
	switch x := f.(type) {
	case *vgi.AndFilter:
		out := triTrue
		for _, c := range x.Children {
			v, err := e.eval(c, row)
			if err != nil {
				return triNull, err
			}
			out = triAnd(out, v)
		}
		return out, nil
	case *vgi.OrFilter:
		out := triFalse
		for _, c := range x.Children {
			v, err := e.eval(c, row)
			if err != nil {
				return triNull, err
			}
			out = triOr(out, v)
		}
		return out, nil
	case *vgi.IsNullFilter:
		col, err := e.column(x.ColumnName())
		if err != nil {
			return triNull, err
		}
		if col.IsNull(row) {
			return triTrue, nil
		}
		return triFalse, nil
	case *vgi.IsNotNullFilter:
		col, err := e.column(x.ColumnName())
		if err != nil {
			return triNull, err
		}
		if col.IsNull(row) {
			return triFalse, nil
		}
		return triTrue, nil
	case *vgi.ConstantFilter:
		col, err := e.column(x.ColumnName())
		if err != nil {
			return triNull, err
		}
		if col.IsNull(row) || x.Value == nil || !x.Value.IsValid() {
			return triNull, nil
		}
		c, err := compareValue(col, row, x.Value)
		if err != nil {
			return triNull, err
		}
		var ok bool
		switch x.Op {
		case vgi.OpEQ:
			ok = c == 0
		case vgi.OpNE:
			ok = c != 0
		case vgi.OpLT:
			ok = c < 0
		case vgi.OpLE:
			ok = c <= 0
		case vgi.OpGT:
			ok = c > 0
		case vgi.OpGE:
			ok = c >= 0
		default:
			return triNull, errNotNative
		}
		if ok {
			return triTrue, nil
		}
		return triFalse, nil
	case *vgi.InFilter:
		col, err := e.column(x.ColumnName())
		if err != nil {
			return triNull, err
		}
		if col.IsNull(row) {
			return triNull, nil
		}
		sawNull := false
		for i := 0; x.Values != nil && i < x.Values.Len(); i++ {
			if x.Values.IsNull(i) {
				sawNull = true
				continue
			}
			s, err := scalar.GetScalar(x.Values, i)
			if err != nil {
				return triNull, err
			}
			c, err := compareValue(col, row, s)
			if err != nil {
				return triNull, err
			}
			if c == 0 {
				return triTrue, nil
			}
		}
		if sawNull {
			return triNull, nil
		}
		return triFalse, nil
	}
	return triNull, errNotNative
}

// compareValue compares row of col with a literal: -1, 0 or 1.
func compareValue(col arrow.Array, row int, lit scalar.Scalar) (int, error) {
	switch c := col.(type) {
	case *array.Int64:
		v := c.Value(row)
		if i, ok := integerValue(lit); ok {
			return cmpOrdered(v, i), nil
		}
		if f, ok := numericValue(lit); ok {
			return cmpFloat(float64(v), f), nil
		}
	case *array.Float64:
		if f, ok := numericValue(lit); ok {
			return cmpFloat(c.Value(row), f), nil
		}
	case *array.String:
		if s, ok := lit.(*scalar.String); ok {
			return strings.Compare(c.Value(row), s.String()), nil
		}
	case *array.Boolean:
		if b, ok := lit.(*scalar.Boolean); ok {
			return cmpOrdered(boolInt(c.Value(row)), boolInt(b.Value)), nil
		}
	case *array.Timestamp:
		if t, ok := lit.(*scalar.Timestamp); ok {
			unit := t.Type.(*arrow.TimestampType).Unit
			lm := t.Value.ToTime(unit).UnixMicro()
			return cmpOrdered(int64(c.Value(row)), lm), nil
		}
		if d, ok := lit.(*scalar.Date32); ok {
			lm := d.Value.ToTime().UnixMicro()
			return cmpOrdered(int64(c.Value(row)), lm), nil
		}
	case array.ExtensionArray: // JSON: compare the text
		if s, ok := lit.(*scalar.String); ok {
			if st, ok := c.Storage().(*array.String); ok {
				return strings.Compare(st.Value(row), s.String()), nil
			}
		}
	}
	return 0, errNotNative
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func cmpOrdered[T int | int64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func cmpFloat(a, b float64) int {
	// DuckDB orders NaN above every other value and equal to itself.
	switch {
	case math.IsNaN(a) && math.IsNaN(b):
		return 0
	case math.IsNaN(a):
		return 1
	case math.IsNaN(b):
		return -1
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func integerValue(s scalar.Scalar) (int64, bool) {
	switch v := s.(type) {
	case *scalar.Int8:
		return int64(v.Value), true
	case *scalar.Int16:
		return int64(v.Value), true
	case *scalar.Int32:
		return int64(v.Value), true
	case *scalar.Int64:
		return v.Value, true
	case *scalar.Uint8:
		return int64(v.Value), true
	case *scalar.Uint16:
		return int64(v.Value), true
	case *scalar.Uint32:
		return int64(v.Value), true
	case *scalar.Uint64:
		if v.Value <= math.MaxInt64 {
			return int64(v.Value), true
		}
	}
	return 0, false
}

// filterMask evaluates every pushed filter and returns the rows to keep.
// typed is laid out like the bind schema with the real column types; staging
// is the same data with TIMESTAMPTZ and JSON as VARCHAR, for the embedded
// evaluator.
func filterMask(ctx context.Context, pf *vgi.PushdownFilters, typed arrow.RecordBatch, staging func() arrow.RecordBatch) ([]bool, error) {
	n := int(typed.NumRows())
	keep := make([]bool, n)
	for i := range keep {
		keep[i] = true
	}
	ev := newRowEvaluator(typed)
	var stagingBatch arrow.RecordBatch
	defer func() {
		if stagingBatch != nil {
			stagingBatch.Release()
		}
	}()
	for _, f := range pf.Filters {
		if native(f) {
			handled := true
			for r := 0; r < n && handled; r++ {
				if !keep[r] {
					continue
				}
				v, err := ev.eval(f, r)
				if err == errNotNative {
					handled = false
					break
				}
				if err != nil {
					return nil, err
				}
				keep[r] = v == triTrue
			}
			if handled {
				continue
			}
		}
		// SQL semantics (LIKE, functions, casts): the embedded evaluator.
		if stagingBatch == nil {
			stagingBatch = staging()
		}
		mask, err := f.Evaluate(ctx, stagingBatch)
		if err != nil {
			if strings.Contains(err.Error(), "runtime filter has no evaluator") {
				// Dynamic (join / Top-N) filters are advisory: skipping one
				// only returns rows DuckDB discards itself.
				continue
			}
			return nil, fmt.Errorf("applying the WHERE filters: %w (an expression over a TIMESTAMPTZ or JSON column cannot be pushed to this worker; cast the column in the query, e.g. CAST(created AS VARCHAR) LIKE '2024%%')", err)
		}
		b, ok := mask.(*array.Boolean)
		if !ok {
			mask.Release()
			return nil, fmt.Errorf("filter evaluator returned %s, not a boolean mask", mask.DataType())
		}
		for r := 0; r < n; r++ {
			keep[r] = keep[r] && b.IsValid(r) && b.Value(r)
		}
		mask.Release()
	}
	return keep, nil
}
