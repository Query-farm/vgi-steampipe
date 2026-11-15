// Key columns and quals.
//
// A Steampipe table declares key columns: the columns whose WHERE predicates
// the plugin turns into API parameters, each with the operators it accepts
// and whether it is required, optional or one of an any_of group. Steampipe's
// Postgres FDW hands the plugin those predicates as "quals".
//
// Here DuckDB pushes the query's filters to the worker (filter pushdown), and
// every top-level conjunct that is a key-column comparison the plugin accepts
// (=, <>, <, <=, >, >=, IN (...) as = ANY, IS [NOT] NULL) becomes a qual.
// Plugins treat quals as API filters, not as exact predicates, so the worker
// also re-applies every pushed filter to the rows it returns
// (AutoApplyFilters): results are exactly what the SQL asks for, and quals only
// make the plugin fetch less.
//
// Required key columns become the table's required_filters, so DuckDB rejects
// a query that omits them at plan time, before the plugin is called.

package steampipe

import (
	"sort"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// keyColumnOps maps column name -> operators accepted by any Get or List key
// column definition of the table.
func keyColumnOps(ts *proto.TableSchema) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	add := func(kcs []*proto.KeyColumn) {
		for _, kc := range kcs {
			ops := kc.GetOperators()
			if len(ops) == 0 {
				ops = []string{"="}
			}
			m := out[kc.GetName()]
			if m == nil {
				m = map[string]bool{}
				out[kc.GetName()] = m
			}
			for _, op := range ops {
				m[op] = true
			}
		}
	}
	add(ts.GetGetCallKeyColumnList())
	add(ts.GetListCallKeyColumnList())
	return out
}

// connectionNameColumn is the column the SDK adds to every table (and to
// every Get and List key column list) naming the connection a row came from.
const connectionNameColumn = "sp_connection_name"

// userKeyColumns drops the SDK's own sp_connection_name key column.
func userKeyColumns(kcs []*proto.KeyColumn) []*proto.KeyColumn {
	out := make([]*proto.KeyColumn, 0, len(kcs))
	for _, kc := range kcs {
		if kc.GetName() != connectionNameColumn {
			out = append(out, kc)
		}
	}
	return out
}

// requirementCNF is the condition one call (Get or List) needs: an AND of
// OR-groups of column names. nil means the call needs nothing.
func requirementCNF(kcs []*proto.KeyColumn) [][]string {
	var cnf [][]string
	var anyOf []string
	for _, kc := range userKeyColumns(kcs) {
		switch kc.GetRequire() {
		case "required", "":
			// The SDK defaults Require to "required".
			cnf = append(cnf, []string{kc.GetName()})
		case "any_of":
			anyOf = append(anyOf, kc.GetName())
		}
	}
	if len(anyOf) > 0 {
		cnf = append(cnf, anyOf)
	}
	return cnf
}

// requiredFilters turns a table's key columns into the AND-of-OR-groups the
// engine enforces at plan time. The plugin schema only says which key columns
// a call has, not whether the table has a List call at all, so enforcement is
// limited to what is certain: a table with List key columns can be listed,
// and the query must satisfy either the List requirement or, when the table
// also has a Get call, the Get requirement ((list) OR (get), distributed into
// CNF). A table with only Get key columns may or may not be listable; it is
// not enforced here, and a missing qual surfaces as the plugin's own error.
func requiredFilters(ts *proto.TableSchema) [][]string {
	list := userKeyColumns(ts.GetListCallKeyColumnList())
	if len(list) == 0 {
		return nil
	}
	listCNF := requirementCNF(list)
	if len(listCNF) == 0 {
		return nil
	}
	get := userKeyColumns(ts.GetGetCallKeyColumnList())
	if len(get) == 0 {
		return dedupeGroups(listCNF)
	}
	getCNF := requirementCNF(get)
	if len(getCNF) == 0 {
		return nil
	}
	var cnf [][]string
	for _, a := range listCNF {
		for _, b := range getCNF {
			cnf = append(cnf, union(a, b))
		}
	}
	return dedupeGroups(cnf)
}

func union(a, b []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range append(append([]string{}, a...), b...) {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func dedupeGroups(cnf [][]string) [][]string {
	seen := map[string]bool{}
	var out [][]string
	for _, g := range cnf {
		g = union(g, nil)
		k := ""
		for _, s := range g {
			k += s + "\x00"
		}
		if !seen[k] {
			seen[k] = true
			out = append(out, g)
		}
	}
	return out
}

// requiredFilterPaths converts the CNF to vgi's [][][]string path form.
func requiredFilterPaths(cnf [][]string) [][][]string {
	if len(cnf) == 0 {
		return nil
	}
	out := make([][][]string, len(cnf))
	for i, g := range cnf {
		for _, c := range g {
			out[i] = append(out[i], []string{c})
		}
	}
	return out
}

var compareOps = map[vgi.ComparisonOp]string{
	vgi.OpEQ: "=", vgi.OpNE: "<>", vgi.OpLT: "<", vgi.OpLE: "<=", vgi.OpGT: ">", vgi.OpGE: ">=",
}

// buildQuals translates the pushed filters into Steampipe quals for the
// table's key columns. Filters it cannot translate are simply not sent: the
// worker re-applies every filter to the results either way.
func buildQuals(pf *vgi.PushdownFilters, ts *proto.TableSchema) map[string]*proto.Quals {
	out := map[string]*proto.Quals{}
	if pf == nil {
		return out
	}
	ops := keyColumnOps(ts)
	types := map[string]proto.ColumnType{}
	for _, c := range ts.GetColumns() {
		types[c.GetName()] = c.GetType()
	}
	add := func(col, op string, v *proto.QualValue) {
		if v == nil || !ops[col][op] {
			return
		}
		q := out[col]
		if q == nil {
			q = &proto.Quals{}
			out[col] = q
		}
		q.Quals = append(q.Quals, &proto.Qual{FieldName: col, Operator: &proto.Qual_StringValue{StringValue: op}, Value: v})
	}
	var visit func(f vgi.Filter)
	visit = func(f vgi.Filter) {
		switch x := f.(type) {
		case *vgi.AndFilter:
			for _, c := range x.Children {
				visit(c)
			}
		case *vgi.ConstantFilter:
			if op, ok := compareOps[x.Op]; ok {
				add(x.ColumnName(), op, scalarQualValue(x.Value, types[x.ColumnName()]))
			}
		case *vgi.InFilter:
			if x.Values == nil || x.Values.Len() == 0 || x.Values.Len() > 1000 {
				return
			}
			list := &proto.QualValueList{}
			for i := 0; i < x.Values.Len(); i++ {
				s, err := scalar.GetScalar(x.Values, i)
				if err != nil {
					return
				}
				v := scalarQualValue(s, types[x.ColumnName()])
				if v == nil {
					return
				}
				list.Values = append(list.Values, v)
			}
			if len(list.Values) == 1 {
				add(x.ColumnName(), "=", list.Values[0])
				return
			}
			add(x.ColumnName(), "=", &proto.QualValue{Value: &proto.QualValue_ListValue{ListValue: list}})
		case *vgi.IsNullFilter:
			add(x.ColumnName(), "is null", &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: ""}})
		case *vgi.IsNotNullFilter:
			add(x.ColumnName(), "is not null", &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: ""}})
		}
	}
	for _, f := range pf.Filters {
		visit(f)
	}
	return out
}

// scalarQualValue converts a filter literal to the QualValue the plugin
// expects for a column of type t, or nil when it cannot.
func scalarQualValue(s scalar.Scalar, t proto.ColumnType) *proto.QualValue {
	if s == nil || !s.IsValid() {
		return nil
	}
	switch v := s.(type) {
	case *scalar.String:
		str := v.String()
		switch t {
		case proto.ColumnType_JSON:
			return &proto.QualValue{Value: &proto.QualValue_JsonbValue{JsonbValue: str}}
		case proto.ColumnType_INET, proto.ColumnType_IPADDR, proto.ColumnType_CIDR:
			return &proto.QualValue{Value: &proto.QualValue_InetValue{InetValue: &proto.Inet{Addr: str, Cidr: str}}}
		case proto.ColumnType_LTREE:
			return &proto.QualValue{Value: &proto.QualValue_LtreeValue{LtreeValue: str}}
		case proto.ColumnType_STRING, proto.ColumnType_UNKNOWN:
			return &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: str}}
		}
		return nil
	case *scalar.Boolean:
		if t != proto.ColumnType_BOOL {
			return nil
		}
		return &proto.QualValue{Value: &proto.QualValue_BoolValue{BoolValue: v.Value}}
	case *scalar.Timestamp:
		if t != proto.ColumnType_TIMESTAMP && t != columnTypeDatetime {
			return nil
		}
		unit := v.Type.(*arrow.TimestampType).Unit
		tm := v.Value.ToTime(unit)
		return &proto.QualValue{Value: &proto.QualValue_TimestampValue{TimestampValue: timestamppb.New(tm)}}
	}
	// Numbers of any width.
	if f, ok := numericValue(s); ok {
		switch t {
		case proto.ColumnType_INT:
			if f != float64(int64(f)) {
				return nil
			}
			return &proto.QualValue{Value: &proto.QualValue_Int64Value{Int64Value: int64(f)}}
		case proto.ColumnType_DOUBLE:
			return &proto.QualValue{Value: &proto.QualValue_DoubleValue{DoubleValue: f}}
		}
	}
	return nil
}

func numericValue(s scalar.Scalar) (float64, bool) {
	switch v := s.(type) {
	case *scalar.Int8:
		return float64(v.Value), true
	case *scalar.Int16:
		return float64(v.Value), true
	case *scalar.Int32:
		return float64(v.Value), true
	case *scalar.Int64:
		return float64(v.Value), true
	case *scalar.Uint8:
		return float64(v.Value), true
	case *scalar.Uint16:
		return float64(v.Value), true
	case *scalar.Uint32:
		return float64(v.Value), true
	case *scalar.Uint64:
		return float64(v.Value), true
	case *scalar.Float32:
		return float64(v.Value), true
	case *scalar.Float64:
		return v.Value, true
	}
	return 0, false
}
