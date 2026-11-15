// Introspection table functions over the plugins of the current ATTACH:
// plugins(), tables() and key_columns(). They read the schemas discovered
// with the ATTACH's (non-secret) config.

package steampipe

import (
	"context"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// InfoState records whether the single batch has been emitted.
type InfoState struct {
	Done bool
}

type infoRow []any // values in schema order: string, *string, int64, []string

// infoFunc is a no-argument table function that emits rows computed from the
// attach state.
type infoFunc struct {
	name   string
	meta   func() vgi.FunctionMetadata
	schema *arrow.Schema
	rows   func(st *attachState) ([]infoRow, error)
}

func (f *infoFunc) Name() string                                    { return f.name }
func (f *infoFunc) Metadata() vgi.FunctionMetadata                  { return f.meta() }
func (f *infoFunc) ArgumentSpecs() []vgi.ArgSpec                    { return nil }
func (f *infoFunc) NewState(*vgi.ProcessParams) (*InfoState, error) { return &InfoState{}, nil }
func (f *infoFunc) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(f.schema)
}

func (f *infoFunc) Process(_ context.Context, params *vgi.ProcessParams, state *InfoState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	state.Done = true
	st, _ := decodeAttachState(params.AttachScope)
	rows, err := f.rows(st)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return out.Finish()
	}
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, f.schema.NumFields())
	for i, field := range f.schema.Fields() {
		b := array.NewBuilder(mem, field.Type)
		for _, r := range rows {
			switch v := r[i].(type) {
			case nil:
				b.AppendNull()
			case string:
				b.(*array.StringBuilder).Append(v)
			case *string:
				if v == nil {
					b.AppendNull()
				} else {
					b.(*array.StringBuilder).Append(*v)
				}
			case int64:
				b.(*array.Int64Builder).Append(v)
			case []string:
				lb := b.(*array.ListBuilder)
				lb.Append(true)
				vb := lb.ValueBuilder().(*array.StringBuilder)
				for _, s := range v {
					vb.Append(s)
				}
			}
		}
		cols[i] = b.NewArray()
		b.Release()
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	if err := out.EmitArrays(cols, int64(len(rows))); err != nil {
		return err
	}
	return out.Finish()
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// forEachSchema discovers each attached plugin's schema.
func forEachSchema(st *attachState, fn func(ap *attachedPlugin, s *proto.Schema)) error {
	if st == nil {
		return nil
	}
	for i := range st.Plugins {
		ap := &st.Plugins[i]
		s, err := discoverSchema(context.Background(), ap, ap.Config)
		if err != nil {
			return err
		}
		fn(ap, s)
	}
	return nil
}

var listOfString = arrow.ListOf(arrow.BinaryTypes.String)

var pluginsSchema = arrow.NewSchema([]arrow.Field{
	column("schema_name", arrow.BinaryTypes.String, "Schema the plugin's tables live in.", false),
	column("plugin", arrow.BinaryTypes.String, "Plugin short name, e.g. csv or aws.", false),
	column("ref", arrow.BinaryTypes.String, "Resolved plugin reference: OCI image (registry/repository:tag or @digest) or local binary path.", false),
	column("image_digest", arrow.BinaryTypes.String, "sha256 digest of the OCI manifest the binary came from; NULL for a local binary.", true),
	column("layer_digest", arrow.BinaryTypes.String, "Verified sha256 digest of the plugin binary layer; NULL for a local binary.", true),
	column("plugin_version", arrow.BinaryTypes.String, "Version annotation of the OCI image, when published.", true),
	column("binary_path", arrow.BinaryTypes.String, "Path of the executable the worker runs.", false),
	column("sdk_version", arrow.BinaryTypes.String, "steampipe-plugin-sdk version the plugin was built with.", true),
	column("schema_mode", arrow.BinaryTypes.String, "static, or dynamic when the tables depend on the connection config.", true),
	column("table_count", arrow.PrimitiveTypes.Int64, "Number of tables the plugin serves under the ATTACH config.", false),
}, nil)

var tablesSchema = arrow.NewSchema([]arrow.Field{
	column("schema_name", arrow.BinaryTypes.String, "Plugin schema.", false),
	column("table_name", arrow.BinaryTypes.String, "Plugin table name.", false),
	column("description", arrow.BinaryTypes.String, "The plugin's description of the table.", true),
	column("column_count", arrow.PrimitiveTypes.Int64, "Number of columns.", false),
	column("required_filters", arrow.BinaryTypes.String, "WHERE filters a query must include, as 'a; b or c' (AND of OR-groups); NULL when none are required.", true),
	column("get_key_columns", listOfString, "Columns a Get call looks rows up by.", false),
	column("list_key_columns", listOfString, "Columns a List call can filter on through the API.", false),
}, nil)

var keyColumnsSchema = arrow.NewSchema([]arrow.Field{
	column("schema_name", arrow.BinaryTypes.String, "Plugin schema.", false),
	column("table_name", arrow.BinaryTypes.String, "Plugin table name.", false),
	column("column_name", arrow.BinaryTypes.String, "Key column.", false),
	column("call", arrow.BinaryTypes.String, "'get' (look up one row) or 'list' (filter a listing).", false),
	column("require", arrow.BinaryTypes.String, "'required', 'optional' or 'any_of' (at least one of the table's any_of columns).", false),
	column("operators", listOfString, "Operators pushed to the plugin for this column: =, <>, <, <=, >, >=, IN (as =), IS [NOT] NULL.", false),
}, nil)

func pluginsRows(st *attachState) ([]infoRow, error) {
	var rows []infoRow
	err := forEachSchema(st, func(ap *attachedPlugin, s *proto.Schema) {
		rows = append(rows, infoRow{ap.Ref.Schema, ap.Ref.ShortName, ap.Ref.Display(), strPtr(ap.Fetched.ManifestDigest),
			strPtr(ap.Fetched.LayerDigest), strPtr(ap.Fetched.Version), ap.Fetched.Binary, strPtr(s.GetSdkVersion()),
			strPtr(s.GetMode()), int64(len(s.GetSchema()))})
	})
	return rows, err
}

func keyNames(kcs []*proto.KeyColumn) []string {
	out := []string{}
	for _, k := range userKeyColumns(kcs) {
		out = append(out, k.GetName())
	}
	return out
}

func tablesRows(st *attachState) ([]infoRow, error) {
	var rows []infoRow
	err := forEachSchema(st, func(ap *attachedPlugin, s *proto.Schema) {
		for _, name := range sortedTableNames(s) {
			ts := s.GetSchema()[name]
			var req *string
			if cnf := requiredFilters(ts); len(cnf) > 0 {
				var parts []string
				for _, g := range cnf {
					parts = append(parts, strings.Join(g, " or "))
				}
				r := strings.Join(parts, "; ")
				req = &r
			}
			rows = append(rows, infoRow{ap.Ref.Schema, name, strPtr(ts.GetDescription()), int64(len(visibleColumns(ts))), req,
				keyNames(ts.GetGetCallKeyColumnList()), keyNames(ts.GetListCallKeyColumnList())})
		}
	})
	return rows, err
}

func keyColumnsRows(st *attachState) ([]infoRow, error) {
	var rows []infoRow
	err := forEachSchema(st, func(ap *attachedPlugin, s *proto.Schema) {
		for _, name := range sortedTableNames(s) {
			ts := s.GetSchema()[name]
			for _, call := range []struct {
				name string
				kcs  []*proto.KeyColumn
			}{{"get", ts.GetGetCallKeyColumnList()}, {"list", ts.GetListCallKeyColumnList()}} {
				for _, kc := range mergeKeyColumns(call.kcs) {
					rows = append(rows, infoRow{ap.Ref.Schema, name, kc.GetName(), call.name, kc.GetRequire(), kc.GetOperators()})
				}
			}
		}
	})
	return rows, err
}

// mergeKeyColumns folds repeated definitions of one column into one: the
// union of their operators, and the strictest requirement.
func mergeKeyColumns(kcs []*proto.KeyColumn) []*proto.KeyColumn {
	rank := map[string]int{"optional": 0, "any_of": 1, "required": 2}
	var out []*proto.KeyColumn
	byName := map[string]*proto.KeyColumn{}
	for _, kc := range kcs {
		require := kc.GetRequire()
		if require == "" {
			require = "required"
		}
		ops := kc.GetOperators()
		if len(ops) == 0 {
			ops = []string{"="}
		}
		m, ok := byName[kc.GetName()]
		if !ok {
			m = &proto.KeyColumn{Name: kc.GetName(), Require: require}
			byName[kc.GetName()] = m
			out = append(out, m)
		}
		if rank[require] > rank[m.Require] {
			m.Require = require
		}
		for _, op := range ops {
			seen := false
			for _, have := range m.Operators {
				seen = seen || have == op
			}
			if !seen {
				m.Operators = append(m.Operators, op)
			}
		}
	}
	return out
}

// NewInfoTables returns the main schema's plugin, plugin_table and
// plugin_key_column tables, each scanned by a table-only function.
func NewInfoTables() []vgi.CatalogTable {
	mk := func(table, fn string, schema *arrow.Schema, pk []string, rows func(*attachState) ([]infoRow, error), d tableDocs) vgi.CatalogTable {
		f := &infoFunc{name: fn, schema: schema, rows: rows, meta: func() vgi.FunctionMetadata {
			return vgi.FunctionMetadata{Description: d.Comment, Stability: vgi.StabilityVolatile}
		}}
		cols := make([][]string, len(pk))
		for i, c := range pk {
			cols[i] = []string{c}
		}
		record, err := vgi.ConstraintRecord(vgi.ConstraintRecordSpec{
			SchemaPath: []string{"main"}, Table: table, ID: table + ":primary_key",
			Kind: vgi.ConstraintPrimaryKey, Columns: cols,
			Enforcement: vgi.ConstraintEnforced, Validation: vgi.ConstraintAllRows,
		})
		if err != nil {
			panic(err)
		}
		records := [][]byte{record}
		inPK := map[string]bool{}
		for _, c := range pk {
			inPK[c] = true
		}
		for _, fld := range schema.Fields() {
			if fld.Nullable || inPK[fld.Name] {
				continue
			}
			nn, err := vgi.ConstraintRecord(vgi.ConstraintRecordSpec{
				SchemaPath: []string{"main"}, Table: table, ID: table + ":not_null:" + fld.Name,
				Kind: vgi.ConstraintNotNull, Columns: [][]string{{fld.Name}},
				Enforcement: vgi.ConstraintEnforced, Validation: vgi.ConstraintAllRows,
			})
			if err != nil {
				panic(err)
			}
			records = append(records, nn)
		}
		return vgi.CatalogTable{
			Name:        table,
			Columns:     schema,
			Comment:     d.Comment,
			Tags:        d.tags(),
			Function:    vgi.AsTableFunction[InfoState](f),
			Constraints: records,
		}
	}
	return []vgi.CatalogTable{
		mk("plugin", "plugin_scan", pluginsSchema, []string{"schema_name"}, pluginsRows, pluginsDocs),
		mk("plugin_table", "plugin_table_scan", tablesSchema, []string{"schema_name", "table_name"}, tablesRows, pluginTablesDocs),
		mk("plugin_key_column", "plugin_key_column_scan", keyColumnsSchema, []string{"schema_name", "table_name", "column_name", "call"}, keyColumnsRows, keyColumnsDocs),
	}
}
