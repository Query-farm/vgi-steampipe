// Discovery metadata generated for plugin schemas and tables, from what the
// plugin reports: descriptions, key columns and required filters.

package steampipe

import (
	"fmt"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// pluginCategory is the single category of a plugin schema.
const pluginCategory = "tables"

func titleCase(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '-' })
	for i, p := range parts {
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

// placeholder is an example literal for a filter on a column of type t.
func placeholder(t proto.ColumnType) string {
	switch t {
	case proto.ColumnType_INT, proto.ColumnType_DOUBLE:
		return "1"
	case proto.ColumnType_BOOL:
		return "true"
	}
	return "'example'"
}

// exampleSQL is a generated example query for a plugin table: a few columns,
// and a filter on each required key column group.
func exampleSQL(schema, table string, ts *proto.TableSchema) string {
	types := map[string]proto.ColumnType{}
	var cols []string
	for _, c := range visibleColumns(ts) {
		types[c.GetName()] = c.GetType()
		if len(cols) < 3 && !strings.HasPrefix(c.GetName(), "sp_") {
			cols = append(cols, c.GetName())
		}
	}
	if len(cols) == 0 {
		cols = []string{"*"}
	}
	q := fmt.Sprintf("SELECT %s FROM %s.%s.%s", strings.Join(cols, ", "), CatalogName, schema, table)
	var where []string
	for _, g := range requiredFilters(ts) {
		where = append(where, fmt.Sprintf("%s = %s", g[0], placeholder(types[g[0]])))
	}
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	return q + " LIMIT 10"
}

func keyColumnSummary(ts *proto.TableSchema) string {
	var parts []string
	for _, call := range []struct {
		name string
		kcs  []*proto.KeyColumn
	}{{"list", ts.GetListCallKeyColumnList()}, {"get", ts.GetGetCallKeyColumnList()}} {
		for _, kc := range mergeKeyColumns(userKeyColumns(call.kcs)) {
			parts = append(parts, fmt.Sprintf("%s (%s, %s: %s)", kc.GetName(), call.name, kc.GetRequire(), strings.Join(kc.GetOperators(), " ")))
		}
	}
	return strings.Join(parts, "; ")
}

func pluginTableTags(schema, table string, ts *proto.TableSchema) map[string]string {
	desc := strings.TrimSpace(ts.GetDescription())
	if desc == "" {
		desc = "Steampipe table " + table + "."
	}
	name := strings.TrimPrefix(table, schema+"_")
	llm := fmt.Sprintf("%s A table of the Steampipe %s plugin, with %d columns.", strings.TrimRight(desc, ".")+".", schema, len(visibleColumns(ts)))
	md := fmt.Sprintf("## %s\n\n%s", table, desc)
	if req := requiredFilters(ts); len(req) > 0 {
		var groups []string
		for _, g := range req {
			groups = append(groups, strings.Join(g, " or "))
		}
		llm += " A query must filter on " + strings.Join(groups, " and ") + "."
		md += "\n\nRequires a `WHERE` filter on " + strings.Join(groups, " and ") + "."
	}
	if kc := keyColumnSummary(ts); kc != "" {
		llm += " Filters on these key columns are pushed to the plugin's API: " + kc + "."
	}
	keywords := []string{"steampipe", schema, strings.ReplaceAll(name, "_", " ")}
	ex := exampleSQL(schema, table, ts)
	return map[string]string{
		"vgi.title":           titleCase(name),
		"vgi.category":        pluginCategory,
		"provider":            schema,
		"vgi.keywords":        mustJSON(keywords),
		"vgi.doc_llm":         llm,
		"vgi.doc_md":          md,
		"vgi.example_queries": mustJSON([]exampleQuery{{Description: "A few rows of " + table, SQL: ex}}),
	}
}

func pluginSchemaTags(ap *attachedPlugin, s *proto.Schema) map[string]string {
	tables := sortedTableNames(s)
	var examples []exampleQuery
	for _, t := range tables {
		if len(examples) >= 5 {
			break
		}
		examples = append(examples, exampleQuery{Description: "A few rows of " + t, SQL: exampleSQL(ap.Ref.Schema, t, s.GetSchema()[t])})
	}
	mode := s.GetMode()
	if mode == "" {
		mode = "static"
	}
	llm := fmt.Sprintf("Tables of the Steampipe %s plugin (%s), %d in all, schema mode %s. Each table is read through the plugin; "+
		"filters on key columns reach the plugin's API, and some tables require one (see steampipe.main.plugin_table).",
		ap.Ref.ShortName, ap.Ref.Display(), len(tables), mode)
	md := fmt.Sprintf("## %s\n\nThe tables of the Steampipe **%s** plugin, loaded from `%s`.\n\n"+
		"Filters on a table's key columns are sent to the plugin as quals; see `main.plugin_key_column`.", ap.Ref.Schema, ap.Ref.ShortName, ap.Ref.Display())
	return map[string]string{
		"vgi.title":             fmt.Sprintf("Steampipe %s Plugin", titleCase(ap.Ref.ShortName)),
		"domain":                "integrations",
		"vgi.keywords":          mustJSON([]string{"steampipe", ap.Ref.ShortName, "plugin tables"}),
		"vgi.doc_llm":           llm,
		"vgi.doc_md":            md,
		"vgi.categories":        mustJSON([]category{{Name: pluginCategory, Title: "Plugin tables", Description: "Tables served by the " + ap.Ref.ShortName + " plugin."}}),
		"vgi.example_queries":   mustJSON(examples),
		"steampipe.plugin":      ap.Ref.Display(),
		"steampipe.sdk_version": s.GetSdkVersion(),
	}
}
