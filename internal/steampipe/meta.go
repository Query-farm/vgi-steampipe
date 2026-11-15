// Discovery metadata: the one file to edit when you rename or extend the worker.
//
// DuckDB, coding agents and the vgi-lint metadata checker all read these tags.
// Keep every object documented and `make lint-explain` stays at 100.
//
//   - Catalog-level tags (title, license, author, links, long description)
//     live in CatalogTags below.
//   - Every function carries objectTags(...): a title, a category from
//     SchemaCategories, search keywords, and LLM- and human-facing docs.

package steampipe

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// CatalogName is the name DuckDB attaches the worker under.
const CatalogName = "steampipe"

// SourceURL is where this worker's code lives.
const SourceURL = "https://github.com/Query-farm/vgi-steampipe"

// category is one entry of the schema's vgi.categories list.
type category struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// SchemaCategories group functions in docs and agent tools. Every function's
// vgi.category must name one of these.
var SchemaCategories = []category{
	{Name: "scan", Title: "Plugin tables", Description: "Read rows from the tables of attached Steampipe plugins."},
	{Name: "introspection", Title: "Introspection", Description: "List attached plugins, their tables and their key columns."},
}

// mustJSON marshals v, panicking on failure (only static data is passed).
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// keywordsArray turns "a, b, c" into the JSON array string vgi.keywords requires.
func keywordsArray(keywords string) string {
	out := []string{}
	for _, k := range strings.Split(keywords, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return mustJSON(out)
}

// docs is the set of standard per-object discovery fields.
type docs struct {
	Title    string
	Category string
	Keywords string
	DocLLM   string
	DocMD    string
}

// objectTags builds the standard per-object discovery tags.
func objectTags(d docs) map[string]string {
	known := false
	for _, c := range SchemaCategories {
		known = known || c.Name == d.Category
	}
	if !known {
		panic(fmt.Sprintf("unknown category %q; add it to SchemaCategories", d.Category))
	}
	return map[string]string{
		"vgi.title":    d.Title,
		"vgi.category": d.Category,
		"vgi.keywords": keywordsArray(d.Keywords),
		"vgi.doc_llm":  d.DocLLM,
		"vgi.doc_md":   d.DocMD,
	}
}

// column builds an Arrow field whose comment shows up in DuckDB's DESCRIBE and agent tools.
func column(name string, typ arrow.DataType, comment string, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: typ, Nullable: nullable,
		Metadata: arrow.NewMetadata([]string{"comment"}, []string{comment})}
}

// exampleQuery is one entry of vgi.example_queries.
type exampleQuery struct {
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

// exampleQueries converts catalog examples to vgi.example_queries entries.
func exampleQueries(lists ...[]vgi.CatalogExample) []exampleQuery {
	out := []exampleQuery{}
	for _, examples := range lists {
		for _, ex := range examples {
			out = append(out, exampleQuery{Description: ex.Description, SQL: ex.SQL})
		}
	}
	return out
}

// withExamples publishes examples (with descriptions) as vgi.example_queries.
// Overloads share one SQL name, so pass every overload's examples.
func withExamples(tags map[string]string, lists ...[]vgi.CatalogExample) map[string]string {
	tags["vgi.example_queries"] = mustJSON(exampleQueries(lists...))
	return tags
}
