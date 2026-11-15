// Catalog, schema, secret and function documentation.

package steampipe

import (
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// SecretType is the DuckDB secret type holding a plugin's connection config.
const SecretType = "steampipe"

// PluginSecretType is registered with DuckDB so users can run
// CREATE SECRET (TYPE steampipe, PLUGIN 'aws', CONFIG '...').
var PluginSecretType = vgi.SecretTypeSpec{
	Name: SecretType,
	Description: "Connection config for a hosted Steampipe plugin: the HCL (or JSON) body of a Steampipe connection block, " +
		"typically its credentials. Merged over the ATTACH config option at query time.",
	Schema: arrow.NewSchema([]arrow.Field{
		{Name: "plugin", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"vgi_doc"},
				[]string{"Schema or plugin name the secret configures (e.g. 'aws'); the secret is ignored for other plugins. Empty applies to any."})},
		{Name: "config", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"redact", "vgi_doc"},
				[]string{"true", "Connection settings as an HCL body (token = \"...\") or a JSON object ({\"token\": \"...\"})."})},
	}, &secretSchemaMetadata),
}

var secretSchemaMetadata = arrow.NewMetadata(
	[]string{"vgi_doc_url"},
	[]string{SourceURL + "#credentials"})

const catalogComment = "Host Steampipe plugins and query their tables from DuckDB: each plugin named at ATTACH becomes a schema, " +
	"each plugin table a table, with key-column filters pushed down. No Steampipe CLI or Postgres needed."

const catalogDocLLM = "This worker hosts Steampipe plugins (the turbot/steampipe-plugin-sdk ecosystem: aws, github, csv, net, rss and " +
	"hundreds more) directly over their gRPC plugin protocol and exposes their tables as DuckDB tables. Pick plugins with the " +
	"plugins ATTACH option, a comma-separated list of [schema=]ref where ref is an OCI image such as csv, csv@1.3.0 or " +
	"turbot/aws, or a local plugin binary path. Each plugin becomes a schema of the attached catalog (named after the plugin " +
	"unless schema= is given) and each plugin table a table there, such as sp.csv.products. Non-secret connection settings go in " +
	"the config ATTACH option as one HCL block per schema (a csv block holding paths, for example). Credentials go in a DuckDB " +
	"secret of type steampipe whose PLUGIN field names the plugin, whose CONFIG field holds the HCL or JSON settings, and whose " +
	"SCOPE is steampipe://<schema>; it is merged over the ATTACH config at query time. WHERE filters on a table's key columns " +
	"(=, <>, <, <=, >, >=, IN, IS NULL) are sent to the plugin as quals, so the API does the filtering; every filter is still " +
	"applied exactly to the result. Some tables have required key columns: a query must filter on them, and DuckDB says which. " +
	"The main schema's plugin_table and plugin_key_column tables show each table's required filters and pushable columns, " +
	"plugin shows what is attached, and scan(schema, table_name) reads a plugin table directly. Replace steampipe with the ATTACH alias."

// CatalogDocMD is the catalog's long human-facing description.
const CatalogDocMD = "# Steampipe plugins in DuckDB\n\n" +
	"Hosts [Steampipe](https://steampipe.io) plugins and turns their tables into DuckDB tables, talking to each plugin directly " +
	"over the steampipe-plugin-sdk gRPC protocol: no Steampipe CLI, no embedded Postgres. Name plugins in the `plugins` ATTACH " +
	"option; each becomes a schema, and its tables are queried like any other.\n\n" +
	"- **Plugins**: OCI images from ghcr.io (fetched once, sha256-verified, cached) or local binaries.\n" +
	"- **Credentials**: a DuckDB secret of type `steampipe`, never the ATTACH string.\n" +
	"- **Pushdown**: key-column filters become plugin quals, and only the selected columns are hydrated.\n"

const schemaDocLLM = "The main schema describes and reads the plugins of the current ATTACH; the plugin tables themselves live " +
	"in one schema per plugin. Start with plugin_table to find a table and the filters it requires, then plugin_key_column to see " +
	"which filters reach the plugin's API (those are fast; others are applied after fetching). scan(schema, table_name, " +
	"max_rows := 0) reads a plugin table directly; max_rows is Steampipe's plugin-side limit. Everything here reflects the " +
	"current ATTACH, and is empty when it named no plugins."

// SchemaDocMD is the main schema's human-facing description.
const SchemaDocMD = "## main\n\n" +
	"Introspection of the plugins this ATTACH hosts: what the `plugins` option resolved to, which tables exist, which filters " +
	"each table needs and which filters the plugin's API can apply.\n\n" +
	"The plugin tables themselves live in one schema per plugin. When a plugin's tables depend on credentials that only a " +
	"secret holds, read them with `scan` instead."

type agentTask struct {
	Name   string `json:"name"`
	Prompt string `json:"prompt"`
}

var agentTestTasks = []agentTask{
	{Name: "attached_plugin_count", Prompt: "Using the steampipe worker, how many Steampipe plugins are attached to the steampipe catalog?"},
	{Name: "required_filter_tables", Prompt: "Using the steampipe worker, how many attached plugin tables require a WHERE filter?"},
	{Name: "pushable_columns", Prompt: "Using the steampipe worker, which columns of the fixture plugin's fixture_item table can be filtered through the plugin's API (excluding sp_connection_name)? List them alphabetically."},
	{Name: "items_by_category", Prompt: "Using the steampipe worker's scan function, how many fixture_item rows with id up to 30 are in each category?"},
	{Name: "expensive_software", Prompt: "In the steampipe fixture plugin, how many software items cost more than 20 dollars?"},
	{Name: "greet_grace", Prompt: "What greeting does the steampipe fixture plugin give the name 'Grace'?"},
	{Name: "lookup_item_seven", Prompt: "Using the steampipe fixture plugin's lookup table, what is the name of the item with id 7?"},
	{Name: "fixture_token", Prompt: "Does the steampipe fixture plugin's connection carry an API token?"},
}

// CatalogTags are the catalog-level discovery tags.
var CatalogTags = map[string]string{
	"vgi.title":              "Steampipe Plugins",
	"vgi.subtitle":           "Every Steampipe plugin's tables as DuckDB tables, with key-column pushdown and secret-held credentials.",
	"vgi.doc_llm":            catalogDocLLM,
	"vgi.doc_md":             CatalogDocMD,
	"vgi.source_url":         SourceURL,
	"vgi.icon_url":           "https://query.farm/media-kit/products/vgi/vgi-logo.png",
	"vgi.license":            "0BSD",
	"vgi.author":             "Query.Farm",
	"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
	"vgi.keywords":           keywordsArray("steampipe, steampipe plugins, cloud inventory, saas apis, aws, github, csv, oci, grpc, key columns"),
	"vgi.support_contact":    SourceURL + "/issues",
	"vgi.support_policy_url": SourceURL + "#support",
	"vgi.agent_test_tasks":   mustJSON(agentTestTasks),
}

func schemaTags(allExamples []exampleQuery) map[string]string {
	return map[string]string{
		"vgi.title":           "Steampipe Plugin Introspection",
		"domain":              "integrations",
		"vgi.keywords":        keywordsArray("steampipe, plugins, tables, key columns, quals, scan"),
		"vgi.doc_llm":         schemaDocLLM,
		"vgi.doc_md":          SchemaDocMD,
		"vgi.categories":      mustJSON(SchemaCategories),
		"vgi.example_queries": mustJSON(allExamples),
	}
}

var scanExamples = []vgi.CatalogExample{
	{SQL: "SELECT category, count(*) AS items FROM steampipe.main.scan('fixture', 'fixture_item') WHERE id <= 12 GROUP BY category ORDER BY category",
		Description: "Read a plugin table directly, filtering on a key column"},
	{SQL: "SELECT count(*) AS fetched FROM steampipe.main.scan('fixture', 'fixture_item', max_rows := 10)",
		Description: "Ask the plugin for at most 10 rows (Steampipe's limit pushdown)"},
}

func scanTags() map[string]string {
	tags := objectTags(docs{
		Title:    "Scan Plugin Table",
		Category: "scan",
		Keywords: "steampipe, plugin table, query, quals, key columns, limit",
		DocLLM: "Read one table of an attached Steampipe plugin: scan(schema, table_name, max_rows := 0). schema is a plugin schema " +
			"of this ATTACH (see the plugin table), table_name a table of that plugin (see plugin_table). The output columns are " +
			"the plugin table's own columns. WHERE filters on key columns are sent to the plugin as quals and every filter is " +
			"applied exactly; tables with required key columns need a WHERE filter on them. max_rows is the plugin-side row limit, " +
			"applied before filters that are not key-column quals. The tables in each plugin schema are the usual way to query; " +
			"call scan() directly when a plugin's tables only appear once the credentials held in a secret are applied.",
		DocMD: "## scan(schema, table_name, max_rows := 0)\n\nReads one plugin table; the output columns are the table's own. " +
			"The tables in each plugin schema are backed by this function, so you rarely need it, except for plugins whose " +
			"tables only appear once the credentials in a secret are applied.",
	})
	tags["vgi.result_dynamic_columns_md"] = "The output columns are the plugin table's own columns, as listed for the table in " +
		"its plugin schema: Steampipe BOOL, INT, DOUBLE, STRING, JSON and TIMESTAMP columns become BOOLEAN, BIGINT, DOUBLE, " +
		"VARCHAR, JSON and TIMESTAMP WITH TIME ZONE; INET, CIDR and LTREE columns are VARCHAR. Every table also has the two " +
		"columns below. For example, scan('fixture', 'fixture_greeting') returns:\n\n" +
		"| Name | Type | Description |\n|---|---|---|\n" +
		"| name | VARCHAR | The name greeted (the table's required key column). |\n" +
		"| message | VARCHAR | The greeting. |\n" +
		"| sp_connection_name | VARCHAR | The plugin schema the row came from. |\n" +
		"| sp_ctx | JSON | Steampipe connection context (connection name, SDK version). |\n"
	tags["vgi.executable_examples"] = mustJSON([]map[string]any{
		{"description": "Items per category among the first twelve ids",
			"sql":             "SELECT category, count(*) AS items FROM steampipe.main.scan('fixture', 'fixture_item') WHERE id <= 12 GROUP BY category ORDER BY category",
			"expected_result": [][]any{{"hardware", 4}, {"services", 4}, {"software", 4}}},
		{"description": "A plugin-side limit of 10 rows",
			"sql":             "SELECT count(*) AS fetched FROM steampipe.main.scan('fixture', 'fixture_item', max_rows := 10)",
			"expected_result": [][]any{{10}}},
	})
	return tags
}

// tableDocs is the discovery metadata of one of the main schema's tables.
type tableDocs struct {
	Title, Comment, Keywords, DocLLM, DocMD string
	Examples                                []vgi.CatalogExample
	Executable                              []map[string]any
}

func (d tableDocs) tags() map[string]string {
	tags := map[string]string{
		"vgi.title":    d.Title,
		"vgi.category": "introspection",
		"domain":       "integrations",
		"vgi.keywords": keywordsArray(d.Keywords),
		"vgi.doc_llm":  d.DocLLM,
		"vgi.doc_md":   d.DocMD,
	}
	tags = withExamples(tags, d.Examples)
	if len(d.Executable) > 0 {
		tags["vgi.executable_examples"] = mustJSON(d.Executable)
	}
	return tags
}

var pluginsDocs = tableDocs{
	Title:    "Attached Plugin Inventory",
	Comment:  "The Steampipe plugins of this ATTACH, one row per plugin schema, with image digests and table counts.",
	Keywords: "steampipe, plugins, attached plugins, oci image, digest, version",
	DocLLM: "One row per Steampipe plugin of the current ATTACH: schema_name (where its tables live), plugin (short name), ref " +
		"(the OCI image or local binary it came from), image_digest and layer_digest (sha256 digests verified at download, NULL " +
		"for a local binary), plugin_version, binary_path, sdk_version, schema_mode (static, or dynamic when the tables depend " +
		"on the connection config) and table_count. Use it to see what an ATTACH resolved its plugins option to.",
	DocMD: "## plugin\n\nWhat the ATTACH's `plugins` option resolved to: one row per plugin schema, with the verified image " +
		"digests of plugins fetched from a registry and the SDK version each plugin was built with.",
	Examples: []vgi.CatalogExample{{SQL: "SELECT schema_name, plugin, plugin_version, table_count FROM steampipe.main.plugin ORDER BY schema_name",
		Description: "The attached plugins and how many tables each serves"}},
	Executable: []map[string]any{{"description": "The plugin the linter's worker launches with",
		"sql":             "SELECT schema_name, plugin, sdk_version, table_count FROM steampipe.main.plugin ORDER BY schema_name",
		"expected_result": [][]any{{"fixture", "fixture", "5.14.0", 4}}}},
}

var pluginTablesDocs = tableDocs{
	Title:    "Plugin Table Catalog",
	Comment:  "Every table of the attached plugins, with the WHERE filters a query on it must include and its key columns.",
	Keywords: "steampipe, plugin tables, required filters, key columns, get, list",
	DocLLM: "One row per table of the attached plugins: schema_name, table_name, description, column_count, required_filters, " +
		"get_key_columns and list_key_columns. required_filters names the columns a query on the table must filter on, written " +
		"'a; b or c' for a AND (b OR c); it is NULL when the table can be scanned without a filter. Key columns are the columns " +
		"whose filters the plugin turns into API parameters. Read this before querying an unfamiliar plugin table.",
	DocMD: "## plugin_table\n\nThe tables each plugin serves. Steampipe tables often need a filter on a *required key column* " +
		"(an RSS item table needs the feed URL, for example); `required_filters` says which, and DuckDB refuses a query that omits it.",
	Examples: []vgi.CatalogExample{{SQL: "SELECT schema_name, table_name, required_filters FROM steampipe.main.plugin_table WHERE required_filters IS NOT NULL ORDER BY ALL",
		Description: "Tables that need a WHERE filter, and on which columns"}},
	Executable: []map[string]any{{"description": "Tables of the fixture plugin that need a filter",
		"sql":             "SELECT table_name, required_filters FROM steampipe.main.plugin_table WHERE required_filters IS NOT NULL ORDER BY table_name",
		"expected_result": [][]any{{"fixture_greeting", "name"}, {"fixture_lookup", "id"}}}},
}

var keyColumnsDocs = tableDocs{
	Title:    "Pushable Key Columns",
	Comment:  "The key columns of every attached plugin table: the filters the plugin pushes to its API, and which are required.",
	Keywords: "steampipe, key columns, quals, filter pushdown, operators, required",
	DocLLM: "One row per key column per table and call: schema_name, table_name, column_name, call ('get' looks one row up, " +
		"'list' filters a listing), require ('required', 'optional', or 'any_of' meaning at least one of the table's any_of " +
		"columns) and operators (the comparisons the plugin accepts: =, <>, <, <=, >, >=; IN is sent as =, IS NULL as is null). " +
		"Filtering on a key column with one of its operators makes the plugin's API do the filtering, which is much faster and " +
		"cheaper than filtering a full listing. sp_connection_name, which every table has, is listed too.",
	DocMD: "## plugin_key_column\n\nWhich `WHERE` filters reach the plugin's API. A filter on any other column, or with another " +
		"operator, is still applied exactly, but after the plugin has fetched the rows.",
	Examples: []vgi.CatalogExample{{SQL: "SELECT table_name, column_name, require, operators FROM steampipe.main.plugin_key_column WHERE call = 'list' AND column_name <> 'sp_connection_name' ORDER BY ALL",
		Description: "Columns each table can filter on through the plugin's API"}},
	Executable: []map[string]any{{"description": "Required key columns of the fixture plugin",
		"sql":             "SELECT table_name, column_name, call FROM steampipe.main.plugin_key_column WHERE require = 'required' ORDER BY ALL",
		"expected_result": [][]any{{"fixture_greeting", "name", "list"}, {"fixture_lookup", "id", "get"}, {"fixture_lookup", "id", "list"}}}},
}
