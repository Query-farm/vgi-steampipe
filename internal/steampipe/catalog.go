// The per-ATTACH catalog: one schema per plugin, one table per plugin table.
//
//	ATTACH 'steampipe' AS sp (TYPE vgi, LOCATION '...',
//	    plugins 'csv@1.3.0, fx=/opt/steampipe-plugin-fixture',
//	    config  'csv { paths = ["/data/*.csv"] }');
//
// At ATTACH the worker resolves every plugin (fetching OCI images into the
// cache), starts it with the ATTACH's non-secret config, and asks it for its
// schema. The schemas are served through catalog_contents, the extension's
// default whole-catalog load, so they exist only for the ATTACH that named
// them. Each table's scan is steampipe.main.scan(schema, table).

package steampipe

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// attachTimeout bounds plugin fetch + start + schema discovery at ATTACH.
const attachTimeout = 10 * time.Minute

// attachedPlugin is one plugin of an ATTACH, as recorded in the attach data.
type attachedPlugin struct {
	Ref     PluginRef     `json:"ref"`
	Fetched FetchedPlugin `json:"fetched"`
	// Config is the ATTACH's non-secret connection config (HCL) for it.
	Config string `json:"config,omitempty"`
}

// attachState is the worker's per-ATTACH data (attach_opaque_data). It holds
// no credentials: secret config is resolved per query.
type attachState struct {
	Marker  string           `json:"vgi_steampipe"`
	Plugins []attachedPlugin `json:"plugins"`
}

const attachMarker = "v1"

func decodeAttachState(data []byte) (*attachState, bool) {
	if len(data) == 0 || data[0] != '{' {
		return nil, false
	}
	var st attachState
	if err := json.Unmarshal(data, &st); err != nil || st.Marker != attachMarker {
		return nil, false
	}
	return &st, true
}

func (st *attachState) plugin(schema string) (*attachedPlugin, bool) {
	if st == nil {
		return nil, false
	}
	schema = strings.ToLower(schema)
	for i := range st.Plugins {
		if st.Plugins[i].Ref.Schema == schema {
			return &st.Plugins[i], true
		}
	}
	return nil, false
}

func (st *attachState) schemaNames() []string {
	var out []string
	if st != nil {
		for _, p := range st.Plugins {
			out = append(out, p.Ref.Schema)
		}
	}
	return out
}

// Attach option names.
const (
	optPlugins  = "plugins"
	optConfig   = "config"
	optCacheDir = "cache_dir"
	optRegistry = "registry"
)

// AttachOptionSpecs are the ATTACH options this worker accepts.
var AttachOptionSpecs = []vgi.AttachOptionSpec{
	{Name: optPlugins, Type: arrow.BinaryTypes.String, Description: "Comma-separated Steampipe plugins to host, each `[schema=]ref`. A ref is an OCI image " +
		"(`csv`, `csv@1.3.0`, `turbot/aws`, `ghcr.io/org/repo/name:tag`, `name@sha256:<digest>`) or a local plugin binary path. Each plugin becomes a schema named " +
		"after it (or `schema`), holding one table per plugin table."},
	{Name: optConfig, Type: arrow.BinaryTypes.String, Description: "Non-secret connection settings, one HCL block per schema (`csv { paths = [\"/data/*.csv\"] }`) or " +
		"a JSON object keyed by schema. Used to discover tables at ATTACH and for every query. Never put credentials here: use CREATE SECRET (TYPE steampipe, ...)."},
	{Name: optCacheDir, Type: arrow.BinaryTypes.String, Description: "Directory for plugins fetched from a registry (default: <user cache dir>/vgi-steampipe/plugins). " +
		"Binaries are stored by verified sha256 digest and reused offline."},
	{Name: optRegistry, Type: arrow.BinaryTypes.String, Description: "OCI registry for plugin refs that name none (default https://ghcr.io); for mirrors and tests."},
}

// optionStrings reads the string ATTACH options from the request batch.
func optionStrings(req *vgi.CatalogAttachRequestWire) (map[string]string, error) {
	out := map[string]string{}
	if req.Options == nil || len(*req.Options) == 0 {
		return out, nil
	}
	batch, err := vgi.DeserializeRecordBatch(*req.Options)
	if err != nil {
		return nil, fmt.Errorf("reading ATTACH options: %w", err)
	}
	defer batch.Release()
	if batch.NumRows() == 0 {
		return out, nil
	}
	for i, f := range batch.Schema().Fields() {
		col := batch.Column(i)
		if col.IsNull(0) {
			continue
		}
		if s, ok := col.(*array.String); ok {
			out[strings.ToLower(f.Name)] = s.Value(0)
		}
	}
	return out, nil
}

// attachDefaults are the worker's launch-time defaults for ATTACH options
// (see SetAttachDefaults).
var attachDefaults = map[string]string{}

// SetAttachDefaults sets the values used for ATTACH options an ATTACH leaves
// unset (keys: plugins, config, cache_dir, registry). When an ATTACH sets
// plugins, the default config does not apply to it.
func SetAttachDefaults(d map[string]string) { attachDefaults = d }

// validateAttach resolves and starts every plugin of an ATTACH.
func validateAttach(req *vgi.CatalogAttachRequestWire, _ *vgirpc.CallContext) (*vgi.AttachDecision, error) {
	opts, err := optionStrings(req)
	if err != nil {
		return nil, err
	}
	if opts[optPlugins] == "" && attachDefaults[optPlugins] != "" {
		opts[optPlugins] = attachDefaults[optPlugins]
		if opts[optConfig] == "" {
			opts[optConfig] = attachDefaults[optConfig]
		}
	}
	for _, k := range []string{optCacheDir, optRegistry} {
		if opts[k] == "" {
			opts[k] = attachDefaults[k]
		}
	}
	refs, err := ParsePluginRefs(opts[optPlugins], opts[optRegistry])
	if err != nil {
		return nil, err
	}
	configs, err := parseAttachConfig(opts[optConfig])
	if err != nil {
		return nil, err
	}
	known := map[string]bool{}
	for _, r := range refs {
		known[r.Schema] = true
	}
	for schema := range configs {
		if !known[schema] {
			return nil, fmt.Errorf("config option has settings for %q, which is not a schema in the plugins option", schema)
		}
	}
	cacheDir := opts[optCacheDir]
	if cacheDir == "" {
		cacheDir = DefaultCacheDir()
	}
	ctx, cancel := context.WithTimeout(context.Background(), attachTimeout)
	defer cancel()
	st := &attachState{Marker: attachMarker}
	for _, r := range refs {
		ap := attachedPlugin{Ref: r, Config: configs[r.Schema]}
		if r.IsLocal() {
			ap.Fetched = FetchedPlugin{Binary: r.Path}
		} else {
			fp, err := FetchPlugin(ctx, r, cacheDir)
			if err != nil {
				return nil, err
			}
			ap.Fetched = *fp
		}
		if _, err := discoverSchema(ctx, &ap, ap.Config); err != nil {
			return nil, fmt.Errorf("schema %s: %w", r.Schema, err)
		}
		st.Plugins = append(st.Plugins, ap)
	}
	data, err := json.Marshal(st)
	if err != nil {
		return nil, err
	}
	return &vgi.AttachDecision{AttachOpaqueData: data}, nil
}

// discoverSchema returns the plugin's schema under the given config, starting
// the plugin if it is not already running with that config.
func discoverSchema(ctx context.Context, ap *attachedPlugin, cfg string) (*proto.Schema, error) {
	l, err := host.acquire(ctx, ap.Ref, ap.Fetched.Binary, ap.Ref.Schema, cfg)
	if err != nil {
		return nil, err
	}
	defer l.release()
	return l.p.schema, nil
}

func schemaComment(ap *attachedPlugin, s *proto.Schema) string {
	c := fmt.Sprintf("Steampipe plugin %s (%s), %d tables.", ap.Ref.ShortName, ap.Ref.Display(), len(s.GetSchema()))
	if ap.Fetched.Version != "" {
		c += " Version " + ap.Fetched.Version + "."
	}
	return c
}

// tableItem serializes one plugin table as a catalog TableInfo whose scan is
// steampipe.main.scan(schema, table).
func tableItem(schema, table string, ts *proto.TableSchema) ([]byte, error) {
	scanBytes, err := vgi.SerializeScanFunctionResult(scanFunctionFor(schema, table))
	if err != nil {
		return nil, err
	}
	comment := ts.GetDescription()
	if comment == "" {
		comment = "Steampipe table " + table + "."
	}
	if req := requiredFilters(ts); len(req) > 0 {
		var parts []string
		for _, g := range req {
			parts = append(parts, strings.Join(g, " or "))
		}
		comment += " Requires a WHERE filter on: " + strings.Join(parts, "; ") + "."
	}
	return vgi.SerializeTableInfo(&vgi.TableInfo{
		Name:            table,
		SchemaPath:      []string{schema},
		Comment:         comment,
		Tags:            pluginTableTags(schema, table, ts),
		Columns:         tableArrowSchema(ts),
		RequiredFilters: requiredFilterPaths(requiredFilters(ts)),
		ScanFunction:    scanBytes,
	})
}

func scanFunctionFor(schema, table string) *vgi.ScanFunctionResult {
	main := []string{"main"}
	return &vgi.ScanFunctionResult{
		FunctionName: scanFunctionName,
		PositionalArguments: []vgi.ScanArg{
			{Value: schema, Type: arrow.BinaryTypes.String},
			{Value: table, Type: arrow.BinaryTypes.String},
		},
		SchemaPath: &main,
	}
}

// pluginSchemaContents builds one plugin schema's catalog_contents entry.
func pluginSchemaContents(ap *attachedPlugin) (generated.SchemaContents, error) {
	s, err := discoverSchema(context.Background(), ap, ap.Config)
	if err != nil {
		return generated.SchemaContents{}, err
	}
	comment := schemaComment(ap, s)
	info, err := vgi.SerializeSchemaInfo(&vgi.SchemaInfo{
		Path: []string{ap.Ref.Schema}, Comment: &comment,
		Tags: pluginSchemaTags(ap, s),
	})
	if err != nil {
		return generated.SchemaContents{}, err
	}
	tables := [][]byte{}
	for _, name := range sortedTableNames(s) {
		item, err := tableItem(ap.Ref.Schema, name, s.GetSchema()[name])
		if err != nil {
			return generated.SchemaContents{}, fmt.Errorf("table %s.%s: %w", ap.Ref.Schema, name, err)
		}
		tables = append(tables, item)
	}
	empty := func() [][]byte { return [][]byte{} }
	return generated.SchemaContents{
		Path: []string{ap.Ref.Schema}, Schema: info, Tables: tables,
		Views: empty(), ScalarFunctions: empty(), AggregateFunctions: empty(), TableFunctions: empty(),
		ScalarMacros: empty(), TableMacros: empty(), Indexes: empty(),
	}, nil
}

// catalogContents serves the static main schema plus one schema per plugin.
func catalogContents(call *vgi.CatalogContentsCall) (vgi.CatalogContentsResult, error) {
	schemas, err := call.Contents()
	if err != nil {
		return vgi.CatalogContentsResult{}, err
	}
	if st, ok := decodeAttachState(call.AttachOpaqueData); ok {
		for i := range st.Plugins {
			sc, err := pluginSchemaContents(&st.Plugins[i])
			if err != nil {
				return vgi.CatalogContentsResult{}, fmt.Errorf("schema %s: %w", st.Plugins[i].Ref.Schema, err)
			}
			schemas = append(schemas, sc)
		}
	}
	return vgi.CatalogContentsResult{Schemas: schemas}, nil
}

// schemaTables is the per-schema fallback (catalog_schema_contents_tables).
func schemaTables(attach []byte, path vgi.SchemaPath) ([]vgi.SerializedSchemaItem, bool) {
	st, ok := decodeAttachState(attach)
	if !ok || len(path) != 1 {
		return nil, false
	}
	ap, ok := st.plugin(path[0])
	if !ok {
		return nil, false
	}
	sc, err := pluginSchemaContents(ap)
	if err != nil {
		return nil, false
	}
	items := make([]vgi.SerializedSchemaItem, len(sc.Tables))
	for i, t := range sc.Tables {
		items[i] = t
	}
	return items, true
}

// tableGet answers a lookup of one plugin table by name.
func tableGet(attach []byte, path vgi.SchemaPath, name string, _, _ *string) ([]byte, bool, error) {
	st, ok := decodeAttachState(attach)
	if !ok || len(path) != 1 {
		return nil, false, nil
	}
	ap, ok := st.plugin(path[0])
	if !ok {
		return nil, false, nil
	}
	s, err := discoverSchema(context.Background(), ap, ap.Config)
	if err != nil {
		return nil, true, err
	}
	ts, ok := s.GetSchema()[name]
	if !ok {
		return nil, false, nil
	}
	data, err := tableItem(ap.Ref.Schema, name, ts)
	return data, true, err
}

// scanFunctionGet names the scan of a plugin table.
func scanFunctionGet(attach []byte, path vgi.SchemaPath, name string, _, _ *string) (*vgi.ScanFunctionResult, bool, error) {
	st, ok := decodeAttachState(attach)
	if !ok || len(path) != 1 {
		return nil, false, nil
	}
	if _, ok := st.plugin(path[0]); !ok {
		return nil, false, nil
	}
	return scanFunctionFor(path[0], name), true, nil
}
