// steampipe.main.scan(schema, table_name, max_rows := 0): read one plugin
// table. Every plugin table in the catalog is backed by this function, and it
// can be called directly too (for example for a dynamic-schema plugin whose
// tables depend on credentials only a secret holds).
//
// Bind is two-phase: the first call asks DuckDB for the `steampipe` secret in
// scope steampipe://<schema>; the second merges that secret's config over the
// ATTACH config, starts (or reuses) the plugin with it, and returns the
// table's columns. Secrets are never stored in scan state.
//
// Each scan holds one plugin Execute stream. Rows are pulled from it into
// Arrow batches of up to scanBatchRows rows, or fewer when the plugin is slow
// (a partial batch is flushed after scanFlushInterval), so the first rows of a
// slow API reach DuckDB quickly. Over HTTP a batch may be served by another
// process: the state keeps how many rows were emitted, and a process that has
// no open stream for the scan re-executes and skips that many rows.

package steampipe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

const (
	scanFunctionName  = "scan"
	scanBatchRows     = 2048
	scanFlushInterval = 250 * time.Millisecond
	// abandonedScanTimeout cancels a plugin stream DuckDB stopped reading
	// (LIMIT satisfied, query cancelled) and never closed.
	abandonedScanTimeout = 2 * time.Minute
)

type scanArgs struct {
	Schema  string `vgi:"pos=0,doc=Plugin schema of this ATTACH, as named by the plugins option (e.g. 'csv')."`
	Table   string `vgi:"pos=1,doc=Plugin table name (e.g. 'csv_products' or 'aws_s3_bucket')."`
	MaxRows int64  `vgi:"default=0,ge=0,doc=Plugin-side row limit (Steampipe's limit pushdown); 0 means no limit. It caps the rows the plugin fetches before any WHERE filter that is not a key-column qual is applied."`
}

// ScanState is the per-scan cursor. It holds no secrets: the config is
// recomputed from the ATTACH data and the resolved secret on every process.
type ScanState struct {
	ScanID  string
	Schema  string
	Table   string
	MaxRows int64
	Emitted int64
	Done    bool
}

// secretScope is the DuckDB secret scope a schema's credentials are looked
// up under.
func secretScope(schema string) string { return "steampipe://" + schema }

// effectiveConfig merges the schema's `steampipe` secret (when one applies)
// over the ATTACH config.
func effectiveConfig(ap *attachedPlugin, secrets vgi.Secrets) (string, error) {
	cfg := ap.Config
	for i, fields := range secrets.OfType(SecretType) {
		name := fmt.Sprintf("#%d", i+1)
		if n := vgi.RenderSecretValue(fields["name"]); n != "" {
			name = n
		}
		target := strings.ToLower(strings.TrimSpace(vgi.RenderSecretValue(fields["plugin"])))
		if target != "" && target != ap.Ref.Schema && target != ap.Ref.ShortName {
			slog.Debug("ignoring a steampipe secret for another plugin", "secret", name, "plugin", target, "schema", ap.Ref.Schema)
			continue
		}
		raw := vgi.RenderSecretValue(fields["config"])
		secretCfg, err := normalizeConfig(raw)
		if err != nil {
			return "", fmt.Errorf("steampipe secret %q: %w", name, err)
		}
		return mergeConfig(cfg, secretCfg)
	}
	return cfg, nil
}

// resolveTable finds the plugin, starts it with the effective config and
// returns its table schema.
func resolveTable(ctx context.Context, attach []byte, secrets vgi.Secrets, schema, table string) (*attachedPlugin, string, *proto.TableSchema, error) {
	st, ok := decodeAttachState(attach)
	if !ok || len(st.Plugins) == 0 {
		return nil, "", nil, fmt.Errorf("no Steampipe plugins are attached: ATTACH with the plugins option, e.g. plugins 'csv'")
	}
	ap, ok := st.plugin(schema)
	if !ok {
		return nil, "", nil, fmt.Errorf("no plugin schema %q in this ATTACH (attached: %s)", schema, strings.Join(st.schemaNames(), ", "))
	}
	cfg, err := effectiveConfig(ap, secrets)
	if err != nil {
		return nil, "", nil, err
	}
	s, err := discoverSchema(ctx, ap, cfg)
	if err != nil {
		return nil, "", nil, err
	}
	ts, ok := s.GetSchema()[table]
	if !ok {
		names := sortedTableNames(s)
		if len(names) > 25 {
			names = append(names[:25], "...")
		}
		return nil, "", nil, fmt.Errorf("plugin %s has no table %q (tables: %s)", ap.Ref.ShortName, table, strings.Join(names, ", "))
	}
	return ap, cfg, ts, nil
}

// Scan is the scan table function.
type Scan struct{}

var _ vgi.TypedTableFunc[ScanState] = (*Scan)(nil)

// Name is the SQL name.
func (*Scan) Name() string { return scanFunctionName }

// Metadata declares pushdown and documents the function.
func (*Scan) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Read one table of an attached Steampipe plugin, pushing key-column filters down as quals",
		Stability:          vgi.StabilityVolatile,
		FilterPushdown:     true,
		ProjectionPushdown: true,
		Tags:               withExamples(scanTags(), scanExamples),
		Examples:           scanExamples,
	}
}

// ArgumentSpecs derives the signature from scanArgs.
func (*Scan) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(scanArgs{}) }

// OnBind resolves the table's columns, after asking for the schema's secret.
func (*Scan) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var args scanArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	schema := strings.ToLower(args.Schema)
	if !params.ResolvedSecretsProvided {
		return &vgi.BindResponse{SecretScopeRequest: []vgi.SecretLookup{{SecretType: SecretType, Scope: secretScope(schema)}}}, nil
	}
	_, _, ts, err := resolveTable(context.Background(), params.AttachOpaqueData, params.Secrets, schema, args.Table)
	if err != nil {
		return nil, err
	}
	return vgi.BindSchema(tableArrowSchema(ts))
}

// NewState starts a cursor for one scan.
func (*Scan) NewState(params *vgi.ProcessParams) (*ScanState, error) {
	var args scanArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	id := make([]byte, 12)
	if _, err := rand.Read(id); err != nil {
		return nil, err
	}
	return &ScanState{ScanID: hex.EncodeToString(id), Schema: strings.ToLower(args.Schema), Table: args.Table, MaxRows: args.MaxRows}, nil
}

// Process emits the next batch of rows that pass the pushed filters.
func (*Scan) Process(ctx context.Context, params *vgi.ProcessParams, state *ScanState, out *vgirpc.OutputCollector) error {
	for !state.Done {
		s, err := openScanStream(ctx, params, state)
		if err != nil {
			return err
		}
		rows, eof, err := s.next(ctx, scanBatchRows, scanFlushInterval)
		if err != nil {
			streams.close(state.ScanID)
			return err
		}
		state.Emitted += int64(len(rows))
		if eof {
			state.Done = true
			streams.close(state.ScanID)
		}
		if len(rows) == 0 {
			continue
		}
		n, err := emitRows(ctx, out, params, rows)
		if err != nil {
			return err
		}
		if n > 0 && !state.Done {
			return nil
		}
	}
	return out.Finish()
}

// stagingType is the type a column takes in the batch the pushed filters are
// evaluated against: the embedded evaluator stages TIMESTAMPTZ and JSON as
// VARCHAR (ISO-8601 instants with a Z suffix, and JSON text), which DuckDB
// casts back implicitly when a predicate compares them.
func stagingType(t arrow.DataType) arrow.DataType {
	switch t.ID() {
	case arrow.TIMESTAMP, arrow.EXTENSION:
		return arrow.BinaryTypes.String
	}
	return t
}

func buildColumn(mem memory.Allocator, typ arrow.DataType, name string, rows []*proto.Row, present bool) arrow.Array {
	b := array.NewBuilder(mem, typ)
	defer b.Release()
	b.Reserve(len(rows))
	for _, r := range rows {
		if !present {
			b.AppendNull()
			continue
		}
		appendColumn(b, r.GetColumns()[name])
	}
	return b.NewArray()
}

// emitRows converts plugin rows to one Arrow batch of the projected schema,
// keeps the rows that satisfy every pushed filter, and emits them. It returns
// how many rows it emitted.
func emitRows(ctx context.Context, out *vgirpc.OutputCollector, params *vgi.ProcessParams, rows []*proto.Row) (int64, error) {
	mem := memory.NewGoAllocator()
	schema := params.OutputSchema
	cols := make([]arrow.Array, schema.NumFields())
	for i, f := range schema.Fields() {
		cols[i] = buildColumn(mem, f.Type, f.Name, rows, true)
	}
	batch := array.NewRecordBatch(schema, cols, int64(len(rows)))
	for _, c := range cols {
		c.Release()
	}
	if pf := params.CurrentPushdownFilters; pf != nil && len(pf.Filters) > 0 {
		filtered, err := applyFilters(ctx, pf, params.BindOutputSchema, rows, batch)
		if err == nil && slog.Default().Enabled(ctx, slog.LevelDebug) {
			slog.Debug("steampipe filters re-applied", "rows_in", len(rows), "rows_out", filtered.NumRows(), "first_row", rows[0].String())
		}
		batch.Release()
		if err != nil {
			return 0, err
		}
		batch = filtered
	}
	n := batch.NumRows()
	if n == 0 {
		batch.Release()
		return 0, nil
	}
	// The collector owns the batch from here on.
	return n, out.Emit(batch)
}

// applyFilters keeps the rows of batch that satisfy every pushed filter. The
// filters are evaluated over batches laid out like the bind schema (columns
// the plugin was not asked for are NULL; no filter reads them).
func applyFilters(ctx context.Context, pf *vgi.PushdownFilters, bind *arrow.Schema, rows []*proto.Row, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	present := map[string]bool{}
	if len(rows) > 0 {
		for name := range rows[0].GetColumns() {
			present[name] = true
		}
	}
	layout := func(staged bool) arrow.RecordBatch {
		fields := make([]arrow.Field, bind.NumFields())
		cols := make([]arrow.Array, bind.NumFields())
		for i, f := range bind.Fields() {
			t := f.Type
			if staged {
				t = stagingType(t)
			}
			fields[i] = arrow.Field{Name: f.Name, Type: t, Nullable: true}
			cols[i] = buildColumn(mem, t, f.Name, rows, present[f.Name])
		}
		b := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, int64(len(rows)))
		for _, c := range cols {
			c.Release()
		}
		return b
	}
	typed := layout(false)
	defer typed.Release()
	keep, err := filterMask(ctx, pf, typed, func() arrow.RecordBatch { return layout(true) })
	if err != nil {
		return nil, err
	}
	mb := array.NewBooleanBuilder(mem)
	mb.AppendValues(keep, nil)
	mask := mb.NewArray()
	mb.Release()
	defer mask.Release()
	return compute.FilterRecordBatch(ctx, batch, mask, compute.DefaultFilterOptions())
}

// requestedColumns is the set of columns the plugin must hydrate: the
// projected ones plus any a pushed filter reads (the worker re-applies
// filters to them).
func requestedColumns(params *vgi.ProcessParams, ts *proto.TableSchema) []string {
	want := map[string]bool{}
	for _, f := range params.OutputSchema.Fields() {
		want[f.Name] = true
	}
	if pf := params.CurrentPushdownFilters; pf != nil {
		var visit func(f vgi.Filter)
		visit = func(f vgi.Filter) {
			if f == nil {
				return
			}
			want[f.ColumnName()] = true
			switch x := f.(type) {
			case *vgi.AndFilter:
				for _, c := range x.Children {
					visit(c)
				}
			case *vgi.OrFilter:
				for _, c := range x.Children {
					visit(c)
				}
			}
		}
		for _, f := range pf.Filters {
			visit(f)
		}
	}
	var cols []string
	for _, c := range visibleColumns(ts) {
		if want[c.GetName()] {
			cols = append(cols, c.GetName())
		}
	}
	if len(cols) == 0 && len(ts.GetColumns()) > 0 {
		// count(*): any one column will do; the cheapest is the connection name.
		cols = []string{ts.GetColumns()[0].GetName()}
		for _, c := range ts.GetColumns() {
			if c.GetName() == "sp_connection_name" {
				cols = []string{c.GetName()}
			}
		}
	}
	return cols
}

// checkRequiredQuals turns a missing required key column into a clear error
// before the plugin is called.
func checkRequiredQuals(ap *attachedPlugin, table string, ts *proto.TableSchema, quals map[string]*proto.Quals) error {
	cnf := requiredFilters(ts)
	var missing []string
	for _, g := range cnf {
		ok := false
		for _, c := range g {
			if quals[c] != nil {
				ok = true
			}
		}
		if !ok {
			missing = append(missing, strings.Join(g, " or "))
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("%s.%s needs a WHERE filter on %s that the plugin accepts (a Steampipe required key column, e.g. WHERE %s = '...')",
		ap.Ref.Schema, table, strings.Join(missing, " and "), cnf[0][0])
}

// ----------------------------------------------------------------------------
// Execute streams

type rowOrErr struct {
	row *proto.Row
	err error
}

type scanStream struct {
	ch      chan rowOrErr
	done    chan struct{}
	cancel  context.CancelFunc
	lease   *lease
	touched time.Time
	mu      sync.Mutex
	skip    int64
	closeMu sync.Once
}

type streamRegistry struct {
	mu      sync.Mutex
	streams map[string]*scanStream
	reaping bool
}

var streams = &streamRegistry{streams: map[string]*scanStream{}}

func (r *streamRegistry) get(id string) (*scanStream, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if ok {
		s.touched = time.Now()
	}
	return s, ok
}

func (r *streamRegistry) put(id string, s *scanStream) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s.touched = time.Now()
	r.streams[id] = s
	if !r.reaping {
		r.reaping = true
		go r.reap()
	}
}

func (r *streamRegistry) close(id string) {
	r.mu.Lock()
	s, ok := r.streams[id]
	delete(r.streams, id)
	r.mu.Unlock()
	if ok {
		s.stop()
	}
}

func (r *streamRegistry) reap() {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for range t.C {
		r.mu.Lock()
		for id, s := range r.streams {
			if time.Since(s.touched) > abandonedScanTimeout {
				delete(r.streams, id)
				go s.stop()
			}
		}
		r.mu.Unlock()
	}
}

func (s *scanStream) stop() {
	s.closeMu.Do(func() {
		close(s.done)
		s.cancel()
		s.lease.release()
	})
}

// openScanStream returns the open stream for this scan, starting the plugin
// Execute call if this process has none (first batch, or a resumed HTTP scan).
func openScanStream(ctx context.Context, params *vgi.ProcessParams, state *ScanState) (*scanStream, error) {
	if s, ok := streams.get(state.ScanID); ok {
		return s, nil
	}
	ap, cfg, ts, err := resolveTable(ctx, params.AttachScope, params.Secrets, state.Schema, state.Table)
	if err != nil {
		return nil, err
	}
	quals := buildQuals(params.CurrentPushdownFilters, ts)

	if err := checkRequiredQuals(ap, state.Table, ts, quals); err != nil {
		return nil, err
	}
	l, err := host.acquire(ctx, ap.Ref, ap.Fetched.Binary, ap.Ref.Schema, cfg)
	if err != nil {
		return nil, err
	}
	connection := ap.Ref.Schema
	execData := &proto.ExecuteConnectionData{CacheEnabled: false}
	if state.MaxRows > 0 {
		execData.Limit = &proto.NullableInt{Value: state.MaxRows}
	}
	req := &proto.ExecuteRequest{
		Table:                 state.Table,
		QueryContext:          &proto.QueryContext{Columns: requestedColumns(params, ts), Quals: quals},
		Connection:            connection,
		CallId:                state.ScanID + fmt.Sprintf("-%d", state.Emitted),
		ExecuteConnectionData: map[string]*proto.ExecuteConnectionData{connection: execData},
	}
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		var qs []string
		for c, q := range quals {
			for _, x := range q.GetQuals() {
				qs = append(qs, c+" "+x.GetStringValue())
			}
		}
		sort.Strings(qs)
		slog.Debug("steampipe execute", "schema", state.Schema, "table", state.Table, "columns", req.QueryContext.Columns, "quals", qs, "limit", state.MaxRows)
	}
	stream, _, cancel, err := l.p.stub.Execute(req)
	if err != nil {
		l.release()
		return nil, fmt.Errorf("%s.%s: %s", state.Schema, state.Table, grpcMessage(err))
	}
	s := &scanStream{ch: make(chan rowOrErr, scanBatchRows), done: make(chan struct{}), cancel: cancel, lease: l, skip: state.Emitted}
	send := func(x rowOrErr) bool {
		select {
		case s.ch <- x:
			return true
		case <-s.done:
			return false
		}
	}
	go func() {
		defer close(s.ch)
		for {
			resp, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					send(rowOrErr{err: fmt.Errorf("%s.%s: %s", state.Schema, state.Table, grpcMessage(err))})
				}
				return
			}
			if resp.GetRow() == nil {
				continue
			}
			if !send(rowOrErr{row: resp.GetRow()}) {
				return
			}
		}
	}()
	streams.put(state.ScanID, s)
	return s, nil
}

// next collects up to n rows, returning early with what it has once flush
// has passed since the first row. eof is true when the plugin is done.
func (s *scanStream) next(ctx context.Context, n int, flush time.Duration) ([]*proto.Row, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rows []*proto.Row
	var timer <-chan time.Time
	for len(rows) < n {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-timer:
			return rows, false, nil
		case r, ok := <-s.ch:
			if !ok {
				return rows, true, nil
			}
			if r.err != nil {
				return nil, false, r.err
			}
			if s.skip > 0 {
				s.skip--
				continue
			}
			rows = append(rows, r.row)
			if timer == nil {
				timer = time.After(flush)
			}
		}
	}
	return rows, false, nil
}
