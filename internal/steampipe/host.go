// Hosting Steampipe plugin processes.
//
// A Steampipe plugin is a standalone executable that serves the
// steampipe-plugin-sdk WrapperPlugin gRPC service through hashicorp/go-plugin:
// the host starts it with a magic-cookie environment variable, reads the
// handshake line it prints on its stdout, and connects over a local socket.
// This file plays the part of the Steampipe CLI's plugin manager, without
// Steampipe or its Postgres:
//
//	start   go-plugin client, Dispense the WrapperPlugin stub
//	config  SetAllConnectionConfigs: one connection named after the schema
//	schema  GetSchema(connection)
//	query   Execute(table, columns, quals, limit) -> stream of rows
//
// Processes are pooled per (binary, connection, config) and stopped after
// sitting idle, when the pool is full, or when the worker exits.

package steampipe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	goplugin "github.com/hashicorp/go-plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/shared"
	"google.golang.org/grpc/status"
)

const (
	// maxPluginProcesses bounds how many plugin processes run at once.
	maxPluginProcesses = 16
	// pluginIdleTimeout stops a plugin nobody has used for this long.
	pluginIdleTimeout = 10 * time.Minute
	// pluginStartTimeout bounds the go-plugin handshake.
	pluginStartTimeout = 2 * time.Minute
)

// pluginProc is one running plugin with one configured connection.
type pluginProc struct {
	key        string
	binary     string
	connection string
	client     *goplugin.Client
	stub       shared.WrapperPluginClient
	schema     *proto.Schema
	lastUsed   time.Time
	active     int
}

type pluginHost struct {
	mu      sync.Mutex
	procs   map[string]*pluginProc
	starts  map[string]*sync.Mutex
	reaping bool
}

var host = &pluginHost{procs: map[string]*pluginProc{}, starts: map[string]*sync.Mutex{}}

// configHash identifies a connection config without keeping it in keys/logs.
func configHash(cfg string) string {
	sum := sha256.Sum256([]byte(cfg))
	return hex.EncodeToString(sum[:8])
}

// lease is a running plugin checked out for one call; release it when done.
type lease struct {
	p    *pluginProc
	host *pluginHost
}

func (l *lease) release() {
	l.host.mu.Lock()
	l.p.active--
	l.p.lastUsed = time.Now()
	l.host.mu.Unlock()
}

// acquire returns a running plugin for (binary, connection, config), starting
// and configuring one if needed.
func (h *pluginHost) acquire(ctx context.Context, ref PluginRef, binary, connection, cfg string) (*lease, error) {
	key := binary + "\x00" + connection + "\x00" + configHash(cfg)
	h.mu.Lock()
	startMu, ok := h.starts[key]
	if !ok {
		startMu = &sync.Mutex{}
		h.starts[key] = startMu
	}
	h.mu.Unlock()

	startMu.Lock()
	defer startMu.Unlock()
	h.mu.Lock()
	if p, ok := h.procs[key]; ok {
		if !p.client.Exited() {
			p.active++
			p.lastUsed = time.Now()
			h.mu.Unlock()
			return &lease{p: p, host: h}, nil
		}
		delete(h.procs, key)
	}
	h.mu.Unlock()

	p, err := startPlugin(ctx, ref, binary, connection, cfg)
	if err != nil {
		return nil, err
	}
	p.key = key
	h.mu.Lock()
	defer h.mu.Unlock()
	h.evictLocked()
	h.procs[key] = p
	p.active = 1
	p.lastUsed = time.Now()
	if !h.reaping {
		h.reaping = true
		go h.reap()
	}
	return &lease{p: p, host: h}, nil
}

// evictLocked stops idle processes until there is room for one more.
func (h *pluginHost) evictLocked() {
	for len(h.procs) >= maxPluginProcesses {
		var oldest *pluginProc
		for _, p := range h.procs {
			if p.active == 0 && (oldest == nil || p.lastUsed.Before(oldest.lastUsed)) {
				oldest = p
			}
		}
		if oldest == nil {
			return
		}
		delete(h.procs, oldest.key)
		go oldest.client.Kill()
	}
}

func (h *pluginHost) reap() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for range t.C {
		h.mu.Lock()
		for k, p := range h.procs {
			if p.active == 0 && time.Since(p.lastUsed) > pluginIdleTimeout {
				delete(h.procs, k)
				go p.client.Kill()
			}
		}
		h.mu.Unlock()
	}
}

// StopPlugins kills every plugin process this worker started.
func StopPlugins() { goplugin.CleanupClients() }

// stderrLevelWriter forwards plugin log lines to the worker's stderr.
func pluginLogger(name string) hclog.Logger {
	level := hclog.Warn
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		level = hclog.Debug
	}
	return hclog.New(&hclog.LoggerOptions{Name: "plugin." + name, Output: os.Stderr, Level: level})
}

func startPlugin(ctx context.Context, ref PluginRef, binary, connection, cfg string) (*pluginProc, error) {
	if st, err := os.Stat(binary); err != nil {
		return nil, fmt.Errorf("plugin binary %s: %w", binary, err)
	} else if st.IsDir() {
		return nil, fmt.Errorf("plugin binary %s is a directory", binary)
	}
	cmd := exec.Command(binary)
	// The SDK reads its log level from STEAMPIPE_LOG_LEVEL; keep plugins quiet
	// unless the worker itself is debugging.
	level := "WARN"
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		level = "DEBUG"
	}
	cmd.Env = append(os.Environ(), "STEAMPIPE_LOG_LEVEL="+level)
	cmd.Stdin = nil
	client := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig:  shared.Handshake,
		Plugins:          map[string]goplugin.Plugin{ref.ShortName: &shared.WrapperPlugin{}},
		Cmd:              cmd,
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolGRPC},
		Logger:           pluginLogger(ref.ShortName),
		StartTimeout:     pluginStartTimeout,
		Managed:          true,
		SyncStdout:       io.Discard,
		SyncStderr:       os.Stderr,
	})
	rpc, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, fmt.Errorf("starting plugin %s: %w", ref.Display(), err)
	}
	raw, err := rpc.Dispense(ref.ShortName)
	if err != nil {
		client.Kill()
		return nil, fmt.Errorf("connecting to plugin %s: %w", ref.Display(), err)
	}
	stub, ok := raw.(shared.WrapperPluginClient)
	if !ok {
		client.Kill()
		return nil, fmt.Errorf("plugin %s does not serve the Steampipe WrapperPlugin interface", ref.Display())
	}
	p := &pluginProc{binary: binary, connection: connection, client: client, stub: stub}
	if err := p.configure(ref, cfg); err != nil {
		client.Kill()
		return nil, err
	}
	resp, err := stub.GetSchema(&proto.GetSchemaRequest{Connection: connection})
	if err != nil {
		client.Kill()
		return nil, fmt.Errorf("plugin %s: GetSchema: %s", ref.Display(), grpcMessage(err))
	}
	p.schema = resp.GetSchema()
	if p.schema == nil {
		client.Kill()
		return nil, fmt.Errorf("plugin %s returned no schema", ref.Display())
	}
	return p, nil
}

func (p *pluginProc) configure(ref PluginRef, cfg string) error {
	// Query caching belongs to the engine; ask the plugin not to keep its own.
	if _, err := p.stub.SetCacheOptions(&proto.SetCacheOptionsRequest{Enabled: false}); err != nil {
		slog.Debug("plugin does not accept cache options", "plugin", ref.ShortName, "err", grpcMessage(err))
	}
	resp, err := p.stub.SetAllConnectionConfigs(&proto.SetAllConnectionConfigsRequest{
		Configs: []*proto.ConnectionConfig{{
			Connection:      p.connection,
			Plugin:          ref.Display(),
			PluginShortName: ref.ShortName,
			PluginInstance:  ref.ShortName,
			Config:          cfg,
		}},
	})
	if err != nil {
		return fmt.Errorf("plugin %s rejected its connection config: %s", ref.Display(), grpcMessage(err))
	}
	if msg, ok := resp.GetFailedConnections()[p.connection]; ok {
		return fmt.Errorf("plugin %s rejected its connection config: %s", ref.Display(), cleanPluginError(msg))
	}
	return nil
}

// grpcMessage strips the gRPC framing from a plugin error.
func grpcMessage(err error) string {
	if s, ok := status.FromError(err); ok {
		return cleanPluginError(s.Message())
	}
	return cleanPluginError(err.Error())
}

func cleanPluginError(msg string) string {
	msg = strings.TrimSpace(msg)
	for _, prefix := range []string{"rpc error: code = Unknown desc = ", "rpc error: code = Internal desc = "} {
		msg = strings.TrimPrefix(msg, prefix)
	}
	return msg
}

// sortedTableNames lists a schema's tables alphabetically.
func sortedTableNames(s *proto.Schema) []string {
	names := make([]string, 0, len(s.GetSchema()))
	for n := range s.GetSchema() {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
