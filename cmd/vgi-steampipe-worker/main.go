// Command vgi-steampipe-worker serves the steampipe catalog to DuckDB.
//
//	vgi-steampipe-worker                                  # stdio (what DuckDB launches)
//	vgi-steampipe-worker --http --host 0.0.0.0 --port 8000  # HTTP, GET /health
//	vgi-steampipe-worker --unix /tmp/steampipe.sock         # AF_UNIX (launch: transport)
//
// Attach it from DuckDB:
//
//	ATTACH 'steampipe' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-steampipe-worker', plugins 'csv');
//
// Server-side defaults for ATTACHes that do not set the option (useful for an
// HTTP deployment that serves a fixed plugin set):
//
//	--plugins 'csv,aws'  --plugin-config 'csv { paths = [...] }'
//	--cache-dir DIR      --registry URL
package main

import (
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Query-farm/vgi-go/vgi"

	"github.com/Query-farm/vgi-steampipe/internal/serve"
	"github.com/Query-farm/vgi-steampipe/internal/steampipe"
)

func main() {
	// Plugin processes are children of the worker: stop them when it exits.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		steampipe.StopPlugins()
		os.Exit(1)
	}()
	defer steampipe.StopPlugins()
	steampipe.SetAttachDefaults(pluginFlags(os.Args[1:]))
	serve.Main("vgi-steampipe-worker", os.Args[1:], func() *vgi.Worker { return steampipe.NewWorker() })
}

// pluginFlags reads the worker's own flags (--plugins, --plugin-config,
// --cache-dir, --registry, as --flag value or --flag=value); serve.Main
// ignores them.
func pluginFlags(args []string) map[string]string {
	names := map[string]string{"plugins": "plugins", "plugin-config": "config", "cache-dir": "cache_dir", "registry": "registry"}
	out := map[string]string{}
	for i := 0; i < len(args); i++ {
		name, value, inline := strings.Cut(strings.TrimLeft(args[i], "-"), "=")
		key, ok := names[name]
		if !ok || !strings.HasPrefix(args[i], "-") {
			continue
		}
		if !inline && i+1 < len(args) {
			i++
			value = args[i]
		}
		out[key] = value
	}
	return out
}
