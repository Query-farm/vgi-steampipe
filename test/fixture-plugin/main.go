// Command steampipe-plugin-fixture is a tiny, fully offline Steampipe plugin
// built with the real steampipe-plugin-sdk. The worker's tests launch it over
// the same hashicorp/go-plugin gRPC protocol as any published plugin, so CI
// exercises discovery, connection config, key columns, quals and limits
// without a network or credentials.
package main

import (
	"github.com/Query-farm/vgi-steampipe/test/fixture-plugin/fixture"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{PluginFunc: fixture.Plugin})
}
