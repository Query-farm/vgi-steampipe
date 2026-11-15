// Package fixture is the deterministic test plugin served by
// steampipe-plugin-fixture.
package fixture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

// Config is the plugin's connection config (HCL attributes).
type Config struct {
	Greeting  *string `hcl:"greeting"`
	ItemCount *int    `hcl:"item_count"`
	APIToken  *string `hcl:"api_token"`
}

func configOf(d *plugin.QueryData) Config {
	if d.Connection == nil || d.Connection.Config == nil {
		return Config{}
	}
	if c, ok := d.Connection.Config.(Config); ok {
		return c
	}
	if c, ok := d.Connection.Config.(*Config); ok && c != nil {
		return *c
	}
	return Config{}
}

// Plugin builds the fixture plugin.
func Plugin(ctx context.Context) *plugin.Plugin {
	return &plugin.Plugin{
		Name:             "steampipe-plugin-fixture",
		DefaultTransform: transform.FromCamel(),
		ConnectionConfigSchema: &plugin.ConnectionConfigSchema{
			NewInstance: func() any { return &Config{} },
		},
		TableMap: map[string]*plugin.Table{
			"fixture_item":     tableItem(),
			"fixture_lookup":   tableLookup(),
			"fixture_greeting": tableGreeting(),
			"fixture_whoami":   tableWhoami(),
		},
	}
}

// Item is one fixture_item row.
type Item struct {
	ID       int64
	Name     string
	Category string
	Price    float64
	Active   bool
	Created  time.Time
	Tags     []string
	Attrs    map[string]any
	IP       string
	Network  string
	Note     *string
}

var categories = []string{"hardware", "software", "services"}

func makeItem(i int64) Item {
	it := Item{
		ID:       i,
		Name:     fmt.Sprintf("item-%04d", i),
		Category: categories[i%3],
		Price:    float64(i%100) + 0.5,
		Active:   i%2 == 0,
		Created:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Hour),
		Tags:     []string{categories[i%3], fmt.Sprintf("t%d", i%5)},
		Attrs:    map[string]any{"rank": i % 7, "even": i%2 == 0},
		IP:       fmt.Sprintf("10.0.%d.%d", (i/256)%256, i%256),
		Network:  fmt.Sprintf("10.%d.0.0/16", i%256),
	}
	if i%10 == 0 {
		note := fmt.Sprintf("milestone %d", i)
		it.Note = &note
	}
	return it
}

func itemColumns() []*plugin.Column {
	return []*plugin.Column{
		{Name: "id", Type: proto.ColumnType_INT, Description: "Item number: a 1-based integer index, from 1 to item_count.", Transform: transform.FromField("ID")},
		{Name: "name", Type: proto.ColumnType_STRING, Description: "Item name, item-NNNN."},
		{Name: "category", Type: proto.ColumnType_STRING, Description: "One of hardware, software, services."},
		{Name: "price", Type: proto.ColumnType_DOUBLE, Description: "Price in US dollars."},
		{Name: "active", Type: proto.ColumnType_BOOL, Description: "True for even ids."},
		{Name: "created", Type: proto.ColumnType_TIMESTAMP, Description: "Creation time, one hour per id after 2024-01-01."},
		{Name: "tags", Type: proto.ColumnType_JSON, Description: "Tags as a JSON array."},
		{Name: "attrs", Type: proto.ColumnType_JSON, Description: "Attributes as a JSON object."},
		{Name: "ip", Type: proto.ColumnType_INET, Description: "An IPv4 address.", Transform: transform.FromField("IP")},
		{Name: "network", Type: proto.ColumnType_CIDR, Description: "An IPv4 network."},
		{Name: "note", Type: proto.ColumnType_STRING, Description: "Set for every tenth id, otherwise NULL."},
	}
}

func tableItem() *plugin.Table {
	return &plugin.Table{
		Name:        "fixture_item",
		Description: "Deterministic items; filter by category or id range.",
		List: &plugin.ListConfig{
			Hydrate: listItems,
			KeyColumns: plugin.KeyColumnSlice{
				{Name: "category", Require: plugin.Optional, Operators: []string{"="}},
				{Name: "id", Require: plugin.Optional, Operators: []string{"=", ">", ">=", "<", "<="}},
			},
		},
		Columns: itemColumns(),
	}
}

func listItems(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (any, error) {
	n := int64(25)
	if c := configOf(d); c.ItemCount != nil {
		n = int64(*c.ItemCount)
	}
	lo, hi := int64(1), n
	if q := d.Quals["id"]; q != nil {
		for _, qual := range q.Quals {
			v := qual.Value.GetInt64Value()
			switch qual.Operator {
			case "=":
				lo, hi = max(lo, v), min(hi, v)
			case ">":
				lo = max(lo, v+1)
			case ">=":
				lo = max(lo, v)
			case "<":
				hi = min(hi, v-1)
			case "<=":
				hi = min(hi, v)
			}
		}
	}
	category := d.EqualsQualString("category")
	for i := lo; i <= hi; i++ {
		item := makeItem(i)
		if category != "" && item.Category != category {
			continue
		}
		d.StreamListItem(ctx, item)
		if d.RowsRemaining(ctx) == 0 {
			return nil, nil
		}
	}
	return nil, nil
}

func tableLookup() *plugin.Table {
	lookup := func(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (any, error) {
		id := d.EqualsQuals["id"].GetInt64Value()
		if id < 1 || id > 1000 {
			return nil, nil
		}
		return makeItem(id), nil
	}
	return &plugin.Table{
		Name:        "fixture_lookup",
		Description: "Look items up by id: id is a required key column of both the Get and the List call.",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("id"),
			Hydrate:    lookup,
		},
		List: &plugin.ListConfig{
			KeyColumns: plugin.KeyColumnSlice{{Name: "id", Require: plugin.Required, Operators: []string{"="}}},
			Hydrate: func(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (any, error) {
				for _, q := range d.Quals["id"].Quals {
					ids := []int64{q.Value.GetInt64Value()}
					if l := q.Value.GetListValue(); l != nil {
						ids = ids[:0]
						for _, v := range l.Values {
							ids = append(ids, v.GetInt64Value())
						}
					}
					for _, id := range ids {
						if id >= 1 && id <= 1000 {
							d.StreamListItem(ctx, makeItem(id))
						}
					}
				}
				return nil, nil
			},
		},
		Columns: itemColumns(),
	}
}

// Greeting is one fixture_greeting row.
type Greeting struct {
	Name    string
	Message string
}

func tableGreeting() *plugin.Table {
	return &plugin.Table{
		Name:        "fixture_greeting",
		Description: "Greets the required name using the connection's greeting.",
		List: &plugin.ListConfig{
			KeyColumns: plugin.KeyColumnSlice{{Name: "name", Require: plugin.Required, Operators: []string{"="}}},
			Hydrate: func(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (any, error) {
				greeting := "Hello"
				if c := configOf(d); c.Greeting != nil {
					greeting = *c.Greeting
				}
				for _, q := range d.Quals["name"].Quals {
					if l := q.Value.GetListValue(); l != nil {
						for _, v := range l.Values {
							d.StreamListItem(ctx, Greeting{Name: v.GetStringValue(), Message: greeting + ", " + v.GetStringValue() + "!"})
						}
						continue
					}
					name := q.Value.GetStringValue()
					d.StreamListItem(ctx, Greeting{Name: name, Message: greeting + ", " + name + "!"})
				}
				return nil, nil
			},
		},
		Columns: []*plugin.Column{
			{Name: "name", Type: proto.ColumnType_STRING, Description: "The name greeted (required key column)."},
			{Name: "message", Type: proto.ColumnType_STRING, Description: "The greeting."},
		},
	}
}

// Whoami describes the credential the connection was configured with.
type Whoami struct {
	HasToken    bool
	TokenSHA256 *string
}

func tableWhoami() *plugin.Table {
	return &plugin.Table{
		Name:        "fixture_whoami",
		Description: "Reports whether the connection config carried api_token (never the token itself).",
		List: &plugin.ListConfig{
			Hydrate: func(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (any, error) {
				w := Whoami{}
				if c := configOf(d); c.APIToken != nil && *c.APIToken != "" {
					sum := sha256.Sum256([]byte(*c.APIToken))
					h := hex.EncodeToString(sum[:])
					w.HasToken, w.TokenSHA256 = true, &h
				}
				d.StreamListItem(ctx, w)
				return nil, nil
			},
		},
		Columns: []*plugin.Column{
			{Name: "has_token", Type: proto.ColumnType_BOOL, Description: "True when api_token is set.", Transform: transform.FromField("HasToken")},
			{Name: "token_sha256", Type: proto.ColumnType_STRING, Description: "SHA-256 of api_token.", Transform: transform.FromField("TokenSHA256")},
		},
	}
}
