<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

<p align="center"><em>A <a href="https://query.farm">Query.Farm</a> VGI worker for DuckDB.</em></p>

# Query Every Steampipe Plugin's Tables in DuckDB

> **vgi-steampipe** · a [Query.Farm](https://query.farm) VGI worker · powered by [steampipe-plugin-sdk](https://github.com/turbot/steampipe-plugin-sdk)

[![CI](https://github.com/Query-farm/vgi-steampipe/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-steampipe/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[Steampipe](https://steampipe.io) has hundreds of plugins that turn cloud and SaaS
APIs (AWS, GitHub, Kubernetes, Slack, CSV files, RSS, DNS, ...) into SQL tables. This
worker hosts those plugins **directly**: it starts each plugin binary and speaks its
gRPC plugin protocol (hashicorp/go-plugin + the steampipe-plugin-sdk `WrapperPlugin`
service) itself. There is no Steampipe CLI and no embedded Postgres; the plugin's
tables become DuckDB tables.

```sql
ATTACH 'steampipe' AS sp (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-steampipe@v0.2.0',
    plugins 'csv@1.3.0, rss',
    config  'csv { paths = ["/data/*.csv"] }');

SELECT name, price FROM sp.csv.products WHERE price::DOUBLE > 10;
SELECT title, published FROM sp.rss.rss_item WHERE feed_link = 'https://duckdb.org/feed.xml';
```

- **Each plugin is a schema, each plugin table a table**, with the plugin's column
  types mapped to DuckDB types.
- **Plugins come from ghcr.io** (`turbot/steampipe/plugins/...`) as OCI images, fetched
  once, sha256-verified and cached, or from a local binary path.
- **Key-column filters are pushed down** to the plugin as Steampipe quals, so the API
  does the filtering. Every filter is still applied exactly to the result.
- **Required key columns are enforced** by DuckDB at plan time, with an error naming
  the column.
- **Credentials live in DuckDB secrets** (`TYPE steampipe`), never in the ATTACH string.

## Quick start

Download the release archive for your platform from the
[releases page](https://github.com/Query-farm/vgi-steampipe/releases), or let the VGI
extension fetch it with `github-auto://` (change `@v0.1.0` to the release you want):

```sql
INSTALL vgi FROM community;
LOAD vgi;

ATTACH 'steampipe' AS sp (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-steampipe@v0.2.0',
    plugins 'csv', config 'csv { paths = ["/data/*.csv"] }');

FROM sp.main.plugin;          -- what was attached
FROM sp.main.plugin_table;    -- every table, with its required filters
FROM sp.csv.products;
```

Other ways to run it: point `LOCATION` at the extracted binary
(`/opt/vgi-steampipe-worker`), run it as an HTTP service (`vgi-steampipe-worker --http
--port 8000`, then `LOCATION 'http://host:8000'`), or build from source (`make build`,
Go 1.26+ with cgo).

Steampipe publishes plugin binaries for Linux (amd64, arm64) and macOS. The worker
runs them on those platforms.

## ATTACH options

| Option | Meaning |
|---|---|
| `plugins` | Comma-separated `[schema=]ref` entries. Each plugin becomes a schema, named after the plugin unless `schema=` is given. |
| `config` | Non-secret connection settings: one HCL block per schema (`csv { paths = ["/data/*.csv"] }`) or a JSON object keyed by schema (`{"csv": {"paths": ["/data/*.csv"]}}`). Used to discover tables at ATTACH and for every query. |
| `cache_dir` | Where fetched plugins are kept (default `<user cache dir>/vgi-steampipe/plugins`). |
| `registry` | Registry for refs that name none (default `https://ghcr.io`), for mirrors. |

Plugin refs, in the forms the Steampipe CLI accepts:

| Ref | Resolves to |
|---|---|
| `csv` | `ghcr.io/turbot/steampipe/plugins/turbot/csv:latest` |
| `csv@1.3.0` or `csv:1.3.0` | that version |
| `turbot/github` | `ghcr.io/turbot/steampipe/plugins/turbot/github:latest` |
| `csv@sha256:<64 hex>` | pinned to a manifest digest |
| `ghcr.io/acme/plugins/foo:2` | any registry and repository |
| `/opt/steampipe-plugin-aws.plugin` | a local binary (also `./`, `../`, `~/`) |

A worker can also be started with defaults for ATTACHes that name no plugins, which
suits a hosted HTTP worker that serves a fixed set:
`vgi-steampipe-worker --http --plugins 'csv,rss' --plugin-config 'csv { paths = [...] }'`
(also `--cache-dir`, `--registry`).

## Credentials

Put credentials in a DuckDB secret. Its `CONFIG` is the same HCL (or JSON) you would
write in a Steampipe `connection` block, and it is merged over the ATTACH `config` at
query time (an attribute set in both is taken from the secret):

```sql
CREATE SECRET github (TYPE steampipe, PLUGIN 'github', CONFIG 'token = "ghp_..."',
                      SCOPE 'steampipe://github');
SELECT name, stargazers_count FROM sp.github.github_my_repository ORDER BY 2 DESC LIMIT 5;
```

- The worker looks the secret up in scope `steampipe://<schema>`, so give each plugin's
  secret that scope when you have more than one.
- `PLUGIN` names the schema or plugin the secret configures; a secret naming another
  plugin is ignored, so credentials never reach the wrong plugin.
- `CONFIG` is redacted in `duckdb_secrets()` and never stored in scan state or logs.

## How queries run

- **Projection**: only the selected columns are requested, so the plugin skips the
  hydrate calls for the others.
- **Key columns and quals**: top-level `WHERE` conjuncts on a key column with an
  operator the plugin declares (`=`, `<>`, `<`, `<=`, `>`, `>=`; `IN (...)` as `= ANY`;
  `IS [NOT] NULL`) become quals. `main.plugin_key_column` lists them per table.
  Plugins treat quals as API parameters, not exact predicates, so the worker re-applies
  every pushed filter, LIKE and other expressions included, before returning rows,
  like Steampipe's Postgres does.
- **Required key columns**: a table whose List call has required (or `any_of`) key
  columns carries them as `required_filters`. DuckDB refuses a query that omits
  them (`Table 'sp.rss.rss_item' requires WHERE filters on: feed_link`).
  `main.plugin_table.required_filters` shows them. When a table also has a Get call,
  filtering on its Get key columns satisfies the requirement too. A Get-only table
  cannot be told apart from a listable one in the plugin schema, so its missing qual
  is reported by the plugin when the query runs.
- **Limit**: DuckDB does not pass `LIMIT` to table functions. It stops reading early
  and the worker cancels the plugin stream. `steampipe.main.scan(schema, table,
  max_rows := N)` passes Steampipe's own limit to the plugin, which stops paging
  after N rows (applied before filters that are not quals).
- **Streaming**: rows are read from the plugin's Execute stream into Arrow batches of up
  to 2048 rows. A partial batch is flushed after 250 ms, so a slow API still shows
  its first rows promptly. Over HTTP, a batch served by another worker process resumes
  by re-executing and skipping the rows already returned.
- **Processes**: plugin processes are pooled per (binary, schema, config), stopped
  after 10 idle minutes, and killed when the worker exits.

Type mapping: `BOOL` to BOOLEAN, `INT` to BIGINT, `DOUBLE` to DOUBLE, `STRING` to VARCHAR,
`JSON` to JSON, `TIMESTAMP`/`DATETIME` to TIMESTAMP WITH TIME ZONE, and `INET`,
`CIDR`, `LTREE` to VARCHAR. Every table keeps Steampipe's `sp_connection_name` and
`sp_ctx` columns (the deprecated `_ctx` is dropped).

## Functions and tables

| Object | What it is |
|---|---|
| `<schema>.<table>` | One per plugin table. |
| `main.plugin` | The attached plugins: ref, verified digests, version, SDK version, table count. |
| `main.plugin_table` | Every plugin table, with `required_filters` and its Get/List key columns. |
| `main.plugin_key_column` | Each key column, its call, requirement and operators. |
| `main.scan(schema, table_name, max_rows := 0)` | Reads one plugin table directly. Use it for plugins whose tables depend on credentials only a secret holds, since catalog tables are discovered with the ATTACH config. |

## Plugin downloads

A ref is resolved with the OCI distribution API, using the registry's anonymous Bearer
token handshake for public images. The worker:

1. fetches the manifest and, for a pinned `@sha256:` ref, checks the manifest digest;
2. picks the layer for this platform
   (`application/vnd.turbot.steampipe.plugin.<os>-<arch>.layer.v1+gzip`);
3. downloads the blob, checks its size and sha256 against the manifest, and only then
   unpacks it into `cache_dir/blobs/sha256/<digest>/`.

A pinned ref already in the cache never touches the network. When a tag cannot be
resolved because the registry is unreachable, the cached copy of that tag is used.

## Known limitations

- **TIMESTAMPTZ filters and the session time zone**: vgi-go 0.42 rejects a pushed
  comparison on a TIMESTAMPTZ column when the DuckDB session `TimeZone` is not `UTC`
  (`comparison operands have incompatible types`): it compares the literal's
  time-zone label instead of the instant. Run `SET TimeZone = 'UTC'` or compare
  `created::TIMESTAMP`. This is an SDK issue, not a worker one.
- An expression filter other than a plain comparison (LIKE, a function) on a TIMESTAMPTZ
  or JSON column can't be re-applied by the SDK's evaluator. Cast the column in the
  query.
- Dynamic-schema plugins whose tables depend on credentials (Google Sheets, for
  example) list only the tables the ATTACH `config` reveals. Read the others with
  `scan()`, which discovers them with the secret applied.

## Testing

`make check` runs gofmt/vet/golangci-lint, the Go unit tests, the SQL tests over stdio,
launch and HTTP, and the metadata lint. The SQL tests use:

- **`test/fixture-plugin/`**: a small plugin built with the real steampipe-plugin-sdk
  (its own Go module). It has key columns of each kind, a required key column, a
  Get+List table, every column type, a config-dependent row count and a credential
  echo, so the suite needs no network or credentials.
- **`test/mock_registry/server.py`**: an OCI registry serving that plugin as a plugin
  image (Bearer handshake, digest pins, a tampered layer), for the download path.
- **`test/sql/csv_plugin.test`**: the published `turbot/csv@1.3.0` from ghcr.io over
  `test/data/*.csv`. It is cached in `.tools/plugin-cache` and skipped when ghcr.io is
  unreachable and nothing is cached.

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the
[BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm

The worker links [steampipe-plugin-sdk](https://github.com/turbot/steampipe-plugin-sdk)
(Apache-2.0) for the plugin protocol types. Steampipe and its plugins are products of
Turbot HQ, Inc.: each plugin you fetch is distributed under its own license.
