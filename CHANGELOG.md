# Changelog

## 0.1.0 (unreleased)

First release: hosts Steampipe plugins (local binaries or OCI images from ghcr.io,
digest-verified and cached) over the steampipe-plugin-sdk gRPC protocol. Each plugin
becomes a schema and each plugin table a table; key-column filters are pushed down as
quals and re-applied exactly; required key columns become required filters;
connection credentials come from `TYPE steampipe` secrets.

Targets: vgi-go 0.42 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · steampipe-plugin-sdk v5.14 · vgi-lint-check 2.x (unpinned)
