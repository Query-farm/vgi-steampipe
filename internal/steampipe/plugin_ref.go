// Plugin references: which plugin each schema runs.
//
// The `plugins` ATTACH option is a comma-separated list of entries
// `[schema=]ref`. A ref is either a local plugin binary path or an OCI image
// reference in the forms the Steampipe CLI accepts:
//
//	csv                       ghcr.io/turbot/steampipe/plugins/turbot/csv:latest
//	csv@1.3.0                 ...turbot/csv:1.3.0 (Steampipe's version syntax)
//	turbot/csv:1.3.0          ...turbot/csv:1.3.0
//	csv@sha256:<hex>          pinned to a manifest digest
//	ghcr.io/org/repo/name:tag any registry, full repository path
//	/opt/plugins/csv.plugin   a local binary (also ./, ../ and ~/ paths)

package steampipe

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// DefaultRegistry is where turbot publishes Steampipe plugin images.
const DefaultRegistry = "https://ghcr.io"

// steampipeRepoPrefix is the repository prefix for short plugin names.
const steampipeRepoPrefix = "turbot/steampipe/plugins/"

// PluginRef is one parsed entry of the plugins option.
type PluginRef struct {
	Schema string `json:"schema"`
	Raw    string `json:"raw"`
	// Local binary path (absolute) when the ref is a file.
	Path string `json:"path,omitempty"`
	// OCI coordinates when the ref is an image.
	Registry string `json:"registry,omitempty"` // scheme://host[:port]
	Repo     string `json:"repo,omitempty"`     // e.g. turbot/steampipe/plugins/turbot/csv
	Tag      string `json:"tag,omitempty"`
	Digest   string `json:"digest,omitempty"` // sha256:<hex>, when pinned
	// ShortName is the plugin's name (csv, aws, fixture).
	ShortName string `json:"short_name"`
}

// IsLocal reports whether the ref names a local binary.
func (r PluginRef) IsLocal() bool { return r.Path != "" }

// Display renders the ref for users.
func (r PluginRef) Display() string {
	if r.IsLocal() {
		return r.Path
	}
	s := r.Registry + "/" + r.Repo
	if r.Digest != "" {
		return s + "@" + r.Digest
	}
	return s + ":" + r.Tag
}

var (
	identRe  = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	digestRe = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	tagRe    = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
	repoRe   = regexp.MustCompile(`^[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*$`)
)

func looksLocal(ref string) bool {
	return strings.HasPrefix(ref, "/") || strings.HasPrefix(ref, "./") || strings.HasPrefix(ref, "../") ||
		strings.HasPrefix(ref, "~/") || strings.HasPrefix(ref, `\\`) || filepath.VolumeName(ref) != "" ||
		strings.HasSuffix(ref, ".plugin") || strings.HasSuffix(ref, ".exe")
}

// localShortName derives a plugin name from a binary file name:
// steampipe-plugin-csv.plugin -> csv.
func localShortName(path string) string {
	base := filepath.Base(path)
	base = strings.TrimSuffix(base, ".exe")
	base = strings.TrimSuffix(base, ".plugin")
	if i := strings.Index(base, "_"); strings.HasPrefix(base, "steampipe-plugin-") && i > 0 {
		base = base[:i] // steampipe-plugin-csv_darwin_arm64
	}
	base = strings.TrimPrefix(base, "steampipe-plugin-")
	return strings.ToLower(strings.ReplaceAll(base, "-", "_"))
}

// ParsePluginRefs parses the plugins option. registry replaces the default
// registry for refs that do not name one.
func ParsePluginRefs(option, registry string) ([]PluginRef, error) {
	if registry == "" {
		registry = DefaultRegistry
	}
	registry = strings.TrimRight(registry, "/")
	if !strings.HasPrefix(registry, "https://") && !strings.HasPrefix(registry, "http://") {
		registry = "https://" + registry
	}
	var refs []PluginRef
	seen := map[string]bool{}
	for _, entry := range strings.Split(option, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		schema, ref := "", entry
		if i := strings.Index(entry, "="); i >= 0 {
			schema, ref = strings.ToLower(strings.TrimSpace(entry[:i])), strings.TrimSpace(entry[i+1:])
		}
		r, err := parseOneRef(ref, registry)
		if err != nil {
			return nil, fmt.Errorf("plugins option, entry %q: %w", entry, err)
		}
		if schema == "" {
			schema = r.ShortName
		}
		if !identRe.MatchString(schema) {
			return nil, fmt.Errorf("plugins option, entry %q: schema name %q must be a lower-case SQL identifier (write name=ref to choose one)", entry, schema)
		}
		if schema == "main" || schema == "information_schema" || schema == "pg_catalog" {
			return nil, fmt.Errorf("plugins option, entry %q: schema name %q is reserved; write another_name=%s", entry, schema, ref)
		}
		if seen[schema] {
			return nil, fmt.Errorf("plugins option: schema %q is used twice; write name=ref to give each plugin its own schema", schema)
		}
		seen[schema] = true
		r.Schema = schema
		r.Raw = ref
		refs = append(refs, r)
	}
	return refs, nil
}

func parseOneRef(ref, registry string) (PluginRef, error) {
	if ref == "" {
		return PluginRef{}, fmt.Errorf("empty plugin reference")
	}
	if looksLocal(ref) {
		p := ref
		if strings.HasPrefix(p, "~/") {
			home, err := os.UserHomeDir()
			if err != nil {
				return PluginRef{}, err
			}
			p = filepath.Join(home, p[2:])
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			return PluginRef{}, err
		}
		return PluginRef{Path: abs, ShortName: localShortName(abs)}, nil
	}
	r := PluginRef{Registry: registry, Tag: "latest"}
	rest := ref
	explicitHost := false
	// Explicit registry host: the first component has a dot or a port.
	if i := strings.Index(rest, "/"); i > 0 && strings.ContainsAny(rest[:i], ".:") {
		host := rest[:i]
		rest = rest[i+1:]
		explicitHost = true
		scheme := "https://"
		if strings.HasPrefix(registry, "http://") && strings.HasPrefix(host, strings.TrimPrefix(registry, "http://")) {
			scheme = "http://"
		}
		r.Registry = scheme + host
	} else {
		r.Registry = registry
	}
	if i := strings.Index(rest, "@"); i >= 0 {
		v := rest[i+1:]
		rest = rest[:i]
		if strings.HasPrefix(v, "sha256:") {
			if !digestRe.MatchString(v) {
				return PluginRef{}, fmt.Errorf("digest %q must be sha256: followed by 64 lower-case hex digits", v)
			}
			r.Digest = v
		} else {
			r.Tag = v
		}
	} else if i := strings.LastIndex(rest, ":"); i >= 0 {
		r.Tag = rest[i+1:]
		rest = rest[:i]
	}
	if r.Digest == "" && !tagRe.MatchString(r.Tag) {
		return PluginRef{}, fmt.Errorf("invalid tag %q", r.Tag)
	}
	rest = strings.ToLower(rest)
	if !repoRe.MatchString(rest) {
		return PluginRef{}, fmt.Errorf("invalid plugin name %q", rest)
	}
	parts := strings.Split(rest, "/")
	switch {
	case explicitHost || strings.HasPrefix(rest, steampipeRepoPrefix):
		r.Repo = rest // full repository path
	case len(parts) == 1:
		r.Repo = steampipeRepoPrefix + "turbot/" + rest
	case len(parts) == 2:
		r.Repo = steampipeRepoPrefix + rest
	default:
		r.Repo = rest
	}
	r.ShortName = strings.ReplaceAll(parts[len(parts)-1], "-", "_")
	return r, nil
}
