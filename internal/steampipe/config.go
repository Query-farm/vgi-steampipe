// Connection config handling.
//
// Steampipe passes a connection's settings to a plugin as the HCL body of its
// `connection` block (`paths = ["/data/*.csv"]`, `region = "us-east-1"`).
// This worker accepts the same HCL, or a JSON object of the same attributes,
// from two places:
//
//   - the `config` ATTACH option: non-secret settings, one HCL block (or JSON
//     member) per plugin schema, used both to discover the plugin's tables at
//     ATTACH time and for every query;
//   - a `steampipe` secret: credentials, merged over the ATTACH settings at
//     query time (an attribute set in both is taken from the secret).

package steampipe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

// normalizeConfig turns one connection config (HCL body or JSON object) into
// HCL body text, validating that it parses.
func normalizeConfig(src string) (string, error) {
	src = strings.TrimSpace(src)
	if src == "" {
		return "", nil
	}
	if strings.HasPrefix(src, "{") {
		var obj map[string]json.RawMessage
		dec := json.NewDecoder(strings.NewReader(src))
		dec.UseNumber()
		if err := dec.Decode(&obj); err != nil {
			return "", fmt.Errorf("connection config looks like JSON but does not parse: %w", err)
		}
		return jsonToHCL(obj)
	}
	if _, diags := hclsyntax.ParseConfig([]byte(src), "config.hcl", hcl.InitialPos); diags.HasErrors() {
		return "", fmt.Errorf("connection config is not valid HCL: %s", diags.Error())
	}
	return src + "\n", nil
}

// jsonToHCL renders a JSON object as HCL attributes. HCL's expression syntax
// is a superset of JSON values, so each value is written verbatim.
func jsonToHCL(obj map[string]json.RawMessage) (string, error) {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		if !hclsyntax.ValidIdentifier(k) {
			return "", fmt.Errorf("connection config key %q is not a valid HCL attribute name", k)
		}
		v := bytes.TrimSpace(obj[k])
		if string(v) == "null" {
			continue
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, v); err != nil {
			return "", fmt.Errorf("connection config key %q: %w", k, err)
		}
		fmt.Fprintf(&b, "%s = %s\n", k, compact.String())
	}
	out := b.String()
	if _, diags := hclsyntax.ParseConfig([]byte(out), "config.hcl", hcl.InitialPos); diags.HasErrors() {
		return "", fmt.Errorf("connection config cannot be expressed as HCL: %s", diags.Error())
	}
	return out, nil
}

// mergeConfig overlays one HCL body on another: every attribute and block of
// overlay is kept, and base keeps only the attributes overlay does not set.
func mergeConfig(base, overlay string) (string, error) {
	if strings.TrimSpace(base) == "" {
		return overlay, nil
	}
	if strings.TrimSpace(overlay) == "" {
		return base, nil
	}
	ov, diags := hclsyntax.ParseConfig([]byte(overlay), "secret.hcl", hcl.InitialPos)
	if diags.HasErrors() {
		return "", fmt.Errorf("secret config: %s", diags.Error())
	}
	src := []byte(base)
	bf, diags := hclsyntax.ParseConfig(src, "attach.hcl", hcl.InitialPos)
	if diags.HasErrors() {
		return "", fmt.Errorf("attach config: %s", diags.Error())
	}
	overridden := ov.Body.(*hclsyntax.Body).Attributes
	body := bf.Body.(*hclsyntax.Body)
	type span struct{ start, end int }
	var keep []span
	for name, attr := range body.Attributes {
		if _, ok := overridden[name]; ok {
			continue
		}
		r := attr.SrcRange
		keep = append(keep, span{r.Start.Byte, r.End.Byte})
	}
	for _, blk := range body.Blocks {
		r := blk.Range()
		keep = append(keep, span{r.Start.Byte, r.End.Byte})
	}
	slices.SortFunc(keep, func(a, b span) int { return a.start - b.start })
	var b strings.Builder
	for _, s := range keep {
		b.Write(src[s.start:s.end])
		b.WriteByte('\n')
	}
	b.WriteString(overlay)
	return b.String(), nil
}

// parseAttachConfig parses the `config` ATTACH option: one block (HCL) or
// member (JSON) per schema, each holding that plugin's connection config:
//
//	csv { paths = ["/data/*.csv"] }
//	{"csv": {"paths": ["/data/*.csv"]}}
func parseAttachConfig(src string) (map[string]string, error) {
	out := map[string]string{}
	src = strings.TrimSpace(src)
	if src == "" {
		return out, nil
	}
	if strings.HasPrefix(src, "{") {
		var obj map[string]json.RawMessage
		dec := json.NewDecoder(strings.NewReader(src))
		dec.UseNumber()
		if err := dec.Decode(&obj); err != nil {
			return nil, fmt.Errorf("config option looks like JSON but does not parse: %w", err)
		}
		for schema, raw := range obj {
			inner := strings.TrimSpace(string(raw))
			if !strings.HasPrefix(inner, "{") {
				return nil, fmt.Errorf("config option: %q must map to an object of connection settings", schema)
			}
			hclText, err := normalizeConfig(inner)
			if err != nil {
				return nil, fmt.Errorf("config option, schema %q: %w", schema, err)
			}
			out[strings.ToLower(schema)] = hclText
		}
		return out, nil
	}
	bs := []byte(src)
	f, diags := hclsyntax.ParseConfig(bs, "config.hcl", hcl.InitialPos)
	if diags.HasErrors() {
		return nil, fmt.Errorf("config option is not valid HCL: %s", diags.Error())
	}
	body := f.Body.(*hclsyntax.Body)
	if len(body.Attributes) > 0 {
		return nil, fmt.Errorf("config option: put each plugin's settings in a block named after its schema, e.g. csv { paths = [\"/data/*.csv\"] }")
	}
	for _, blk := range body.Blocks {
		if len(blk.Labels) > 0 {
			return nil, fmt.Errorf("config option: block %q must not have labels", blk.Type)
		}
		inner := string(bs[blk.OpenBraceRange.End.Byte:blk.CloseBraceRange.Start.Byte])
		hclText, err := normalizeConfig(inner)
		if err != nil {
			return nil, fmt.Errorf("config option, schema %q: %w", blk.Type, err)
		}
		out[strings.ToLower(blk.Type)] = hclText
	}
	return out, nil
}
