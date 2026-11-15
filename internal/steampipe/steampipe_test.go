package steampipe

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

func TestParsePluginRefs(t *testing.T) {
	refs, err := ParsePluginRefs("csv, aws@0.118.0, gh=turbot/github:1.2.3, x=ghcr.io/acme/steampipe/foo-bar:2, pinned=net@sha256:"+strings.Repeat("a", 64)+", local=/opt/steampipe-plugin-rss.plugin", "")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][3]string{}
	for _, r := range refs {
		got[r.Schema] = [3]string{r.Display(), r.ShortName, r.Path}
	}
	want := map[string][3]string{
		"csv":    {"https://ghcr.io/turbot/steampipe/plugins/turbot/csv:latest", "csv", ""},
		"aws":    {"https://ghcr.io/turbot/steampipe/plugins/turbot/aws:0.118.0", "aws", ""},
		"gh":     {"https://ghcr.io/turbot/steampipe/plugins/turbot/github:1.2.3", "github", ""},
		"x":      {"https://ghcr.io/acme/steampipe/foo-bar:2", "foo_bar", ""},
		"pinned": {"https://ghcr.io/turbot/steampipe/plugins/turbot/net@sha256:" + strings.Repeat("a", 64), "net", ""},
		"local":  {"/opt/steampipe-plugin-rss.plugin", "rss", "/opt/steampipe-plugin-rss.plugin"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("refs:\n got %v\nwant %v", got, want)
	}

	refs, err = ParsePluginRefs("fixture@1.0.0", "http://127.0.0.1:5000/")
	if err != nil || refs[0].Display() != "http://127.0.0.1:5000/turbot/steampipe/plugins/turbot/fixture:1.0.0" {
		t.Fatalf("registry override: %v %v", refs, err)
	}

	for _, bad := range []string{"main=csv", "csv,csv", "Bad Name=csv", "csv@sha256:abc", "c$v", "x=csv:bad tag"} {
		if _, err := ParsePluginRefs(bad, ""); err == nil {
			t.Errorf("ParsePluginRefs(%q) should fail", bad)
		}
	}
}

func TestNormalizeConfig(t *testing.T) {
	out, err := normalizeConfig(`{"paths": ["/a/*.csv"], "n": 3, "skip": null, "nested": {"k": true}}`)
	if err != nil {
		t.Fatal(err)
	}
	want := "n = 3\nnested = {\"k\":true}\npaths = [\"/a/*.csv\"]\n"
	if out != want {
		t.Fatalf("got %q want %q", out, want)
	}
	if _, err := normalizeConfig(`token = `); err == nil {
		t.Fatal("invalid HCL should fail")
	}
	if _, err := normalizeConfig(`{"bad-key!": 1}`); err == nil {
		t.Fatal("invalid attribute name should fail")
	}
	if out, _ := normalizeConfig("  "); out != "" {
		t.Fatalf("empty config: %q", out)
	}
}

func TestMergeConfig(t *testing.T) {
	out, err := mergeConfig("region = \"us-east-1\"\ntoken = \"public\"\n", "token = \"secret\"\n")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(out, "token") != 1 || !strings.Contains(out, `token = "secret"`) || !strings.Contains(out, `region = "us-east-1"`) {
		t.Fatalf("merge: %q", out)
	}
	if out, _ := mergeConfig("", "a = 1\n"); out != "a = 1\n" {
		t.Fatalf("merge into empty: %q", out)
	}
}

func TestParseAttachConfig(t *testing.T) {
	got, err := parseAttachConfig("csv {\n  paths = [\"/d/*.csv\"]\n}\nAWS {\n regions = [\"*\"]\n}\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !strings.Contains(got["csv"], `paths = ["/d/*.csv"]`) || !strings.Contains(got["aws"], "regions") {
		t.Fatalf("hcl: %v", got)
	}
	got, err = parseAttachConfig(`{"csv": {"paths": ["/d/*.csv"]}}`)
	if err != nil || got["csv"] != "paths = [\"/d/*.csv\"]\n" {
		t.Fatalf("json: %v %v", got, err)
	}
	for _, bad := range []string{`paths = []`, `{"csv": 1}`, `csv "label" { }`} {
		if _, err := parseAttachConfig(bad); err == nil {
			t.Errorf("parseAttachConfig(%q) should fail", bad)
		}
	}
}

func kc(name, require string, ops ...string) *proto.KeyColumn {
	return &proto.KeyColumn{Name: name, Require: require, Operators: ops}
}

func TestRequiredFilters(t *testing.T) {
	conn := kc("sp_connection_name", "optional", "=")
	cases := []struct {
		name string
		ts   *proto.TableSchema
		want [][]string
	}{
		{"none", &proto.TableSchema{ListCallKeyColumnList: []*proto.KeyColumn{kc("a", "optional"), conn}}, nil},
		{"list required", &proto.TableSchema{ListCallKeyColumnList: []*proto.KeyColumn{kc("a", "required"), conn}, GetCallKeyColumnList: []*proto.KeyColumn{conn}}, [][]string{{"a"}}},
		{"any_of", &proto.TableSchema{ListCallKeyColumnList: []*proto.KeyColumn{kc("a", "any_of"), kc("b", "any_of"), kc("c", "required")}}, [][]string{{"c"}, {"a", "b"}}},
		{"list or get", &proto.TableSchema{
			ListCallKeyColumnList: []*proto.KeyColumn{kc("domain", "required")},
			GetCallKeyColumnList:  []*proto.KeyColumn{kc("id", "")},
		}, [][]string{{"domain", "id"}}},
		{"get only is not enforced", &proto.TableSchema{GetCallKeyColumnList: []*proto.KeyColumn{kc("id", "required")}, ListCallKeyColumnList: []*proto.KeyColumn{conn}}, nil},
	}
	for _, c := range cases {
		if got := requiredFilters(c.ts); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestEffectiveConfig(t *testing.T) {
	ap := &attachedPlugin{Ref: PluginRef{Schema: "gh", ShortName: "github"}, Config: "base_url = \"https://example\"\n"}
	secrets := vgi.Secrets{"other": {"type": SecretType, "plugin": "aws", "config": "token = \"no\""}}
	cfg, err := effectiveConfig(ap, secrets)
	if err != nil || cfg != ap.Config {
		t.Fatalf("a secret for another plugin must be ignored: %q %v", cfg, err)
	}
	secrets = vgi.Secrets{"gh": {"type": SecretType, "plugin": "github", "config": `{"token": "s3"}`}}
	cfg, err = effectiveConfig(ap, secrets)
	if err != nil || !strings.Contains(cfg, `token = "s3"`) || !strings.Contains(cfg, "base_url") {
		t.Fatalf("merged: %q %v", cfg, err)
	}
}

// registry is a minimal OCI registry for FetchPlugin tests.
type registry struct {
	srv       *httptest.Server
	manifests map[string][]byte
	blobs     map[string][]byte
	hits      atomic.Int64
}

func sha(b []byte) string {
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:])
}

func newRegistry(t *testing.T, binary []byte, tamper bool) (*registry, string) {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(binary)
	_ = zw.Close()
	blob := buf.Bytes()
	d := sha(blob)
	r := &registry{manifests: map[string][]byte{}, blobs: map[string][]byte{d: blob}}
	if tamper {
		r.blobs[d] = append([]byte("x"), blob...)
	}
	m, _ := json.Marshal(ociManifest{SchemaVersion: 2, Layers: []ociDescriptor{{MediaType: platformMediaType(), Digest: d, Size: int64(len(blob))}},
		Annotations: map[string]string{"org.opencontainers.image.version": "9.9.9"}})
	r.manifests["1.0.0"] = m
	r.manifests[sha(m)] = m
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.hits.Add(1)
		if req.URL.Path == "/token" {
			_, _ = fmt.Fprint(w, `{"token":"t"}`)
			return
		}
		if req.Header.Get("Authorization") != "Bearer t" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+r.srv.URL+`/token",service="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		p := strings.TrimPrefix(req.URL.Path, "/v2/turbot/steampipe/plugins/turbot/fx/")
		if strings.HasPrefix(p, "manifests/") {
			if b, ok := r.manifests[strings.TrimPrefix(p, "manifests/")]; ok {
				_, _ = w.Write(b)
				return
			}
		}
		if strings.HasPrefix(p, "blobs/") {
			if b, ok := r.blobs[strings.TrimPrefix(p, "blobs/")]; ok {
				_, _ = w.Write(b)
				return
			}
		}
		http.NotFound(w, req)
	}))
	t.Cleanup(r.srv.Close)
	return r, sha(m)
}

func TestFetchPlugin(t *testing.T) {
	binary := []byte("#!/bin/sh\necho plugin\n")
	reg, manifestDigest := newRegistry(t, binary, false)
	cache := t.TempDir()
	refs, err := ParsePluginRefs("fx@1.0.0", reg.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := FetchPlugin(context.Background(), refs[0], cache)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(fp.Binary)
	if !bytes.Equal(got, binary) || fp.ManifestDigest != manifestDigest || fp.Version != "9.9.9" {
		t.Fatalf("fetched %q %+v", got, fp)
	}
	if st, _ := os.Stat(fp.Binary); st.Mode().Perm()&0o100 == 0 {
		t.Fatalf("binary is not executable: %v", st.Mode())
	}
	if !strings.HasPrefix(fp.Binary, filepath.Join(cache, "blobs", "sha256")) {
		t.Fatalf("binary outside the content-addressed cache: %s", fp.Binary)
	}

	// Pinned by digest: served from the cache without the registry.
	reg.srv.Close()
	pinned, _ := ParsePluginRefs("fx@"+manifestDigest, reg.srv.URL)
	fp2, err := FetchPlugin(context.Background(), pinned[0], cache)
	if err != nil || fp2.Binary != fp.Binary {
		t.Fatalf("pinned offline: %+v %v", fp2, err)
	}
	// A tag with the registry down falls back to the cached copy.
	fp3, err := FetchPlugin(context.Background(), refs[0], cache)
	if err != nil || fp3.Binary != fp.Binary {
		t.Fatalf("tag offline: %+v %v", fp3, err)
	}
	// Nothing cached and offline: an error naming the plugin.
	if _, err := FetchPlugin(context.Background(), refs[0], t.TempDir()); err == nil || !strings.Contains(err.Error(), "fetching plugin") {
		t.Fatalf("offline with an empty cache: %v", err)
	}
}

func TestFetchPluginVerifiesDigests(t *testing.T) {
	reg, _ := newRegistry(t, []byte("binary"), true)
	refs, _ := ParsePluginRefs("fx@1.0.0", reg.srv.URL)
	cache := t.TempDir()
	if _, err := FetchPlugin(context.Background(), refs[0], cache); err == nil || !strings.Contains(err.Error(), "layer size mismatch") && !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("tampered blob: %v", err)
	}
	if matches, _ := filepath.Glob(filepath.Join(cache, "blobs", "sha256", "*", "steampipe-plugin-*")); len(matches) != 0 {
		t.Fatalf("an unverified binary was installed: %v", matches)
	}
	wrong, _ := ParsePluginRefs("fx@sha256:"+strings.Repeat("0", 64), reg.srv.URL)
	if _, err := FetchPlugin(context.Background(), wrong[0], cache); err == nil {
		t.Fatal("a digest the registry does not serve must fail")
	}
}

func TestAppendColumnConversions(t *testing.T) {
	ts := &proto.TableSchema{Columns: []*proto.ColumnDefinition{
		{Name: "i", Type: proto.ColumnType_INT}, {Name: "d", Type: proto.ColumnType_DOUBLE},
		{Name: "b", Type: proto.ColumnType_BOOL}, {Name: "j", Type: proto.ColumnType_JSON},
		{Name: "s", Type: proto.ColumnType_INET}, {Name: "_ctx", Type: proto.ColumnType_JSON},
	}}
	s := tableArrowSchema(ts)
	if s.NumFields() != 5 {
		t.Fatalf("_ctx should be hidden: %v", s)
	}
	if got := sqlTypeName(proto.ColumnType_TIMESTAMP); got != "TIMESTAMP WITH TIME ZONE" {
		t.Fatal(got)
	}
}
