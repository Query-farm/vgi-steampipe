// Fetching plugins from an OCI registry.
//
// Steampipe publishes each plugin as an OCI image whose layers are one gzip'd
// binary per platform (media type
// application/vnd.turbot.steampipe.plugin.<os>-<arch>.layer.v1+gzip). This file
// does the minimum needed to install one:
//
//  1. GET the manifest (answering the registry's anonymous Bearer challenge),
//     checking its sha256 against the pinned digest when the ref has one;
//  2. pick the layer for this platform;
//  3. download the blob, verify its sha256 and size against the manifest, and
//     gunzip it into the cache as an executable.
//
// Cache layout (cache_dir, default <user cache dir>/vgi-steampipe/plugins):
//
//	blobs/sha256/<layer hex>/<plugin binary>   verified, content-addressed
//	refs/<registry host>/<repo>/<tag>.json   -> {manifest digest, layer digest, binary}
//	refs/by-digest/<manifest digest>.json    (the same, for pinned refs)
//
// A cached binary is reused without contacting the registry when the ref is
// pinned by digest, and as an offline fallback for tags.

package steampipe

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// maxManifestBytes and maxBlobBytes bound what a registry can make us read.
const (
	maxManifestBytes = 4 << 20
	maxBlobBytes     = 1 << 30
)

var ociClient = &http.Client{Timeout: 10 * time.Minute}

// FetchedPlugin is a plugin binary resolved from a ref.
type FetchedPlugin struct {
	Binary         string `json:"binary"`
	ManifestDigest string `json:"manifest_digest,omitempty"`
	LayerDigest    string `json:"layer_digest,omitempty"`
	Version        string `json:"version,omitempty"`
}

type ociDescriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Annotations map[string]string `json:"annotations"`
}

type ociManifest struct {
	SchemaVersion int               `json:"schemaVersion"`
	MediaType     string            `json:"mediaType"`
	Layers        []ociDescriptor   `json:"layers"`
	Annotations   map[string]string `json:"annotations"`
}

type refRecord struct {
	ManifestDigest string `json:"manifest_digest"`
	LayerDigest    string `json:"layer_digest"`
	Binary         string `json:"binary"`
	Version        string `json:"version,omitempty"`
}

// DefaultCacheDir is where fetched plugins live unless cache_dir is given.
func DefaultCacheDir() string {
	if d, err := os.UserCacheDir(); err == nil {
		return filepath.Join(d, "vgi-steampipe", "plugins")
	}
	return filepath.Join(os.TempDir(), "vgi-steampipe", "plugins")
}

// platformMediaType is the layer media type for this OS/architecture.
func platformMediaType() string {
	return fmt.Sprintf("application/vnd.turbot.steampipe.plugin.%s-%s.layer.v1+gzip", runtime.GOOS, runtime.GOARCH)
}

var unsafePath = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func refRecordPath(cacheDir string, ref PluginRef) string {
	if ref.Digest != "" {
		// A manifest digest names the same content on every registry.
		return filepath.Join(cacheDir, "refs", "by-digest", unsafePath.ReplaceAllString(ref.Digest, "_")+".json")
	}
	u, _ := url.Parse(ref.Registry)
	host := "registry"
	if u != nil {
		host = unsafePath.ReplaceAllString(u.Host, "_")
	}
	key := ref.Tag
	if ref.Digest != "" {
		key = ref.Digest
	}
	return filepath.Join(cacheDir, "refs", host, filepath.FromSlash(ref.Repo), unsafePath.ReplaceAllString(key, "_")+".json")
}

func readRefRecord(cacheDir string, ref PluginRef) (*refRecord, bool) {
	b, err := os.ReadFile(refRecordPath(cacheDir, ref))
	if err != nil {
		return nil, false
	}
	var rec refRecord
	if json.Unmarshal(b, &rec) != nil || rec.Binary == "" {
		return nil, false
	}
	if st, err := os.Stat(rec.Binary); err != nil || st.IsDir() {
		return nil, false
	}
	return &rec, true
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), perm); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// isNetworkError reports whether err means the registry was unreachable (as
// opposed to answering with an error).
func isNetworkError(err error) bool {
	var ne net.Error
	var ue *url.Error
	return errors.As(err, &ne) || errors.As(err, &ue)
}

// FetchPlugin resolves an OCI ref to a verified local binary.
func FetchPlugin(ctx context.Context, ref PluginRef, cacheDir string) (*FetchedPlugin, error) {
	if cacheDir == "" {
		cacheDir = DefaultCacheDir()
	}
	if ref.Digest != "" {
		if rec, ok := readRefRecord(cacheDir, ref); ok && rec.ManifestDigest == ref.Digest {
			return &FetchedPlugin{Binary: rec.Binary, ManifestDigest: rec.ManifestDigest, LayerDigest: rec.LayerDigest, Version: rec.Version}, nil
		}
	}
	fp, err := fetchFromRegistry(ctx, ref, cacheDir)
	if err != nil {
		if isNetworkError(err) {
			if rec, ok := readRefRecord(cacheDir, ref); ok {
				slog.Warn("registry unreachable; using the cached plugin", "ref", ref.Display(), "err", err)
				return &FetchedPlugin{Binary: rec.Binary, ManifestDigest: rec.ManifestDigest, LayerDigest: rec.LayerDigest, Version: rec.Version}, nil
			}
		}
		return nil, fmt.Errorf("fetching plugin %s: %w", ref.Display(), err)
	}
	return fp, nil
}

func fetchFromRegistry(ctx context.Context, ref PluginRef, cacheDir string) (*FetchedPlugin, error) {
	reg := &registryClient{base: ref.Registry, repo: ref.Repo}
	reference := ref.Tag
	if ref.Digest != "" {
		reference = ref.Digest
	}
	body, err := reg.get(ctx, "/v2/"+ref.Repo+"/manifests/"+reference, maxManifestBytes,
		"application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json")
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	manifestDigest := "sha256:" + hex.EncodeToString(sum[:])
	if ref.Digest != "" && manifestDigest != ref.Digest {
		return nil, fmt.Errorf("manifest digest mismatch: pinned %s, registry served %s", ref.Digest, manifestDigest)
	}
	var m ociManifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("decoding manifest: %w", err)
	}
	want := platformMediaType()
	var layer *ociDescriptor
	for i := range m.Layers {
		if m.Layers[i].MediaType == want {
			layer = &m.Layers[i]
			break
		}
	}
	if layer == nil {
		return nil, fmt.Errorf("the image has no plugin binary for %s/%s (layer %s)", runtime.GOOS, runtime.GOARCH, want)
	}
	if !digestRe.MatchString(layer.Digest) {
		return nil, fmt.Errorf("layer digest %q is not a sha256 digest", layer.Digest)
	}
	if layer.Size <= 0 || layer.Size > maxBlobBytes {
		return nil, fmt.Errorf("layer size %d is out of range", layer.Size)
	}
	name := "steampipe-plugin-" + strings.ReplaceAll(ref.ShortName, "_", "-") + ".plugin"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binDir := filepath.Join(cacheDir, "blobs", "sha256", strings.TrimPrefix(layer.Digest, "sha256:"))
	binary := filepath.Join(binDir, name)
	if st, err := os.Stat(binary); err != nil || st.IsDir() {
		if err := reg.downloadLayer(ctx, ref.Repo, *layer, binary); err != nil {
			return nil, err
		}
	}
	version := m.Annotations["org.opencontainers.image.version"]
	rec, _ := json.Marshal(refRecord{ManifestDigest: manifestDigest, LayerDigest: layer.Digest, Binary: binary, Version: version})
	pinned := ref
	pinned.Digest = manifestDigest
	for _, path := range []string{refRecordPath(cacheDir, ref), refRecordPath(cacheDir, pinned)} {
		if err := writeFileAtomic(path, rec, 0o644); err != nil {
			slog.Warn("could not record the plugin ref in the cache", "err", err)
		}
	}
	return &FetchedPlugin{Binary: binary, ManifestDigest: manifestDigest, LayerDigest: layer.Digest, Version: version}, nil
}

// downloadLayer streams a blob to disk, verifying size and digest before
// unpacking it, so an unverified binary never lands at its final path.
func (c *registryClient) downloadLayer(ctx context.Context, repo string, layer ociDescriptor, binary string) error {
	resp, err := c.do(ctx, "/v2/"+repo+"/blobs/"+layer.Digest, "")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if err := os.MkdirAll(filepath.Dir(binary), 0o755); err != nil {
		return err
	}
	gz, err := os.CreateTemp(filepath.Dir(binary), ".blob-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(gz.Name()) }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(gz, h), io.LimitReader(resp.Body, layer.Size+1))
	if err != nil {
		_ = gz.Close()
		return fmt.Errorf("downloading layer: %w", err)
	}
	if n != layer.Size {
		_ = gz.Close()
		return fmt.Errorf("layer size mismatch: manifest says %d bytes, registry sent %d", layer.Size, n)
	}
	if got := "sha256:" + hex.EncodeToString(h.Sum(nil)); got != layer.Digest {
		_ = gz.Close()
		return fmt.Errorf("layer digest mismatch: manifest says %s, downloaded %s", layer.Digest, got)
	}
	if _, err := gz.Seek(0, io.SeekStart); err != nil {
		_ = gz.Close()
		return err
	}
	zr, err := gzip.NewReader(gz)
	if err != nil {
		_ = gz.Close()
		return fmt.Errorf("layer is not gzip: %w", err)
	}
	out, err := os.CreateTemp(filepath.Dir(binary), ".bin-*")
	if err != nil {
		_ = gz.Close()
		return err
	}
	defer func() { _ = os.Remove(out.Name()) }()
	_, err = io.Copy(out, io.LimitReader(zr, 4*maxBlobBytes))
	_ = gz.Close()
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("unpacking layer: %w", err)
	}
	if err := os.Chmod(out.Name(), 0o755); err != nil {
		return err
	}
	return os.Rename(out.Name(), binary)
}

// registryClient speaks just enough of the OCI distribution API, including
// the anonymous Bearer token handshake ghcr.io uses for public images.
type registryClient struct {
	base  string
	repo  string
	token string
}

var bearerParam = regexp.MustCompile(`(\w+)="([^"]*)"`)

func (c *registryClient) get(ctx context.Context, path string, limit int64, accept string) ([]byte, error) {
	resp, err := c.do(ctx, path, accept)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("response from %s exceeds %d bytes", path, limit)
	}
	return b, nil
}

// do performs a GET, authenticating once on a 401 challenge, and returns a
// 200 response (any other status is an error naming it).
func (c *registryClient) do(ctx context.Context, path, accept string) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "vgi-steampipe-worker/"+Version)
		if accept != "" {
			req.Header.Set("Accept", accept)
		}
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		resp, err := ociClient.Do(req)
		if err != nil {
			return nil, err
		}
		switch {
		case resp.StatusCode == http.StatusOK:
			return resp, nil
		case resp.StatusCode == http.StatusUnauthorized && attempt == 0:
			challenge := resp.Header.Get("WWW-Authenticate")
			_ = resp.Body.Close()
			if err := c.authenticate(ctx, challenge); err != nil {
				return nil, err
			}
			continue
		case (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500) && attempt < 3:
			_ = resp.Body.Close()
			wait := time.Duration(1<<attempt) * time.Second
			if s := resp.Header.Get("Retry-After"); s != "" {
				var secs int
				if _, err := fmt.Sscanf(s, "%d", &secs); err == nil && secs > 0 && secs < 60 {
					wait = time.Duration(secs) * time.Second
				}
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
			continue
		default:
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusNotFound {
				return nil, fmt.Errorf("registry returned HTTP 404 for %s (no such plugin or version)", path)
			}
			return nil, fmt.Errorf("registry returned HTTP %d for %s", resp.StatusCode, path)
		}
	}
}

// authenticate answers `WWW-Authenticate: Bearer realm=...,service=...,scope=...`
// with an anonymous token request.
func (c *registryClient) authenticate(ctx context.Context, challenge string) error {
	if !strings.HasPrefix(strings.ToLower(challenge), "bearer ") {
		return fmt.Errorf("registry requires authentication this worker does not support (%q); only public images can be fetched", challenge)
	}
	params := map[string]string{}
	for _, m := range bearerParam.FindAllStringSubmatch(challenge, -1) {
		params[strings.ToLower(m[1])] = m[2]
	}
	realm := params["realm"]
	if realm == "" {
		return fmt.Errorf("registry auth challenge has no realm")
	}
	u, err := url.Parse(realm)
	if err != nil {
		return err
	}
	q := u.Query()
	if s := params["service"]; s != "" {
		q.Set("service", s)
	}
	scope := params["scope"]
	if scope == "" {
		scope = "repository:" + c.repo + ":pull"
	}
	q.Set("scope", scope)
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := ociClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registry token endpoint returned HTTP %d", resp.StatusCode)
	}
	var tok struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&tok); err != nil {
		return fmt.Errorf("decoding registry token: %w", err)
	}
	c.token = tok.Token
	if c.token == "" {
		c.token = tok.AccessToken
	}
	if c.token == "" {
		return fmt.Errorf("registry token endpoint returned no token")
	}
	return nil
}
