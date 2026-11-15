#!/usr/bin/env python3
"""A tiny OCI registry serving the fixture plugin as a Steampipe plugin image.

The worker's OCI fetch path is tested against this server, so the suite runs
offline: it speaks the parts of the distribution API the worker uses,
including ghcr.io's anonymous Bearer-token handshake.

Repository ``turbot/steampipe/plugins/turbot/fixture``:

``GET /v2/<repo>/manifests/<tag or digest>``
    Tags ``1.0.0`` and ``latest``: an image manifest with one gzip'd layer per
    platform, built from FIXTURE_PLUGIN (only the host platform's layer is a
    real binary; the others are tiny placeholders).
    Tag ``tampered``: the same manifest, but the host layer's blob does not
    match its declared digest.
``GET /v2/<repo>/blobs/<digest>``
    The layer blobs.
``GET /token``
    Issues the anonymous token every /v2 request must carry.
``GET /stats``
    JSON request counters (manifests, blobs), for cache tests.

Prints ``PORT:<n>`` and ``DIGEST:<manifest digest of tag 1.0.0>`` once
listening.

Usage::

    FIXTURE_PLUGIN=bin/steampipe-plugin-fixture python3 test/mock_registry/server.py
"""

from __future__ import annotations

import argparse
import contextlib
import gzip
import hashlib
import io
import json
import os
import platform
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO = "turbot/steampipe/plugins/turbot/fixture"
TOKEN = "anonymous-fixture-token"
PLATFORMS = ["darwin-amd64", "darwin-arm64", "linux-amd64", "linux-arm64", "windows-amd64"]


def host_platform() -> str:
    os_name = {"darwin": "darwin", "linux": "linux", "win32": "windows"}.get(sys.platform, sys.platform)
    machine = platform.machine().lower()
    arch = {"x86_64": "amd64", "amd64": "amd64", "arm64": "arm64", "aarch64": "arm64"}.get(machine, machine)
    return f"{os_name}-{arch}"


def gz(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as f:
        f.write(data)
    return buf.getvalue()


def digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


BLOBS: dict[str, bytes] = {}
MANIFESTS: dict[str, bytes] = {}
STATS = {"manifests": 0, "blobs": 0, "tokens": 0}
LOCK = threading.Lock()


def build(plugin_path: str) -> str:
    with open(plugin_path, "rb") as f:
        binary = f.read()
    host = host_platform()
    layers = []
    host_digest = ""
    for plat in PLATFORMS:
        blob = gz(binary) if plat == host else gz(f"placeholder for {plat}\n".encode())
        d = digest(blob)
        BLOBS[d] = blob
        if plat == host:
            host_digest = d
        layers.append({
            "mediaType": f"application/vnd.turbot.steampipe.plugin.{plat}.layer.v1+gzip",
            "digest": d,
            "size": len(blob),
            "annotations": {"org.opencontainers.image.title": f"steampipe-plugin-fixture_{plat.replace('-', '_')}.gz"},
        })
    config = b'{"plugin":{"name":"fixture","organization":"turbot"}}'
    BLOBS[digest(config)] = config

    def manifest(lays: list[dict]) -> bytes:
        return json.dumps({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"mediaType": "application/vnd.turbot.steampipe.config.v1+json",
                       "digest": digest(config), "size": len(config)},
            "layers": lays,
            "annotations": {"org.opencontainers.image.title": "fixture",
                            "org.opencontainers.image.version": "1.0.0"},
        }, sort_keys=True).encode()

    good = manifest(layers)
    MANIFESTS["1.0.0"] = good
    MANIFESTS["latest"] = good
    MANIFESTS[digest(good)] = good
    # tampered: the host layer claims a digest whose blob is different bytes.
    bad_blob = gz(b"not the plugin you are looking for\n")
    fake = digest(b"tampered:" + bad_blob)
    BLOBS[fake] = bad_blob
    tampered = [dict(l) for l in layers]
    for l in tampered:
        if l["digest"] == host_digest:
            l["digest"], l["size"] = fake, len(bad_blob)
    MANIFESTS["tampered"] = manifest(tampered)
    return digest(good)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        pass

    def send(self, code: int, body: bytes, ctype: str = "application/json", headers: dict | None = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - stdlib name
        path = self.path.split("?", 1)[0]
        if path == "/token":
            with LOCK:
                STATS["tokens"] += 1
            self.send(200, json.dumps({"token": TOKEN}).encode())
            return
        if path == "/stats":
            with LOCK:
                self.send(200, json.dumps(STATS).encode())
            return
        if not path.startswith("/v2/"):
            self.send(404, b'{"errors":[{"code":"NOT_FOUND"}]}')
            return
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            host = self.headers.get("Host", "127.0.0.1")
            challenge = f'Bearer realm="http://{host}/token",service="mock-registry",scope="repository:{REPO}:pull"'
            self.send(401, b'{"errors":[{"code":"UNAUTHORIZED"}]}', headers={"WWW-Authenticate": challenge})
            return
        prefix = f"/v2/{REPO}/"
        if not path.startswith(prefix):
            self.send(404, b'{"errors":[{"code":"NAME_UNKNOWN"}]}')
            return
        rest = path[len(prefix):]
        if rest.startswith("manifests/"):
            ref = rest[len("manifests/"):]
            body = MANIFESTS.get(ref)
            if body is None:
                self.send(404, b'{"errors":[{"code":"MANIFEST_UNKNOWN"}]}')
                return
            with LOCK:
                STATS["manifests"] += 1
            self.send(200, body, "application/vnd.oci.image.manifest.v1+json",
                      {"Docker-Content-Digest": digest(body)})
            return
        if rest.startswith("blobs/"):
            body = BLOBS.get(rest[len("blobs/"):])
            if body is None:
                self.send(404, b'{"errors":[{"code":"BLOB_UNKNOWN"}]}')
                return
            with LOCK:
                STATS["blobs"] += 1
            self.send(200, body, "application/octet-stream")
            return
        self.send(404, b'{"errors":[{"code":"NOT_FOUND"}]}')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    plugin = os.environ.get("FIXTURE_PLUGIN")
    if not plugin:
        sys.exit("FIXTURE_PLUGIN must name the fixture plugin binary")
    manifest_digest = build(plugin)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    print(f"DIGEST:{manifest_digest}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()
