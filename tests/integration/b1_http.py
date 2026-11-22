"""Loopback HTTP scenario server for B1 gate tests.

Network guard stays on; only 127.0.0.1 binds are used.
"""

from __future__ import annotations

import gzip
import json
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


@dataclass
class ScenarioState:
    """Mutable counters shared between handler and tests."""

    hits: dict[str, int] = field(default_factory=dict)
    in_flight: int = 0
    peak_in_flight: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def enter(self) -> None:
        with self.lock:
            self.in_flight += 1
            self.peak_in_flight = max(self.peak_in_flight, self.in_flight)

    def leave(self) -> None:
        with self.lock:
            self.in_flight = max(0, self.in_flight - 1)

    def bump(self, key: str) -> int:
        with self.lock:
            self.hits[key] = self.hits.get(key, 0) + 1
            return self.hits[key]


@dataclass
class B1HttpServer:
    """Threading HTTP server serving scripted B1 failure/success paths."""

    host: str
    port: int
    base_url: str
    state: ScenarioState
    _httpd: ThreadingHTTPServer
    _thread: threading.Thread

    def url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def stop(self) -> None:
        self._httpd.shutdown()
        self._thread.join(timeout=5)
        self._httpd.server_close()


def _read_fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def start_b1_http_server(
    *,
    robots_body: bytes | None = None,
    slow_delay_s: float = 0.35,
    oversized_bytes: int = 200_000,
) -> B1HttpServer:
    """Bind an ephemeral loopback server with the B1 scenario matrix routes."""
    state = ScenarioState()
    robots = robots_body if robots_body is not None else _read_fixture("b1_robots.txt")
    challenge = _read_fixture("b1_challenge.html")
    feed = _read_fixture("b1_alternate_feed.xml")

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return  # silence

        def _send(
            self,
            code: int,
            body: bytes = b"",
            *,
            headers: dict[str, str] | None = None,
        ) -> None:
            self.send_response(code)
            hdrs = {"Content-Length": str(len(body))}
            if headers:
                hdrs.update(headers)
            for k, v in hdrs.items():
                self.send_header(k, v)
            self.end_headers()
            if body and self.command != "HEAD":
                self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            state.enter()
            try:
                path = urlparse(self.path).path
                state.bump(path)
                self._dispatch(path)
            finally:
                state.leave()

        def do_HEAD(self) -> None:  # noqa: N802
            self.do_GET()

        def _dispatch(self, path: str) -> None:
            if path == "/robots.txt":
                self._send(
                    200,
                    robots,
                    headers={"Content-Type": "text/plain; charset=utf-8"},
                )
                return

            if path == "/ok":
                self._send(
                    200,
                    b'{"ok":true,"label":"synthetic"}',
                    headers={"Content-Type": "application/json"},
                )
                return

            if path == "/redirect-loop-a":
                self._send(302, headers={"Location": self.path.replace("a", "b")})
                return
            if path == "/redirect-loop-b":
                self._send(302, headers={"Location": "/redirect-loop-a"})
                return

            if path == "/etag-resource":
                etag = '"b1-etag-v1"'
                inm = self.headers.get("If-None-Match")
                if inm == etag:
                    self._send(304, b"", headers={"ETag": etag})
                    return
                body = b"cached-body-v1"
                self._send(
                    200,
                    body,
                    headers={
                        "ETag": etag,
                        "Content-Type": "text/plain; charset=utf-8",
                    },
                )
                return

            if path == "/timeout":
                time.sleep(slow_delay_s * 4)
                self._send(200, b"too-late")
                return

            if path == "/rate-limit-then-ok":
                n = state.hits.get(path, 0)
                if n <= 1:
                    self._send(
                        429,
                        b"slow down",
                        headers={"Retry-After": "0", "Content-Type": "text/plain"},
                    )
                    return
                self._send(
                    200,
                    b'{"recovered":true}',
                    headers={"Content-Type": "application/json"},
                )
                return

            if path == "/always-503":
                self._send(503, b"unavailable", headers={"Retry-After": "0"})
                return

            if path == "/missing":
                self._send(404, b"gone")
                return

            if path == "/secret/denied":
                self._send(200, b"should-not-fetch")
                return

            if path == "/redirect-to-private":
                # Metadata / link-local style destination — policy must reject.
                self._send(302, headers={"Location": "http://169.254.169.254/"})
                return

            if path == "/redirect-to-public":
                self._send(302, headers={"Location": "https://example.com/"})
                return

            if path == "/truncated-gzip":
                # Claim gzip but send truncated payload.
                bad = gzip.compress(b"hello-truncated-gzip-body")[:8]
                self._send(
                    200,
                    bad,
                    headers={
                        "Content-Type": "application/octet-stream",
                        "Content-Encoding": "gzip",
                    },
                )
                return

            if path == "/unexpected-mime":
                self._send(
                    200,
                    b"%PDF-1.4 fake",
                    headers={"Content-Type": "application/pdf"},
                )
                return

            if path == "/oversized":
                body = b"X" * oversized_bytes
                self._send(
                    200,
                    body,
                    headers={"Content-Type": "text/plain; charset=utf-8"},
                )
                return

            if path == "/challenge":
                self._send(
                    200,
                    challenge,
                    headers={"Content-Type": "text/html; charset=utf-8"},
                )
                return

            if path == "/alternate-feed.xml":
                self._send(
                    200,
                    feed,
                    headers={"Content-Type": "application/atom+xml; charset=utf-8"},
                )
                return

            if path == "/slow":
                time.sleep(slow_delay_s)
                self._send(
                    200,
                    b"slow-ok",
                    headers={"Content-Type": "text/plain; charset=utf-8"},
                )
                return

            if path == "/primary-blocked":
                self._send(
                    200,
                    challenge,
                    headers={"Content-Type": "text/html; charset=utf-8"},
                )
                return

            if path.startswith("/dup/"):
                self._send(
                    200,
                    json.dumps({"path": path}).encode(),
                    headers={"Content-Type": "application/json"},
                )
                return

            self._send(404, b"unknown-route")

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    host, port = httpd.server_address[:2]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base = f"http://{host}:{port}"
    return B1HttpServer(
        host=str(host),
        port=int(port),
        base_url=base,
        state=state,
        _httpd=httpd,
        _thread=thread,
    )


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout_s: float = 5.0,
    interval_s: float = 0.05,
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval_s)
    raise AssertionError("condition not met before timeout")
