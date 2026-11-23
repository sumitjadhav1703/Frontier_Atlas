"""Loopback route server for B8 listing/row-shortcut cases (reopened B2 gate).

Serves fixed bodies per path and counts every GET. Binds 127.0.0.1 only, so the
network guard stays on.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


@dataclass
class ListingServer:
    base_url: str
    routes: dict[str, tuple[str, bytes]]
    hits: dict[str, int] = field(default_factory=dict)
    request_headers: dict[str, dict[str, str]] = field(default_factory=dict)
    _httpd: ThreadingHTTPServer | None = None
    _thread: threading.Thread | None = None

    def url(self, path: str) -> str:
        return f"{self.base_url}{path if path.startswith('/') else '/' + path}"

    def item_hits(self) -> dict[str, int]:
        """Hits excluding robots.txt."""
        return {k: v for k, v in self.hits.items() if k != "/robots.txt"}

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)


def start_listing_server(routes: dict[str, tuple[str, bytes]]) -> ListingServer:
    """routes: path -> (content_type, body). Unknown paths return 404."""
    lock = threading.Lock()
    server = ListingServer(base_url="", routes=dict(routes))
    server.routes.setdefault("/robots.txt", ("text/plain", b"User-agent: *\nAllow: /\n"))

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):  # keep test output clean
            return

        def do_GET(self):  # noqa: N802
            path = urlsplit(self.path).path
            with lock:
                server.hits[path] = server.hits.get(path, 0) + 1
                server.request_headers[path] = {k.lower(): v for k, v in self.headers.items()}
            route = server.routes.get(path)
            if route is None:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            ctype, body = route
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server._httpd = httpd
    server.base_url = f"http://127.0.0.1:{httpd.server_address[1]}"
    server._thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    server._thread.start()
    return server
