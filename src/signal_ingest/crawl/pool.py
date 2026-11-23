"""Shared async HTTPX pool with global and per-origin backpressure."""

from __future__ import annotations

import asyncio
import time
import zlib
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlsplit

import httpx


class BackpressureError(RuntimeError):
    """Raised when pending waiters would exceed the configured bound."""


class BodyTooLargeError(RuntimeError):
    """Raised when compressed or decoded body exceeds limits."""


@dataclass
class PoolLimits:
    max_active: int = 8
    max_pending: int = 32
    per_origin_concurrency: int = 2
    per_origin_min_interval_s: float = 0.05
    connect_timeout_s: float = 5.0
    read_timeout_s: float = 15.0
    overall_timeout_s: float = 30.0
    max_compressed_bytes: int = 2_000_000
    max_decoded_bytes: int = 5_000_000
    max_redirects: int = 8
    user_agent: str = "signal-ingest/0.1 (+assessment)"


class HttpPool:
    """Bounded shared HTTP client. Cancel releases slots cleanly."""

    def __init__(
        self,
        limits: PoolLimits | None = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.limits = limits or PoolLimits()
        self._global_sem = asyncio.Semaphore(self.limits.max_active)
        self._origin_sems: dict[str, asyncio.Semaphore] = defaultdict(
            lambda: asyncio.Semaphore(self.limits.per_origin_concurrency)
        )
        self._origin_next: dict[str, float] = defaultdict(float)
        # Per-origin lock: pacing one slow origin must not stall the others.
        self._origin_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._waiting = 0
        self._inflight = 0
        self._peak_inflight = 0
        self._closed = False
        timeout = httpx.Timeout(
            self.limits.overall_timeout_s,
            connect=self.limits.connect_timeout_s,
            read=self.limits.read_timeout_s,
            write=self.limits.read_timeout_s,
            pool=self.limits.connect_timeout_s,
        )
        # Only advertise codings _read_body can inflate under its byte caps (no br/zstd).
        headers = {"User-Agent": self.limits.user_agent, "Accept-Encoding": "gzip, deflate"}
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = httpx.AsyncClient(
                timeout=timeout,
                headers=headers,
                follow_redirects=False,
                transport=transport,
            )
            self._owns_client = True

    @property
    def peak_inflight(self) -> int:
        return self._peak_inflight

    @property
    def inflight(self) -> int:
        return self._inflight

    @property
    def waiting(self) -> int:
        return self._waiting

    def origin_of(self, url: str) -> str:
        parts = urlsplit(url)
        return f"{parts.scheme}://{parts.netloc}".lower()

    async def aclose(self) -> None:
        self._closed = True
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> HttpPool:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        extensions: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        """Perform one HTTP exchange under global/origin limits; release on exit/cancel."""
        if self._closed:
            raise RuntimeError("HttpPool is closed")
        origin = self.origin_of(url)
        # Bound pending waiters before blocking on the active semaphore.
        if (
            self._inflight >= self.limits.max_active
            and self._waiting >= self.limits.max_pending
        ):
            raise BackpressureError(
                f"pending queue full ({self.limits.max_pending}) with "
                f"{self._inflight} active requests"
            )
        self._waiting += 1
        try:
            await self._global_sem.acquire()
        except asyncio.CancelledError:
            self._waiting -= 1
            raise
        self._waiting -= 1
        self._inflight += 1
        if self._inflight > self._peak_inflight:
            self._peak_inflight = self._inflight
        origin_sem = self._origin_sems[origin]
        try:
            await origin_sem.acquire()
            try:
                await self._pace(origin)
                return await self._send(method, url, headers=headers, extensions=extensions)
            finally:
                origin_sem.release()
        except asyncio.CancelledError:
            raise
        finally:
            self._inflight -= 1
            self._global_sem.release()

    async def _pace(self, origin: str) -> None:
        interval = self.limits.per_origin_min_interval_s
        if interval <= 0:
            return
        async with self._origin_locks[origin]:
            now = time.monotonic()
            ready_at = self._origin_next[origin]
            delay = ready_at - now
            if delay > 0:
                await asyncio.sleep(delay)
                now = time.monotonic()
            self._origin_next[origin] = now + interval

    async def _send(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None,
        extensions: Mapping[str, Any] | None,
    ) -> httpx.Response:
        req_headers = dict(headers or {})
        # Stream so we can enforce compressed size before buffering fully.
        async with self._client.stream(
            method,
            url,
            headers=req_headers,
            extensions=dict(extensions or {}),
        ) as response:
            body, out_headers = await self._read_limited(response)
            return httpx.Response(
                status_code=response.status_code,
                headers=out_headers,
                content=body,
                request=response.request,
                extensions=dict(response.extensions),
                history=list(response.history),
            )

    async def _read_limited(
        self, response: httpx.Response
    ) -> tuple[bytes, httpx.Headers]:
        max_c = self.limits.max_compressed_bytes
        max_d = self.limits.max_decoded_bytes
        cl = response.headers.get("content-length")
        if cl is not None:
            try:
                if int(cl) > max_c:
                    await response.aclose()
                    raise BodyTooLargeError(f"content-length {cl} exceeds {max_c}")
            except ValueError:
                pass
        chunks: list[bytes] = []
        total = 0
        try:
            # Prefer raw bytes so we control decode errors (truncated/bad gzip).
            async for chunk in response.aiter_raw():
                total += len(chunk)
                if total > max_c:
                    await response.aclose()
                    raise BodyTooLargeError(f"compressed body exceeds {max_c} bytes")
                chunks.append(chunk)
        except BodyTooLargeError:
            raise
        except Exception as exc:
            await response.aclose()
            raise DecodingError(str(exc)) from exc
        compressed = b"".join(chunks)
        encoding = (response.headers.get("content-encoding") or "").lower().strip()
        if encoding in {"", "identity"}:
            body = compressed
        elif encoding == "gzip":
            body = _inflate(compressed, 16 + zlib.MAX_WBITS, max_d)
        elif encoding == "deflate":
            try:
                body = _inflate(compressed, zlib.MAX_WBITS, max_d)
            except DecodingError:
                # Some servers send raw deflate without the zlib header.
                body = _inflate(compressed, -zlib.MAX_WBITS, max_d)
        else:
            # br/zstd/stacked codings cannot be inflated under a cap here; httpx
            # would decode them fully first (bomb risk). We never advertise them.
            raise DecodingError(f"unsupported content-encoding: {encoding!r}")
        if len(body) > max_d:
            raise BodyTooLargeError(f"decoded body exceeds {max_d} bytes")
        out_headers = httpx.Headers(response.headers)
        out_headers.pop("content-encoding", None)
        out_headers["content-length"] = str(len(body))
        return body, out_headers


class DecodingError(RuntimeError):
    """Raised when content-encoding decode fails (e.g. truncated gzip)."""


def _inflate(data: bytes, wbits: int, max_decoded: int) -> bytes:
    """Decompress with a hard output cap so a small bomb never fully inflates."""
    d = zlib.decompressobj(wbits)
    try:
        out = d.decompress(data, max_decoded + 1)
        if len(out) > max_decoded or d.unconsumed_tail:
            raise BodyTooLargeError(f"decoded body exceeds {max_decoded} bytes")
        out += d.flush()
    except zlib.error as exc:
        raise DecodingError(str(exc)) from exc
    if len(out) > max_decoded:
        raise BodyTooLargeError(f"decoded body exceeds {max_decoded} bytes")
    if not d.eof:
        raise DecodingError("truncated compressed stream")
    return out
