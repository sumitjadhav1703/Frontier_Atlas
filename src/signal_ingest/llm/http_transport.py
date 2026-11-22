"""Mockable HTTP boundary for LLM provider adapters (httpx)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import httpx


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    headers: Mapping[str, str]
    content: bytes
    url: str = ""
    elapsed_s: float = 0.0

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def header(self, name: str) -> str | None:
        lower = name.lower()
        for key, value in self.headers.items():
            if key.lower() == lower:
                return value
        return None


@runtime_checkable
class HttpTransport(Protocol):
    """Async HTTP request surface — mock in tests; never call live from B5 gates."""

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        json: Any = None,
        content: bytes | None = None,
        params: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse: ...


@dataclass
class HttpxTransport:
    """Thin httpx.AsyncClient wrapper. Inject ``transport=`` for MockTransport."""

    client: httpx.AsyncClient | None = None
    timeout_s: float = 30.0
    _owns_client: bool = field(default=False, init=False, repr=False)

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        timeout_s: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
        base_url: str = "",
    ) -> None:
        self.timeout_s = timeout_s
        if client is not None:
            self.client = client
            self._owns_client = False
        else:
            self.client = httpx.AsyncClient(
                transport=transport,
                base_url=base_url,
                timeout=httpx.Timeout(timeout_s),
            )
            self._owns_client = True

    async def aclose(self) -> None:
        if self._owns_client and self.client is not None:
            await self.client.aclose()

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        json: Any = None,
        content: bytes | None = None,
        params: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        assert self.client is not None
        resp = await self.client.request(
            method,
            url,
            headers=dict(headers or {}),
            json=json,
            content=content,
            params=dict(params or {}) if params else None,
            timeout=timeout if timeout is not None else self.timeout_s,
        )
        # Normalize header map to plain str→str for adapters.
        hdrs = {k: v for k, v in resp.headers.multi_items()}
        elapsed = resp.elapsed.total_seconds() if resp.elapsed is not None else 0.0
        return HttpResponse(
            status_code=resp.status_code,
            headers=hdrs,
            content=resp.content,
            url=str(resp.url),
            elapsed_s=elapsed,
        )


@dataclass
class ScriptedHttpTransport:
    """In-memory scripted responses for offline adapter tests (no sockets)."""

    script: list[HttpResponse | Exception]
    calls: list[dict[str, Any]] = field(default_factory=list)
    _index: int = field(default=0, init=False, repr=False)

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        json: Any = None,
        content: bytes | None = None,
        params: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        record = {
            "method": method,
            "url": url,
            "headers": dict(headers or {}),
            "json": json,
            "content": content,
            "params": dict(params or {}) if params else None,
            "timeout": timeout,
        }
        # Never retain Authorization / API key material in call logs.
        redacted = dict(record)
        hdr = dict(redacted.get("headers") or {})
        for key in list(hdr):
            if key.lower() in {"authorization", "x-api-key", "api-key"}:
                hdr[key] = "[REDACTED]"
        redacted["headers"] = hdr
        if isinstance(redacted.get("params"), dict) and "key" in redacted["params"]:
            redacted["params"] = {**redacted["params"], "key": "[REDACTED]"}
        self.calls.append(redacted)

        if self._index >= len(self.script):
            raise RuntimeError("ScriptedHttpTransport exhausted")
        item = self.script[self._index]
        self._index += 1
        if isinstance(item, Exception):
            raise item
        return item
