"""Shared HTTP adapter helpers and live-qualification rules."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

import httpx

from signal_ingest.config.clock import Clock, SystemClock
from signal_ingest.llm.errors import (
    AuthError,
    InvalidOutputError,
    LiveQualificationError,
    MissingCredentialsError,
    ProviderDisabledError,
    QuotaForbiddenError,
    RateLimitedError,
    TransientProviderError,
)
from signal_ingest.llm.http_transport import HttpResponse, HttpTransport
from signal_ingest.llm.provider import (
    ContextLengthExceeded,
    PayloadTooLarge,
    ProviderError,
    ProviderLimits,
    ProviderRequest,
    ProviderResult,
    measure_provider_request,
)
from signal_ingest.llm.retry import parse_retry_after

# Fake / replay never qualifies as a live provider for B5/B8 gates.
_LIVE_NAMES = frozenset({"gemini", "groq", "ollama"})
_FAKE_PREFIXES = ("fake", "replay", "mock", "scripted")


def is_live_provider(provider: str) -> bool:
    """Return True only for real free-tier/local adapter names — never fake."""
    name = (provider or "").strip().lower()
    if not name:
        return False
    if name in _FAKE_PREFIXES or any(name.startswith(p) for p in _FAKE_PREFIXES):
        return False
    return name in _LIVE_NAMES


def assert_not_fake_as_live(provider: str, *, claim_live: bool) -> None:
    if claim_live and not is_live_provider(provider):
        raise LiveQualificationError(
            f"provider {provider!r} cannot qualify as live",
            provider=provider,
        )


def classify_http_error(
    response: HttpResponse,
    *,
    provider: str,
    now: datetime,
    body_text: str | None = None,
) -> ProviderError:
    """Map HTTP status + headers to typed provider errors."""
    status = response.status_code
    text = body_text if body_text is not None else response.text
    lower = text.lower()
    retry_after = parse_retry_after(response.header("Retry-After"), now=now)
    reset_at = None
    if retry_after is not None:
        reset_at = now + timedelta(seconds=retry_after)

    headers = {k: v for k, v in response.headers.items()}

    if status == 401:
        return AuthError(
            f"{provider} unauthorized",
            status_code=401,
            provider=provider,
            permanent=True,
        )
    if status == 403:
        if _looks_like_quota(lower, headers):
            return QuotaForbiddenError(
                f"{provider} quota forbidden",
                provider=provider,
                retry_after_s=retry_after,
                reset_at=reset_at,
            )
        return AuthError(
            f"{provider} forbidden",
            status_code=403,
            provider=provider,
            permanent=True,
        )
    if status == 429:
        return RateLimitedError(
            f"{provider} rate limited",
            provider=provider,
            retry_after_s=retry_after,
            reset_at=reset_at,
            headers=headers,
        )
    if status == 413 or _looks_like_context(lower):
        return PayloadTooLarge(
            f"{provider} payload too large",
            serialized_bytes=0,
            input_tokens=0,
        )
    if status == 400 and _looks_like_context(lower):
        return ContextLengthExceeded(
            f"{provider} context length exceeded",
            serialized_bytes=0,
            input_tokens=0,
        )
    if status >= 500 or status in {408, 425}:
        return TransientProviderError(
            f"{provider} transient HTTP {status}",
            status_code=status,
            provider=provider,
        )
    return TransientProviderError(
        f"{provider} unexpected HTTP {status}: {text[:200]}",
        status_code=status,
        provider=provider,
        retryable=status >= 500,
    )


_FENCE = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.S | re.I)
_TRUNCATED = frozenset({"max_tokens", "length"})


def loads_model_json(text: str) -> dict[str, Any] | None:
    """Parse model JSON, tolerating ```json fences and prose around one object."""
    fenced = _FENCE.match(text.strip())
    candidates = [text, fenced.group(1) if fenced else None]
    start, end = text.find("{"), text.rfind("}")
    if 0 <= start < end:
        candidates.append(text[start : end + 1])
    for candidate in candidates:
        if not candidate:
            continue
        try:
            loaded = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        return loaded if isinstance(loaded, dict) else {"value": loaded}
    return None


def check_model_output(
    provider: str,
    *,
    parsed: dict[str, Any] | None,
    finish_reason: str,
    request: ProviderRequest,
    raw_text: str,
) -> None:
    """Truncated or non-JSON output is invalid, so the orchestrator retries it."""
    if finish_reason.strip().lower() in _TRUNCATED:
        raise InvalidOutputError(
            f"{provider} output truncated ({finish_reason})", provider=provider, raw_text=raw_text
        )
    if parsed is None and request.response_schema:
        raise InvalidOutputError(
            f"{provider} output is not JSON", provider=provider, raw_text=raw_text
        )


def _looks_like_quota(lower: str, headers: Mapping[str, str]) -> bool:
    if "quota" in lower or "rate limit" in lower or "resource_exhausted" in lower:
        return True
    for key, value in headers.items():
        lk = key.lower()
        if "ratelimit" in lk or lk.endswith("remaining"):
            if str(value).strip() in {"0", "0.0"}:
                return True
    return False


def _looks_like_context(lower: str) -> bool:
    needles = (
        "context length",
        "context_length",
        "maximum context",
        "too many tokens",
        "token limit",
        "payload too large",
        "request too large",
        "maximum number of tokens",
        "input token count",
    )
    return any(n in lower for n in needles)


class HttpProviderBase:
    """Common complete() scaffolding: budget assert, HTTP call, error mapping."""

    provider_name: str = "http"

    def __init__(
        self,
        limits: ProviderLimits,
        *,
        transport: HttpTransport,
        api_key: str | None = None,
        clock: Clock | None = None,
        enforce_budget: bool = True,
        require_api_key: bool = True,
    ) -> None:
        self._limits = limits
        self._transport = transport
        self._api_key = api_key
        self._clock = clock or SystemClock()
        self.enforce_budget = enforce_budget
        self.require_api_key = require_api_key
        self.calls: list[dict[str, Any]] = []

    @property
    def limits(self) -> ProviderLimits:
        return self._limits

    @property
    def is_live(self) -> bool:
        return is_live_provider(self._limits.provider)

    def ensure_ready(self) -> None:
        if not self._limits.enabled:
            raise ProviderDisabledError(
                f"{self._limits.provider} disabled in config",
                provider=self._limits.provider,
            )
        if self.require_api_key and not (self._api_key and self._api_key.strip()):
            raise MissingCredentialsError(
                f"{self._limits.provider} missing API key",
                provider=self._limits.provider,
            )

    async def complete(self, request: ProviderRequest) -> ProviderResult:
        self.ensure_ready()
        measured = measure_provider_request(request)
        self.calls.append(
            {
                "provider": request.provider,
                "model": request.model,
                "request_bytes": measured.request_bytes,
                "input_tokens": measured.input_tokens,
            }
        )
        if self.enforce_budget:
            self._assert_budget(request, measured.request_bytes, measured.input_tokens)

        method, url, headers, body = self.build_http_request(request)
        try:
            response = await self._transport.request(
                method,
                url,
                headers=headers,
                json=body,
            )
        except TimeoutError as exc:
            raise TransientProviderError(
                f"{self._limits.provider} timeout",
                status_code=408,
                provider=self._limits.provider,
            ) from exc
        except Exception as exc:
            # httpx.TimeoutException / HTTPError / OSError — keep transport mockable
            # without importing httpx into every failure path.
            exc_name = type(exc).__name__
            if "Timeout" in exc_name:
                raise TransientProviderError(
                    f"{self._limits.provider} timeout",
                    status_code=408,
                    provider=self._limits.provider,
                ) from exc
            if (
                isinstance(exc, (OSError, httpx.HTTPError))
                or "HTTPError" in exc_name
                or "ConnectError" in exc_name
            ):
                raise TransientProviderError(
                    f"{self._limits.provider} transport error: {exc}",
                    status_code=None,
                    provider=self._limits.provider,
                ) from exc
            raise

        if response.status_code >= 400:
            err = classify_http_error(
                response,
                provider=self._limits.provider,
                now=self._clock.now(),
            )
            if isinstance(err, (PayloadTooLarge, ContextLengthExceeded)):
                # Attach measured sizes for re-budget.
                if isinstance(err, PayloadTooLarge):
                    raise PayloadTooLarge(
                        str(err),
                        serialized_bytes=measured.request_bytes,
                        input_tokens=measured.input_tokens,
                    )
                raise ContextLengthExceeded(
                    str(err),
                    serialized_bytes=measured.request_bytes,
                    input_tokens=measured.input_tokens,
                )
            raise err

        try:
            result = self.parse_http_response(response, request=request, measured_bytes=measured.request_bytes, measured_input=measured.input_tokens)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise InvalidOutputError(
                f"{self._limits.provider} invalid output: {exc}",
                provider=self._limits.provider,
                raw_text=response.text,
            ) from exc
        return result

    def build_http_request(
        self, request: ProviderRequest
    ) -> tuple[str, str, dict[str, str], dict[str, Any]]:
        raise NotImplementedError

    def parse_http_response(
        self,
        response: HttpResponse,
        *,
        request: ProviderRequest,
        measured_bytes: int,
        measured_input: int,
    ) -> ProviderResult:
        raise NotImplementedError

    def _assert_budget(
        self,
        request: ProviderRequest,
        request_bytes: int,
        input_tokens: int,
    ) -> None:
        if request_bytes > request.budget.max_request_bytes:
            raise PayloadTooLarge(
                f"serialized request {request_bytes}b exceeds budget "
                f"{request.budget.max_request_bytes}b",
                serialized_bytes=request_bytes,
                input_tokens=input_tokens,
            )
        if input_tokens > request.budget.max_input_tokens:
            raise PayloadTooLarge(
                f"input tokens {input_tokens} exceed budget "
                f"{request.budget.max_input_tokens}",
                serialized_bytes=request_bytes,
                input_tokens=input_tokens,
            )
        if request_bytes > self._limits.max_request_bytes:
            raise PayloadTooLarge(
                f"serialized request {request_bytes}b exceeds provider cap "
                f"{self._limits.max_request_bytes}b",
                serialized_bytes=request_bytes,
                input_tokens=input_tokens,
            )
        if input_tokens > self._limits.max_input_tokens:
            raise ContextLengthExceeded(
                f"input tokens {input_tokens} exceed provider cap "
                f"{self._limits.max_input_tokens}",
                serialized_bytes=request_bytes,
                input_tokens=input_tokens,
            )


def resolve_api_key(env_name: str | None, explicit: str | None = None) -> str | None:
    if explicit is not None:
        return explicit or None
    if not env_name:
        return None
    value = os.environ.get(env_name)
    return value if value else None


def provider_from_config(
    name: str,
    *,
    transport: HttpTransport,
    config: Mapping[str, Any] | None = None,
    api_key: str | None = None,
    clock: Clock | None = None,
    limits: ProviderLimits | None = None,
) -> HttpProviderBase:
    """Build a named adapter from providers.yaml-shaped config."""
    from signal_ingest.llm.adapters.gemini import GeminiProvider
    from signal_ingest.llm.adapters.groq import GroqProvider
    from signal_ingest.llm.adapters.ollama import OllamaProvider
    from signal_ingest.llm.providers_config import provider_limits_from_config

    block = dict(config or {})
    resolved_limits = limits or provider_limits_from_config(name)
    key = resolve_api_key(block.get("api_key_env"), api_key)
    name_l = name.lower()
    if name_l == "gemini":
        return GeminiProvider(
            resolved_limits,
            transport=transport,
            api_key=key,
            clock=clock,
        )
    if name_l == "groq":
        return GroqProvider(
            resolved_limits,
            transport=transport,
            api_key=key,
            clock=clock,
        )
    if name_l == "ollama":
        return OllamaProvider(
            resolved_limits,
            transport=transport,
            api_key=key,
            clock=clock,
            base_url=str(block.get("base_url") or "http://127.0.0.1:11434"),
        )
    raise KeyError(f"unknown adapter: {name}")
