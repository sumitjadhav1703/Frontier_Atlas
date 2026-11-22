"""B5 gate: provider fallback, circuits, quota and zero-spend control.

Oracle: docs/build-plan.md (B5) and docs/contracts.md extract/provider path.
Table-driven scripted failures; offline only — zero live sockets / paid spend.
Cases must FAIL when behavior is missing/wrong and PASS when integrated correctly.
"""

from __future__ import annotations

import asyncio
import json
import random
import socket
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from tests.integration import b5_api
from tests.support import FIXTURES

ARTIFACTS = Path(__file__).resolve().parents[2] / "artifacts" / "gates"
FROZEN = datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc)
B5_FIXTURES = FIXTURES / "llm" / "b5"


# ---------------------------------------------------------------------------
# Test clocks / helpers
# ---------------------------------------------------------------------------


class AdvanceableClock:
    """Injectable clock with advance/set for delay tests (no real sleep)."""

    def __init__(self, instant: datetime = FROZEN) -> None:
        if instant.tzinfo is None:
            raise ValueError("AdvanceableClock requires aware UTC")
        self._instant = instant.astimezone(timezone.utc)

    def now(self) -> datetime:
        return self._instant

    def advance(self, seconds: float) -> None:
        self._instant = self._instant + timedelta(seconds=float(seconds))

    def set(self, instant: datetime) -> None:
        if instant.tzinfo is None:
            raise ValueError("aware UTC required")
        self._instant = instant.astimezone(timezone.utc)


def _limits(name: str, *, enabled: bool = True, model: str | None = None) -> Any:
    from signal_ingest.llm.providers_config import provider_limits_from_config

    base = provider_limits_from_config(name)
    updates: dict[str, Any] = {"enabled": enabled}
    if model is not None:
        updates["model"] = model
    return base.model_copy(update=updates)


def _budget(*, max_input: int = 512, max_output: int = 64, max_bytes: int = 50_000) -> Any:
    from signal_ingest.llm.provider import RequestBudget

    return RequestBudget(
        max_input_tokens=max_input,
        max_output_tokens=max_output,
        max_request_bytes=max_bytes,
        framing_tokens=16,
    )


def _request(
    *,
    provider: str = "gemini",
    model: str = "test-model",
    content: str = "extract product fields",
    budget: Any | None = None,
) -> Any:
    from signal_ingest.llm.provider import ProviderRequest

    return ProviderRequest(
        messages=[
            {"role": "system", "content": "Return JSON fields only."},
            {"role": "user", "content": content},
        ],
        response_schema={"type": "object", "properties": {"productName": {"type": "string"}}},
        budget=budget or _budget(),
        provider=provider,
        model=model,
    )


def _http_ok_gemini(text: str = '{"productName":"AcmeAI"}') -> Any:
    from signal_ingest.llm.http_transport import HttpResponse

    body = {
        "candidates": [{"content": {"parts": [{"text": text}]}, "finishReason": "STOP"}],
        "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 5},
    }
    return HttpResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        content=json.dumps(body).encode("utf-8"),
    )


def _http_ok_groq(text: str = '{"productName":"AcmeAI"}') -> Any:
    from signal_ingest.llm.http_transport import HttpResponse

    body = {
        "choices": [
            {"message": {"content": text}, "finish_reason": "stop"},
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
    }
    return HttpResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        content=json.dumps(body).encode("utf-8"),
    )


def _http_status(
    status: int,
    *,
    body: str = "",
    headers: dict[str, str] | None = None,
) -> Any:
    from signal_ingest.llm.http_transport import HttpResponse

    return HttpResponse(
        status_code=status,
        headers=headers or {"content-type": "application/json"},
        content=body.encode("utf-8"),
    )


def _quota(
    clock: AdvanceableClock,
    *,
    providers: list[str] | None = None,
    requests_per_run: int | None = None,
    requests_per_day: int = 10_000,
    paid_spend_cap: int = 0,
) -> Any:
    quota_mod = b5_api.load_quota()
    QuotaLedger = b5_api.require_attr(quota_mod, "QuotaLedger")
    ProviderQuotaConfig = b5_api.require_attr(quota_mod, "ProviderQuotaConfig")
    ledger = QuotaLedger(paid_spend_cap=paid_spend_cap, clock=clock)
    for name in providers or ("gemini", "groq", "ollama"):
        ledger.configure(
            ProviderQuotaConfig(
                provider=name,
                requests_per_day=requests_per_day,
                tokens_per_day=10_000_000,
                requests_per_run=requests_per_run,
                billing_enabled=False,
                is_local=(name == "ollama"),
                is_fake=False,
            )
        )
    return ledger


def _orchestrator(
    providers: list[Any],
    *,
    clock: AdvanceableClock | None = None,
    rng: random.Random | None = None,
    quota: Any | None = None,
    circuits: Any | None = None,
    max_attempts: int = 8,
    local_retry_budget_s: float = 15.0,
    failure_threshold: int = 3,
    cooldown_s: float = 30.0,
) -> Any:
    fb = b5_api.load_fallback()
    FallbackOrchestrator = b5_api.require_attr(fb, "FallbackOrchestrator")
    circuit_mod = b5_api.load_circuit()
    CircuitBreakerRegistry = b5_api.require_attr(circuit_mod, "CircuitBreakerRegistry")
    clk = clock or AdvanceableClock()
    if circuits is None:
        circuits = CircuitBreakerRegistry(
            clock=clk,
            failure_threshold=failure_threshold,
            cooldown_s=cooldown_s,
        )
    return FallbackOrchestrator(
        providers=providers,
        quota=quota or _quota(clk),
        circuits=circuits,
        clock=clk,
        rng=rng or random.Random(0),
        max_attempts=max_attempts,
        max_wall_clock_s=120.0,
        retry_base_s=0.25,
        retry_cap_s=8.0,
        local_retry_budget_s=local_retry_budget_s,
        invalid_output_retries=1,
    )


def _gemini(transport: Any, *, clock: AdvanceableClock, api_key: str | None = "test-gemini-key") -> Any:
    adapters = b5_api.load_adapters()
    GeminiProvider = b5_api.require_attr(adapters, "GeminiProvider")
    return GeminiProvider(
        _limits("gemini", model="gemini-test"),
        transport=transport,
        api_key=api_key,
        clock=clock,
    )


def _groq(transport: Any, *, clock: AdvanceableClock, api_key: str | None = "test-groq-key") -> Any:
    adapters = b5_api.load_adapters()
    GroqProvider = b5_api.require_attr(adapters, "GroqProvider")
    return GroqProvider(
        _limits("groq", model="groq-test"),
        transport=transport,
        api_key=api_key,
        clock=clock,
    )


def _ollama(transport: Any, *, clock: AdvanceableClock) -> Any:
    adapters = b5_api.load_adapters()
    OllamaProvider = b5_api.require_attr(adapters, "OllamaProvider")
    return OllamaProvider(
        _limits("ollama", model="ollama-test"),
        transport=transport,
        api_key=None,
        clock=clock,
    )


def _scripted(*items: Any) -> Any:
    transport_mod = b5_api.load_transport()
    ScriptedHttpTransport = b5_api.require_attr(transport_mod, "ScriptedHttpTransport")
    return ScriptedHttpTransport(script=list(items))


def _assert_no_secrets(blob: str) -> None:
    lowered = blob.lower()
    assert "bearer test-" not in lowered
    assert "test-gemini-key" not in blob
    assert "test-groq-key" not in blob
    assert "sk-live" not in lowered


@pytest.fixture
def offsite_counter(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Count non-loopback DNS/connect attempts during the gate."""
    from signal_ingest.config.network import _is_loopback_host

    state = {"attempts": 0}
    orig_getaddrinfo = socket.getaddrinfo
    orig_connect = socket.socket.connect

    def counting_getaddrinfo(host, port, *args, **kwargs):
        host_s = host.decode() if isinstance(host, bytes) else host
        if host_s is not None and not _is_loopback_host(str(host_s)):
            state["attempts"] += 1
        return orig_getaddrinfo(host, port, *args, **kwargs)

    def counting_connect(self, address):
        host = str(address[0]) if isinstance(address, tuple) and address else None
        if host is not None and not _is_loopback_host(host):
            state["attempts"] += 1
        return orig_connect(self, address)

    monkeypatch.setattr(socket, "getaddrinfo", counting_getaddrinfo)
    monkeypatch.setattr(socket.socket, "connect", counting_connect)
    return state


# ---------------------------------------------------------------------------
# Matrix: Gemini 429 → Groq OK
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gemini_429_falls_back_to_groq_ok(offsite_counter):
    """Gemini rate-limit with Retry-After beyond local budget routes to Groq success.

    Detects: ignoring Retry-After, staying forever on Gemini, or calling live HTTP.
    """
    clock = AdvanceableClock()
    # Large Retry-After forces next eligible provider (never shorten server cooldown).
    gemini_t = _scripted(
        _http_status(429, body='{"error":"rate"}', headers={"Retry-After": "120"})
    )
    groq_t = _scripted(_http_ok_groq('{"productName":"FallbackOK"}'))
    orch = _orchestrator(
        [_gemini(gemini_t, clock=clock), _groq(groq_t, clock=clock)],
        clock=clock,
        local_retry_budget_s=15.0,
    )
    t0 = time.perf_counter()
    result = await orch.complete(_request())
    wall = time.perf_counter() - t0

    assert result.ok, (result.status, result.reason, result.transcript and result.transcript.as_dict())
    assert result.provider == "groq"
    assert result.result is not None
    assert result.result.parsed and result.result.parsed.get("productName") == "FallbackOK"
    assert result.attempts >= 2
    assert wall < 1.0, f"must not sleep on Retry-After; wall={wall:.3f}s"
    assert len(gemini_t.calls) == 1
    assert len(groq_t.calls) == 1
    assert offsite_counter["attempts"] == 0
    assert result.transcript is not None
    statuses = [a.status for a in result.transcript.attempts]
    assert "rate_limited" in statuses
    assert "ok" in statuses


# ---------------------------------------------------------------------------
# Oversize → shrink
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_oversize_then_shrink_same_provider(offsite_counter):
    """413 / payload-too-large re-budgets via shrink callback; second call is smaller.

    Detects: retrying identical oversized bytes, or skipping shrink.
    """
    clock = AdvanceableClock()
    transport = _scripted(
        _http_status(413, body="payload too large"),
        _http_ok_gemini('{"productName":"Shrunk"}'),
    )
    provider = _gemini(transport, clock=clock)
    orch = _orchestrator([provider], clock=clock)
    sizes: list[int] = []

    def shrink(req: Any, exc: Any) -> Any:
        from signal_ingest.llm.provider import RequestBudget

        sizes.append(getattr(exc, "serialized_bytes", 0) or 0)
        # Smaller content + tighter byte cap.
        return req.model_copy(
            update={
                "messages": [{"role": "user", "content": "tiny"}],
                "budget": RequestBudget(
                    max_input_tokens=128,
                    max_output_tokens=32,
                    max_request_bytes=8_000,
                    framing_tokens=8,
                ),
            }
        )

    big = _request(content="X" * 400)
    result = await orch.complete(big, shrink=shrink)
    assert result.ok
    assert result.provider == "gemini"
    assert result.result is not None
    assert result.result.parsed.get("productName") == "Shrunk"
    assert len(transport.calls) == 2
    # Second serialized body must be strictly smaller than the first.
    first_body = json.dumps(transport.calls[0]["json"], ensure_ascii=False)
    second_body = json.dumps(transport.calls[1]["json"], ensure_ascii=False)
    assert len(second_body) < len(first_body)
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# Context error on second provider
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_context_error_on_second_provider(offsite_counter):
    """After Gemini exhausts local retry budget, Groq context-length 400 is recorded.

    Detects: treating context errors as success, or not advancing the chain.
    """
    clock = AdvanceableClock()
    from signal_ingest.llm.http_transport import HttpResponse

    gemini_t = _scripted(
        _http_status(429, body="rate", headers={"Retry-After": "999"})
    )
    groq_t = _scripted(
        _http_status(
            400,
            body='{"error":{"message":"context length exceeded"}}',
        )
    )
    ollama_body = {
        "message": {"content": '{"productName":"LocalOK"}'},
        "done": True,
        "eval_count": 3,
        "prompt_eval_count": 8,
    }
    ollama_t = _scripted(
        HttpResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            content=json.dumps(ollama_body).encode("utf-8"),
        )
    )
    orch = _orchestrator(
        [
            _gemini(gemini_t, clock=clock),
            _groq(groq_t, clock=clock),
            _ollama(ollama_t, clock=clock),
        ],
        clock=clock,
        local_retry_budget_s=15.0,
    )
    result = await orch.complete(_request())
    assert result.ok
    assert result.provider == "ollama"
    assert result.transcript is not None
    statuses = [a.status for a in result.transcript.attempts]
    assert "rate_limited" in statuses
    assert "oversize" in statuses
    assert "ok" in statuses
    # classify_http_error maps context-length 400 bodies to PayloadTooLarge /
    # ContextLengthExceeded; either is a context-class failure on provider #2.
    assert any(
        a.provider == "groq"
        and a.status == "oversize"
        and a.error_type in {"ContextLengthExceeded", "PayloadTooLarge"}
        for a in result.transcript.attempts
    )
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# Bad JSON → retry once → abstain
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bad_json_retries_then_abstains(offsite_counter):
    """Malformed JSON → InvalidOutputError; one retry then abstain when all fail.

    Detects: accepting invalid JSON, infinite invalid retries, or raising uncaught.
    """
    clock = AdvanceableClock()
    # Outer JSON parse fails → InvalidOutputError via HttpProviderBase.
    bad = _http_status(200, body="NOT_JSON{{{")
    gemini_t = _scripted(bad, bad)  # retry once on same provider
    groq_t = _scripted(bad, bad)
    orch = _orchestrator(
        [_gemini(gemini_t, clock=clock), _groq(groq_t, clock=clock)],
        clock=clock,
        max_attempts=6,
    )
    result = await orch.complete(_request())
    assert result.status == "abstain"
    assert result.result is None
    assert result.reason in {"InvalidOutputError", "all_providers_failed"} or (
        result.reason and "Invalid" in result.reason
    )
    assert result.transcript is not None
    invalids = [a for a in result.transcript.attempts if a.status == "invalid_output"]
    assert len(invalids) >= 2
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# Missing key
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_api_key_skips_tier(offsite_counter):
    """Absent credentials disable the tier; next eligible provider may succeed.

    Detects: treating missing key as transient retry, or crashing the chain.
    """
    clock = AdvanceableClock()
    gemini = _gemini(_scripted(), clock=clock, api_key=None)
    groq_t = _scripted(_http_ok_groq('{"productName":"NoKeyFallback"}'))
    orch = _orchestrator(
        [gemini, _groq(groq_t, clock=clock)],
        clock=clock,
    )
    result = await orch.complete(_request())
    assert result.ok
    assert result.provider == "groq"
    assert result.transcript is not None
    # Missing key is recorded before any HTTP call on gemini.
    assert len(gemini._transport.calls) == 0  # type: ignore[attr-defined]
    assert any(
        a.error_type == "MissingCredentialsError"
        or (a.provider == "gemini" and a.status in {"disabled", "auth_error"})
        for a in result.transcript.attempts
    )
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# Permanent 401
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_permanent_401_opens_circuit_and_falls_back(offsite_counter):
    """401 is permanent auth failure: circuit forced open; chain advances.

    Detects: retrying permanent 401, or not distinguishing from quota 403.
    """
    clock = AdvanceableClock()
    gemini_t = _scripted(_http_status(401, body='{"error":"unauthorized"}'))
    groq_t = _scripted(_http_ok_groq('{"productName":"After401"}'))
    orch = _orchestrator(
        [_gemini(gemini_t, clock=clock), _groq(groq_t, clock=clock)],
        clock=clock,
        failure_threshold=1,
    )
    result = await orch.complete(_request())
    assert result.ok
    assert result.provider == "groq"
    breaker = orch.circuits.get("gemini")
    from signal_ingest.llm.circuit import CircuitState

    assert breaker.state is CircuitState.OPEN
    assert result.transcript is not None
    assert any(
        a.provider == "gemini" and a.status_code == 401 for a in result.transcript.attempts
    )
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# Endless 429
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_endless_429_bounded_abstain_no_sleep(offsite_counter):
    """Persistent 429 across providers abstains with bounded attempts and no sleep.

    Detects: unbounded retry loops, real-time sleep, or inventing success.
    """
    clock = AdvanceableClock()
    forever = [
        _http_status(429, body="rate", headers={"Retry-After": "1"}) for _ in range(20)
    ]
    gemini_t = _scripted(*forever[:10])
    groq_t = _scripted(*forever[10:])
    orch = _orchestrator(
        [_gemini(gemini_t, clock=clock), _groq(groq_t, clock=clock)],
        clock=clock,
        max_attempts=6,
        failure_threshold=2,
        local_retry_budget_s=15.0,
        cooldown_s=5.0,
    )
    t0 = time.perf_counter()
    result = await orch.complete(_request())
    wall = time.perf_counter() - t0
    assert result.status == "abstain"
    assert result.result is None
    assert result.attempts <= 6
    assert wall < 1.0, f"endless 429 must not sleep; wall={wall:.3f}s"
    assert result.transcript is not None
    assert all(a.status != "ok" for a in result.transcript.attempts)
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# All circuits open
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_circuits_open_abstains(offsite_counter):
    """When every provider circuit is open, orchestrator abstains without HTTP.

    Detects: ignoring open circuits or probing all providers concurrently.
    """
    clock = AdvanceableClock()
    gemini_t = _scripted(_http_ok_gemini())
    groq_t = _scripted(_http_ok_groq())
    orch = _orchestrator(
        [_gemini(gemini_t, clock=clock), _groq(groq_t, clock=clock)],
        clock=clock,
    )
    orch.circuits.get("gemini").force_open(open_for_s=60.0)
    orch.circuits.get("groq").force_open(open_for_s=60.0)
    result = await orch.complete(_request())
    assert result.status == "abstain"
    assert result.result is None
    assert len(gemini_t.calls) == 0
    assert len(groq_t.calls) == 0
    assert result.reason in {"circuit_open", "all_circuits_open", "all_providers_failed"} or (
        result.reason and "circuit" in result.reason
    )
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# Half-open success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_half_open_probe_success_closes_circuit(offsite_counter):
    """After cooldown, one half-open probe may succeed and close the circuit.

    Detects: never leaving OPEN, or allowing multiple concurrent half-open probes.
    """
    clock = AdvanceableClock()
    transport = _scripted(_http_ok_gemini('{"productName":"ProbeOK"}'))
    orch = _orchestrator(
        [_gemini(transport, clock=clock)],
        clock=clock,
        cooldown_s=10.0,
        failure_threshold=1,
    )
    breaker = orch.circuits.get("gemini")
    breaker.force_open(open_for_s=10.0)
    from signal_ingest.llm.circuit import CircuitState

    assert breaker.state is CircuitState.OPEN
    # Before cooldown: blocked.
    with pytest.raises(Exception) as blocked:
        breaker.allow()
    assert "circuit" in str(blocked.value).lower() or blocked.value.__class__.__name__ == "CircuitOpenError"

    clock.advance(10.0)
    result = await orch.complete(_request())
    assert result.ok
    assert result.provider == "gemini"
    assert breaker.state is CircuitState.CLOSED
    assert len(transport.calls) == 1
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# One-slot concurrent quota race
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_slot_concurrent_quota_race(offsite_counter):
    """Two workers contend for one remaining request slot; exactly one reserves.

    Detects: non-atomic reserve allowing oversubscription under concurrency.
    """
    clock = AdvanceableClock()
    quota = _quota(clock, providers=["gemini"], requests_per_run=1, requests_per_day=100)
    # Slow-ish scripted success so both tasks overlap on reserve.
    transport_a = _scripted(_http_ok_gemini('{"productName":"A"}'))
    transport_b = _scripted(_http_ok_gemini('{"productName":"B"}'))
    orch_a = _orchestrator([_gemini(transport_a, clock=clock)], clock=clock, quota=quota)
    orch_b = _orchestrator([_gemini(transport_b, clock=clock)], clock=clock, quota=quota)

    results = await asyncio.gather(
        orch_a.complete(_request(), work_key="race-a"),
        orch_b.complete(_request(), work_key="race-b"),
    )
    ok = [r for r in results if r.ok]
    denied = [r for r in results if not r.ok]
    assert len(ok) == 1, f"expected exactly one success, got {[r.status for r in results]}"
    assert len(denied) == 1
    assert denied[0].status == "abstain"
    assert denied[0].reason in {"QuotaExhaustedError", "all_providers_failed"} or (
        denied[0].transcript
        and any(a.status == "quota_denied" for a in denied[0].transcript.attempts)
    )
    # Ledger shows at most one committed request for the run.
    snap = quota.snapshot()
    used = snap["day"]["gemini"]["requests_used"]
    reserved = snap["day"]["gemini"]["requests_reserved"]
    assert used + reserved <= 1 or used == 1
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# Cancel in-flight
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_in_flight_request(offsite_counter):
    """In-flight CancelledError and pre-set cancel_event both abort cleanly.

    Detects: ignoring cancel, or leaking quota reservations.
    """
    clock = AdvanceableClock()

    class SlowProvider:
        def __init__(self) -> None:
            self.limits = _limits("gemini", model="slow")
            self.started = asyncio.Event()
            self.calls = 0

        async def complete(self, request: Any) -> Any:
            self.calls += 1
            self.started.set()
            await asyncio.sleep(60)
            raise AssertionError("should have been cancelled before finish")

    # Path 1: task cancellation while provider.complete is awaited.
    slow = SlowProvider()
    quota = _quota(clock, providers=["gemini"], requests_per_run=5)
    orch = _orchestrator([slow], clock=clock, quota=quota, max_attempts=2)
    task = asyncio.create_task(orch.complete(_request()))
    await slow.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert quota.snapshot()["open_reservations"] == 0

    # Path 2: cancel_event already set → cancelled before dispatch.
    cancel = asyncio.Event()
    cancel.set()
    transport = _scripted(_http_ok_gemini())
    orch2 = _orchestrator(
        [_gemini(transport, clock=clock)],
        clock=clock,
        quota=_quota(clock, providers=["gemini"]),
    )
    result = await orch2.complete(_request(), cancel_event=cancel)
    assert result.status == "cancelled"
    assert result.result is None
    assert len(transport.calls) == 0
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# Cache hit → 0 provider calls
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_hit_makes_zero_provider_calls(offsite_counter):
    """cache_lookup hit returns success with attempts=0 and no HTTP.

    Detects: ignoring cache, or still dispatching providers on hit.
    """
    from signal_ingest.llm.provider import ProviderResult

    clock = AdvanceableClock()
    transport = _scripted(_http_ok_gemini())
    provider = _gemini(transport, clock=clock)
    orch = _orchestrator([provider], clock=clock)
    cached = ProviderResult(
        text='{"productName":"Cached"}',
        parsed={"productName": "Cached"},
        input_tokens=1,
        output_tokens=1,
        request_bytes=10,
        finish_reason="stop",
        raw={"provider": "gemini"},
    )
    calls = {"n": 0}

    def lookup(key: str) -> ProviderResult | None:
        calls["n"] += 1
        assert key == "cache-key-1"
        return cached

    result = await orch.complete(
        _request(),
        work_key="cache-key-1",
        cache_lookup=lookup,
    )
    assert result.ok
    assert result.cache_hit is True
    assert result.attempts == 0
    assert result.result is not None
    assert result.result.parsed["productName"] == "Cached"
    assert len(transport.calls) == 0
    assert calls["n"] == 1
    assert result.live_qualified is False
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# Clock / RNG delay bounds (no sleep)
# ---------------------------------------------------------------------------


def test_retry_after_and_full_jitter_bounds_without_sleep():
    """Retry-After (seconds|HTTP-date) and full jitter stay in documented bounds.

    Detects: shortening server cooldown, sleeping, or RNG outside [0, cap].
    """
    retry = b5_api.load_retry()
    parse_retry_after = b5_api.require_attr(retry, "parse_retry_after")
    full_jitter = b5_api.require_attr(retry, "full_jitter")
    resolve_backoff_delay = b5_api.require_attr(retry, "resolve_backoff_delay")

    now = FROZEN
    assert parse_retry_after("30", now=now) == 30.0
    http_date = (now + timedelta(seconds=45)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    delay = parse_retry_after(http_date, now=now)
    assert delay is not None
    assert 44.0 <= delay <= 46.0

    rng = random.Random(42)
    for attempt in range(6):
        d = full_jitter(attempt, base_s=0.5, cap_s=4.0, rng=rng)
        high = min(4.0, 0.5 * (2**attempt))
        assert 0.0 <= d <= high

    # Server Retry-After must not be shortened by local cap.
    delay_s, exceeds = resolve_backoff_delay(
        attempt=0,
        retry_after_s=120.0,
        base_s=0.25,
        cap_s=8.0,
        rng=rng,
        local_budget_s=15.0,
    )
    assert delay_s == 120.0
    assert exceeds is True


@pytest.mark.asyncio
async def test_orchestrator_advances_clock_not_wall_time(offsite_counter):
    """Transient/rate-limit path advances injectable clock instead of sleeping."""
    clock = AdvanceableClock()
    # Short Retry-After within local budget → stay on provider; clock advances.
    transport = _scripted(
        _http_status(429, body="rate", headers={"Retry-After": "2"}),
        _http_ok_gemini('{"productName":"AfterWait"}'),
    )
    orch = _orchestrator(
        [_gemini(transport, clock=clock)],
        clock=clock,
        local_retry_budget_s=15.0,
        failure_threshold=5,
        max_attempts=4,
    )
    before = clock.now()
    t0 = time.perf_counter()
    result = await orch.complete(_request())
    wall = time.perf_counter() - t0
    assert result.ok
    assert clock.now() >= before + timedelta(seconds=2)
    assert wall < 1.0
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# Redacted transcript + zero live/paid
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redacted_transcript_and_zero_live_paid(offsite_counter, tmp_path: Path):
    """Readable transcript redacts secrets; live_calls=0 and paid_spend_cap=0 hold.

    Detects: logging Authorization/API keys, counting Fake as live, or paid fallback.
    """
    clock = AdvanceableClock()
    gemini_t = _scripted(
        _http_status(429, body="rate", headers={"Retry-After": "999"}),
    )
    groq_t = _scripted(_http_ok_groq('{"productName":"Secure"}'))
    quota = _quota(clock, paid_spend_cap=0)
    orch = _orchestrator(
        [
            _gemini(gemini_t, clock=clock, api_key="test-gemini-key"),
            _groq(groq_t, clock=clock, api_key="test-groq-key"),
        ],
        clock=clock,
        quota=quota,
        local_retry_budget_s=15.0,
    )
    result = await orch.complete(
        _request(),
        work_key="b5-redact-demo",
        estimated_paid_micros=0,
    )
    assert result.ok
    assert result.provider == "groq"
    assert result.transcript is not None
    blob = result.transcript.to_json()
    _assert_no_secrets(blob)
    result.transcript.assert_no_secrets()
    data = result.transcript.as_dict()
    assert data["live_calls"] == 0 or data["paid_fallback"] is False
    # Scripted adapters are live-named but transport is mock — gate cares about sockets + spend.
    assert data["paid_fallback"] is False
    assert quota.paid_spend_cap == 0
    assert quota.paid_spend_used == 0
    assert offsite_counter["attempts"] == 0

    # Persist readable artifact for reviewers.
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    out = ARTIFACTS / "b5_redacted_transcript.json"
    out.write_text(blob, encoding="utf-8")
    assert out.is_file()
    # Also keep a copy under tmp for isolation.
    (tmp_path / "transcript.json").write_text(blob, encoding="utf-8")

    # Fake never qualifies as live.
    adapters = b5_api.load_adapters()
    is_live_provider = b5_api.require_attr(adapters, "is_live_provider")
    assert is_live_provider("fake") is False
    assert is_live_provider("fake-extract-v1") is False
    assert is_live_provider("gemini") is True


@pytest.mark.asyncio
async def test_paid_spend_cap_blocks_billing_unknown(offsite_counter):
    """paid_spend_cap=0 denies billing-unknown / paid estimated spend routes.

    Detects: allowing paid fallback when cap is zero.
    """
    errors = b5_api.load_errors()
    PaidSpendBlockedError = b5_api.require_attr(errors, "PaidSpendBlockedError")
    quota_mod = b5_api.load_quota()
    QuotaLedger = b5_api.require_attr(quota_mod, "QuotaLedger")
    ProviderQuotaConfig = b5_api.require_attr(quota_mod, "ProviderQuotaConfig")

    clock = AdvanceableClock()
    ledger = QuotaLedger(paid_spend_cap=0, clock=clock)
    ledger.configure(
        ProviderQuotaConfig(
            provider="gemini",
            billing_enabled=None,  # unknown → disabled under zero-spend
            requests_per_day=100,
        )
    )
    with pytest.raises(PaidSpendBlockedError):
        await ledger.reserve("gemini", requests=1, estimated_paid_micros=0)

    ledger2 = QuotaLedger(paid_spend_cap=0, clock=clock)
    ledger2.configure(
        ProviderQuotaConfig(
            provider="gemini",
            billing_enabled=True,
            requests_per_day=100,
        )
    )
    with pytest.raises(PaidSpendBlockedError):
        await ledger2.reserve("gemini", requests=1, estimated_paid_micros=100)
    assert offsite_counter["attempts"] == 0


# ---------------------------------------------------------------------------
# Fixture labeling (synthetic isolation)
# ---------------------------------------------------------------------------


def test_b5_fixture_dir_labeled_or_absent():
    """If B5 fixtures exist they must be labeled synthetic / not for live export."""
    if not B5_FIXTURES.exists():
        return
    meta_path = B5_FIXTURES / "meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        root = meta.get("_meta") or meta
        assert root.get("label") == "synthetic"
        assert root.get("not_for_live_export") is True


# ---------------------------------------------------------------------------
# R2: production fallback behavior (real clock, per-tier sizing, cooldowns)
# ---------------------------------------------------------------------------


class _StubProvider:
    """Minimal Provider: scripted exceptions/results, counts calls."""

    def __init__(self, limits: Any, script: list[Any]) -> None:
        self._limits = limits
        self.script = list(script)
        self.calls = 0

    @property
    def limits(self) -> Any:
        return self._limits

    async def complete(self, request: Any) -> Any:
        self.calls += 1
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _ok_result(text: str = '{"fields": []}') -> Any:
    from signal_ingest.llm.provider import ProviderResult

    return ProviderResult(text=text, parsed=json.loads(text), input_tokens=5,
                          output_tokens=5, request_bytes=100)


@pytest.mark.asyncio
async def test_r2_429_backoff_really_waits_on_system_clock(offsite_counter):
    """With a wall clock (no advance()), a short Retry-After is actually awaited."""
    from signal_ingest.config.clock import SystemClock

    clock = SystemClock()
    gemini_t = _scripted(
        _http_status(429, body='{"error":"rate"}', headers={"Retry-After": "0.4"}),
        _http_ok_gemini('{"productName":"AfterWait"}'),
    )
    orch = _orchestrator([_gemini(gemini_t, clock=clock)], clock=clock)
    t0 = time.perf_counter()
    result = await orch.complete(_request())
    wall = time.perf_counter() - t0
    assert result.ok and result.provider == "gemini", (result.status, result.reason)
    assert len(gemini_t.calls) == 2
    assert wall >= 0.35, f"retried without waiting: {wall:.3f}s"


@pytest.mark.asyncio
async def test_r2_retry_after_cooldown_survives_into_next_request(offsite_counter):
    """One 429 with Retry-After=120 parks Gemini; the next request goes straight to Groq."""
    clock = AdvanceableClock()
    gemini_t = _scripted(
        _http_status(429, body='{"error":"rate"}', headers={"Retry-After": "120"})
    )
    groq_t = _scripted(_http_ok_groq(), _http_ok_groq())
    orch = _orchestrator(
        [_gemini(gemini_t, clock=clock), _groq(groq_t, clock=clock)], clock=clock
    )
    first = await orch.complete(_request())
    second = await orch.complete(_request())
    assert first.provider == "groq" and second.provider == "groq"
    assert len(gemini_t.calls) == 1, "Gemini hit again inside its Retry-After window"
    clock.advance(121)
    gemini_t.script.append(_http_ok_gemini())
    third = await orch.complete(_request())
    assert third.provider == "gemini"


@pytest.mark.asyncio
async def test_r2_quota_reserves_measured_tokens_not_budget_ceiling(offsite_counter):
    """A small request under a 1M-token ceiling must fit a 1M/day free quota."""
    quota_mod = b5_api.load_quota()
    clock = AdvanceableClock()
    ledger = quota_mod.QuotaLedger(paid_spend_cap=0, clock=clock)
    ledger.configure(quota_mod.ProviderQuotaConfig(
        provider="gemini", requests_per_day=1500, tokens_per_day=1_000_000,
        billing_enabled=False,
    ))
    gemini_t = _scripted(_http_ok_gemini())
    orch = _orchestrator([_gemini(gemini_t, clock=clock)], clock=clock, quota=ledger)
    result = await orch.complete(
        _request(budget=_budget(max_input=1_000_000, max_output=2048, max_bytes=2_000_000))
    )
    assert result.ok, (result.status, result.reason)


@pytest.mark.asyncio
async def test_r2_request_too_big_for_tier_skips_without_tripping_circuit(offsite_counter):
    """A request sized past a small tier's input cap skips it (no call, no failure)."""
    from signal_ingest.llm.circuit import CircuitState

    clock = AdvanceableClock()
    small = _limits("groq", model="groq-small").model_copy(update={"max_input_tokens": 50})
    tiny = _StubProvider(small, [])
    big = _StubProvider(_limits("gemini", model="g"), [_ok_result()])
    orch = _orchestrator([tiny, big], clock=clock)
    result = await orch.complete(_request(content="x" * 4000, budget=_budget(max_input=5000)))
    assert result.ok and result.provider == "gemini"
    assert tiny.calls == 0
    breaker = orch.circuits.get("groq")
    assert breaker.state is CircuitState.CLOSED and breaker.consecutive_failures == 0


@pytest.mark.asyncio
async def test_r2_half_open_probe_released_after_non_health_outcome(offsite_counter):
    """A half-open probe ending in invalid output must not leave the circuit blocked."""
    from signal_ingest.llm.errors import InvalidOutputError

    clock = AdvanceableClock()
    stub = _StubProvider(
        _limits("gemini", model="g"),
        [InvalidOutputError("bad json"), InvalidOutputError("bad json")],
    )
    orch = _orchestrator([stub], clock=clock, cooldown_s=10.0)
    breaker = orch.circuits.get("gemini")
    breaker.force_open(open_for_s=10.0)
    clock.advance(11)
    result = await orch.complete(_request())
    assert not result.ok
    assert breaker.allow() is True, "probe flag stuck: provider blocked for process lifetime"


def test_r2_paid_billing_blocked_when_cap_zero():
    """billing_enabled=true with paid_spend_cap=0 is denied even at estimate 0."""
    quota_mod = b5_api.load_quota()
    errors = b5_api.load_errors()
    ledger = quota_mod.QuotaLedger(paid_spend_cap=0, clock=AdvanceableClock())
    ledger.configure(quota_mod.ProviderQuotaConfig(provider="gemini", billing_enabled=True))
    with pytest.raises(errors.PaidSpendBlockedError):
        ledger.reserve_sync("gemini", requests=1, input_tokens=10, output_tokens=10)


def test_r2_providers_yaml_inline_comment_and_zero_limit(tmp_path: Path):
    from signal_ingest.llm.providers_config import (
        load_providers_config,
        provider_limits_from_config,
        quota_ledger_from_config,
    )

    cfg = tmp_path / "providers.yaml"
    cfg.write_text(
        "defaults:\n  paid_spend_cap: 0\nproviders:\n  groq:\n"
        "    model: llama  # pick at B8\n    enabled: false # off until keys\n"
        "    context_tokens: 32768\n    reserved_output_tokens: 2048\n"
        "    max_input_tokens: 30000\n    max_request_bytes: 500000\n"
        "    requests_per_day: 0\n    billing_enabled: false\n",
        encoding="utf-8",
    )
    assert load_providers_config(cfg)["providers"]["groq"]["model"] == "llama"
    assert provider_limits_from_config("groq", path=cfg).enabled is False
    ledger = quota_ledger_from_config(cfg)
    assert ledger.providers["groq"].requests_per_day == 0


@pytest.mark.asyncio
async def test_r2_extract_runs_through_fallback_chain(offsite_counter):
    """extract() on a ChainProvider: Gemini 429 → Groq grounded answer is accepted."""
    from signal_ingest.llm import ExtractCandidate, extract, html_to_segments
    from signal_ingest.llm.fallback import ChainProvider
    from signal_ingest.llm.types import FieldDecision

    html = "<html><body><h1>Acme Cloud</h1><p>Acme Cloud costs $29 per month.</p></body></html>"
    seg = next(s for s in html_to_segments(html, entity_hints=("Acme Cloud",)) if "$29" in s.text)
    payload = {"fields": [{"field": "price", "value": "$29",
                           "quote": "Acme Cloud costs $29 per month.",
                           "segment_ids": [seg.segment_id]}]}
    clock = AdvanceableClock()
    gemini_t = _scripted(_http_status(429, body="{}", headers={"Retry-After": "120"}))
    groq_t = _scripted(_http_ok_groq(json.dumps(payload)))
    orch = _orchestrator([_gemini(gemini_t, clock=clock), _groq(groq_t, clock=clock)],
                         clock=clock)
    chain = ChainProvider(orch)
    assert chain.limits.max_input_tokens == min(
        p.limits.max_input_tokens for p in orch.providers
    )
    cand = ExtractCandidate(candidate_id="r2", record_type="PRODUCT", html=html,
                            target_entity="Acme Cloud", fields_requested=("price",))
    result = await extract(cand, chain)
    assert result.fields["price"].decision is FieldDecision.ACCEPTED, result.fields
    assert len(groq_t.calls) == 1


@pytest.mark.asyncio
async def test_r2_fenced_json_parsed_and_truncated_output_falls_back(offsite_counter):
    """```json fences parse; a MAX_TOKENS-truncated answer is invalid → next tier."""
    from signal_ingest.llm.http_transport import HttpResponse

    def gemini_body(text: str, finish: str) -> Any:
        body = {"candidates": [{"content": {"parts": [{"text": text}]}, "finishReason": finish}],
                "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 5}}
        return HttpResponse(status_code=200, headers={"content-type": "application/json"},
                            content=json.dumps(body).encode())

    clock = AdvanceableClock()
    fenced = _scripted(gemini_body('Sure!\n```json\n{"productName":"Fenced"}\n```', "STOP"))
    orch = _orchestrator([_gemini(fenced, clock=clock)], clock=clock)
    ok = await orch.complete(_request())
    assert ok.ok and ok.result.parsed == {"productName": "Fenced"}

    cut = _scripted(gemini_body('{"productName":"Acm', "MAX_TOKENS"),
                    gemini_body('{"productName":"Acm', "MAX_TOKENS"))
    groq_t = _scripted(_http_ok_groq('{"productName":"Whole"}'))
    orch2 = _orchestrator([_gemini(cut, clock=clock), _groq(groq_t, clock=clock)], clock=clock)
    res = await orch2.complete(_request())
    assert res.ok and res.provider == "groq"


@pytest.mark.asyncio
async def test_r2_pg_quota_shared_across_processes(tmp_fixture_env):
    """Two ledgers (two worker processes) share one Postgres daily window: no double spend."""
    import uuid

    from signal_ingest.config import Mode, load_settings
    from signal_ingest.llm.errors import QuotaExhaustedError
    from signal_ingest.llm.quota import PgQuotaLedger, ProviderQuotaConfig
    from signal_ingest.storage import connect, init_db

    settings = load_settings(mode=Mode.FIXTURE)
    init_db(settings)
    name = f"r2-prov-{uuid.uuid4().hex[:8]}"

    def ledger():
        led = PgQuotaLedger(paid_spend_cap=0, clock=AdvanceableClock(),
                            connect=lambda: connect(settings))
        led.configure(ProviderQuotaConfig(provider=name, requests_per_day=1,
                                          tokens_per_day=1000, billing_enabled=False))
        return led

    a, b = ledger(), ledger()
    held = await a.reserve(name, requests=1, input_tokens=10, output_tokens=10)
    with pytest.raises(QuotaExhaustedError):
        await b.reserve(name, requests=1, input_tokens=10, output_tokens=10)
    await a.release(held)
    again = await b.reserve(name, requests=1, input_tokens=10, output_tokens=10)
    await b.commit(again, input_tokens=7, output_tokens=3)
    assert a.remaining_requests(name) == 0
    with connect(settings) as conn:
        row = conn.execute(
            "SELECT requests_used, tokens_used, requests_reserved FROM provider_budgets "
            "WHERE provider = %s", (name,)
        ).fetchone()
    assert tuple(row) == (1, 10, 0)


@pytest.mark.asyncio
async def test_r2_build_chain_from_config_uses_enabled_tiers(tmp_path: Path, monkeypatch):
    from signal_ingest.llm.providers_config import build_chain

    cfg = tmp_path / "providers.yaml"
    tier = ("    context_tokens: 32768\n    reserved_output_tokens: 2048\n"
            "    max_input_tokens: 30000\n    max_request_bytes: 500000\n"
            "    billing_enabled: false\n")
    cfg.write_text(
        "defaults:\n  paid_spend_cap: 0\nproviders:\n"
        "  gemini:\n    model: g\n    enabled: false\n    api_key_env: GEMINI_API_KEY\n" + tier +
        "  groq:\n    model: llama-test\n    enabled: true\n    api_key_env: R2_GROQ_KEY\n" + tier,
        encoding="utf-8",
    )
    monkeypatch.setenv("R2_GROQ_KEY", "test-groq-key")
    transport = _scripted(_http_ok_groq('{"productName":"FromConfig"}'))
    chain = build_chain(path=cfg, transport=transport)
    assert chain.limits.model == "groq"
    result = await chain.complete(_request(provider="chain", model="groq"))
    assert result.parsed == {"productName": "FromConfig"}
    assert chain.last.provider == "groq"


@pytest.mark.asyncio
async def test_r2_chain_reports_oversize_even_when_a_later_tier_is_skipped(offsite_counter):
    """413 on tier 1 then circuit-open tier 2 still maps to PayloadTooLarge (re-plan)."""
    from signal_ingest.llm.fallback import ChainProvider
    from signal_ingest.llm.provider import PayloadTooLarge

    clock = AdvanceableClock()
    first = _StubProvider(_limits("gemini", model="g"),
                          [PayloadTooLarge("413", serialized_bytes=10, input_tokens=10)])
    second = _StubProvider(_limits("groq", model="q"), [])
    orch = _orchestrator([first, second], clock=clock)
    orch.circuits.get("groq").force_open(open_for_s=60)
    with pytest.raises(PayloadTooLarge):
        await ChainProvider(orch).complete(_request())
