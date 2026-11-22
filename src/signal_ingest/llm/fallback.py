"""Ordered Gemini → Groq → Ollama fallback with quota, circuits, and transcripts."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from signal_ingest.config.clock import Clock, SystemClock
from signal_ingest.llm.adapters.base import is_live_provider
from signal_ingest.llm.circuit import CircuitBreaker, CircuitBreakerRegistry, CircuitState
from signal_ingest.llm.errors import (
    AuthError,
    CircuitOpenError,
    InvalidOutputError,
    MissingCredentialsError,
    PaidSpendBlockedError,
    ProviderDisabledError,
    QuotaExhaustedError,
    QuotaForbiddenError,
    RateLimitedError,
    TransientProviderError,
)
from signal_ingest.llm.provider import (
    ContextLengthExceeded,
    PayloadTooLarge,
    Provider,
    ProviderError,
    ProviderLimits,
    ProviderRequest,
    ProviderResult,
    measure_provider_request,
)
from signal_ingest.llm.quota import QuotaLedger, QuotaReservation
from signal_ingest.llm.retry import resolve_backoff_delay
from signal_ingest.llm.transcript import TranscriptRecorder

ShrinkFn = Callable[[ProviderRequest, ProviderError], ProviderRequest | None]
CacheLookup = Callable[[str], Awaitable[ProviderResult | None] | ProviderResult | None]
CacheStore = Callable[[str, ProviderResult], Awaitable[None] | None]


@dataclass
class FallbackResult:
    result: ProviderResult | None
    status: str
    provider: str | None = None
    model: str | None = None
    attempts: int = 0
    transcript: TranscriptRecorder | None = None
    next_attempt_at: datetime | None = None
    reason: str | None = None
    cache_hit: bool = False
    live_qualified: bool = False
    oversize: bool = False  # some tier rejected the payload as too large

    @property
    def ok(self) -> bool:
        return self.status == "ok" and self.result is not None


@dataclass
class FallbackOrchestrator:
    """Multi-tier provider orchestration with zero-spend defaults.

    Features:
    - Ordered provider chain (typically Gemini → Groq → Ollama)
    - Reserve-before-dispatch via QuotaLedger (paid_spend_cap=0)
    - Circuit breaker with one half-open probe
    - Retry-After (seconds|HTTP-date) never shortened
    - Full jitter without server guidance (injectable RNG + clock)
    - In-flight dedupe by work_key
    - Redacted transcripts; Fake never counts as live
    """

    providers: Sequence[Provider]
    quota: QuotaLedger
    circuits: CircuitBreakerRegistry | None = None
    clock: Clock = field(default_factory=SystemClock)
    rng: random.Random = field(default_factory=random.Random)
    max_attempts: int = 8
    max_wall_clock_s: float = 60.0
    retry_base_s: float = 0.25
    retry_cap_s: float = 8.0
    local_retry_budget_s: float = 15.0
    invalid_output_retries: int = 1
    allow_fake_in_chain: bool = True
    require_live_qualification: bool = False
    transcript: TranscriptRecorder | None = None
    _inflight: dict[str, asyncio.Future[FallbackResult]] = field(
        default_factory=dict, repr=False
    )

    def __post_init__(self) -> None:
        if self.circuits is None:
            self.circuits = CircuitBreakerRegistry(clock=self.clock)

    def _breaker(self, provider: str) -> CircuitBreaker:
        assert self.circuits is not None
        return self.circuits.get(provider)

    async def _wait(self, seconds: float) -> None:
        """Back off for real; test clocks with advance()/set() move virtually instead."""
        if seconds <= 0:
            return
        if hasattr(self.clock, "advance"):
            self.clock.advance(seconds)  # type: ignore[attr-defined]
        elif hasattr(self.clock, "set"):
            self.clock.set(self.clock.now() + timedelta(seconds=seconds))  # type: ignore[attr-defined]
        else:
            await asyncio.sleep(seconds)

    def _eligible(self, provider: Provider) -> tuple[bool, str | None]:
        name = provider.limits.provider
        if not provider.limits.enabled:
            return False, "disabled"
        if not self.allow_fake_in_chain and not is_live_provider(name):
            return False, "fake_excluded"
        if self.require_live_qualification and not is_live_provider(name):
            return False, "not_live"
        return True, None

    async def complete(
        self,
        request: ProviderRequest,
        *,
        work_key: str | None = None,
        cancel_event: asyncio.Event | None = None,
        shrink: ShrinkFn | None = None,
        cache_lookup: CacheLookup | None = None,
        cache_store: CacheStore | None = None,
        fallback_reason: str | None = None,
        estimated_paid_micros: int = 0,
    ) -> FallbackResult:
        """Try providers in order until success or exhaustion."""
        if work_key:
            existing = self._inflight.get(work_key)
            if existing is not None and not existing.done():
                return await asyncio.shield(existing)
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[FallbackResult] = loop.create_future()
            self._inflight[work_key] = fut
            try:
                result = await self._complete_inner(
                    request,
                    work_key=work_key,
                    cancel_event=cancel_event,
                    shrink=shrink,
                    cache_lookup=cache_lookup,
                    cache_store=cache_store,
                    fallback_reason=fallback_reason,
                    estimated_paid_micros=estimated_paid_micros,
                )
                if not fut.done():
                    fut.set_result(result)
                return result
            except BaseException as exc:
                if not fut.done():
                    fut.set_exception(exc)
                raise
            finally:
                self._inflight.pop(work_key, None)

        return await self._complete_inner(
            request,
            work_key=work_key,
            cancel_event=cancel_event,
            shrink=shrink,
            cache_lookup=cache_lookup,
            cache_store=cache_store,
            fallback_reason=fallback_reason,
            estimated_paid_micros=estimated_paid_micros,
        )

    async def _complete_inner(
        self,
        request: ProviderRequest,
        *,
        work_key: str | None,
        cancel_event: asyncio.Event | None,
        shrink: ShrinkFn | None,
        cache_lookup: CacheLookup | None,
        cache_store: CacheStore | None,
        fallback_reason: str | None,
        estimated_paid_micros: int,
    ) -> FallbackResult:
        recorder = self.transcript or TranscriptRecorder(work_key=work_key)
        if work_key:
            recorder.work_key = work_key
        started = self.clock.now()
        deadline = started + timedelta(seconds=self.max_wall_clock_s)
        attempts = 0
        next_attempt_at: datetime | None = None
        last_reason: str | None = None
        current = request
        invalid_retries_left = self.invalid_output_retries
        oversize_seen = False

        if work_key and cache_lookup is not None:
            cached = cache_lookup(work_key)
            if asyncio.iscoroutine(cached) or asyncio.isfuture(cached):
                cached = await cached  # type: ignore[misc]
            if cached is not None:
                rec = recorder.begin(
                    provider=cached.raw.get("provider", "cache")
                    if isinstance(cached.raw, dict)
                    else "cache",
                    model=request.model,
                    started_at=self.clock.now(),
                    live_qualified=False,
                    fallback_reason="cache_hit",
                )
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="cache_hit",
                    cache_hit=True,
                    input_tokens=cached.input_tokens,
                    output_tokens=cached.output_tokens,
                    request_bytes=cached.request_bytes,
                    response={"finish_reason": cached.finish_reason},
                )
                return FallbackResult(
                    result=cached,
                    status="ok",
                    provider=str(
                        cached.raw.get("provider")
                        if isinstance(cached.raw, dict)
                        else request.provider
                    ),
                    model=request.model,
                    attempts=0,
                    transcript=recorder,
                    cache_hit=True,
                    live_qualified=False,
                    reason="cache_hit",
                )

        providers = list(self.providers)
        provider_index = 0
        per_provider_attempt: dict[str, int] = {}

        while attempts < self.max_attempts and provider_index < len(providers):
            if cancel_event is not None and cancel_event.is_set():
                return FallbackResult(
                    result=None,
                    status="cancelled",
                    attempts=attempts,
                    transcript=recorder,
                    reason="cancelled",
                )
            now = self.clock.now()
            if now >= deadline:
                last_reason = "wall_clock_budget"
                break

            provider = providers[provider_index]
            name = provider.limits.provider
            model = provider.limits.model
            ok, skip_reason = self._eligible(provider)
            if not ok:
                last_reason = skip_reason
                provider_index += 1
                continue

            breaker = self._breaker(name)
            try:
                breaker.allow()
            except CircuitOpenError as exc:
                last_reason = "circuit_open"
                rec = recorder.begin(
                    provider=name,
                    model=model,
                    started_at=now,
                    live_qualified=is_live_provider(name),
                    fallback_reason="circuit_open",
                )
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="circuit_open",
                    error=exc,
                )
                provider_index += 1
                continue

            # Recount request for this provider's identity, capped to its envelope.
            limits = provider.limits
            routed = current.model_copy(
                update={
                    "provider": name,
                    "model": model,
                    "budget": current.budget.model_copy(
                        update={
                            "max_input_tokens": min(
                                current.budget.max_input_tokens, limits.max_input_tokens
                            ),
                            "max_output_tokens": min(
                                current.budget.max_output_tokens,
                                limits.reserved_output_tokens,
                            ),
                            "max_request_bytes": min(
                                current.budget.max_request_bytes, limits.max_request_bytes
                            ),
                        }
                    ),
                }
            )
            measured = measure_provider_request(routed)
            if (
                measured.input_tokens > limits.max_input_tokens
                or measured.request_bytes > limits.max_request_bytes
            ):
                # Too big for this tier: not a provider failure, try a larger tier.
                breaker.release_probe()
                last_reason = "too_large_for_tier"
                oversize_seen = True
                provider_index += 1
                continue
            reservation: QuotaReservation | None = None
            attempt_no = per_provider_attempt.get(name, 0)
            rec = recorder.begin(
                provider=name,
                model=model,
                started_at=self.clock.now(),
                request={
                    "provider": name,
                    "model": model,
                    "budget": routed.budget.model_dump(mode="json"),
                    "messages": routed.messages,
                    "metadata": routed.metadata,
                },
                live_qualified=is_live_provider(name),
                fallback_reason=fallback_reason or last_reason,
            )

            try:
                reservation = await self.quota.reserve(
                    name,
                    requests=1,
                    input_tokens=measured.input_tokens,
                    output_tokens=routed.budget.max_output_tokens,
                    estimated_paid_micros=estimated_paid_micros,
                )
            except (QuotaExhaustedError, PaidSpendBlockedError) as exc:
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="quota_denied",
                    error=exc,
                )
                last_reason = type(exc).__name__
                # Our own ledger said no; the provider itself is healthy.
                breaker.release_probe()
                provider_index += 1
                continue
            except BaseException as exc:
                # Ledger outage (e.g. DB down) or cancel: never strand the probe.
                breaker.release_probe()
                recorder.finish(
                    rec, finished_at=self.clock.now(), status="quota_error", error=exc
                )
                if not isinstance(exc, Exception):
                    raise
                last_reason = "quota_error"
                provider_index += 1
                continue

            attempts += 1
            per_provider_attempt[name] = attempt_no + 1

            try:
                if cancel_event is not None and cancel_event.is_set():
                    await self.quota.release(reservation)
                    breaker.release_probe()
                    recorder.finish(
                        rec,
                        finished_at=self.clock.now(),
                        status="cancelled",
                    )
                    return FallbackResult(
                        result=None,
                        status="cancelled",
                        attempts=attempts,
                        transcript=recorder,
                        reason="cancelled",
                    )

                result = await provider.complete(routed)
                try:
                    await self.quota.commit(
                        reservation,
                        input_tokens=result.input_tokens,
                        output_tokens=result.output_tokens,
                        paid_micros=0,
                    )
                except Exception:
                    # Bookkeeping failed but the answer is real: keep it rather than
                    # paying a second tier for the same request.
                    pass
                reservation = None
                breaker.record_success()
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="ok",
                    input_tokens=result.input_tokens,
                    output_tokens=result.output_tokens,
                    request_bytes=result.request_bytes,
                    response={
                        "finish_reason": result.finish_reason,
                        "parsed_keys": list((result.parsed or {}).keys()),
                    },
                )
                if work_key and cache_store is not None:
                    maybe = cache_store(work_key, result)
                    if asyncio.iscoroutine(maybe) or asyncio.isfuture(maybe):
                        await maybe  # type: ignore[misc]
                return FallbackResult(
                    result=result,
                    status="ok",
                    provider=name,
                    model=model,
                    attempts=attempts,
                    transcript=recorder,
                    live_qualified=is_live_provider(name),
                )

            except asyncio.CancelledError:
                breaker.release_probe()
                if reservation is not None:
                    await self.quota.release(reservation)
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="cancelled",
                )
                raise

            except (
                MissingCredentialsError,
                ProviderDisabledError,
                AuthError,
            ) as exc:
                if reservation is not None:
                    await self.quota.release(reservation)
                if isinstance(exc, AuthError):
                    breaker.force_open(open_for_s=self.max_wall_clock_s)
                else:
                    breaker.release_probe()
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="auth_error" if isinstance(exc, AuthError) else "disabled",
                    status_code=exc.status_code,
                    error=exc,
                )
                last_reason = type(exc).__name__
                provider_index += 1
                continue

            except (PayloadTooLarge, ContextLengthExceeded) as exc:
                if reservation is not None:
                    await self.quota.release(reservation)
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="oversize",
                    status_code=exc.status_code,
                    error=exc,
                    request_bytes=getattr(exc, "serialized_bytes", None),
                    input_tokens=getattr(exc, "input_tokens", None),
                )
                last_reason = type(exc).__name__
                oversize_seen = True
                breaker.release_probe()
                if shrink is not None:
                    shrunk = shrink(routed, exc)
                    if shrunk is not None:
                        current = shrunk
                        # Stay on same provider with smaller payload.
                        continue
                # Fall through to next provider with same (or last) request.
                provider_index += 1
                continue

            except InvalidOutputError as exc:
                if reservation is not None:
                    await self.quota.release(reservation)
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="invalid_output",
                    error=exc,
                )
                last_reason = "InvalidOutputError"
                breaker.release_probe()
                if invalid_retries_left > 0:
                    invalid_retries_left -= 1
                    continue
                invalid_retries_left = self.invalid_output_retries
                provider_index += 1
                continue

            except (RateLimitedError, QuotaForbiddenError) as exc:
                if reservation is not None:
                    await self.quota.release(reservation)
                retry_after = exc.retry_after_s
                delay, exceeds = resolve_backoff_delay(
                    attempt=attempt_no,
                    retry_after_s=retry_after,
                    base_s=self.retry_base_s,
                    cap_s=self.retry_cap_s,
                    rng=self.rng,
                    local_budget_s=self.local_retry_budget_s,
                )
                # Never shorten server cooldown: open circuit at least until then.
                open_for = delay if retry_after is not None else max(delay, self.retry_base_s)
                breaker.record_failure(open_for_s=open_for)
                next_at = self.clock.now() + timedelta(seconds=delay)
                next_attempt_at = (
                    next_at
                    if next_attempt_at is None
                    else max(next_attempt_at, next_at)
                )
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="rate_limited",
                    status_code=exc.status_code,
                    error=exc,
                    retry_after_s=retry_after if retry_after is not None else delay,
                )
                last_reason = type(exc).__name__
                if exceeds or (retry_after is not None and retry_after > self.local_retry_budget_s):
                    # Leave the provider parked for the whole server cooldown, so later
                    # requests skip it too instead of hitting it again immediately.
                    if retry_after is not None:
                        breaker.force_open(open_for_s=retry_after)
                    provider_index += 1
                    continue
                await self._wait(delay)
                # Stay on provider for another attempt within budget.
                continue

            except TransientProviderError as exc:
                if reservation is not None:
                    await self.quota.release(reservation)
                delay, exceeds = resolve_backoff_delay(
                    attempt=attempt_no,
                    retry_after_s=None,
                    base_s=self.retry_base_s,
                    cap_s=self.retry_cap_s,
                    rng=self.rng,
                    local_budget_s=self.local_retry_budget_s,
                )
                breaker.record_failure()
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="transient",
                    status_code=exc.status_code,
                    error=exc,
                    retry_after_s=delay,
                )
                last_reason = "TransientProviderError"
                if exceeds or not exc.retryable:
                    provider_index += 1
                    continue
                await self._wait(delay)
                continue

            except ProviderError as exc:
                if reservation is not None:
                    await self.quota.release(reservation)
                breaker.record_failure()
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="provider_error",
                    status_code=exc.status_code,
                    error=exc,
                )
                last_reason = type(exc).__name__
                provider_index += 1
                continue

            except Exception as exc:
                if reservation is not None:
                    await self.quota.release(reservation)
                breaker.record_failure()
                recorder.finish(
                    rec,
                    finished_at=self.clock.now(),
                    status="error",
                    error=exc,
                )
                last_reason = type(exc).__name__
                provider_index += 1
                continue

        # All providers failed / skipped.
        open_all = False
        if self.circuits is not None and self.circuits._breakers:
            open_all = all(
                b.state is CircuitState.OPEN for b in self.circuits._breakers.values()
            )
            if open_all:
                last_reason = last_reason or "all_circuits_open"

        return FallbackResult(
            result=None,
            status="abstain",
            attempts=attempts,
            transcript=recorder,
            next_attempt_at=next_attempt_at,
            reason=last_reason or "all_providers_failed",
            oversize=oversize_seen,
        )


class ChainProvider:
    """Expose a FallbackOrchestrator as a single Provider for extract().

    Limits are the tightest of the enabled tiers, so every planned chunk fits
    every tier and a fallback never meets a payload its context cannot take.
    """

    def __init__(self, orchestrator: FallbackOrchestrator) -> None:
        self.orchestrator = orchestrator
        tiers = [p.limits for p in orchestrator.providers if p.limits.enabled]
        if not tiers:
            raise ValueError("ChainProvider needs at least one enabled provider")
        self._limits = ProviderLimits(
            provider="chain",
            model=">".join(t.provider for t in tiers),
            context_tokens=min(t.context_tokens for t in tiers),
            reserved_output_tokens=min(t.reserved_output_tokens for t in tiers),
            max_input_tokens=min(t.max_input_tokens for t in tiers),
            max_request_bytes=min(t.max_request_bytes for t in tiers),
            safety_margin_tokens=max(t.safety_margin_tokens for t in tiers),
            framing_tokens=max(t.framing_tokens for t in tiers),
        )
        self.last: FallbackResult | None = None

    @property
    def limits(self) -> ProviderLimits:
        return self._limits

    async def complete(self, request: ProviderRequest) -> ProviderResult:
        out = await self.orchestrator.complete(request)
        self.last = out
        if out.ok and out.result is not None:
            return out.result.model_copy(
                update={"raw": {**out.result.raw, "provider": out.provider, "model": out.model}}
            )
        if out.oversize:
            measured = measure_provider_request(request)
            # Let extract() re-plan smaller chunks instead of giving up.
            raise PayloadTooLarge(
                f"every tier rejected the payload ({out.reason})",
                serialized_bytes=measured.request_bytes,
                input_tokens=measured.input_tokens,
            )
        from signal_ingest.llm.errors import AllProvidersFailedError

        raise AllProvidersFailedError(
            f"fallback chain abstained: {out.reason}", next_attempt_at=out.next_attempt_at
        )


def default_chain_names() -> tuple[str, ...]:
    return ("gemini", "groq", "ollama")
