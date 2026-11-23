"""LiteLLM Router chain (Gemini > Groq > Ollama) with per-attempt quota and zero spend."""

from __future__ import annotations

import copy
import json
import os
import sys
import logging
import re
from collections.abc import Mapping, Sequence
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# No network at import: LiteLLM must read its bundled cost map, never fetch it.
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")
# litellm's DEV mode runs dotenv.load_dotenv() at import; keys must come only from the caller's env.
os.environ["LITELLM_MODE"] = "PRODUCTION"

# LiteLLM's own handlers print INFO lines to stdout, which corrupts the CLI's JSON output.
os.environ.setdefault("LITELLM_LOG", "WARNING")

import litellm  # noqa: E402
from litellm.integrations.custom_logger import CustomLogger  # noqa: E402

litellm.suppress_debug_info = True
for _name in ("LiteLLM", "LiteLLM Router", "LiteLLM Proxy"):
    _lg = logging.getLogger(_name)
    _lg.setLevel(logging.WARNING)
    for _h in list(_lg.handlers):
        if isinstance(_h, logging.StreamHandler) and getattr(_h, "stream", None) is sys.stdout:
            _h.setStream(sys.stderr)
from litellm.utils import _get_retry_after_from_exception_header  # noqa: E402

from signal_ingest.config.clock import SystemClock  # noqa: E402
from signal_ingest.llm.errors import (  # noqa: E402
    AllProvidersFailedError,
    MissingCredentialsError,
    PaidSpendBlockedError,
    QuotaExhaustedError,
)
from signal_ingest.llm.provider import (  # noqa: E402
    MeasuredRequest,
    PayloadTooLarge,
    ProviderLimits,
    ProviderRequest,
    ProviderResult,
    measure_provider_request,
)
from signal_ingest.llm.quota import QuotaLedger, QuotaReservation  # noqa: E402
from signal_ingest.llm.schema import EXTRACTION_JSON_SCHEMA  # noqa: E402
from signal_ingest.llm.transcript import redact_text  # noqa: E402

litellm.telemetry = False

_TOO_LARGE_RE = re.compile(
    r"context.{0,20}(length|window|limit)|too large|too long|maximum.{0,20}tokens", re.I
)


@dataclass(frozen=True)
class Tier:
    name: str
    litellm_model: str
    limits: ProviderLimits
    api_key_env: str | None = None
    api_base: str | None = None
    timeout_s: float = 30.0
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Attempt:
    """One real dispatch (or pre-dispatch refusal), redacted: no keys, no headers."""

    tier: str
    status: str
    status_code: int | None
    retry_after_s: float | None
    input_tokens: int
    output_tokens: int


def classify(exc: BaseException) -> str:
    """Map a LiteLLM / ledger exception to an Attempt.status."""
    if isinstance(exc, litellm.ContextWindowExceededError):
        return "too_large"
    status = getattr(exc, "status_code", None)
    if status == 413:
        return "too_large"
    if isinstance(exc, litellm.BadRequestError) and _TOO_LARGE_RE.search(str(exc)):
        return "too_large"
    if isinstance(exc, PaidSpendBlockedError):
        return "paid_blocked"
    if isinstance(exc, QuotaExhaustedError):
        return "quota_denied"
    if isinstance(exc, litellm.RateLimitError) or status == 429:
        return "rate_limited"
    if isinstance(exc, (litellm.AuthenticationError, litellm.PermissionDeniedError)) or status in (
        401,
        403,
    ):
        return "auth"
    return "error"


def schema_for(tier: Tier) -> dict[str, Any]:
    """Gemini rejects `additionalProperties` and an untyped `{}`; others get the strict schema."""
    if tier.name != "gemini" and not tier.litellm_model.startswith("gemini/"):
        return EXTRACTION_JSON_SCHEMA
    relaxed = copy.deepcopy(EXTRACTION_JSON_SCHEMA)

    def _strip(node: Any) -> None:
        if isinstance(node, dict):
            node.pop("additionalProperties", None)
            for value in node.values():
                _strip(value)
        elif isinstance(node, list):
            for value in node:
                _strip(value)

    _strip(relaxed)
    relaxed["properties"]["fields"]["items"]["properties"]["value"] = {
        "type": ["string", "number", "boolean", "null"]
    }
    return relaxed


def _response_format(tier: Tier) -> dict[str, Any]:
    return {"type": "json_schema", "json_schema": {"name": "extraction", "schema": schema_for(tier)}}


_FENCE = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.S | re.I)


def _json_text(content: str | None) -> str | None:
    """JSON object text, tolerating one ```json fence or prose around one object (as the old adapters did)."""
    text = (content or "").strip()
    fenced = _FENCE.match(text)
    start, end = text.find("{"), text.rfind("}")
    candidates = [text, fenced.group(1) if fenced else None]
    if 0 <= start < end:
        candidates.append(text[start : end + 1])
    for candidate in candidates:
        if not candidate:
            continue
        try:
            if isinstance(json.loads(candidate), dict):
                return candidate
        except ValueError:
            continue
    return None


def _retry_after_s(exc: BaseException) -> float | None:
    headers = getattr(exc, "litellm_response_headers", None)
    if headers is None:
        response = getattr(exc, "response", None)
        headers = getattr(response, "headers", None)
    if headers is None:
        return None
    seconds = _get_retry_after_from_exception_header(headers)
    return float(seconds) if seconds is not None and seconds >= 0 else None


@dataclass
class _Dispatch:
    attempt: Attempt
    reservation: QuotaReservation | None


@dataclass
class _CallState:
    chain: LiteLLMChain
    measured: MeasuredRequest
    max_output: int
    messages: list[dict[str, Any]]
    dispatches: list[_Dispatch] = field(default_factory=list)
    max_retry_after_s: float | None = None

    def pending(self, tier: str) -> _Dispatch | None:
        for d in reversed(self.dispatches):
            if d.attempt.tier == tier and d.attempt.status == "pending":
                return d
        return None


# One complete() call's state; the hook is a no-op outside it, so chains never leak.
_CALL: ContextVar[_CallState | None] = ContextVar("signal_ingest_llm_call", default=None)


def _deployment_id(params: Mapping[str, Any] | None) -> str | None:
    info = (params or {}).get("model_info") or {}
    return info.get("id") if isinstance(info, Mapping) else None


class _QuotaHook(CustomLogger):
    """Reserve quota before every real Router dispatch; record each outcome."""

    def _tier(self, deployment_id: str | None) -> tuple[_CallState, Tier] | None:
        state = _CALL.get()
        if state is None or deployment_id is None:
            return None
        tier = state.chain._by_id.get(deployment_id)
        return (state, tier) if tier is not None else None

    async def async_pre_call_check(self, deployment: dict, parent_otel_span: Any) -> dict | None:
        found = self._tier(_deployment_id(deployment))
        if found is None:
            if _CALL.get() is not None:
                # Fail closed: no dispatch inside a chain call without a reservation.
                raise RuntimeError("deployment has no quota tier; refusing unreserved dispatch")
            return None
        state, tier = found
        needed = state.measured.input_tokens + state.max_output
        if needed > tier.limits.max_input_tokens:
            state.dispatches.append(
                _Dispatch(Attempt(tier.name, "too_large", None, None, 0, 0), None)
            )
            raise litellm.ContextWindowExceededError(
                message=f"request needs {needed} tokens; tier allows {tier.limits.max_input_tokens}",
                model=tier.litellm_model,
                llm_provider=tier.name,
            )
        try:
            reservation = await state.chain._quota.reserve(
                tier.name,
                requests=1,
                input_tokens=state.measured.input_tokens,
                output_tokens=state.max_output,
            )
        except QuotaExhaustedError as exc:
            state.dispatches.append(
                _Dispatch(Attempt(tier.name, classify(exc), None, None, 0, 0), None)
            )
            # RateLimitError makes the Router cool this tier and fall over.
            raise litellm.RateLimitError(
                message=redact_text(str(exc)) or "quota denied",
                llm_provider=tier.name,
                model=tier.litellm_model,
            ) from exc
        state.dispatches.append(
            _Dispatch(
                Attempt(tier.name, "pending", None, None, state.measured.input_tokens, 0),
                reservation,
            )
        )
        return None

    async def async_pre_call_deployment_hook(
        self, kwargs: dict[str, Any], call_type: Any
    ) -> dict | None:
        found = self._tier(_deployment_id(kwargs.get("litellm_params")) or _deployment_id(kwargs))
        if found is None:
            return None
        state, tier = found
        if kwargs.get("messages") != state.messages:
            # Grounded chunks must reach the model byte-for-byte (no compaction/rewrite).
            raise RuntimeError("router altered request messages before dispatch")
        return {**kwargs, "response_format": _response_format(tier)}

    async def async_log_failure_event(
        self, kwargs: Any, response_obj: Any, start_time: Any, end_time: Any
    ) -> None:
        params = kwargs.get("litellm_params") if isinstance(kwargs, Mapping) else None
        found = self._tier(_deployment_id(params))
        if found is None:
            return
        state, tier = found
        exc = kwargs.get("exception")
        dispatch = state.pending(tier.name)
        if dispatch is None or exc is None:
            return
        retry_after = _retry_after_s(exc)
        dispatch.attempt.status = classify(exc)
        dispatch.attempt.status_code = getattr(exc, "status_code", None)
        dispatch.attempt.retry_after_s = retry_after
        if retry_after is not None:
            state.max_retry_after_s = max(state.max_retry_after_s or 0.0, retry_after)
            if retry_after > state.chain.max_retry_after_s:
                # Never sleep that long: cool the tier so the Router falls over now.
                state.chain.router.cooldown_cache.add_deployment_to_cooldown(
                    model_id=_deployment_id(params),
                    original_exception=exc,
                    exception_status=429,
                    cooldown_time=max(state.chain.cooldown_time_s, retry_after),
                )


_HOOK = _QuotaHook()


def _ensure_hook() -> None:
    if not any(cb is _HOOK for cb in litellm.callbacks):
        litellm.callbacks.append(_HOOK)


class LiteLLMChain:
    """Provider over one litellm.Router: fallback, retries, cooldown, per-attempt quota."""

    def __init__(
        self,
        tiers: Sequence[Tier],
        quota: QuotaLedger,
        *,
        num_retries: int = 2,
        max_retry_after_s: float = 15.0,
        cooldown_time_s: float = 60.0,
        allowed_fails: int = 1,
        router: litellm.Router | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        env = os.environ if env is None else env
        self._quota = quota
        self.max_retry_after_s = max_retry_after_s
        self.cooldown_time_s = cooldown_time_s
        self.skipped: dict[str, str] = {}
        usable: list[Tier] = []
        keys: dict[str, str | None] = {}
        for tier in tiers:
            if not tier.limits.enabled:
                self.skipped[tier.name] = "disabled"
                continue
            key = env.get(tier.api_key_env) if tier.api_key_env else None
            if tier.api_key_env and not key:
                self.skipped[tier.name] = "missing_credentials"
                continue
            keys[tier.name] = key
            usable.append(tier)
        if not usable:
            if "missing_credentials" in self.skipped.values():
                raise MissingCredentialsError(
                    f"no tier has credentials: {sorted(self.skipped)}", provider="chain"
                )
            raise ValueError("LiteLLMChain needs at least one enabled tier")
        self._tiers = tuple(usable)
        names = [t.name for t in self._tiers]
        if router is None:
            model_list = []
            for tier in self._tiers:
                params: dict[str, Any] = {"model": tier.litellm_model, "timeout": tier.timeout_s}
                if keys[tier.name]:
                    params["api_key"] = keys[tier.name]
                if tier.api_base:
                    params["api_base"] = tier.api_base
                params.update(tier.extra)
                model_list.append({"model_name": tier.name, "litellm_params": params})
            router = litellm.Router(
                model_list=model_list,
                fallbacks=[{names[0]: names[1:]}],
                context_window_fallbacks=[{names[0]: names[1:]}],
                num_retries=num_retries,
                retry_after=0,
                allowed_fails=allowed_fails,
                cooldown_time=cooldown_time_s,
            )
        # Compaction needs no init check: the deployment hook rejects any rewritten messages.
        self.router = router
        self._by_id: dict[str, Tier] = {}
        by_name = {t.name: t for t in self._tiers}
        for deployment in router.model_list:
            name = deployment.get("model_name")
            tier = by_name.get(name)
            dep_id = _deployment_id(deployment)
            if tier is not None and dep_id is not None:
                self._by_id[dep_id] = tier
            elif name not in self.skipped:
                # A skipped tier's deployment is never routed to; anything else must be quota-mapped.
                raise ValueError(f"router deployment {name!r} is not mapped to a quota tier")
        self._limits = ProviderLimits(
            provider="chain",
            model=">".join(names),
            context_tokens=min(t.limits.context_tokens for t in self._tiers),
            reserved_output_tokens=min(t.limits.reserved_output_tokens for t in self._tiers),
            max_input_tokens=min(t.limits.max_input_tokens for t in self._tiers),
            max_request_bytes=min(t.limits.max_request_bytes for t in self._tiers),
            safety_margin_tokens=max(t.limits.safety_margin_tokens for t in self._tiers),
            framing_tokens=max(t.limits.framing_tokens for t in self._tiers),
        )
        self.last_attempts: list[Attempt] = []
        _ensure_hook()

    @property
    def limits(self) -> ProviderLimits:
        return self._limits

    @property
    def tiers(self) -> tuple[Tier, ...]:
        return self._tiers

    async def complete(self, request: ProviderRequest) -> ProviderResult:
        _ensure_hook()  # someone may have reset litellm.callbacks since __init__
        measured = measure_provider_request(request)
        state = _CallState(
            chain=self,
            measured=measured,
            max_output=request.budget.max_output_tokens,
            messages=request.messages,
        )
        token = _CALL.set(state)
        try:
            response, tier = await self._run(request, state)
        except BaseException:
            for d in state.dispatches:
                if d.attempt.status == "pending":
                    d.attempt.status = "error"
            await self._settle(state)
            raise
        finally:
            _CALL.reset(token)
            self.last_attempts = [d.attempt for d in state.dispatches]
        await self._settle(state)
        choice = response.choices[0]
        usage = getattr(response, "usage", None)
        return ProviderResult(
            text=_json_text(choice.message.content) or "",
            parsed=None,
            input_tokens=int(getattr(usage, "prompt_tokens", 0) or 0),
            output_tokens=int(getattr(usage, "completion_tokens", 0) or 0),
            request_bytes=measured.request_bytes,
            finish_reason=str(choice.finish_reason or "stop"),
            raw={
                "provider": tier.name,
                "model": tier.litellm_model,
                "attempts": [asdict(d.attempt) for d in state.dispatches],
            },
        )

    async def _settle(self, state: _CallState) -> None:
        """Commit dispatches that got a model answer; release every other reservation."""
        for d in state.dispatches:
            if d.reservation is None:
                continue
            if d.attempt.status in ("ok", "invalid_output"):
                await self._quota.commit(
                    d.reservation,
                    input_tokens=d.attempt.input_tokens,
                    output_tokens=d.attempt.output_tokens,
                )
            else:
                await self._quota.release(d.reservation)

    async def _ask(self, request: ProviderRequest, names: list[str], *, reask: bool) -> Any:
        rest = names[1:]
        kwargs: dict[str, Any] = {
            "model": names[0],
            "messages": request.messages,
            "response_format": _response_format(self._tiers[0]),  # per-tier copy set in the hook
            "max_tokens": request.budget.max_output_tokens,
        }
        if reask or not rest:
            kwargs["disable_fallbacks"] = True
        else:
            kwargs["fallbacks"] = [{names[0]: rest}]
            kwargs["context_window_fallbacks"] = [{names[0]: rest}]
        return await self.router.acompletion(**kwargs)

    def _answered(self, response: Any, state: _CallState) -> tuple[Tier, _Dispatch | None]:
        hidden = getattr(response, "_hidden_params", None) or {}
        tier = self._by_id.get(hidden.get("model_id") or "")
        if tier is None:
            pending = [d for d in state.dispatches if d.attempt.status == "pending"]
            if not pending:
                raise AllProvidersFailedError("router answered from an unknown deployment")
            tier = next(t for t in self._tiers if t.name == pending[-1].attempt.tier)
        dispatch = state.pending(tier.name)
        if dispatch is not None:
            usage = getattr(response, "usage", None)
            dispatch.attempt.input_tokens = int(
                getattr(usage, "prompt_tokens", 0) or dispatch.attempt.input_tokens
            )
            dispatch.attempt.output_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
        return tier, dispatch

    @staticmethod
    def _usable(response: Any) -> bool:
        choice = response.choices[0]
        # Truncated output is invalid even if it happens to parse.
        return choice.finish_reason != "length" and _json_text(choice.message.content) is not None

    async def _run(self, request: ProviderRequest, state: _CallState) -> tuple[Any, Tier]:
        names = [t.name for t in self._tiers]
        start = 0
        last_exc: BaseException | None = None
        while start < len(names):
            try:
                response = await self._ask(request, names[start:], reask=False)
            except Exception as exc:
                last_exc = exc
                break
            tier, dispatch = self._answered(response, state)
            for reask in (False, True):
                if reask:
                    try:
                        response = await self._ask(request, [tier.name], reask=True)
                    except Exception as exc:
                        last_exc = exc
                        break
                    tier, dispatch = self._answered(response, state)
                ok = self._usable(response)
                if dispatch is not None:
                    dispatch.attempt.status = "ok" if ok else "invalid_output"
                if ok:
                    return response, tier
            start = names.index(tier.name) + 1
        raise self._failure(state, last_exc)

    def _failure(self, state: _CallState, exc: BaseException | None) -> Exception:
        for d in state.dispatches:
            if d.attempt.status == "pending":
                d.attempt.status = classify(exc) if exc is not None else "error"
        # Tiers refused before dispatch (quota / paid spend) never saw the payload.
        attempted = {
            d.attempt.tier
            for d in state.dispatches
            if d.attempt.status not in ("quota_denied", "paid_blocked")
        }
        sized = {d.attempt.tier for d in state.dispatches if d.attempt.status == "too_large"}
        if attempted and attempted == sized:
            # extract() re-plans smaller chunks on this.
            return PayloadTooLarge(
                f"every tier refused the payload for size: {sorted(sized)}",
                serialized_bytes=state.measured.request_bytes,
                input_tokens=state.measured.input_tokens,
            )
        next_at: datetime | None = None
        if state.max_retry_after_s is not None:
            clock = getattr(self._quota, "clock", None) or SystemClock()
            next_at = clock.now() + timedelta(seconds=state.max_retry_after_s)
        reason = redact_text(f"{type(exc).__name__}: {exc}") if exc is not None else "no usable output"
        return AllProvidersFailedError(
            f"fallback chain exhausted: {reason}",
            attempts=[asdict(d.attempt) for d in state.dispatches],
            next_attempt_at=next_at,
        )


def build_chain(
    *,
    path: Path | None = None,
    connect: Any = None,
    env: Mapping[str, str] | None = None,
    order: tuple[str, ...] = ("gemini", "groq", "ollama"),
    router: Any = None,
) -> LiteLLMChain:
    """Live extraction chain from providers.yaml, for extract(candidate, chain).

    Enabled tiers in `order`; keys come from each block's api_key_env. With
    `connect` (zero-arg psycopg connection factory) the daily quota is shared
    across worker processes through PostgreSQL.
    """
    from signal_ingest.llm.providers_config import (
        load_providers_config,
        provider_limits_from_config,
        quota_ledger_from_config,
    )
    from signal_ingest.llm.quota import PgQuotaLedger

    blocks = load_providers_config(path).get("providers") or {}
    ledger = quota_ledger_from_config(path)
    if connect is not None:
        ledger = PgQuotaLedger(
            paid_spend_cap=ledger.paid_spend_cap, providers=ledger.providers, connect=connect
        )
    tiers = []
    for name in order:
        block = blocks.get(name)
        if not isinstance(block, dict) or not block.get("enabled"):
            continue
        tiers.append(
            Tier(
                name=name,
                litellm_model=str(block["litellm_model"]),
                limits=provider_limits_from_config(name, path=path),
                api_key_env=block.get("api_key_env"),
                api_base=block.get("base_url"),
                # Local CPU inference is slow; hosted tiers answer well within 30s.
                timeout_s=120.0 if block.get("is_local") else 30.0,
                extra={
                    **({"num_ctx": int(block["context_tokens"])} if block.get("is_local") else {}),
                    # Optional litellm params from providers.yaml, e.g. reasoning_effort.
                    **(block.get("litellm_params") or {}),
                },
            )
        )
    return LiteLLMChain(tiers, ledger, router=router, env=env)
