"""Import helpers and LiteLLM test seam for the B5 (R2b) provider chain.

The chain under test is ``signal_ingest.llm.router.LiteLLMChain`` driving a real
``litellm.Router``. Provider behaviour is scripted at ``litellm.acompletion``:
the Router calls it once per real dispatch, after its pre-call hooks (where the
chain reserves quota). Successful outcomes delegate to the real
``litellm.acompletion`` with ``mock_response`` so no socket is opened.
"""

from __future__ import annotations

import asyncio
import importlib
import os
from dataclasses import dataclass, field
from typing import Any

os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")

import httpx  # noqa: E402
import litellm  # noqa: E402
import openai  # noqa: E402

from signal_ingest.llm.quota import QuotaLedger  # noqa: E402

MODELS = {
    "gemini": "gemini/gemini-2.5-flash",
    "groq": "groq/openai/gpt-oss-120b",  # B8: llama-3.3 retired on Groq
    "ollama": "ollama_chat/llama3.2:3b",
}
KEY_ENV = {"gemini": "GEMINI_API_KEY", "groq": "GROQ_API_KEY", "ollama": None}
FAKE_ENV = {"GEMINI_API_KEY": "test-gemini-key", "GROQ_API_KEY": "test-groq-key"}
OK_TEXT = '{"productName":"AcmeAI"}'
_REAL_SLEEP = asyncio.sleep
_REQ = httpx.Request("POST", "https://provider.invalid/v1/chat/completions")


class MissingB5APIError(AssertionError):
    """Required B5 API is not importable."""


def _import(path: str) -> Any:
    try:
        return importlib.import_module(path)
    except ImportError as exc:
        raise MissingB5APIError(f"Cannot import {path}: {exc}") from exc


def require_attr(module: Any, *names: str) -> Any:
    tried = []
    for name in names:
        tried.append(f"{getattr(module, '__name__', module)}.{name}")
        if hasattr(module, name):
            return getattr(module, name)
    raise MissingB5APIError("Missing API: " + ", ".join(tried))


def load_router() -> Any:
    return _import("signal_ingest.llm.router")


def load_quota() -> Any:
    return _import("signal_ingest.llm.quota")


def load_transcript() -> Any:
    return _import("signal_ingest.llm.transcript")


def load_errors() -> Any:
    return _import("signal_ingest.llm.errors")


def load_providers_config() -> Any:
    return _import("signal_ingest.llm.providers_config")


# ---------------------------------------------------------------------------
# LiteLLM exceptions shaped like the provider-mapped ones
# ---------------------------------------------------------------------------


def rate_limited(tier: str, *, retry_after: float | None = None) -> Exception:
    headers = {"retry-after": str(retry_after)} if retry_after is not None else {}
    exc = litellm.RateLimitError(
        "rate limited",
        llm_provider=tier,
        model=MODELS.get(tier, tier),
        response=httpx.Response(429, headers=headers, request=_REQ),
    )
    exc.litellm_response_headers = httpx.Headers(headers)  # as exception_mapping sets it
    return exc


def context_window(tier: str) -> Exception:
    return litellm.ContextWindowExceededError(
        "This model's maximum context length is exceeded", model=MODELS.get(tier, tier),
        llm_provider=tier,
    )


def auth_error(tier: str, message: str = "invalid api key") -> Exception:
    return litellm.AuthenticationError(message, llm_provider=tier, model=MODELS.get(tier, tier))


def server_error(tier: str) -> Exception:
    return litellm.InternalServerError("upstream 503", llm_provider=tier, model=MODELS.get(tier, tier))


def model_response(text: str, *, finish_reason: str = "stop") -> dict[str, Any]:
    """A mock_response dict (mock_completion builds a ModelResponse from it)."""
    return {
        "choices": [{"index": 0, "finish_reason": finish_reason,
                     "message": {"role": "assistant", "content": text}}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }


# ---------------------------------------------------------------------------
# Dispatch-boundary script
# ---------------------------------------------------------------------------

Outcome = Any  # str | dict | Exception | Callable[[dict], Any] (may be async)


class LiteLLMScript:
    """Replaces ``litellm.acompletion``; each call is one real provider dispatch.

    ``on(tier, *outcomes)``: outcomes are consumed in order and the last one
    repeats. A str/dict/LiteLLM exception becomes ``mock_response`` of the real
    ``litellm.acompletion``; a callable receives the call kwargs and returns
    one of those.
    """

    def __init__(self) -> None:
        self._orig = litellm.acompletion
        self.script: dict[str, list[Outcome]] = {}
        self.calls: list[dict[str, Any]] = []

    def on(self, tier: str, *outcomes: Outcome) -> "LiteLLMScript":
        self.script[tier] = list(outcomes)
        return self

    @staticmethod
    def tier_of(kwargs: dict[str, Any]) -> str:
        model = str(kwargs.get("model"))
        for name, litellm_model in MODELS.items():
            if model == litellm_model or model == litellm_model.split("/", 1)[1]:
                return name
        return model

    def dispatched(self, tier: str) -> int:
        return sum(1 for c in self.calls if c["tier"] == tier)

    def calls_for(self, tier: str) -> list[dict[str, Any]]:
        return [c for c in self.calls if c["tier"] == tier]

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        tier = self.tier_of(kwargs)
        self.calls.append({"tier": tier, "kwargs": kwargs})
        queue = self.script.get(tier)
        if not queue:
            raise AssertionError(f"unscripted dispatch to {tier!r}")
        outcome = queue.pop(0) if len(queue) > 1 else queue[0]
        if callable(outcome) and not isinstance(outcome, Exception):
            outcome = outcome(kwargs)
            if asyncio.iscoroutine(outcome):
                outcome = await outcome
        if isinstance(outcome, BaseException) and not isinstance(outcome, openai.APIError):
            raise outcome
        # Exceptions go through the real acompletion too, so LiteLLM's failure
        # logging/callbacks fire exactly as for a provider-raised error.
        return await self._orig(*args, **{**kwargs, "mock_response": outcome})


async def real_sleep(seconds: float) -> None:
    await _REAL_SLEEP(seconds)


# ---------------------------------------------------------------------------
# Chain construction
# ---------------------------------------------------------------------------


def limits(name: str, **updates: Any) -> Any:
    from signal_ingest.llm.providers_config import provider_limits_from_config

    base = provider_limits_from_config(name)
    return base.model_copy(update={"enabled": True, "model": MODELS[name], **updates})


def tier(name: str, *, limits_obj: Any | None = None, **kw: Any) -> Any:
    Tier = require_attr(load_router(), "Tier")
    params: dict[str, Any] = {
        "name": name,
        "litellm_model": MODELS[name],
        "limits": limits_obj or limits(name),
        "api_key_env": KEY_ENV[name],
    }
    if name == "ollama":
        params.update(api_base="http://127.0.0.1:11434", timeout_s=120.0,
                      extra={"num_ctx": params["limits"].context_tokens})
    params.update(kw)
    return Tier(**params)


@dataclass
class RecordingLedger(QuotaLedger):
    """QuotaLedger that logs (op, provider) for every reserve/commit/release."""

    log: list[tuple[str, str, Any]] = field(default_factory=list)

    async def reserve(self, provider: str, **kw: Any) -> Any:  # type: ignore[override]
        self.log.append(("reserve", provider, kw))
        return await super().reserve(provider, **kw)

    async def commit(self, reservation: Any, **kw: Any) -> Any:  # type: ignore[override]
        self.log.append(("commit", reservation.provider, kw))
        return await super().commit(reservation, **kw)

    async def release(self, reservation: Any) -> None:  # type: ignore[override]
        self.log.append(("release", reservation.provider, None))
        await super().release(reservation)

    def ops(self, op: str) -> list[str]:
        return [p for o, p, _ in self.log if o == op]


def quota(
    *,
    ledger_cls: type = RecordingLedger,
    providers: tuple[str, ...] = ("gemini", "groq", "ollama"),
    requests_per_day: int = 10_000,
    tokens_per_day: int = 10_000_000,
    billing: dict[str, bool | None] | None = None,
) -> Any:
    from signal_ingest.llm.quota import ProviderQuotaConfig

    ledger = ledger_cls(paid_spend_cap=0)
    for name in providers:
        ledger.configure(ProviderQuotaConfig(
            provider=name,
            requests_per_day=requests_per_day,
            tokens_per_day=tokens_per_day,
            billing_enabled=(billing or {}).get(name, False),
            is_local=(name == "ollama"),
        ))
    return ledger


def chain(tiers: list[Any], ledger: Any | None = None, **kw: Any) -> Any:
    LiteLLMChain = require_attr(load_router(), "LiteLLMChain")
    kw.setdefault("num_retries", 0)
    kw.setdefault("env", dict(FAKE_ENV))
    return LiteLLMChain(tiers, ledger if ledger is not None else quota(), **kw)


def router_for(tiers: list[Any], *, num_retries: int = 0, env: dict[str, str] | None = None,
               cooldown_time: float = 60.0) -> Any:
    """A real litellm.Router built as the interface agreement specifies."""
    env = env or FAKE_ENV
    names = [t.name for t in tiers]
    model_list = [
        {"model_name": t.name, "litellm_params": {
            "model": t.litellm_model,
            "api_key": env.get(t.api_key_env) if t.api_key_env else None,
            "api_base": t.api_base,
            "timeout": t.timeout_s,
            **t.extra,
        }}
        for t in tiers
    ]
    return litellm.Router(
        model_list=model_list,
        fallbacks=[{names[0]: names[1:]}] if len(names) > 1 else [],
        context_window_fallbacks=[{names[0]: names[1:]}] if len(names) > 1 else [],
        num_retries=num_retries,
        retry_after=0,
        allowed_fails=1,
        cooldown_time=cooldown_time,
    )


def request(content: str = "extract product fields", *, max_input: int = 512,
            max_output: int = 64, max_bytes: int = 50_000) -> Any:
    from signal_ingest.llm.provider import ProviderRequest, RequestBudget

    return ProviderRequest(
        messages=[
            {"role": "system", "content": "Return JSON fields only."},
            {"role": "user", "content": content},
        ],
        response_schema={"type": "object", "properties": {"productName": {"type": "string"}}},
        budget=RequestBudget(max_input_tokens=max_input, max_output_tokens=max_output,
                             max_request_bytes=max_bytes, framing_tokens=16),
        provider="chain",
        model="gemini>groq",
    )


def attempts_of(result_or_chain: Any) -> list[dict[str, Any]]:
    from dataclasses import asdict, is_dataclass

    if hasattr(result_or_chain, "last_attempts"):
        return [asdict(a) if is_dataclass(a) else dict(a) for a in result_or_chain.last_attempts]
    return list(result_or_chain.raw.get("attempts") or [])


def statuses(result_or_chain: Any, tier_name: str) -> list[str]:
    return [a["status"] for a in attempts_of(result_or_chain) if a["tier"] == tier_name]

