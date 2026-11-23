"""B5 gate (R2b): LiteLLM Router chain, quota and zero-spend control.

Oracle: docs/build-plan.md (B5), docs/contracts.md and notes/r2b-interface.md.
A real ``litellm.Router`` is driven with fake keys; provider outcomes are
scripted at ``litellm.acompletion`` (one call per real dispatch, after the
chain's quota pre-call hook) and successes use LiteLLM ``mock_response``.
Offline only: the project network guard is installed for every test.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")

import pytest  # noqa: E402

from tests.integration import b5_api  # noqa: E402
from tests.integration.b5_api import (  # noqa: E402
    FAKE_ENV,
    MODELS,
    OK_TEXT,
    LiteLLMScript,
    attempts_of,
    statuses,
)
from tests.support import FIXTURES  # noqa: E402

ARTIFACTS = Path(__file__).resolve().parents[2] / "artifacts" / "gates"
FROZEN = datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc)
B5_FIXTURES = FIXTURES / "llm" / "b5"
SECRETS = ("test-gemini-key", "test-groq-key")


@pytest.fixture(autouse=True)
def network_guard():
    from signal_ingest.config.network import install_network_guard, uninstall_network_guard

    install_network_guard()
    try:
        yield
    finally:
        uninstall_network_guard()


@pytest.fixture(autouse=True)
def fake_keys_only(monkeypatch: pytest.MonkeyPatch):
    """Real keys (e.g. from a local .env) must never reach these tests."""
    for name in ("GEMINI_API_KEY", "GROQ_API_KEY", "R2_GROQ_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SIGNAL_INGEST_MODE", "fixture")
    monkeypatch.setenv("SIGNAL_INGEST_NETWORK_GUARD", "1")


@pytest.fixture
def llm(monkeypatch: pytest.MonkeyPatch) -> LiteLLMScript:
    import litellm

    script = LiteLLMScript()
    monkeypatch.setattr(litellm, "acompletion", script)
    return script


def _gemini_groq(**kw: Any) -> list[Any]:
    return [b5_api.tier("gemini", **kw), b5_api.tier("groq", **kw)]


def _assert_no_secrets(blob: str) -> None:
    for secret in SECRETS:
        assert secret not in blob
    lowered = blob.lower()
    assert "authorization" not in lowered
    assert "bearer " not in lowered


def _grounded_candidate(fields: tuple[str, ...] = ("price",)) -> tuple[Any, str, Any]:
    from signal_ingest.llm import ExtractCandidate, html_to_segments

    html = "<html><body><h1>Acme Cloud</h1><p>Acme Cloud costs $29 per month.</p></body></html>"
    seg = next(s for s in html_to_segments(html, entity_hints=("Acme Cloud",)) if "$29" in s.text)
    cand = ExtractCandidate(candidate_id="r2b", record_type="PRODUCT", html=html,
                            target_entity="Acme Cloud", fields_requested=fields)
    return cand, html, seg


# ---------------------------------------------------------------------------
# Fallover
# ---------------------------------------------------------------------------


async def test_r2b_gemini_429_falls_over_to_groq(llm):
    """Gemini 429 → Groq answers; Gemini's reservation released, Groq's committed."""
    llm.on("gemini", b5_api.rate_limited("gemini")).on("groq", OK_TEXT)
    ledger = b5_api.quota()
    chain = b5_api.chain(_gemini_groq(), ledger)

    result = await chain.complete(b5_api.request())

    assert result.raw["provider"] == "groq"
    assert result.raw["model"] == MODELS["groq"]
    assert json.loads(result.text) == {"productName": "AcmeAI"}
    assert llm.dispatched("gemini") >= 1 and llm.dispatched("groq") == 1
    assert "rate_limited" in statuses(chain, "gemini")
    assert statuses(chain, "groq") == ["ok"]
    assert result.raw["attempts"] == attempts_of(chain)
    # Quota: one reservation per real attempt; only the answering tier commits.
    assert "gemini" in ledger.ops("reserve") and "groq" in ledger.ops("reserve")
    assert ledger.ops("commit") == ["groq"]
    assert "gemini" in ledger.ops("release")
    snap = ledger.snapshot()
    assert snap["open_reservations"] == 0
    assert snap["day"]["gemini"]["requests_used"] == 0
    assert snap["day"]["gemini"]["requests_reserved"] == 0
    assert snap["day"]["groq"]["requests_used"] == 1


async def test_r2b_retry_after_above_cap_cools_tier_without_sleeping(llm):
    """Retry-After 120 > max_retry_after_s: no long sleep, tier cooled, chain falls over."""
    from signal_ingest.llm.errors import AllProvidersFailedError

    llm.on("gemini", b5_api.rate_limited("gemini", retry_after=120)).on("groq", OK_TEXT)
    chain = b5_api.chain(_gemini_groq(), num_retries=1, max_retry_after_s=15.0)

    t0 = time.perf_counter()
    first = await chain.complete(b5_api.request())
    assert time.perf_counter() - t0 < 3.0, "slept on a Retry-After above the cap"
    assert first.raw["provider"] == "groq"
    assert llm.dispatched("gemini") == 1, "retried a tier whose Retry-After exceeds the cap"
    assert any(a["tier"] == "gemini" and a["retry_after_s"] == pytest.approx(120)
               for a in attempts_of(chain))

    second = await chain.complete(b5_api.request())
    assert second.raw["provider"] == "groq"
    assert llm.dispatched("gemini") == 1, "cooled tier dispatched again inside its window"

    # No tier left: AllProvidersFailedError carries the latest Retry-After.
    solo = b5_api.chain([b5_api.tier("gemini")], num_retries=1, max_retry_after_s=15.0)
    before = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    with pytest.raises(AllProvidersFailedError) as info:
        await solo.complete(b5_api.request())
    assert time.perf_counter() - t0 < 3.0
    assert info.value.next_attempt_at is not None
    assert info.value.next_attempt_at >= before + timedelta(seconds=110)


async def test_r2b_short_retry_after_is_honored_on_same_tier(llm, monkeypatch):
    """Retry-After within the cap: LiteLLM sleeps for it, then retries the same tier."""
    slept: list[float] = []

    async def recording_sleep(seconds: float, *a: Any, **k: Any) -> None:
        slept.append(float(seconds))
        await b5_api.real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", recording_sleep)
    # Zero jitter so the sleep is exact. 3s is not a backoff step (0.5*2^n), so only the header yields it.
    import litellm.utils as litellm_utils
    monkeypatch.setattr(litellm_utils.random, "random", lambda: 0.0)
    llm.on("gemini", b5_api.rate_limited("gemini", retry_after=3), OK_TEXT)
    chain = b5_api.chain([b5_api.tier("gemini")], num_retries=1, max_retry_after_s=15.0)
    result = await chain.complete(b5_api.request())
    assert result.raw["provider"] == "gemini"
    assert llm.dispatched("gemini") == 2
    assert 3.0 in slept, f"Retry-After not honored: {slept}"


async def test_r2b_auth_error_skips_tier(llm):
    """401 on tier 1 is not retried; tier 2 answers."""
    llm.on("gemini", b5_api.auth_error("gemini")).on("groq", OK_TEXT)
    chain = b5_api.chain(_gemini_groq(), num_retries=1)
    result = await chain.complete(b5_api.request())
    assert result.raw["provider"] == "groq"
    assert llm.dispatched("gemini") == 1
    assert any(a["tier"] == "gemini" and a["status"] == "auth" and a["status_code"] == 401
               for a in attempts_of(chain))


async def test_r2b_injected_router_is_used(llm):
    """The test seam: a real litellm.Router injected via router= performs the fallover."""
    tiers = _gemini_groq()
    llm.on("gemini", b5_api.rate_limited("gemini")).on("groq", OK_TEXT)
    chain = b5_api.chain(tiers, router=b5_api.router_for(tiers))
    result = await chain.complete(b5_api.request())
    assert result.raw["provider"] == "groq"
    assert llm.dispatched("groq") == 1


# ---------------------------------------------------------------------------
# Size errors
# ---------------------------------------------------------------------------


async def test_r2b_context_window_on_every_tier_raises_payload_too_large(llm):
    """Every tier refuses for size → PayloadTooLarge measured from the request."""
    from signal_ingest.llm.provider import PayloadTooLarge, measure_provider_request

    llm.on("gemini", b5_api.context_window("gemini")).on("groq", b5_api.context_window("groq"))
    chain = b5_api.chain(_gemini_groq())
    req = b5_api.request()
    with pytest.raises(PayloadTooLarge) as info:
        await chain.complete(req)
    measured = measure_provider_request(req)
    assert info.value.serialized_bytes == measured.request_bytes
    assert info.value.input_tokens == measured.input_tokens
    assert llm.dispatched("groq") == 1, "context-window fallback did not reach tier 2"
    assert set(statuses(chain, "gemini")) == {"too_large"}
    assert set(statuses(chain, "groq")) == {"too_large"}


async def test_r2b_extract_rechunks_after_chain_overflow(llm):
    """extract(): the first plan overflows on every tier → re-chunk smaller → not FAILED."""
    from signal_ingest.llm import ExtractCandidate, ExtractStatus, extract

    paras = "".join(
        f"<p>Acme Cloud feature {i}: secure backups, fast restores and audit logs "
        f"for teams number {i}.</p>" for i in range(40)
    )
    html = f"<html><body><h1>Acme Cloud</h1>{paras}<p>Acme Cloud costs $29 per month.</p></body></html>"
    tight = {"context_tokens": 2000, "max_input_tokens": 1900, "reserved_output_tokens": 256,
             "safety_margin_tokens": 16, "framing_tokens": 16}
    tiers = [b5_api.tier(n, limits_obj=b5_api.limits(n, **tight)) for n in ("gemini", "groq")]
    first: dict[str, int] = {}
    sizes: list[int] = []

    def overflow_first_plan(kwargs: dict[str, Any]) -> Any:
        n = len(kwargs["messages"][-1]["content"])
        sizes.append(n)
        first.setdefault("n", n)
        tier = LiteLLMScript.tier_of(kwargs)
        return b5_api.context_window(tier) if n >= first["n"] else '{"fields": []}'

    llm.on("gemini", overflow_first_plan).on("groq", overflow_first_plan)
    cand = ExtractCandidate(candidate_id="r2b-rechunk", record_type="PRODUCT", html=html,
                            target_entity="Acme Cloud", fields_requested=("price",))
    result = await extract(cand, b5_api.chain(tiers))
    assert result.status is not ExtractStatus.FAILED, result.reason
    assert result.attempts > 1
    assert min(sizes) < first["n"], "re-chunked request was not smaller"


async def test_r2b_request_too_big_for_one_tier_skips_it_before_dispatch(llm):
    """A request past one tier's input cap skips that tier (no dispatch); the next answers."""
    small = b5_api.tier("gemini", limits_obj=b5_api.limits("gemini", max_input_tokens=200))
    llm.on("gemini", OK_TEXT).on("groq", OK_TEXT)
    chain = b5_api.chain([small, b5_api.tier("groq")])
    result = await chain.complete(b5_api.request("x" * 4000, max_input=5000))
    assert result.raw["provider"] == "groq"
    assert llm.dispatched("gemini") == 0
    assert set(statuses(chain, "gemini")) <= {"too_large"}


async def test_r2b_overflow_then_paid_blocked_tier_still_payload_too_large(llm):
    """Gemini context-window + Groq billing-unknown (never dispatched) → PayloadTooLarge,
    and extract() re-chunks instead of FAILED."""
    from signal_ingest.llm import ExtractCandidate, ExtractStatus, extract
    from signal_ingest.llm.provider import PayloadTooLarge

    llm.on("gemini", b5_api.context_window("gemini")).on("groq", OK_TEXT)
    chain = b5_api.chain(_gemini_groq(), b5_api.quota(billing={"groq": None}))
    with pytest.raises(PayloadTooLarge):
        await chain.complete(b5_api.request())
    assert llm.dispatched("groq") == 0
    assert set(statuses(chain, "groq")) <= {"paid_blocked"}

    llm.calls.clear()
    first: dict[str, int] = {}

    def overflow_first_plan(kwargs: dict[str, Any]) -> Any:
        n = len(kwargs["messages"][-1]["content"])
        first.setdefault("n", n)
        return b5_api.context_window("gemini") if n >= first["n"] else '{"fields": []}'

    llm.on("gemini", overflow_first_plan)
    tight = {"context_tokens": 2000, "max_input_tokens": 1900, "reserved_output_tokens": 256,
             "safety_margin_tokens": 16, "framing_tokens": 16}
    tiers = [b5_api.tier(n, limits_obj=b5_api.limits(n, **tight)) for n in ("gemini", "groq")]
    paras = "".join(f"<p>Acme Cloud feature {i}: secure backups, fast restores and audit "
                    f"logs for teams number {i}.</p>" for i in range(40))
    cand = ExtractCandidate(candidate_id="r2b-paid", record_type="PRODUCT",
                            html=f"<html><body><h1>Acme Cloud</h1>{paras}</body></html>",
                            target_entity="Acme Cloud", fields_requested=("price",))
    result = await extract(cand, b5_api.chain(tiers, b5_api.quota(billing={"groq": None})))
    assert result.status is not ExtractStatus.FAILED, result.reason
    assert result.attempts > 1
    assert llm.dispatched("groq") == 0


async def test_r2b_fenced_json_accepted_first_attempt(llm):
    """A ```json fenced answer is valid output: no re-ask, no fallback."""
    llm.on("gemini", '```json\n{"productName":"Fenced"}\n```').on("groq", OK_TEXT)
    chain = b5_api.chain(_gemini_groq())
    result = await chain.complete(b5_api.request())
    assert result.raw["provider"] == "gemini"
    assert llm.dispatched("gemini") == 1 and llm.dispatched("groq") == 0
    assert statuses(chain, "gemini") == ["ok"]


def test_r2b_router_with_unmapped_deployment_rejected():
    """An injected Router deployment with no matching Tier fails at construction."""
    router = b5_api.router_for(_gemini_groq())
    with pytest.raises(ValueError):
        b5_api.chain([b5_api.tier("gemini")], router=router)


def test_r2b_litellm_import_does_not_load_dotenv():
    """conftest pins LITELLM_MODE=PRODUCTION so importing litellm never runs load_dotenv()."""
    import subprocess
    import sys

    import litellm

    assert os.environ.get("LITELLM_MODE") == "PRODUCTION"
    assert litellm.litellm_mode == "PRODUCTION"
    # Fresh interpreter from the repo root (where a local .env may live): import the
    # conftest and the helpers, then report only the NAMES of env vars that appeared.
    code = (
        "import os, json; before = set(os.environ); import tests.conftest; "
        "import tests.integration.b5_api; "
        "new = set(os.environ) - before - {'LITELLM_MODE', 'LITELLM_LOCAL_MODEL_COST_MAP'}; "
        "print(json.dumps(sorted(n for n in new "
        "if n in {'GEMINI_API_KEY', 'GROQ_API_KEY', 'GITHUB_TOKEN'})), len(new))"
    )
    root = Path(__file__).resolve().parents[2]
    env = {k: v for k, v in os.environ.items() if k != "LITELLM_MODE"}
    for k in ("GEMINI_API_KEY", "GROQ_API_KEY", "GITHUB_TOKEN"):
        env.pop(k, None)
    out = subprocess.run([sys.executable, "-c", code], cwd=root, env=env,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr[-2000:]
    names, count = out.stdout.strip().rsplit(" ", 1)
    assert json.loads(names) == [], f"litellm import loaded secrets from .env: {names}"
    assert int(count) == 0, "litellm import added env vars (dotenv load ran)"


def test_r2b_chain_limits_are_tightest_tier():
    """Gemini 1M + Groq 32k: chain limits never exceed Groq's."""
    groq = b5_api.limits("groq")
    chain = b5_api.chain(_gemini_groq())
    lim = chain.limits
    assert lim.max_input_tokens <= groq.max_input_tokens
    assert lim.context_tokens <= groq.context_tokens
    assert lim.max_request_bytes <= groq.max_request_bytes
    assert lim.provider == "chain"
    assert lim.model == "gemini>groq"


def test_r2b_classify_maps_litellm_exceptions():
    import litellm

    classify = b5_api.require_attr(b5_api.load_router(), "classify")
    assert classify(b5_api.rate_limited("gemini")) == "rate_limited"
    assert classify(b5_api.context_window("groq")) == "too_large"
    assert classify(litellm.BadRequestError(
        "Request too large for model", model=MODELS["groq"], llm_provider="groq")) == "too_large"
    assert classify(b5_api.auth_error("gemini")) == "auth"
    assert classify(b5_api.server_error("groq")) == "error"


# ---------------------------------------------------------------------------
# Failure / skip paths
# ---------------------------------------------------------------------------


async def test_r2b_all_tiers_fail_extract_failed_and_not_cached(llm):
    """Non-size failures on every tier → AllProvidersFailedError; extract() → FAILED, no cache."""
    from signal_ingest.llm import ExtractionCache, ExtractStatus, extract
    from signal_ingest.llm.errors import AllProvidersFailedError

    llm.on("gemini", b5_api.server_error("gemini")).on("groq", b5_api.rate_limited("groq"))
    with pytest.raises(AllProvidersFailedError):
        await b5_api.chain(_gemini_groq()).complete(b5_api.request())

    cand, _, _ = _grounded_candidate()
    cache = ExtractionCache()
    ledger = b5_api.quota()
    result = await extract(cand, b5_api.chain(_gemini_groq(), ledger), cache=cache)
    assert result.status is ExtractStatus.FAILED
    assert result.cache_key is not None and result.cache_key not in cache
    assert not cache._store
    assert ledger.snapshot()["open_reservations"] == 0


async def test_r2b_missing_api_key_skips_tier(llm):
    """No GEMINI_API_KEY → gemini tier skipped and never dispatched."""
    llm.on("gemini", OK_TEXT).on("groq", OK_TEXT)
    chain = b5_api.chain(_gemini_groq(), env={"GROQ_API_KEY": "test-groq-key"})
    assert [t.name for t in chain.tiers] == ["groq"]
    result = await chain.complete(b5_api.request())
    assert result.raw["provider"] == "groq"
    assert llm.dispatched("gemini") == 0


async def test_r2b_quota_race_one_slot(llm):
    """requests_per_day=1, two concurrent complete(): exactly one succeeds."""
    from signal_ingest.llm.errors import AllProvidersFailedError

    async def slow_ok(_kwargs: dict[str, Any]) -> str:
        await b5_api.real_sleep(0.05)
        return OK_TEXT

    llm.on("gemini", slow_ok)
    ledger = b5_api.quota(providers=("gemini",), requests_per_day=1)
    chain = b5_api.chain([b5_api.tier("gemini")], ledger)
    results = await asyncio.gather(chain.complete(b5_api.request()),
                                   chain.complete(b5_api.request()), return_exceptions=True)
    ok = [r for r in results if not isinstance(r, BaseException)]
    failed = [r for r in results if isinstance(r, AllProvidersFailedError)]
    assert len(ok) == 1 and len(failed) == 1, results
    assert llm.dispatched("gemini") == 1
    snap = ledger.snapshot()
    assert snap["day"]["gemini"]["requests_used"] == 1
    assert snap["day"]["gemini"]["requests_reserved"] == 0
    assert snap["open_reservations"] == 0


async def test_r2b_billing_unknown_tier_never_dispatched(llm):
    """billing_enabled null under paid_spend_cap=0 → PaidSpendBlocked → no dispatch."""
    llm.on("gemini", OK_TEXT).on("groq", OK_TEXT)
    ledger = b5_api.quota(billing={"gemini": None})
    chain = b5_api.chain(_gemini_groq(), ledger)
    result = await chain.complete(b5_api.request())
    assert result.raw["provider"] == "groq"
    assert llm.dispatched("gemini") == 0
    assert set(statuses(chain, "gemini")) <= {"paid_blocked"}
    assert ledger.paid_spend_used == 0
    assert ledger.snapshot()["open_reservations"] == 0


async def test_r2b_cancel_in_flight_releases_reservations(llm):
    """Cancelling complete() mid-dispatch leaves no open quota reservation."""
    started = asyncio.Event()

    async def hang(_kwargs: dict[str, Any]) -> str:
        started.set()
        await b5_api.real_sleep(60)
        raise AssertionError("not cancelled")

    llm.on("gemini", hang)
    ledger = b5_api.quota(providers=("gemini",))
    chain = b5_api.chain([b5_api.tier("gemini")], ledger)
    task = asyncio.create_task(chain.complete(b5_api.request()))
    await asyncio.wait_for(started.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert ledger.snapshot()["open_reservations"] == 0


async def test_r2b_invalid_output_reasks_once_then_falls_over(llm):
    """finish_reason=length or non-JSON: one re-ask on the same tier, then the next tier."""
    for bad in (b5_api.model_response('{"productName":"Acm', finish_reason="length"),
                "NOT_JSON{{{"):
        llm.calls.clear()
        llm.on("gemini", bad).on("groq", OK_TEXT)
        chain = b5_api.chain(_gemini_groq())
        result = await chain.complete(b5_api.request())
        assert result.raw["provider"] == "groq"
        assert llm.dispatched("gemini") == 2
        assert statuses(chain, "gemini") == ["invalid_output", "invalid_output"]


# ---------------------------------------------------------------------------
# Request fidelity, quota sizing, secrets
# ---------------------------------------------------------------------------


async def test_r2b_router_passes_grounded_messages_unchanged(llm):
    """The Router must not compact or rewrite the grounded chunk."""
    llm.on("gemini", OK_TEXT)
    grounded = "\n".join(f"seg-{i}: Acme Cloud ユニコード 🚀 costs ${i} per month." for i in range(600))
    req = b5_api.request(grounded, max_input=200_000, max_output=128, max_bytes=2_000_000)
    chain = b5_api.chain([b5_api.tier("gemini")])
    await chain.complete(req)
    sent = llm.calls_for("gemini")[0]["kwargs"]
    assert sent["messages"] == req.messages
    assert sent["max_tokens"] == 128
    fmt = sent["response_format"]
    assert fmt["type"] == "json_schema" and fmt["json_schema"]["name"] == "extraction"


async def test_r2_quota_reserves_measured_tokens_not_budget_ceiling(llm):
    """A small request under a 1M-token ceiling fits a 1M/day free quota."""
    from signal_ingest.llm.provider import measure_provider_request

    llm.on("gemini", OK_TEXT)
    ledger = b5_api.quota(providers=("gemini",), requests_per_day=1500, tokens_per_day=1_000_000)
    chain = b5_api.chain([b5_api.tier("gemini")], ledger)
    req = b5_api.request(max_input=1_000_000, max_output=2048, max_bytes=2_000_000)
    result = await chain.complete(req)
    assert result.raw["provider"] == "gemini"
    reserved = [kw for op, _, kw in ledger.log if op == "reserve"]
    assert reserved and reserved[0]["input_tokens"] == measure_provider_request(req).input_tokens
    assert reserved[0]["output_tokens"] == 2048


async def test_r2b_no_secrets_in_raw_or_attempts(llm):
    """API keys / Authorization never reach result.raw or Attempt records."""
    from signal_ingest.llm.transcript import redact_headers

    llm.on("gemini", b5_api.auth_error("gemini", "Authorization: Bearer test-gemini-key rejected"))
    llm.on("groq", OK_TEXT)
    chain = b5_api.chain(_gemini_groq())
    result = await chain.complete(b5_api.request())
    assert result.raw["provider"] == "groq"
    # The keys really were handed to LiteLLM, so absence below is meaningful.
    assert {c["kwargs"].get("api_key") for c in llm.calls} == set(FAKE_ENV.values())
    blob = json.dumps({"raw": result.raw, "attempts": attempts_of(chain)}, default=str)
    _assert_no_secrets(blob)
    assert "test-groq-key" not in json.dumps(redact_headers({"Authorization": "Bearer test-groq-key"}))
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / "b5_redacted_transcript.json").write_text(blob, encoding="utf-8")


def test_r2b_fake_never_live():
    from signal_ingest.llm import is_live_provider as exported
    from signal_ingest.llm.providers_config import is_live_provider

    assert exported is is_live_provider
    assert is_live_provider("fake") is False
    assert is_live_provider("fake-extract-v1") is False
    assert is_live_provider("gemini") is True


# ---------------------------------------------------------------------------
# Quota ledger (unchanged module)
# ---------------------------------------------------------------------------


async def test_paid_spend_cap_blocks_billing_unknown():
    """paid_spend_cap=0 denies billing-unknown / paid estimated spend routes."""
    from signal_ingest.config import FrozenClock
    from signal_ingest.llm.errors import PaidSpendBlockedError
    from signal_ingest.llm.quota import ProviderQuotaConfig, QuotaLedger

    ledger = QuotaLedger(paid_spend_cap=0, clock=FrozenClock(FROZEN))
    ledger.configure(ProviderQuotaConfig(provider="gemini", billing_enabled=None,
                                         requests_per_day=100))
    with pytest.raises(PaidSpendBlockedError):
        await ledger.reserve("gemini", requests=1, estimated_paid_micros=0)

    ledger2 = QuotaLedger(paid_spend_cap=0, clock=FrozenClock(FROZEN))
    ledger2.configure(ProviderQuotaConfig(provider="gemini", billing_enabled=True,
                                          requests_per_day=100))
    with pytest.raises(PaidSpendBlockedError):
        await ledger2.reserve("gemini", requests=1, estimated_paid_micros=100)


def test_r2_paid_billing_blocked_when_cap_zero():
    """billing_enabled=true with paid_spend_cap=0 is denied even at estimate 0."""
    from signal_ingest.config import FrozenClock
    from signal_ingest.llm.errors import PaidSpendBlockedError
    from signal_ingest.llm.quota import ProviderQuotaConfig, QuotaLedger

    ledger = QuotaLedger(paid_spend_cap=0, clock=FrozenClock(FROZEN))
    ledger.configure(ProviderQuotaConfig(provider="gemini", billing_enabled=True))
    with pytest.raises(PaidSpendBlockedError):
        ledger.reserve_sync("gemini", requests=1, input_tokens=10, output_tokens=10)


@pytest.mark.asyncio
async def test_r2_pg_quota_shared_across_processes(tmp_fixture_env):
    """Two ledgers (two worker processes) share one Postgres daily window: no double spend."""
    import uuid

    from signal_ingest.config import FrozenClock, Mode, load_settings
    from signal_ingest.llm.errors import QuotaExhaustedError
    from signal_ingest.llm.quota import PgQuotaLedger, ProviderQuotaConfig
    from signal_ingest.storage import connect, init_db

    settings = load_settings(mode=Mode.FIXTURE)
    init_db(settings)
    name = f"r2-prov-{uuid.uuid4().hex[:8]}"

    def ledger():
        led = PgQuotaLedger(paid_spend_cap=0, clock=FrozenClock(FROZEN),
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


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


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
        "    requests_per_day: 0\n    billing_enabled: false\n"
        "  gemini:\n    model: g\n    enabled: false\n"
        "    context_tokens: 32768\n    reserved_output_tokens: 2048\n"
        "    max_input_tokens: 30000\n    max_request_bytes: 500000\n"
        "    billing_enabled: null\n",
        encoding="utf-8",
    )
    assert load_providers_config(cfg)["providers"]["groq"]["model"] == "llama"
    assert provider_limits_from_config("groq", path=cfg).enabled is False
    ledger = quota_ledger_from_config(cfg)
    assert ledger.providers["groq"].requests_per_day == 0
    assert ledger.providers["gemini"].billing_enabled is None


def test_r2b_providers_yaml_declares_litellm_models():
    from signal_ingest.llm.providers_config import load_providers_config

    providers = load_providers_config()["providers"]
    for name, model in MODELS.items():
        assert providers[name]["litellm_model"] == model, name


async def test_r2_build_chain_from_config_uses_enabled_tiers(tmp_path: Path, llm):
    from signal_ingest.llm.providers_config import build_chain

    LiteLLMChain = b5_api.require_attr(b5_api.load_router(), "LiteLLMChain")
    cfg = tmp_path / "providers.yaml"
    tier = ("    context_tokens: 32768\n    reserved_output_tokens: 2048\n"
            "    max_input_tokens: 30000\n    max_request_bytes: 500000\n"
            "    billing_enabled: false\n")
    cfg.write_text(
        "defaults:\n  paid_spend_cap: 0\nproviders:\n"
        f"  gemini:\n    model: g\n    litellm_model: {MODELS['gemini']}\n    enabled: false\n"
        "    api_key_env: GEMINI_API_KEY\n" + tier +
        f"  groq:\n    model: llama-test\n    litellm_model: {MODELS['groq']}\n    enabled: true\n"
        "    api_key_env: R2_GROQ_KEY\n" + tier +
        f"  ollama:\n    model: llama3.2:3b\n    litellm_model: {MODELS['ollama']}\n"
        "    enabled: true\n    api_key_env: null\n    is_local: true\n"
        "    base_url: http://127.0.0.1:11434\n" + tier,
        encoding="utf-8",
    )
    chain = build_chain(path=cfg, env={"R2_GROQ_KEY": "test-groq-key"})
    assert isinstance(chain, LiteLLMChain)
    assert [t.name for t in chain.tiers] == ["groq", "ollama"]
    assert [t.litellm_model for t in chain.tiers] == [MODELS["groq"], MODELS["ollama"]]
    assert "127.0.0.1:11434" in (chain.tiers[1].api_base or "")
    assert chain.limits.model == "groq>ollama"

    llm.on("groq", '{"productName":"FromConfig"}')
    result = await chain.complete(b5_api.request())
    assert json.loads(result.text) == {"productName": "FromConfig"}
    assert result.raw["provider"] == "groq"
    assert llm.calls_for("groq")[0]["kwargs"]["api_key"] == "test-groq-key"


# ---------------------------------------------------------------------------
# extract() end to end through the chain
# ---------------------------------------------------------------------------


async def test_r2_extract_runs_through_fallback_chain(llm):
    """extract() on a LiteLLMChain: Gemini 429 → Groq grounded answer is accepted."""
    from signal_ingest.llm import extract
    from signal_ingest.llm.types import FieldDecision

    cand, _, seg = _grounded_candidate()
    payload = {"fields": [{"field": "price", "value": "$29",
                           "quote": "Acme Cloud costs $29 per month.",
                           "segment_ids": [seg.segment_id]}]}
    llm.on("gemini", b5_api.rate_limited("gemini")).on("groq", json.dumps(payload))
    chain = b5_api.chain(_gemini_groq())
    result = await extract(cand, chain)
    assert result.fields["price"].decision is FieldDecision.ACCEPTED, result.fields
    assert llm.dispatched("groq") == 1


async def test_r2b_second_extract_is_cache_hit_without_provider_calls(llm):
    from signal_ingest.llm import ExtractionCache, ExtractStatus, extract

    cand, _, seg = _grounded_candidate()
    payload = {"fields": [{"field": "price", "value": "$29",
                           "quote": "Acme Cloud costs $29 per month.",
                           "segment_ids": [seg.segment_id]}]}
    llm.on("gemini", json.dumps(payload))
    chain = b5_api.chain(_gemini_groq())
    cache = ExtractionCache()
    first = await extract(cand, chain, cache=cache)
    assert first.status is not ExtractStatus.FAILED
    calls = len(llm.calls)
    assert calls >= 1
    second = await extract(cand, chain, cache=cache)
    assert second.status is ExtractStatus.CACHE_HIT
    assert len(llm.calls) == calls


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


async def test_meta_billed_tier_needs_estimate_and_respects_cap():
    """User-approved Meta credits: a billed route without a cost estimate is refused,
    and estimated spend can never pass paid_spend_cap (whole USD)."""
    from signal_ingest.config import FrozenClock
    from signal_ingest.llm.errors import PaidSpendBlockedError
    from signal_ingest.llm.quota import ProviderQuotaConfig, QuotaLedger
    from signal_ingest.llm.router import Tier

    ledger = QuotaLedger(paid_spend_cap=5, clock=FrozenClock(FROZEN))
    ledger.configure(ProviderQuotaConfig(provider="meta", billing_enabled=True))
    with pytest.raises(PaidSpendBlockedError):
        await ledger.reserve("meta", requests=1, input_tokens=100, output_tokens=10)
    tier = Tier(name="meta", litellm_model="openai/muse-ember-1.0", limits=b5_api.tier("groq").limits,
                usd_per_m_input=0.10, usd_per_m_output=0.50)
    micros = tier.estimated_micros(8000, 1024)
    assert micros == 8000 * 0.10 + 1024 * 0.50  # 1312 micro-USD
    r = await ledger.reserve("meta", requests=1, input_tokens=8000, output_tokens=1024,
                             estimated_paid_micros=micros)
    await ledger.commit(r)
    with pytest.raises(PaidSpendBlockedError):
        await ledger.reserve("meta", requests=1, input_tokens=1, output_tokens=1,
                             estimated_paid_micros=5_000_000)


def test_meta_tier_is_third_in_live_chain_config():
    from signal_ingest.llm.providers_config import is_live_provider, load_providers_config

    providers = load_providers_config()["providers"]
    meta = providers["meta"]
    assert meta["billing_enabled"] is True and float(meta["usd_per_m_input"]) > 0
    assert meta["litellm_model"] == "openai/muse-ember-1.0" and "contributor" not in meta["model"]
    assert is_live_provider("meta")
    import inspect

    from signal_ingest.llm.router import build_chain

    order = inspect.signature(build_chain).parameters["order"].default
    assert order.index("gemini") < order.index("groq") < order.index("meta")
