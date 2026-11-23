# R2b interface agreement (coordinator-owned)

Block R2b replaces the hand-written provider stack with a LiteLLM Router plus a LangGraph extraction graph. It reopens B4 and B5. Pinned deps (already installed in .venv, Python 3.13.14): litellm==1.108.3, langgraph==1.2.20, langchain-core==1.7.6, respx (dev).

## Deleted
- `llm/fallback.py`, `llm/circuit.py`, `llm/retry.py`, `llm/http_transport.py`
- `llm/adapters/gemini.py`, `llm/adapters/groq.py`, `llm/adapters/ollama.py`, `llm/adapters/base.py`, `llm/adapters/__init__.py` (the whole `adapters/` package)
- From `llm/errors.py`: provider-transport classes (`AuthError`, `RateLimitedError`, `TransientProviderError`, `InvalidOutputError`, `CircuitOpenError`).

Kept in `errors.py`: `MissingCredentialsError`, `QuotaExhaustedError`, `PaidSpendBlockedError`, `AllProvidersFailedError` (keeps `next_attempt_at`) and `LiveQualificationError`.

## Moved
`is_live_provider(name: str) -> bool` moves to `llm/providers_config.py`. It is re-exported from `signal_ingest.llm`. Callers to update: `live/canary.py`, `tests/integration/b8_api.py`, `tests/gates/test_b8.py`.

## New: `llm/router.py`

The module sets `os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")` before importing litellm, and also `litellm.telemetry = False`. No network at import.

```python
@dataclass(frozen=True)
class Tier:
    name: str                 # "gemini" | "groq" | "ollama" | test names
    litellm_model: str        # e.g. "gemini/gemini-2.5-flash", "groq/llama-3.3-70b-versatile", "ollama_chat/llama3.2:3b"
    limits: ProviderLimits    # from providers.yaml (provider_limits_from_config)
    api_key_env: str | None = None
    api_base: str | None = None
    timeout_s: float = 30.0   # ollama 120
    extra: dict[str, Any] = field(default_factory=dict)  # e.g. {"num_ctx": 8192} for ollama

@dataclass
class Attempt:                 # one real dispatch, redacted
    tier: str
    status: str                # "ok" | "rate_limited" | "too_large" | "auth" | "quota_denied" | "paid_blocked" | "invalid_output" | "error"
    status_code: int | None
    retry_after_s: float | None
    input_tokens: int
    output_tokens: int

class LiteLLMChain:            # implements llm.provider.Provider
    def __init__(self, tiers: Sequence[Tier], quota: QuotaLedger, *,
                 num_retries: int = 2,          # per tier, LiteLLM exponential backoff + jitter, honors Retry-After
                 max_retry_after_s: float = 15.0,  # Retry-After above this -> cooldown tier and fall over, never sleep longer
                 cooldown_time_s: float = 60.0,
                 allowed_fails: int = 1,
                 router: "litellm.Router | None" = None,   # injected for tests
                 env: Mapping[str, str] | None = None) -> None: ...
    @property
    def limits(self) -> ProviderLimits   # tightest of enabled tiers; provider="chain", model="gemini>groq>..."
    @property
    def tiers(self) -> tuple[Tier, ...]  # enabled tiers that have credentials (missing key -> skipped, recorded)
    async def complete(self, request: ProviderRequest) -> ProviderResult
    last_attempts: list[Attempt]         # attempts of the most recent complete()

def classify(exc: BaseException) -> str  # LiteLLM exception -> Attempt.status (single mapping function)
def build_chain(*, path: Path | None = None, connect: Any = None, env: Mapping[str,str] | None = None,
                order: tuple[str, ...] = ("gemini", "groq", "ollama"), router: Any = None) -> LiteLLMChain
```
`build_chain` also stays importable from `llm/providers_config.py`, via a thin re-export.

Semantics:
1. **Router.** Build one `litellm.Router(model_list=[{model_name: tier.name, litellm_params: {model, api_key, api_base, timeout, **extra}}], fallbacks=[{tiers[0]: tiers[1:]}], context_window_fallbacks=[{tiers[0]: tiers[1:]}], num_retries, retry_after=0, allowed_fails, cooldown_time)`. Call it with `router.acompletion(model=tiers[0].name, messages=..., response_format=..., max_tokens=budget.max_output_tokens)`. Router performs fallback.
   - Disable any Router input compaction (`compact_to_fit`). Grounded chunks must reach the model byte-for-byte.
2. **Quota per real attempt.** A `litellm.integrations.custom_logger.CustomLogger` subclass implements `async_pre_call_check(deployment, parent_otel_span)`. It reserves `QuotaLedger.reserve(tier, requests=1, input_tokens=measured, output_tokens=max_output)` for that deployment's tier. On `QuotaExhaustedError` / `PaidSpendBlockedError` it raises `litellm.RateLimitError`, so the Router cools that tier and falls over.
   - Track reservations in a `contextvars.ContextVar` scoped to one `complete()` call.
   - When `complete()` returns: commit the reservation of the tier that answered (`response._hidden_params["model_id"]` or the last reservation) and release all others. On exception, release all.
   - Register the hook on the Router instance's callbacks, or `litellm.callbacks`. It must not leak between chains: remove it in `finally`, or key by chain id.
3. **Zero spend.** Keep the existing `QuotaLedger` / `PgQuotaLedger` rules unchanged. Fake routes never go through LiteLLM.
4. **Errors out of `complete()`:**
   - Every tier refused for size (`ContextWindowExceededError`, or 413 / `BadRequestError` whose message matches context/too large) → raise `provider.PayloadTooLarge(serialized_bytes, input_tokens)` measured from `measure_provider_request`. `extract()` re-chunks.
   - Anything else exhausted → raise `AllProvidersFailedError(reason, next_attempt_at)`. `next_attempt_at` is the latest Retry-After seen.
5. **Output.**
   - `response_format={"type":"json_schema","json_schema":{"name":"extraction","schema": schema_for(tier)}}`. For Gemini use a relaxed copy of `EXTRACTION_JSON_SCHEMA`: drop `additionalProperties`, replace `"value": {}` with `{"type":["string","number","boolean","null"]}`.
   - Ollama passes `num_ctx`.
   - `finish_reason == "length"` or non-JSON → that attempt counts as `invalid_output`, with one re-ask on the same tier, then the next tier.
   - Return `ProviderResult(text, parsed=None, input_tokens, output_tokens (usage), request_bytes=measured, finish_reason, raw={"provider": tier, "model": litellm_model, "attempts": [asdict(a) ...]})`.
   - Never put API keys or headers in `raw` / attempts.
6. **Per-tier sizing.** If `measured.input_tokens + max_output > tier.limits.max_input_tokens`, that tier is skipped as `too_large` before dispatch. Limits are tightest-of-tiers, so normally nothing is skipped.

## New: `llm/graph.py` (LangGraph)
`extract()` in `llm/extract.py` keeps its exact signature and `ExtractResult` semantics. It becomes a thin wrapper: `return await run_extraction_graph(candidate, provider, budget=budget, cache=cache, max_depth=max_depth)`.

`graph.py` builds `StateGraph(ExtractState)`, compiled once at module level. `ExtractState` is a TypedDict carrying candidate, provider, computed budget, segments, cache_key, plan, batches, attempts, usage, last_overflow_bytes, payload_error, discarded and result.

Nodes, each reusing the existing functions:
1. `budget` → `compute_budget`. A route-disabled budget goes to END with ROUTE_DISABLED.
2. `segment` → `_segments_for`
3. `cache_lookup` → `build_cache_key`/`cache.get`. A hit goes to END with CACHE_HIT.
4. `plan` → `plan_chunks`. No chunks goes to `finalize`.
5. `call_llm` runs all chunks: `_build_request`, then `measure_provider_request`, the unchanged-bytes guard, `provider.complete`, `parse_model_payload` and `validate_payload`.
6. `rebudget` (conditional loop back to `call_llm`) → `rebudget_on_overflow(factor=0.5)`, with the same `UnchangedOversizedRetry` rules and the same `max_depth` re-raise.
7. `merge` → `merge_field_evidence`
8. `finalize` → `_finalize` + cache put (only if `payload_error is None`; a `no_chunks` result is cached too, same as the baseline).

An `AllProvidersFailedError` in `call_llm` produces FAILED, not cached. Exceptions that `extract()` raises today (`UnchangedOversizedRetry`, `PayloadTooLarge` at max depth) must still propagate to the caller.

## Unchanged
`FakeProvider`, `quota.py`, `predicates.py`, `validate.py`, `chunk.py`, `merge.py`, `segments.py`, `cache.py`, `schema.py`, `transcript.py` (redaction helpers stay; the chain uses `redact_text` on error messages it stores).

## Config
In `config/providers.yaml`, add `litellm_model:` per live tier: gemini `gemini/gemini-2.5-flash`, groq `groq/llama-3.3-70b-versatile`, ollama `ollama_chat/llama3.2:3b`. Keep `enabled: false` until B8 verifies them live.

Replace the hand-written YAML parser in `providers_config.py` with `yaml.safe_load`. The test for an inline `# comment` must still pass.

## Test seam (verifier)
B5 tests construct `LiteLLMChain(tiers, QuotaLedger(...), router=<real litellm.Router>)` with fake keys. They drive provider failures in one of two ways:
- `litellm_params: {"mock_response": litellm.RateLimitError(...)}` / `mock_response="..."` per deployment, or
- respx on the provider HTTP endpoints.

Tests must not hit the network. The network guard stays on. Timing assertions use small `num_retries`/`retry_after` values or monkeypatched `asyncio.sleep`; no fake-clock internals.
