# AI Signal ingestion: architecture (3 pages)

Source for `architecture.pdf` (render: `signal-ingest render-pdf`). Each `## Page N` heading starts a new PDF page. Every `{{TODO_MEASURED: ...}}` marks a number that must come from a captured artifact; none are estimated here.

## Page 1 · Pipeline topology and 500k projection

### Topology

Sources → discover → PostgreSQL task queue (`FOR UPDATE SKIP LOCKED` leases with fencing tokens) → bounded async HTTPX pool → content-addressed raw store (`content_key` under one mode-scoped root) → parse / enrich → candidates and typed relationships → grounded LLM extraction → deterministic entity resolution → six-tab export (CSV + `.xlsx` snapshot, imported into a view-only Google Sheet).

- **Bulk adapters:** YC directory, AI product directory, paper/repo archive, arXiv, GitHub metrics (star snapshot per refresh interval).
- **Monitors:** five news sources and five job boards, each with a per-source run ledger, watermark and pagination-termination record.
- **Bounded workers:** N slots per process; global active and pending caps, per-origin concurrency 2 and 50 ms pace; body size caps; heartbeat lease renewal and a reaper for dead workers.
- **Shared state:** workers share one database and one mounted raw root. A second worker reopens an observation from its persisted `content_key` alone.

### Measured throughput (loopback fixture benchmark)

Source: `artifacts/gates/b7_benchmark.json`, mode `fixture_loopback`: a local HTTP fixture server driven through the real `work` loop and PostgreSQL queue. This is not web-scrape throughput.

| Run | Workers | Tasks/s | p95 fetch ms | Bytes/item |
| --- | --- | --- | --- | --- |
| 1k | 1 | {{TODO_MEASURED: tps}} | {{TODO_MEASURED: p95}} | {{TODO_MEASURED: bytes}} |
| 1k | 4 | {{TODO_MEASURED: tps}} | {{TODO_MEASURED: p95}} | {{TODO_MEASURED: bytes}} |
| 10k | 1 | {{TODO_MEASURED: tps}} | {{TODO_MEASURED: p95}} | {{TODO_MEASURED: bytes}} |
| 10k | 4 | {{TODO_MEASURED: tps}} | {{TODO_MEASURED: p95}} | {{TODO_MEASURED: bytes}} |

### 500k projection

\[
T(500k) >= max( W_stage / theta_eff,
               N_requests / R_source_limit,
               N_tokens / R_provider_tokens ) + T_cold_start
\]

- **Local pipeline bound:** 500k ÷ {{TODO_MEASURED: 4-worker tasks/s}} = {{TODO_MEASURED: hours}}.
- **Upstream bound:** per-source published limits × live yield {{TODO_MEASURED: accepted/fetched per class}}; retries add {{TODO_MEASURED: retry rate}}.
- **Provider bound:** {{TODO_MEASURED: LLM-eligible share}} × tokens/item ÷ free-tier daily token quota.
- **Raw storage:** {{TODO_MEASURED: compressed bytes/item}} × 500k + indexes + retention = {{TODO_MEASURED: GB}}.
- Scope: 500k candidates per class or in total, as stated with the measured coefficients. More workers help until the source or provider limit dominates.

## Page 2 · Budgets, 413/429 and the provider chain

### Byte and token arithmetic

\[
allowable_input = context_window - reserved_output
                  - (instructions + schema + framing) - safety_margin
\]

The allowable input is further capped by the provider's input-per-request limit and by the transport byte cap of the fully serialized request. Tool and schema overhead count once. Chunks split by semantic section, then paragraph, then safe boundaries, keeping evidence offsets and overlap. Each fallback recounts the final payload for that provider.

| Provider tier | Context | Max input | Reserved output | Daily quota |
| --- | --- | --- | --- | --- |
| Gemini (free) | {{TODO_MEASURED: from console}} | {{TODO_MEASURED}} | {{TODO_MEASURED}} | {{TODO_MEASURED}} |
| Groq (free) | {{TODO_MEASURED: from console}} | {{TODO_MEASURED}} | {{TODO_MEASURED}} | {{TODO_MEASURED}} |
| Ollama (local) | {{TODO_MEASURED: model card}} | {{TODO_MEASURED}} | {{TODO_MEASURED}} | local |

### Fallback states: 413 and 429

| Signal | Behavior |
| --- | --- |
| 413 / context-length 400 | Re-budget and split smaller; strict max depth and minimum chunk; never resend unchanged bytes |
| 429 with Retry-After | Persist `next_attempt` at the server time or fall over to the next tier; never shorten a cooldown |
| 429 / 5xx without guidance | Full jitter in [0, min(cap, base × 2^attempt)], bounded attempts and wall clock across tiers |
| 401/403 credential | Disable tier for the run (not a quota event) |
| Quota exhausted | Deny dispatch before the call; abstain or queue |
| Circuit open | Shared durable cooldown; one half-open probe |

### LiteLLM Router and LangGraph chain

LangGraph extraction graph: `budget → segment → cache_lookup → plan → call_llm (⇄ rebudget) → merge → finalize`. `call_llm` goes through a LiteLLM Router chain Gemini → Groq → optional Ollama, with paid spend cap 0.

**Quota hook.** A Router `CustomLogger` reserves requests and tokens in the shared quota store before every dispatch (`async_pre_call_check`). It refuses unreserved dispatch, raises a context error when the measured request exceeds the tier limit, and turns a quota denial into a Router rate-limit so the tier cools down and the chain falls over. It also checks that the Router did not rewrite the grounded messages.

**Example trace (redacted).** {{TODO_MEASURED: live trace id, tier sequence, tokens, latency from captured transcript}}

## Page 3 · Identity, freshness, storage and results

### Leases, idempotency and self-healing

- Semantic task keys with unique constraints make duplicate discovery a no-op. One observation per task (`UNIQUE(task_id)`).
- Lease fencing tokens: a stale worker cannot commit after its lease is reclaimed. A crash after the blob write and before commit reuses the content hash.
- Self-healing: heartbeat renewal, reaper (retry or dead-letter at max attempts), pooled connections replaced when dropped, backoff on DB errors, SIGTERM drains in-flight work.
- Measured recovery: {{TODO_MEASURED: kill -9 restart time and committed-effect count}}.

### Freshness

Export cutoff T. A news or job row is admitted only if its whole publication interval lies within [T − 24h, T]. Original structured dates are preferred; conflicts go to quarantine. Release cutoff: {{TODO_MEASURED: UTC and IST}}. Star snapshot age: {{TODO_MEASURED: hours at cutoff}}.

### Storage trade-offs

- **PostgreSQL** holds exact identity, metrics history, leases and typed, time-bounded relationship edges (`OWNS`, `IMPLEMENTED_BY`, `ALIASED_AS` ...).
- **pgvector** (384-dim HNSW on entities) only proposes resolution candidates. Deterministic rules decide merges, so an embedding never causes a false merge.
- A separate graph database or vector service was not adopted: the sync and recovery cost outweighs the multi-hop queries this dataset needs.

### Results and gaps

| Item | Value |
| --- | --- |
| Rows per tab at release | {{TODO_MEASURED: six tab counts}} |
| Bulk classes ≥ 1,000 rows | {{TODO_MEASURED}} |
| Entity resolution precision / recall | {{TODO_MEASURED: from b6_metrics.json and live register}} |
| Grounded LLM rows | {{TODO_MEASURED: count and traced example}} |
| Source scans complete / partial | {{TODO_MEASURED}} |

**Known gaps.** {{TODO_MEASURED: sources blocked or partial, unresolved fresh items with reasons}}.
