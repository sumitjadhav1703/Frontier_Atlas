# AI Signal ingestion: architecture (3 pages)

Source for `architecture.pdf` (render: `signal-ingest render-pdf`). Each `## Page N` heading starts a new PDF page. Every number below comes from a captured artifact of live run `a4fee584` or the gate suite; none are estimated.

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
| 1k | 1 | 112.9 | 4.6 | 148.8 |
| 1k | 4 | 147.7 | 48.3 | 148.8 |
| 10k | 1 | 89.1 | 5.2 | 150.8 |
| 10k | 4 | 147.2 | 42.3 | 150.8 |

### 500k projection

\[
T(500k) >= max( W_stage / theta_eff,
               N_requests / R_source_limit,
               N_tokens / R_provider_tokens ) + T_cold_start
\]

- **Local pipeline bound:** 500k tasks ÷ 147 tasks/s (4 workers, loopback, per-origin pace bound) ≈ 0.95 h per stage; about 3 stages per item (fetch, parse, enrich) ≈ 2.8 h. Live run a4fee584: 11,948 tasks for 4,757 accepted bulk rows.
- **Upstream bound:** live yield (accepted/discovered): startups 1906/1906, AgiPicks products 850/1166 (owner and pricing evidence both required), OpenRouter 345/366, papers 1656/1700. Retries: 2 of 11,948 tasks (0.02%). GitHub REST at 5,000 req/h per token caps stars at 500k ÷ 5,000 = 100 h; GraphQL batches of 100 repos bring that to about 1 h.
- **Provider bound:** 17% of AgiPicks homepages (175/1043) needed the LLM owner fallback, at about 8k input tokens each. At 500k products that is about 85k calls ≈ 0.7B tokens, far above a 1M-token/day free tier. Bulk rows therefore come from structured fields, and the LLM is reserved for fields that need it (paid or local tiers at scale).
- **Raw storage:** measured mean bytes/item: JSON listing rows 0.6–6 KB, vendor homepages 427 KB (raw store 497 MB for this run). 500k homepages ≈ 213 GB raw; storing extracted text and hashes instead of full HTML cuts this by more than 10×.
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
| Gemini 2.5 Flash (free) | 1,048,576 | 1,000,000 | 2,048 | ledger cap 200 req / 1M tok |
| Groq gpt-oss-120b (free) | 131,072 | 5,500 (8,000 TPM measured) | 1,024 | 1,000 req (header); ledger 900 |
| Meta muse-ember-1.0 (billed credits) | 1,048,576 | 200,000 | 2,048 | $0.10/$0.50 per 1M; spend cap $5 |
| Ollama (local) | 8,192 | 6,912 | 1,024 | disabled: not installed |

### Fallback states: 413 and 429

| Signal | Behavior |
| --- | --- |
| 413 / context-length 400 | Re-budget and split smaller; strict max depth and minimum chunk; never resend unchanged bytes |
| 429 with Retry-After | Persist `next_attempt` at the server time or fall over to the next tier; never shorten a cooldown |
| 429 / 5xx without guidance | LiteLLM backoff 0.5·2^n s (cap 8 s) + up to 0.75 s jitter, bounded retries per tier, then fall over |
| 401/403 credential | Disable tier for the run (not a quota event) |
| Quota exhausted | Deny dispatch before the call; abstain or queue |
| Repeated failures | Router cooldown per tier (allowed_fails, cooldown_time); Retry-After > 15 s cools the tier without sleeping |

### LiteLLM Router and LangGraph chain

LangGraph extraction graph: `budget → segment → cache_lookup → plan → call_llm (⇄ rebudget) → merge → finalize`. `call_llm` goes through a LiteLLM Router chain Gemini → Groq → Meta Model API (muse-ember-1.0, `reasoning_effort` minimal) → optional Ollama. Free tiers come first. The billed Meta tier must carry a per-token cost estimate, and the ledger enforces a $5 cap.

**Quota hook.** A Router `CustomLogger` reserves requests and tokens in the shared quota store before every dispatch (`async_pre_call_check`). It refuses unreserved dispatch, raises a context error when the measured request exceeds the tier limit, and turns a quota denial into a Router rate-limit so the tier cools down and the chain falls over. It also checks that the Router did not rewrite the grounded messages.

**Example trace (redacted).** Live run a4fee584, 2026-11-22T20:32Z, product `agent-crm`: tier sequence gemini (ok), 8,033 input / 41 output tokens. Owner "Agent CRM" was accepted only because its quote (`author: Agent CRM`) sits in segment `seg:0002:meta` of the fetched page. In the owner-fallback batch (40 attempts), Gemini's free per-minute limit triggered 429 cooldowns; the chain fell over to Groq, where 3 requests above the 5,500-token tier limit raised a context error and were not dispatched. Ledger: Gemini 7 and Groq 3 real dispatches, $0.

## Page 3 · Identity, freshness, storage and results

### Leases, idempotency and self-healing

- Semantic task keys with unique constraints make duplicate discovery a no-op. One observation per task (`UNIQUE(task_id)`).
- Lease fencing tokens: a stale worker cannot commit after its lease is reclaimed. A crash after the blob write and before commit reuses the content hash.
- Self-healing: heartbeat renewal, reaper (retry or dead-letter at max attempts), pooled connections replaced when dropped, backoff on DB errors, SIGTERM drains in-flight work.
- Measured recovery (`artifacts/gates/r1_kill9_demo.txt`): after `kill -9` mid-run, 4 leased + 8 ready tasks were reclaimed; the healer finished 12/12 in 11.2 s with 0 duplicate observations.

### Freshness

Export cutoff T. A news or job row is admitted only if its whole publication interval lies within [T − 24h, T]. Original structured dates are preferred; conflicts go to quarantine. Release cutoff: 2026-11-23T08:00:18Z (13:30:18 IST). Star snapshot age at cutoff: about 11.7 h (stars observed 2026-11-22 ≈20:20Z).

### Storage trade-offs

- **PostgreSQL** holds exact identity, metrics history, leases and typed, time-bounded relationship edges (`OWNS`, `IMPLEMENTED_BY`, `ALIASED_AS` ...).
- **pgvector** (384-dim HNSW on entities) only proposes resolution candidates. Deterministic rules decide merges, so an embedding never causes a false merge.
- A separate graph database or vector service was not adopted: the sync and recovery cost outweighs the multi-hop queries this dataset needs.

### Results and gaps

| Item | Value |
| --- | --- |
| Rows per tab at release | Startups 1,906 · Products 1,195 · Research Papers 1,656 · Jobs 4 · News 14 · Entity Mapping Log 3,101 |
| Bulk classes ≥ 1,000 rows | Yes, recomputed from the export CSVs by `b8-status` (unique durableIdentity, lineageMode=live) |
| Entity resolution precision / recall | 1.00 / 1.00 on the 27-case gate set (TP 15, FP 0, FN 0, 10 abstentions). Live: 103 seeds (50 YC + 53 OpenRouter-attributed labs), 3,101 mentions → 388 match, 859 abstain, 1,854 new |
| Grounded LLM rows | 2 product owners extracted by Gemini, each quote-grounded; e.g. `agent-crm` → "Agent CRM" (`author: Agent CRM`). All tiers (Gemini, Groq, Meta) verified live |
| Source scans complete / partial | Bulk: 4/4 complete. Monitors: 10 configured; news fresh from Guardian 11, TechCrunch 3, MIT 1, ScienceDaily 1. Jobs: 4 fresh via the Ashby feed (25 AI-company boards) |

**Known gaps.** Ars Technica article pages sit behind an AWS WAF captcha (not bypassed). aijobs.net robots disallows `/job/`. One AI-community board renders client-side. aiworkportal postings have no publish time. inferencejobs and 495 Ashby postings lack a source-backed remote flag (quarantined, not guessed). Paper dates are kept only inside the arXiv v1 month, else `YYYY-MM`. Three relaunch duplicates remain among products (1,192 unique).
