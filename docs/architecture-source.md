# Architecture source (B9 scaffold)

This draft feeds `architecture.pdf` (≤3 pages). **All live measurements remain PENDING until B8 closes and a release cutoff exists.** Synthetic bench figures are labeled and must not be cited as live scrape throughput. Do not invent ≥1000 row counts or live RPS/token numbers.

## Page 1 · Pipeline topology and scale

**Topology.** Sources → discover → PostgreSQL task queue (`FOR UPDATE SKIP LOCKED` leases + fencing) → async HTTPX fetch pool → content-addressed raw store (`content_key` under a shared mode-scoped root) → parse / enrich → candidates + relationships → grounded LLM extract → deterministic resolution → six-tab export projection.

**Adapters.** Bulk: YC, products, papers, arXiv, GitHub metrics. Monitors: five news + five job boards. Workers share one DB and one mounted raw root; temporary working directories may differ. A second worker reopens an observation using only the persisted relative `content_key`.

**500k projection (formula; live coefficients PENDING).**

\[
T \ge \max\!\left(\frac{W_{\text{stage}}}{\theta_{\text{eff}}},\; T_{\text{rate-limited requests}},\; T_{\text{provider tokens}}\right)
\]

Include cold start, rejection yield, and retries. Raw storage ≈ measured compressed bytes/item × candidates + indexes + retention. More workers help until source/provider limits dominate.

| Item | Status |
| --- | --- |
| Synthetic 1k/10k × 1/4 workers | See `artifacts/gates/b7_benchmark.json` (**synthetic**) |
| Live bytes/item, yield, request RPS | **PENDING live evidence (B8)** |
| Live ≥1000 accepted / bulk class | **PENDING** (pilots only; see `b8_blocker.json`) |
| Optional 500k synthetic stress | Not default; never label as web scrape |

## Page 2 · LLM budgets, 413/429, circuits

**Budget arithmetic.** Allowable input = context − reserved output − instructions/schema/framing − safety margin; also capped by provider input-per-request and serialized transport bytes. Split by section→paragraph; recount on each fallback.

**Fallback.** Gemini → Groq → optional Ollama. Parse Retry-After; full jitter otherwise. Shared durable cooldown + circuit breaker (one half-open probe). Paid spend cap = 0 offline; replay/fake ≠ live qualification.

**Example trace.** Offline fake-provider transcripts under B4/B5 gates. **Live redacted trace: PENDING B8** (no free keys / Ollama unreachable at last qualification).

| Hard limit | Behavior |
| --- | --- |
| 413 / context 400 | Re-budget + smaller split (bounded depth) |
| 429 | Honor server delay; never shorten cooldown |
| Quota exhausted | Deny dispatch; abstain or queue |

## Page 3 · Identity, freshness, storage trade-offs

**Distributed identity.** Semantic task keys + unique constraints; lease fencing tokens; idempotent candidate upserts; resolution observation keys. Crash after blob write / before commit reuses content hash; stale fences cannot commit.

**Freshness.** Export cutoff \(T\); admit signals iff entire publication interval ⊆ \([T-24h, T]\). Prefer original structured dates; quarantine conflicts. Release cutoff UTC/IST: **PENDING** until B9 refresh after B8.

**Storage.** PostgreSQL for exact identity, metrics history, leases, and relational edges (`OWNS`, `IMPLEMENTED_BY`). Optional pgvector for candidate recall or Neo4j for multi-hop graphs would add sync/recovery cost — trial keeps exact store in Postgres.

**Known gaps / trial status.** Offline B0–B7 PASS. B8 BLOCKED: no live LLM canary, no ≥1000 live export, seeds 30/50. Public Sheet read-back and signed-out viewer checks: **PENDING** (`workbook_id` null). Fixture exports remain segregated and labeled `not_for_live_export`.

## Replay (fixture)

```bash
signal-ingest --mode fixture init-db
RUN=$(signal-ingest --mode fixture crawl --source bulk.yc | python3 -c 'import sys,json; print(json.load(sys.stdin)["run_id"])')
signal-ingest --mode fixture work --stage fetch --run "$RUN" --worker-id w1
signal-ingest --mode fixture work --stage parse --run "$RUN" --worker-id w2
signal-ingest --mode fixture resume --run "$RUN"
signal-ingest --mode fixture audit --run "$RUN"
signal-ingest --mode fixture export --run "$RUN" --as-of 2026-11-21T12:00:00Z
signal-ingest --mode fixture bench
```
