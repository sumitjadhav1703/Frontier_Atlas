# AI Signal · signal-ingest

A resumable ingestion pipeline for AI startups, products, research papers, jobs and news. It has a grounded, multi-tier LLM extractor (LangGraph + LiteLLM Router) and deterministic entity resolution. Every exported row traces back to a fetched source URL.

## Results (live run `a4fee584`, 2026-11-22/23)

| Tab | Rows | Source |
| --- | --- | --- |
| Startups | 1,906 | yc-oss `tags/ai.json` (YC company directory mirror) |
| Products | 1,195 | AgiPicks open dataset (CC BY 4.0): pricing from its attributed field, owner from the vendor's own homepage. OpenRouter public model directory: owner and pricing from the listing |
| Research Papers | 1,656 | pwc-archive paper→code links (official repos only), OpenAlex metadata (CC0), GitHub REST stars |
| Jobs | 4 | 24 h fresh at cutoff 2026-11-23T08:00:18Z. Exact `publishedAt` and source-backed remote flag from the Ashby feed (25 AI-company boards); 5 AI job boards monitored |
| News | 14 | 24 h fresh at the same cutoff: Guardian 11, TechCrunch 3, MIT News 1, ScienceDaily 1 (Ars Technica detail blocked by WAF) |
| Entity Mapping Log | 3,101 | raw name → canonical ID, method, decision, embedding suggestions |

Offline gates B0–B8 pass: 379 cumulative cases under `TZ=UTC` and `TZ=Asia/Kolkata`. Entity resolution on the gate set scores precision 1.0 / recall 1.0 (FP 0).

## Architecture

```
sources ─▶ discover ─▶ PostgreSQL task queue (SKIP LOCKED leases, fencing, heartbeat,
                        reaper, dead-letter) ─▶ async fetch pool (robots, per-origin pacing,
                        challenge detection, capped decode) ─▶ content-addressed raw store
        ─▶ parse / enrich (GitHub stars) ─▶ candidates + typed relationships
        ─▶ grounded LLM extract (LangGraph graph → LiteLLM Router chain)
        ─▶ deterministic entity resolution (+ pgvector suggestions only)
        ─▶ six-tab export (CSV + .xlsx) at a UTC cutoff
```

- **LLM orchestration** (`src/signal_ingest/llm/`)
  - `graph.py` runs extraction as a LangGraph `StateGraph`: `budget → segment → cache_lookup → plan → call_llm ⇄ rebudget → merge → finalize`. A 413 or context-length error loops back to a smaller re-chunk, up to a fixed depth, and never re-sends unchanged bytes.
  - `router.py` (`LiteLLMChain`) sends calls through a LiteLLM 1.108 LTS Router with native fallbacks. The tiers are **Gemini 2.5 Flash → Groq gpt-oss-120b → Meta Model API muse-ember-1.0**. Ollama is optional.
  - On 429 it backs off exponentially with jitter. A Retry-After of 15 s or less is honored; a longer one cools the tier and falls over.
  - Chunks are sized to the tightest tier.
  - A Router `async_pre_call_check` hook reserves quota in the shared PostgreSQL ledger before every real dispatch. A billed route without a cost estimate is refused, and total spend is capped (`paid_spend_cap` in `config/providers.yaml`).
- **Grounding**: a model value is accepted only if its quote is inside a cited, retained page segment, the quote is about the target entity, and the value appears in the quote. Everything else is rejected, never guessed.
- **Entity resolution** (`src/signal_ingest/resolution/`):
  - Normalization folds legal suffixes, accents and spacing, so "OpenAI", "OpenAI, Inc." and "Open AI" all resolve to OpenAI.
  - Trusted source IDs (YC id, OpenRouter owner key) and approved aliases decide matches. Weak single-word names need corroboration.
  - The live register holds 103 seeds: 50 YC startups plus 53 labs attributed in OpenRouter's directory.
  - pgvector (bge-small, HNSW) only proposes `suggested_candidates`; it never merges.
- **Freshness**: a news/job row is exported only if its whole publication interval lies inside [cutoff − 24 h, cutoff]. Relative dates ("2 hours ago") are anchored to the listing fetch time. A row with no date can be flagged "new since last run" but is never exported as fresh.
- **Self-healing**: lease heartbeat, reaper, dead-letter + `requeue-failed`, SIGTERM drain. In the measured kill -9 recovery, 12/12 tasks were done in 11.2 s with 0 duplicate observations.
- Full design: [architecture.pdf](architecture.pdf) (3 pages), source [docs/architecture-source.md](docs/architecture-source.md).

## Setup

Requires Python 3.13 (litellm's clean releases declare `<3.15`), PostgreSQL 16 with pgvector, and `uv`.

```bash
uv venv --python 3.13 .venv
VIRTUAL_ENV=.venv uv pip sync --require-hashes requirements.lock
VIRTUAL_ENV=.venv uv pip install --no-deps -e .
cp .env.example .env            # then add your own keys; never commit .env
brew services start postgresql@16   # or: docker compose up -d postgres
# pgvector for PG16: build v0.8.7 from source with PG_CONFIG=$(brew --prefix postgresql@16)/bin/pg_config
.venv/bin/signal-ingest init-db
```

The default mode is `fixture`, with a separate DSN, raw root and export root from `live`. The network guard blocks non-loopback sockets except in live stages.

**Keys** (live only, read from `.env` at runtime):
- `GEMINI_API_KEY`, `GROQ_API_KEY` (free tiers)
- `MODEL_API_KEY` (Meta Model API, optional, billed)
- `GITHUB_TOKEN` (public star counts)

## Run

Offline replay, no network:

```bash
signal-ingest --mode fixture crawl --source bulk.yc          # prints run_id
signal-ingest --mode fixture work --stage any --run "$RUN" --drain
signal-ingest --mode fixture export --run "$RUN" --as-of 2026-11-21T12:00:00Z
```

Live acquisition (`scripts/b8_live_run.sh` has the full sequence):

```bash
export SIGNAL_INGEST_MODE=live SIGNAL_INGEST_NETWORK_GUARD=0
signal-ingest crawl --source bulk.yc                        # creates RUN
signal-ingest crawl --source bulk.openrouter --run "$RUN"   # also bulk.products, bulk.papers
signal-ingest crawl-monitors --run "$RUN"                   # 10 news/job monitors
signal-ingest work --stage any --run "$RUN" --concurrency 8 --drain
signal-ingest owner-llm --run "$RUN" --limit 40             # grounded LLM owner fallback
signal-ingest resolve --run "$RUN" --embed                  # entity resolution + suggestions
signal-ingest export --run "$RUN" --as-of "$(date -u +%FT%TZ)"
signal-ingest export-xlsx --snapshot var/live/exports/$RUN/<snapshot>
signal-ingest b8-status                                     # recompute counts from the export
```

Other commands: `resume`, `audit`, `requeue-failed`, `qualify`, `bench`, `render-pdf`.

## Tests and gates

```bash
SIGNAL_INGEST_MODE=fixture SIGNAL_INGEST_NETWORK_GUARD=1 .venv/bin/python -m pytest -q tests/gates
.venv/bin/python tools/gate.py status
```

Each block has a review in `notes/reviews/Bn.md`, and every attempt is logged in `notes/progress.md`, including failures and deviations.

## Phase V · challenged monitor case (`news_arstechnica`)

| Field | Observed |
| --- | --- |
| Discovery | RSS `https://feeds.arstechnica.com/arstechnica/index` → HTTP 200, ordered feed |
| Detail block | Article HTML hit an AWS WAF captcha (`x-amzn-waf-action: captcha`), HTTP 405 |
| Handling | Classified as challenged and **not** bypassed. Keep RSS discovery + watermark; take full-body coverage from other qualified publishers |
| Cost | Full body for challenged article URLs is missing until an approved alternate detail path exists |

Other observed limits: aijobs.net robots disallows `/job/`, so it is respected and skipped. One AI-community board renders its jobs client-side, so its HTML has no job links. Postings without a source-backed remote flag are quarantined, never guessed.

## Known deviations

- Paper `published_date`: OpenAlex merges preprints into journal works. Its date is kept only when it falls in the arXiv v1 month encoded in the ID; otherwise the export gives that month as ISO-8601 `YYYY-MM`.
- Three relaunch duplicates remain among products: 1,192 unique, still ≥ 1,000.
- The Meta tier uses the owner's prepaid credits under a $5 cap. All other tiers are free, local or fake.
- The graph demo query (`docs/queries/papers_by_yc_startups.sql`) is tested on synthetic edges. Live organization→repository OWNS edges are not yet extracted.
