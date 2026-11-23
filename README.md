# AI Signal · signal-ingest

Assessment pipeline for startups, products, research papers, jobs, and news.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
docker compose up -d postgres
# Or brew postgresql@16 on :5432 with SIGNAL_INGEST_DSN_* overrides (see .env.example).
signal-ingest init-db
```

Default mode is `fixture` with a separate DSN, raw root, and export root from `live`.
Network guard blocks non-loopback sockets unless explicitly disabled for live stages.
Paid spend cap defaults to 0. Live Sheets / provider calls stay off through B7.

## Fixture replay (reviewer-ready)

One mixed offline path through discover → work → resume → audit → export:

```bash
signal-ingest --mode fixture init-db

# Discover from labeled local fixtures (creates a run).
signal-ingest --mode fixture crawl --source bulk.yc
# Capture run_id from JSON, then:
export RUN=<run_id-from-crawl>

# Dual workers share Postgres + SIGNAL_INGEST_RAW_ROOT_FIXTURE via content_key.
signal-ingest --mode fixture work --stage fetch --run "$RUN" --worker-id w1 --max-tasks 50
signal-ingest --mode fixture work --stage parse --run "$RUN" --worker-id w2 --max-tasks 50

# Kill/restart equivalent: release stale leases and continue.
signal-ingest --mode fixture resume --run "$RUN"

signal-ingest --mode fixture audit --run "$RUN"
signal-ingest --mode fixture export --run "$RUN" --as-of 2026-11-21T12:00:00Z

# Synthetic scale baseline (1k/10k × 1/4 workers) — not live throughput.
signal-ingest --mode fixture bench
```

Other fixture sources: `bulk.products`, `bulk.papers`, `bulk.arxiv`, `bulk.github`,
and monitor IDs such as `news_mit` / `jobs_aijobs` when fixture payloads exist under
`tests/fixtures/`.

Fixture exports land under `var/fixture/exports/<run>/<as_of>/` with a visible
`LABEL.txt` / `NOT_FOR_LIVE_EXPORT` marker. Live export roots stay separate.

## CLI

| Command | Purpose |
| --- | --- |
| `signal-ingest --mode fixture\|live init-db` | Apply migrations for the active mode DSN |
| `crawl --source ID [--run UUID]` | Fixture discover + enqueue |
| `work --stage STAGE --run ID [--worker-id …]` | Claim/process under shared raw root |
| `resume --run ID` | Release stale leases and continue |
| `audit --run ID` | Counts, lineage, quarantines, content_key coverage |
| `export --run ID --as-of UTC` | Six tabs → segregated local export (+ mock Sheets in fixture) |
| `bench` | Write `artifacts/gates/b7_benchmark.json` |
| `qualify --track …` | Live qualification (B8; requires `--mode live`) |

Exact six tab names: Startups, Products, Research Papers, Jobs, News, Entity Mapping Log.

## Gates

```bash
.venv/bin/python tools/gate.py status
.venv/bin/python tools/gate.py B7   # last closed offline gate
# B8 remains open until live ≥1000 + LLM canary; see artifacts/gates/b8_blocker.json
```

## Architecture

- Source: [docs/architecture-source.md](docs/architecture-source.md)
- PDF (≤3 pages): [architecture.pdf](architecture.pdf)
- Live measurements (bytes/item, yield, RPS, token latency, ≥1000 counts): **PENDING** — do not invent numbers.

## R4 · pgvector suggestions and typed relationships

- `migrations/009_r4_vector.sql` adds `entities.embedding vector(384)` + `embedding_model` (HNSW cosine index) and `relationships.evidence_observation_id`, `valid_from`, `valid_to`, plus a CHECK on the relationship types.
- `resolution/embed.py`: `embed_entities(conn, FastEmbedder())` embeds name + description with `BAAI/bge-small-en-v1.5`. `suggest_candidates(...)` returns HNSW top-k candidates above 0.85 cosine.
- Embeddings only suggest. Deterministic rules make every merge, so the B6 FP=0 gate is unchanged. `annotate_mapping_suggestions` fills the Entity Mapping Log `suggested_candidates` column.
- `flag_near_duplicate_news` sets `nearDuplicateOf` on same-run News rows with title cosine ≥0.95. It never drops a row.
- Both columns appear in exports only when embeddings produced a value. Exports without embeddings are byte-identical to the earlier ones.
- Graph demo: `docs/queries/papers_by_yc_startups.sql` (a `WITH RECURSIVE` query over relational edges, up to 6 OWNS hops), run by `resolution.graph_queries.papers_by_yc_startups(conn)`. It is tested on synthetic edges. The bulk adapters do not yet write organization→repository OWNS edges, so on live data it returns rows only after such edges exist.

## Phase V · Challenged monitor case (`news_arstechnica`)

High-value news source with an observed detail-route challenge during B8 qualification.

| Field | Observed |
| --- | --- |
| Source | `news_arstechnica` |
| Discovery | RSS `https://feeds.arstechnica.com/arstechnica/index` → HTTP 200, ordered feed, 20 items scanned |
| Detail block | Article HTML hit AWS WAF captcha (`x-amzn-waf-action: captcha`) and returned HTTP 405 / invalid body |
| Classification | Challenge / blocked detail route — **not** solved; no captcha-bypass dependency |
| Permitted alternate | Keep RSS discovery + watermark; skip captcha HTML; use another qualified publisher for full-body coverage when detail is required |
| Sample traced release row | **PENDING** (no live release export yet) |
| Coverage lost | Full HTML body + original page evidence for challenged article URLs until an approved alternate detail path exists |

Evidence: `var/live/manifests/monitors_qualification.json` (`source_id=news_arstechnica`). Fixture challenge handling remains covered in B1.

## B9 release handoff (local scaffold)

**Status:** `awaiting_b8` (B8 blocked). After B8 closes without a published Sheet, status becomes `awaiting_publication`. Do **not** report **ready** or **submitted** until viewer checks and form receipt exist.

| State | Meaning |
| --- | --- |
| ready | Local packet complete **and** public Sheet verified (workbook_id + two signed-out checks) — packet prepared, form **not** yet sent |
| submitted | Applicant completed [submission form](https://forms.gle/3JJhukomumTFueRW7) with verified repo + Sheet links; keep form receipt separately |

Manifest: `artifacts/release/release_manifest.json` (`workbook_id: null`, viewer checks `pending`).

### Commands (local only — no publish)

```bash
# Offline smoke (clean checkout style)
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
signal-ingest --mode fixture init-db
# … fixture replay commands above …

# Refresh local release scaffold after PDF/docs change
.venv/bin/python -c "
from signal_ingest.release import (
    build_scaffold_manifest, write_release_manifest,
    render_architecture_pdf, sha256_file, count_pdf_pages,
)
p = render_architecture_pdf()
m = build_scaffold_manifest(
    status='awaiting_b8',
    pdf_sha256=sha256_file(p),
    pdf_pages=count_pdf_pages(p),
    repo_rev=None,
)
print(write_release_manifest(m))
"
```

### Ready vs still pending

**Ready locally (scaffold):** `README.md`, `src/`, `architecture.pdf`, architecture source, B0–B7 receipts, B8 blocker + live pilot manifests, release manifest writer.

**Not ready (do not fabricate):** live ≥1000/class export, live LLM→row lineage, `workbook_id` / Sheet URL, signed-out viewer checks, form submit.

Submission form (applicant-owned): https://forms.gle/3JJhukomumTFueRW7
