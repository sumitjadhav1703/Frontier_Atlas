# B8 work order: live acquisition (coordinator-owned)

Goal: `tests/gates/test_b8.py` passes on real data.
- The newest live export has ≥1000 unique rows after dedup in each of Startups, Products and Research_Papers.
- Real LLM lineage lands in a Products row.
- There is no fixture lineage, and the live seeds register has ≥50 entries.

The planner's map of the gaps (summary):
- `pipeline/crawl.py:103` refuses live mode.
- The live fetch stage GETs every item URL; YC/paper item URLs are useless or defunct.
- Papers have no pagination or join.
- AgiPicks rows have no owner, so every product fails `missing_ownership_evidence`.
- `PILOT_LIMIT=30`.

## Rules
- Never read, cat, grep, open or print `.env`. Live code may call `dotenv.load_dotenv(".env", override=False)` at runtime. Keys never appear in logs, manifests, raw blobs or test output.
- Paid spend 0.
- Honor robots.txt and documented endpoints only. Use the pool's per-origin pacing and the existing UA.
- Never fabricate. Every exported field traces to a fetched source URL. Rows that lack required evidence are quarantined, never padded.
- Live and fixture stores stay separate. Live DSN: `postgresql://signal:signal@127.0.0.1:5432/signal_live`. Run live processes with `SIGNAL_INGEST_MODE=live SIGNAL_INGEST_NETWORK_GUARD=0 SIGNAL_INGEST_DSN_LIVE=<live DSN>`, set in the process env.

## Sources (probed live 2026-11-22)
- **Startups:** `https://yc-oss.github.io/api/tags/ai.json` (~1900 rows). One listing GET. Rows are taken from the listing body (row shortcut); there is no per-company GET.
- **Products, pool 1 — AgiPicks `https://agipicks.com/data/tools.json`** (CC BY 4.0, 1166 tools, all have `website`).
  - Pricing comes from the attributed directory field `pricingModel`, with evidence URL = the tool's `url` (agipicks page) and `pricesCheckedAt`.
  - Our contract (`docs/contracts.md:16`) says "contact us alone" and an open-source label are insufficient evidence. Keep `contact-sales` and `open-source` UNMAPPED (quarantine). Do NOT map them.
  - Owner: GET the vendor `website` homepage (one per tool) and extract the owner deterministically, in this order: (1) JSON-LD `Organization`/`Corporation` `legalName` or `name` (also `publisher`/`author`/`provider` of a WebSite/SoftwareApplication node, including `@graph`), (2) the footer copyright holder (`©|&copy;|Copyright` [years] `<Holder>` [Inc./LLC/Ltd/GmbH…]).
  - If there is no deterministic owner, fall back to the LLM: `extract()` via `build_chain()` with `fields_requested=("owner_company",)` on that homepage HTML. Accept only grounded ACCEPTED evidence. Mark the owner evidence `method=llm` with provider/model.
  - Owner evidence URL = the homepage final URL.
- **Products, pool 2 — OpenRouter `https://openrouter.ai/api/v1/models`** (public, no auth, robots Allow /; 476 models, 380 unique base ids, 68 owners). New adapter `bulk.openrouter`.
  - Product = base model id (strip `:free`/`:…` variants; plan tiers don't count separately).
  - Owner = the attributed directory display prefix of `name` before ": " (e.g. "Thinking Machines: Inkling 2 Mini" → "Thinking Machines"), with owner evidence URL = `https://openrouter.ai/<id>`. If `name` has no prefix, quarantine.
  - Pricing: all variants priced >0 → PAID; only free variants → FREE; both → FREEMIUM. Evidence = the same model page URL plus the API listing URL.
  - productName = the name after the prefix.
- **Papers:** HF datasets-server for `pwc-archive/links-between-paper-and-code` and `pwc-archive/papers-with-abstracts`.
  - Prefer `/filter?where="is_official"=true` paged at 100 rows on links, then fetch the matching papers. If `/filter` errors ("index is loading"), retry with backoff; else page `/rows` (length ≤100).
  - Join on `paper_url` (or arXiv id) locally. Collect ≥1600 joined candidates so ≥1000 survive filters.
  - GitHub stars via the existing `enrich_stars` stage (REST, Bearer GITHUB_TOKEN; 5000/h).
- **Seeds:** build ≥50 live seeds from accepted STARTUP rows via `live/seeds.py` (`build_live_seeds_from_startup_rows`, `write_live_seed_register`).

## Code changes (implementer)
- `live/bulk.py`: `fetch_bulk_listing(source_id, settings)` returns rows stamped `_listing={url, sha256, fetched_at}`, with bodies saved via `write_probe_artifact`. Remove the `PILOT_LIMIT` cap from the acquisition path (pilots may keep it).
- `pipeline/crawl.py`: the live branch fetches the listing, then discovers and enqueues into one shared run (`--run`).
- `adapters/bulk/pipeline.py`: the live row shortcut applies when `row["_listing"]` is present (yc, papers, openrouter). Products still GET the vendor homepage (fetch stage) for owner evidence.
- `adapters/bulk/products.py` + `mapping.py`: owner-from-HTML helper; LLM fallback hook; discovery URL = `website`; pricing evidence URL = agipicks `url`.
- New `adapters/bulk/openrouter.py` + its registration (pipeline `get_adapter`, `BULK_SOURCES`, `config/sources/*.yaml` inventory entry).
- `live/canary.py`:
  - Take one accepted product whose owner came from the LLM fallback. If none exists, run extract() on one fetched product homepage.
  - Write `var/live/manifests/llm_lineage.json` with `mode=live`, `live_llm_success=true`, `provider` (bare tier name, e.g. `gemini`), `model`, `durable_identity` (= the export's durableIdentity for that row), `canonical_entity_id`, `accepted_field_evidence`, a redacted transcript, `paid_spend_cap=0` and `_meta={label: live, not_for_live_export: false}`.
  - Flip `llm_canary.json`.
- `cli/main.py`: `qualify --track canary --run`, plus any flag needed to build seeds from a run.
- Blocker rewrite helper: `artifacts/gates/b8_blocker.json` recomputed from the actual export (claimed counts must equal recomputed ones).

## Tests (verifier, offline only)
Add to `tests/gates/test_b2.py`, which reopens B2:
- `fetch_bulk_listing`/row-shortcut with a loopback fixture server
- OpenRouter discover/parse: prefix owner, variant collapse to FREE/PAID/FREEMIUM, no-prefix quarantine
- products owner-from-HTML: JSON-LD Organization, `@graph` publisher, © footer, no-owner quarantine
- `contact-sales`/`open-source` stay quarantined
- papers join on paper_url
- LLM owner fallback accepts only grounded evidence (FakeProvider)

Fixtures go under `tests/fixtures/bulk/` labelled synthetic.
