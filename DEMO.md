# Demo video plan: Frontier_Atlas (signal-ingest)

A 6–8 minute walkthrough for the hiring team. Every scene maps to one of the brief's evaluation criteria, and every number below comes from a real artifact in this repo or the live run. Nothing is staged.

| Criterion (weight) | Where it is shown |
| --- | --- |
| LLM Orchestration (25%) | Scenes 5 and 6 |
| Data Quality (25%) | Scenes 2, 3 and 4 |
| Scale Thinking (20%) | Scene 8 |
| Engineering Rigor (20%) | Scenes 8 and 10 |
| Entity Resolution (10%) | Scene 7 |

Links:
- Repo: https://github.com/sumitjadhav1703/Frontier_Atlas
- Sheet: `<paste your public Google Sheet link>`
- Form: https://forms.gle/3JJhukomumTFueRW7

---

## 0. Pre-recording checklist (5 minutes)

1. Start Postgres with `brew services start postgresql@16`, then check it with `pg_isready -h 127.0.0.1 -p 5432`.
2. Open a terminal in the repo with a large font (18–20 pt) and a dark theme. Run `source .venv/bin/activate`.
3. **Never open, cat or scroll `.env` on screen.** Close any editor tab that shows it. Keys are only read inside the program.
4. Open these tabs beforehand:
   - The Google Sheet, with all 6 tabs visible.
   - `architecture.pdf`.
   - The GitHub repo page.
   - One source URL per tab, for tracing: a ycombinator.com company page, an AgiPicks tool page, an arXiv abs page, an Ashby job URL, and a Guardian or TechCrunch article.
5. Run every command block once before recording, so the outputs are warm and you know what appears.
6. Export these once (read-only queries on the live run):
   ```bash
   export RUN=a4fee584-3ac1-4188-95f4-19d7742dbd33
   export LIVE=postgresql://signal:signal@127.0.0.1:5432/signal_live
   ```

---

## Scene list

### 1. Intro and architecture (0:00–0:45)
- **Screen:** page 1 of `architecture.pdf` (topology).
- **Say:** "This is a resumable ingestion pipeline:
  - Sources feed a PostgreSQL task queue with leases and fencing tokens.
  - An async fetch pool stores raw snapshots by content hash.
  - Parsers and a grounded LLM extractor build the records.
  - Deterministic entity resolution canonicalizes names.
  - Six tabs are exported at a UTC cutoff.

  The same code runs on a laptop or on N worker machines. Only the infrastructure changes."

### 2. The data: six tabs in the Sheet (0:45–1:45)
- **Screen:** the Google Sheet. Click through each tab, with the header row frozen.
- **Numbers** (cutoff 2026-11-23T08:53:50Z, 14:23 IST; refresh from `README.md` after the final export):

  | Tab | Rows |
  | --- | --- |
  | Startups | 1,906 |
  | Products | 1,195 |
  | Research Papers | 1,656, stars filled for all |
  | Jobs | 7 |
  | News | 15 |
  | Entity Mapping Log | 3,109 |

- **Show:** the `recordType`, `source.url`, `durableIdentity` and `lineageMode=live` columns.
- **Trace one row per tab live.** Copy `source.url` from the Sheet, open it in the browser and show that the value matches:
  - **Startup:** name and employee count on the YC page.
  - **Product:** the owner on the vendor's own homepage (JSON-LD or footer), and the pricing on the AgiPicks page.
  - **Paper:** title and authors on arXiv, and the star count on GitHub.
- **Say:** "Every record traces back to a fetched source URL. A row without evidence is quarantined, never padded. That's why Jobs is small: a posting without an exact publish time, or without a stated remote status, is left out rather than guessed."

### 3. Bulk acquisition at scale (1:45–2:30)
- **Screen:** terminal.
  ```bash
  signal-ingest --mode live b8-status | head -30   # recomputes counts from the exported CSVs
  psql "$LIVE" -c "select record_type, source_id, status, count(*) from candidates where run_id='$RUN' group by 1,2,3 order by 1,2;"
  psql "$LIVE" -c "select source_id, reason, count(*) from quarantines where run_id='$RUN' group by 1,2 order by 3 desc limit 12;"
  ```
- **Say:**
  - "Bulk classes come from documented structured sources: the yc-oss AI tag, the AgiPicks open dataset (CC BY 4.0), the OpenRouter model directory, and pwc-archive paper→code links joined with OpenAlex metadata and live GitHub stars."
  - "Product owners come from the vendor's own homepage, domain-first. A widget's or hosting platform's name can't become the owner; reviewers caught that twice and it's covered by regression tests."
  - "Quarantines are visible, with a reason for each one."

### 4. Freshness: the 24-hour guarantee (2:30–3:15)
- **Screen:** the Sheet's News and Jobs tabs (`content.date`, `content.dateExtractionMethod`). Then page 3 of the PDF ("Freshness").
- **Say:**
  - "A news or job row is exported only if its whole publication interval lies inside [cutoff − 24h, cutoff]."
  - "'2 hours ago' becomes an interval anchored to the listing fetch time."
  - "A date with no timezone is widened by up to ±14 hours."
  - "Conflicting date fields send the row to quarantine."
  - "A page with no date is marked 'new since last run' by its watermark, but it's never exported as fresh."
  - "News comes with full article text: the `content.text` column, 2–20k characters."
- **Optional:**
  ```bash
  SIGNAL_INGEST_MODE=fixture SIGNAL_INGEST_NETWORK_GUARD=1 .venv/bin/python -m pytest -q tests/gates/test_b3.py -k "relative or zoneless or interval" 2>&1 | tail -2
  ```

### 5. LLM engine: fallback chain, 413 and 429 (3:15–4:30)
- **Screen:** page 2 of the PDF (provider table and fallback states), then the terminal.
  ```bash
  .venv/bin/python scripts/demo_llm_extract.py                          # Gemini answers
  .venv/bin/python scripts/demo_llm_extract.py --skip gemini            # falls over to Groq
  .venv/bin/python scripts/demo_llm_extract.py --skip gemini --skip groq   # Meta muse-ember tier
  ```
- **Say:**
  - "Extraction is a LangGraph state graph: budget → segment → cache → plan chunks → call the LLM, looping back to re-chunk on a 413 → validate → merge → finalize."
  - "Calls go through a LiteLLM Router chain: Gemini 2.5 Flash, then Groq gpt-oss-120b, then Meta's muse-ember."
  - "On a 429 it backs off exponentially with jitter. A Retry-After of 15 seconds or less is honored; a longer one cools the tier and falls over."
  - "Chunks are sized to the tightest tier, so a payload never exceeds Groq's 8,000 tokens-per-minute free limit, which would otherwise be a real 413."
  - "A quota hook reserves free-tier budget before every real dispatch. A billed tier must carry a cost estimate under a hard $5 cap."
  - "Look at the output: the owner is accepted with its exact quote. The pricing claim is rejected because its quote doesn't support the label. That's how hallucinations are stopped."
- **Then:**
  ```bash
  SIGNAL_INGEST_MODE=fixture SIGNAL_INGEST_NETWORK_GUARD=1 .venv/bin/python -m pytest -q tests/gates/test_b5.py -k "rate_limit or retry_after or too_large or payload" 2>&1 | tail -2
  ```

### 6. Grounded LLM on live data (4:30–5:00)
- **Screen:**
  ```bash
  psql "$LIVE" -c "select payload->'content'->>'company' company, left(payload->'content'->>'title',45) title, evidence->'is_remote'->>'value' is_remote, left(evidence->'is_remote'->>'quote',90) quote, evidence->'is_remote'->>'provider' provider from candidates where run_id='$RUN' and evidence->'is_remote'->>'basis'='llm';"
  ```
- **Say:**
  - "These OpenAI postings had no remote flag in the source. The LLM read the posting and answered not-remote, quoting 'hybrid work model of 3 days in the office'. The validator checked that quote against the page."
  - "Two other answers were rejected as ungrounded."
  - "Product owner fallback works the same way: `agent-crm` → 'Agent CRM', quoting its meta author tag."

### 7. Entity resolution (5:00–5:45)
- **Screen:** the Entity Mapping Log tab, filtered to `decision = match`, then the terminal.
  ```bash
  psql "$LIVE" -c "select raw_name, canonical_name, method, decision from resolution_events where run_id='$RUN' and method='approved_alias' limit 10;"
  SIGNAL_INGEST_MODE=fixture SIGNAL_INGEST_NETWORK_GUARD=1 .venv/bin/python -m pytest -q tests/gates/test_b6.py -k "openai or alias" 2>&1 | tail -2
  ```
- **Say:**
  - "Normalization folds legal suffixes, accents and spacing, so 'OpenAI', 'OpenAI, Inc.' and 'Open AI' all resolve to OpenAI."
  - "The seed list has 103 entries: 50 YC startups plus 53 AI labs taken from OpenRouter's directory."
  - "Matches use trusted source IDs or approved aliases. A weak single-word name needs corroboration, so the test set scores precision 1.0 with zero false merges."
  - "pgvector embeddings only suggest candidates, in the `suggested_candidates` column. They never merge anything."
  - "Products and Jobs export the canonical name, with the raw string kept next to it."

### 8. Scale: horizontal workers and self-healing (5:45–6:45)
- **Screen:** terminal.
  ```bash
  bash scripts/demo_two_workers.sh     # 2 worker processes on one queue; kill -9 one mid-run
  ```
- **Say:**
  - "Two worker processes share one PostgreSQL queue. Claims use `FOR UPDATE SKIP LOCKED`, and every lease has a fencing token and a heartbeat."
  - "I kill one worker with kill -9 mid-run. The survivor and the reaper reclaim its expired leases."
  - "The output shows every task done exactly once: observations equal distinct tasks, with zero duplicates."
  - "Adding machines means pointing more workers at the same database and a shared raw-store mount."
- **Then:** page 1 of the PDF (500k projection and benchmark).
- **Say:**
  - "Measured: 147 tasks per second with 4 slots on loopback, 0.02% retries in the live run."
  - "At 500k, the limits are the upstream source rate limits and the LLM token quota, not the code."
  - "Known limits: per-site pacing is per process, so the site rate is the limit times the number of workers; a shared token bucket is the next step. The billed-spend cap and LLM quota are already shared through PostgreSQL."

### 9. Anti-bot strategy (6:45–7:15)
- **Screen:** the README's "Phase V" section.
- **Say:**
  - "Official feeds and APIs come first. Every class in this run uses one."
  - "One polite client per site: stable User-Agent, 2 concurrent requests with pacing, Retry-After and robots.txt honored."
  - "Cloudflare, DataDome and captcha pages are detected and recorded as blocked. We don't solve or evade them."
  - "Ars Technica article pages sat behind an AWS WAF captcha. The pipeline kept the RSS discovery and took full text from other publishers."
  - "JavaScript-rendered sites would go to Playwright render slots behind the same queue, only where access is permitted."

### 10. Engineering rigor: tests and gates (7:15–7:40)
- **Screen:**
  ```bash
  SIGNAL_INGEST_MODE=fixture SIGNAL_INGEST_NETWORK_GUARD=1 .venv/bin/python -m pytest -q tests/gates 2>&1 | tail -2
  .venv/bin/python tools/gate.py status
  ```
- **Say:** "Every build block had failing tests written first, a gate, and an independent review; the review log is in `notes/reviews`. 395 cases pass under both UTC and India time zones. The B9 gate stays open only until the public Sheet is published."

### 11. Honest limits and close (7:40–8:00)
- **Say:**
  - "Known limits are written in the README:
    - 1,166 paper dates are month precision, taken from the arXiv ID, because arXiv's export API is robots-blocked.
    - The Jobs count is small because we don't guess.
    - Three relaunched products are counted twice."
  - "Thanks. The repo and the Sheet are linked in the submission."

---

## Numbers cheat sheet

| What | Value | Source |
| --- | --- | --- |
| Live run | `a4fee584-3ac1-4188-95f4-19d7742dbd33` | `var/live/exports/` |
| Cutoff | 2026-11-23T08:53:50Z (14:23:50 IST) | release manifest |
| Startups / Products / Papers | 1,906 / 1,195 / 1,656 | `b8-status` |
| Jobs / News / Mapping Log | 7 / 15 / 3,109 | export manifest |
| Seeds | 103 (50 YC + 53 OpenRouter labs) | `config/seeds.live.json` |
| Resolution | 392 match / 863 abstain / 1,854 new; gate set P = R = 1.0, FP 0 | `resolve`, `artifacts/gates/b6_metrics.json` |
| LLM tiers | Gemini 2.5 Flash → Groq gpt-oss-120b → Meta muse-ember-1.0 | `config/providers.yaml` |
| LLM grounded live rows | 2 product owners + 4 job remote flags (2 rejected) | `var/live/manifests/` |
| Spend | free tiers $0; Meta ≈ $0.001 (cap $5) | quota ledger |
| Benchmark | 112.9 / 147.7 tasks/s (1 / 4 slots, loopback) | `artifacts/gates/b7_benchmark.json` |
| Recovery | kill -9 → 12/12 healed in 11.2 s, 0 duplicates | `artifacts/gates/r1_kill9_demo.txt` |
| Two-worker demo | 2 workers, 300 tasks, kill -9 one at 3 s → 300/300 done, 300 obs = 300 distinct | `scripts/demo_two_workers.sh` |
| Tests | 395 passed (UTC and Asia/Kolkata), gates B0–B8 PASS | `tools/gate.py` |

## Likely reviewer questions

- **"Does it really scale horizontally?"** Yes for correctness. Any number of `work` processes can share one PostgreSQL queue, and SKIP LOCKED, fencing tokens and unique keys prevent duplicate effects. Three caveats:
  - Across machines the raw store needs a shared mount (an absolute `SIGNAL_INGEST_RAW_ROOT_LIVE`).
  - Per-site pacing is per process. Today you divide the per-site limit by the worker count; the upgrade is a shared token bucket.
  - LLM quota and billed spend are already enforced in PostgreSQL across processes.
- **"Why not bypass Cloudflare?"** Bypassing protections breaks site terms and doesn't hold up as a production approach. We detect the challenge, back off and use licensed feeds or APIs. That's also why every row is traceable.
- **"Why so few jobs?"** The brief requires guaranteed 24-hour freshness and a remote flag. Postings without an exact publish time or a source-backed (or quote-grounded) remote flag are left out. The 24-hour window also limits how many qualify at any moment.
- **"Why are some paper dates YYYY-MM?"** OpenAlex merges preprints into journal works, so its date can be from a different version. We keep it only when it falls in the arXiv v1 month encoded in the ID. Otherwise we export that month, which is valid reduced-precision ISO-8601, rather than invent a day.
- **"Where is the LLM used?"** Whenever a required field isn't in a structured source: product owners on vendor pages, and job remote status. Every value needs a verbatim quote inside the fetched page. Bulk rows come from structured data, which is cheaper and exact at scale.

## Recording tips (Astra)
- Record at 1080p. Cut each scene separately and stitch them; pre-run the commands so outputs appear quickly.
- Show captions for the key numbers, taken from the cheat sheet.
- Keep the Sheet zoomed so the column names are readable, and freeze the header row.
- Don't show the `.env` file, browser profile names or emails.
