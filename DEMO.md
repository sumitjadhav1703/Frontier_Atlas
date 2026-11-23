# Frontier Atlas: 15-minute animated technical demo

**Format:** animated technical product demo, about 15:00, 1920×1080 at 30 fps. Motion-graphics explainer scenes are cut together with short real screen recordings, which serve as proof.
**Audience:** the hiring team. They may never open the code, so the video has to show how the system works and that it really works.
**Build tool:** [Remotion](https://www.remotion.dev/) (React → MP4), built by Codex from [`video/CODEX_PROMPT.md`](video/CODEX_PROMPT.md).
**Single source of truth for every number on screen:** [`video/facts.json`](video/facts.json) (taken from the live export). Two-worker proof output: [`video/two_worker_demo.txt`](video/two_worker_demo.txt).

Links:
- Repo: https://github.com/sumitjadhav1703/Frontier_Atlas
- Sheet: https://docs.google.com/spreadsheets/d/1cNBMMtAjfSz7o9VXdL2qzBd5I7lfzYyLAZNdFOzHi9w/edit?usp=sharing
- Form: https://forms.gle/3JJhukomumTFueRW7

---

## 1. What makes this video work (research-backed rules)

1. **Story arc, not a feature tour: problem → solution → proof → outcome.** Demos organised around internal menus lose viewers, so this one follows one record from the open web to a verified Sheet row ([Mind the Product](https://www.mindtheproduct.com/how-to-use-storytelling-to-create-powerful-product-demos/), [Demio](https://www.demio.com/blog/compelling-product-demo-script)).
2. **Hook first, give away the ending.** The first 30 seconds show the finished result (4,757 verified records, every one traceable), then rewind to explain how ([ProductPlan](https://www.productplan.com/product-demo)).
3. **One clear thread.** A single guide character, the crawler bot "Atlas", carries one URL through every chapter, so viewers never lose the thread.
4. **Show, then prove.** Every animated concept is followed by 5–15 s of real footage (terminal or Sheet) showing the same thing happening for real. Proof beats claims ([Shopify](https://www.shopify.com/blog/product-demos)).
5. **Motion graphics over full character animation.** Typography, icons, shapes and simple transitions explain systems better than rigged characters; keep the mascot small and simple ([Vimeo](https://vimeo.com/blog/post/whats-motion-graphics-in-video)).
6. **Script and visuals separately.** The narration script below is final text, and visuals are timed to it ([DEV](https://dev.to/jakeg73/how-to-write-a-product-demo-script-2d0c)).
7. **Chapters with titles and a progress bar.** Fifteen minutes is long, so a chapter card every 1–2 minutes and a persistent progress bar keep viewers oriented.
8. **Captions always on.** Many reviewers watch muted. Key numbers appear as large on-screen text exactly when narrated.

## 2. Visual language

- **Palette:** deep navy background `#0B1020`. Cyan `#22D3EE` = data flow; violet `#A78BFA` = LLM; green `#34D399` = verified/accepted; amber `#FBBF24` = waiting/retry; red `#F87171` = rejected/blocked; grey `#64748B` = abstain/quarantine.
- **Type:** Inter (UI and body), JetBrains Mono (code, URLs, numbers).
- **Mascot "Atlas":** a small round crawler robot with one big eye and antenna, in a flat anime/chibi style, drawn as SVG in code. It carries a glowing "URL ticket" through the story. It reacts: it blinks at a challenge page, sweats at a 429, cheers at "verified".
- **Recurring metaphors:**
  - **Task queue:** a ticket conveyor; each ticket shows `semantic_key`.
  - **Lease:** a ticket held by a worker robot, with a shrinking amber timer ring.
  - **Fencing token:** a numbered badge on the ticket; a stale badge can't stamp "done".
  - **robots.txt:** a door sign (Allowed / Disallowed paths).
  - **Challenge page:** a locked door with a shield. Atlas leaves a "blocked" note and takes the side door (RSS/API).
  - **Per-site pacing:** a traffic light per website.
  - **Raw store:** a vault of drawers labelled with sha256 prefixes.
  - **Segments:** the page cut into cards `seg:0001:title`, `seg:0005:pricing`, …
  - **LLM tiers:** three gates in a row (Gemini, Groq, Meta), each with a token-budget meter and a status light.
  - **Grounding:** a highlighter that must find the model's quote inside a segment card. Green check, or a red stamp "REJECTED: not in page".
  - **Entity resolution:** name bubbles that magnetically snap into one canonical node; abstained names stay grey; pgvector suggestions are dotted lines labelled "suggest only".
  - **Freshness:** a horizontal timeline ruler with a glowing 24 h window ending at the cutoff.

## 3. Storyboard and narration (≈ 15:00)

Timings are targets. Each scene lists visuals, what to show, and **narration**, which is the final voiceover text.

### Ch 0 · Cold open (0:00–0:40)
- **Visual:** a storm of URLs, HTML fragments and dates rains down. Atlas appears and catches one ticket. A clean six-tab spreadsheet assembles, and counters roll up: Startups 1,906 · Products 1,195 · Papers 1,656 · Jobs 7 · News 15 · Mapping Log 3,109. Stamp: "Every row traces to a source URL".
- **Narration:** "The internet doesn't hand you clean data. Startups, products, research papers, jobs and news are scattered across thousands of sites, in different formats, with missing dates, and some of it is behind bot walls. In this demo I'll show how Frontier Atlas turns that into verified, structured records, where every single row traces back to the page it came from, and the AI is never allowed to make anything up."

### Ch 1 · The problem (0:40–1:50)
- **Visual:** four pain cards flip in, one at a time:
  1. Scale: "lakhs of records".
  2. Freshness: "published in the last 24 h. Guaranteed?"
  3. LLM limits: "429 Too Many Requests / 413 Payload Too Large".
  4. Messy names: "OpenAI, OpenAI Inc., Open AI".

  Then a fifth card in red: "Hallucinated data = disqualification".
- **Narration:** "The task had five hard parts. Collect thousands of records with an architecture that scales to five hundred thousand. Monitor news and job boards and guarantee everything is from the last twenty-four hours. Use LLMs that rate-limit you and choke on long pages. Turn messy company names into one canonical identity. And the hardest rule of all: if the AI invents a single value, you're out. So I designed the whole system around one principle: no evidence, no row."

### Ch 2 · The big picture (1:50–3:00)
- **Visual:** the architecture diagram draws itself left to right:

  Sources → discover → PostgreSQL task queue → N workers → fetch pool → raw store → parse → candidates → LangGraph + LiteLLM → entity resolution → six-tab export.

  Atlas rides the ticket along the arrows. Each box lights up as it is named.
- **Narration:** "Here's the map. Sources go into a discovery step that turns listings into tasks in a PostgreSQL queue. Any number of workers claim tasks, fetch pages politely, and store the raw bytes by content hash. Parsers build candidate records, and when a field isn't in the structured data, a grounded LLM engine extracts it. Then deterministic entity resolution canonicalizes names, and an export writes six tabs at a fixed UTC cutoff. Let's follow one ticket through each stage."

### Ch 3 · The crawler (3:00–5:40)
- **3a, structured sources first (3:00–3:40).** Visual: five source icons with labels: yc-oss JSON, AgiPicks open data (CC BY 4.0), OpenRouter model directory, pwc-archive + OpenAlex, RSS + Ashby job API. Narration: "First decision: prefer published feeds and APIs over scraping HTML. Every data class in this run comes from one: the YC directory mirror, an open AI-tools dataset, OpenRouter's model directory, a papers-with-code archive joined with OpenAlex, RSS feeds, and Ashby's public job API. They're licensed, stable, and they never throw captchas."
- **3b, the queue (3:40–4:30).** Visual:
  - Tickets drop onto the conveyor; duplicates bounce off with "ON CONFLICT DO NOTHING".
  - Two worker robots grab tickets. A timer ring appears (the lease) and a badge with a number (the fencing token).
  - A heartbeat pulse refreshes the ring.

  Narration: "Each item becomes a task with a stable key, so discovering the same thing twice is a no-op. Workers claim tasks with `SELECT FOR UPDATE SKIP LOCKED`, so two workers never take the same ticket. Each claim comes with a lease that a heartbeat keeps renewing, and a fencing token. If a worker is too slow and its lease is handed to someone else, its old token can no longer write anything."
- **3c, polite fetching (4:30–5:00).** Visual: the robots.txt door sign checked; the traffic light per site; Retry-After shown as a clock. A locked door with a shield (Ars Technica, AWS WAF captcha). Atlas writes "blocked" and takes the RSS side door. Narration: "Fetching is deliberately polite: robots.txt is obeyed, there are two requests per site with spacing, and Retry-After is honored. When a site shows a challenge page, like Ars Technica's firewall, we detect it, record it as blocked, and use its RSS feed instead. We never solve or evade captchas."
- **3d, self-healing (5:00–5:40).** Visual: worker A is struck by a lightning bolt labelled `kill -9`. Its ticket timer runs out and turns amber. Worker B reaps it and finishes. Counter: 300/300 done · observations 300 = distinct 300 · duplicates 0. Cut to the real terminal output (recording R5). Narration: "Here's the part I'm most proud of. I start two workers on three hundred tasks and kill one of them, hard, three seconds in. Its leases expire, the other worker reclaims them, and the run finishes: three hundred of three hundred, and exactly three hundred stored pages. Zero duplicates."

### Ch 4 · Freshness (5:40–7:00)
- **Visual:** a timeline ruler with the cutoff at the right edge and a glowing 24 h window:
  - "2 hours ago" turns into a short bar anchored at fetch time, and fits inside the window.
  - A date with no timezone stretches ±14 h; if it pokes outside the window, it is rejected.
  - Two conflicting dates appear, then a quarantine bin.
  - A dateless item gets a "new since last run" flag, and is still not exported.
  - A job board's posting with no publish time gets grey "missing evidence".
- **Narration:** "Freshness isn't a date comparison, it's interval logic. Every publication time becomes an interval. 'Two hours ago' is anchored to when we fetched the listing. A timestamp with no timezone could be off by up to fourteen hours, so it's widened. A row is exported only if its entire interval sits inside the last twenty-four hours before the cutoff. Conflicting dates go to quarantine. A page with no date can be flagged as new since the last run, but it's never sold as fresh. That's why the Jobs tab is small: we would rather show seven true jobs than seventy guesses."

### Ch 5 · The LLM engine (7:00–9:50)
- **5a, why and when (7:00–7:30).** Visual: a product's homepage with an empty "owner" field; a job posting with an empty "remote?" field. Narration: "Structured data covers most fields. When a required field isn't there, like who owns a product or whether a job is remote, the LLM reads the page."
- **5b, segments and chunking (7:30–8:15).** Visual:
  - The page is sliced into labelled segment cards.
  - A budget meter: context − reserved output − instructions − safety margin.
  - Cards pack into chunks sized to the smallest tier (Groq: 8,000 tokens per minute).

  Narration: "The page is cleaned and cut into labelled segments. A planner packs them into chunks that fit the tightest model in the chain, after reserving room for instructions and output. That's how payloads never trigger a 413."
- **5c, the LangGraph flow (8:15–8:45).** Visual: nodes light up in sequence (budget → segment → cache → plan → call_llm → validate → merge → finalize). A red "413" bounces from call_llm back to "rebudget", which splits a chunk in half. Narration: "The extraction is a LangGraph state graph. If a provider still says 'too large', the graph loops back, splits the chunk smaller and tries again, with a hard limit on depth and never re-sending the same bytes."
- **5d, the fallback chain (8:45–9:20).** Visual: three gates (Gemini 2.5 Flash · Groq gpt-oss-120b · Meta muse-ember):
  - Gemini's light turns amber with "429 Retry-After: 120 s" and a cooldown clock. The ticket slides to Groq.
  - Groq answers, or shows a "too large" note and passes to Meta.
  - A quota meter next to each gate fills before every call. The Meta gate shows "$ cap 5.00".

  Narration: "Calls go through a LiteLLM router: Gemini first, then Groq, then Meta's model. On a 429 it backs off exponentially with jitter. If the server asks for a long wait, that tier cools down and the next one takes over. Before every single call, a hook reserves quota in PostgreSQL, so the free-tier limits are respected across all workers, and the one billed tier has a hard five-dollar cap."
- **5e, grounding, the anti-hallucination wall (9:20–9:50).** Visual: the model returns an answer card: owner = "Agent CRM", quote "author: Agent CRM". The highlighter finds it in `seg:0002:meta`, and the card turns green. Then OpenAI job: is_remote = false, quote "…hybrid work model of 3 days in the office…", green. Then a demo pricing claim whose quote doesn't support the label: red stamp "REJECTED". Cut to the real terminal (recording R3). Narration: "And this is the wall against hallucination. The model must return a verbatim quote and the segment it came from. We check that the quote is really on the page, that it's about the right company, and that the value is in the quote. Agent CRM: verified. OpenAI's 'hybrid, three days in office': remote equals false, verified. A claim the page doesn't support: rejected. The model can propose, but only the page can prove."

### Ch 6 · Entity resolution (9:50–11:10)
- **Visual:**
  - Bubbles "OpenAI", "OpenAI, Inc.", "Open AI" drift and snap into one node "OpenAI" (trusted id / approved alias).
  - "Demodesk GmbH" snaps to "Demodesk".
  - "Centauri AI" approaches "Centaur", and a dotted line says "0.88 · suggestion only", with no merge.
  - Counter: 3,109 mentions → 392 matched · 863 abstained · 1,854 new · false merges 0.
  - The seed list badge: 103 = 50 YC + 53 AI labs.
- **Narration:** "Next, identity. Normalization strips legal suffixes, accents and spacing, so OpenAI, OpenAI Inc. and Open AI become one canonical entity. Matches need a trusted source ID or an approved alias. A generic single-word name needs corroboration. pgvector embeddings help by suggesting near matches, like Centauri AI and Centaur, but they never merge anything on their own. On the test set that's precision one point oh, with zero false merges."

### Ch 7 · Scaling to 500,000 (11:10–12:30)
- **Visual:**
  - The single worker multiplies into a grid of machines, all pointing at one PostgreSQL elephant.
  - Gauges: pipeline throughput 147 tasks/s (4 slots, loopback). Upstream limits: GitHub at 5,000 req/h per token (~1 h with GraphQL batches of 100 at 500k); LLM tokens per day.
  - The bottleneck needle sits on "upstream limits", not "code".
  - A checklist animates: same DB · shared raw-store mount · unique worker IDs · pacing ÷ workers.
- **Narration:** "Scaling to five hundred thousand records means adding workers, not changing code. They all share one queue, writes are fenced, and quotas live in the database. The pipeline itself measured around a hundred and fifty tasks a second on four slots. At that scale the real limits are the sources' own rate limits and the LLM token budget, and the architecture document shows the math. For multiple machines you share the raw-store mount, give each worker an ID, and divide per-site pacing by the number of workers."

### Ch 8 · Proof: the real thing (12:30–14:10)
- **Visual:** real recordings with light animated callouts:
  - **R1:** Sheet tour. Six tabs; zoom on the `recordType`, `source.url`, `lineageMode=live` columns.
  - **R2:** trace one row live. Sheet `source.url` → the browser page → the same value, with a green tick overlay.
  - **R3:** `demo_llm_extract.py`, normal and `--skip gemini`, showing the fallback.
  - **R4:** the test suite: 395 passed; `tools/gate.py status`.
  - **R5:** `scripts/demo_two_workers.sh` → `RESULT: PASS`.
- **Narration:** "Now the real thing, not animation. Here's the published Sheet with six tabs. Pick any row: copy the source URL, open it, and the value is right there. Here's a live extraction through the chain, and here it is again with Gemini switched off, falling over to Groq. Three hundred ninety-five tests pass in two time zones, and every build stage went through failing tests first, a gate and an independent review."

### Ch 9 · Honest limits and close (14:10–15:00)
- **Visual:**
  - A calm checklist card, "Known limits":
    - Paper dates: 1,166 at month precision (`YYYY-MM`) from the arXiv ID.
    - Per-site pacing is per worker, with a shared token bucket next.
    - A few blocked or client-rendered boards.
  - Final card: logo "Frontier Atlas", repo URL, Sheet URL. Atlas waves.
- **Narration:** "It isn't perfect, and the gaps are written down: some paper dates are month precision because arXiv blocks automated date lookups, per-site pacing is per worker for now, and a few job boards block bots or render client-side, so we respect that instead of forcing it. What you get is a pipeline that scales out, heals itself, and never puts a value in the sheet that a real page can't back up. Thanks for watching. The repo and the live Sheet are linked below."

## 4. Real recordings to capture (you record these; Codex places them)

Save them as MP4s (1920×1080, 30 fps) in `video/app/public/recordings/`, using the filenames below. Codex shows a labelled placeholder until each file exists.

| File | What to record | Command / action |
| --- | --- | --- |
| `R1_sheet_tour.mp4` (20 s) | the Google Sheet, scrolling through the 6 tabs | open the Sheet link; zoom to 125% |
| `R2_trace_row.mp4` (20 s) | one row's `source.url` opened in the browser | e.g. Products → Demodesk → `https://demodesk.ai/` and `https://agipicks.com/tools/demodesk/` |
| `R3_llm_fallback.mp4` (30 s) | live grounded extraction and the fallback | `.venv/bin/python scripts/demo_llm_extract.py`, then `... --skip gemini` |
| `R4_tests.mp4` (15 s) | test suite and gates | `SIGNAL_INGEST_MODE=fixture SIGNAL_INGEST_NETWORK_GUARD=1 .venv/bin/python -m pytest -q tests/gates \| tail -2` and `.venv/bin/python tools/gate.py status` |
| `R5_two_workers.mp4` (25 s) | horizontal workers and kill -9 | `bash scripts/demo_two_workers.sh` |

Before recording:
- Run `brew services start postgresql@16` and `source .venv/bin/activate`.
- Set the terminal font to 18–20 pt with a dark theme.
- **Never show `.env`**, browser profiles or emails.

## 5. Production checklist
1. Give Codex the prompt in [`video/CODEX_PROMPT.md`](video/CODEX_PROMPT.md).
2. Record R1–R5 and drop them into `video/app/public/recordings/`.
3. Voiceover. Either record the narration above yourself (best; `video/app/public/audio/vo_chXX.mp3` per chapter), or use the TTS placeholder Codex generates and replace it later.
4. Preview with `cd video/app && npx remotion studio`; render with `npx remotion render`.
5. Watch it once muted, to check that captions alone tell the story, and once at 1.5× to check pacing.
6. Upload, then paste the link into the form.

## 6. Numbers cheat sheet (all from `video/facts.json`)

| What | Value |
| --- | --- |
| Rows | Startups 1,906 · Products 1,195 · Papers 1,656 · Jobs 7 · News 15 · Mapping Log 3,109 |
| Cutoff | 2026-11-23T08:53:50Z (14:23:50 IST) |
| LLM chain | Gemini 2.5 Flash → Groq gpt-oss-120b → Meta muse-ember-1.0 ($5 cap) |
| Resolution | 103 seeds · 3,109 mentions → 392 match / 863 abstain / 1,854 new · gate P = R = 1.0, FP 0 |
| Scale | 2 workers + kill -9 → 300/300, 0 duplicates · 147.7 tasks/s (4 slots) · live retries 0.02% |
| Tests | 395 passed (UTC and Asia/Kolkata), gates B0–B8 PASS |
