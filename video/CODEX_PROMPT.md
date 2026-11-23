# Prompt for Codex: build the Frontier Atlas demo video

Paste everything below the line into Codex. Run Codex from the repo root (`Frontier_Atlas`).

---

You are a senior motion designer and React/TypeScript engineer. Build a **15-minute animated technical product demo video** for this repository using **Remotion**, and render it to MP4.

## Goal
The hiring team may never read the code. The video must make them *understand* how the system works and *believe* it works. The style is a polished motion-graphics explainer in the manner of a top developer-tool launch video, cut together with short real screen recordings as proof. It must not look like a slideshow or a generic stock template.

## Inputs (read these first)
- `DEMO.md`: the creative brief. It has the storyboard with scene timings, the **final narration text**, the visual language (palette, type, metaphors, the mascot "Atlas") and the recording list. Follow it closely.
- `video/facts.json`: **the only allowed source for every number, name, quote, URL and sample row shown on screen.** Import it into the composition; never hand-type numbers.
- `video/two_worker_demo.txt`: real output for the self-healing scene.
- `README.md` and `architecture.pdf`: background for accuracy (diagram, flow names).

## Hard rules
1. **No invented data.** Every number, row, quote and URL on screen comes from `video/facts.json` or `video/two_worker_demo.txt`. If something is missing, show a neutral label instead of making one up.
2. **Never read, open, print or show `.env`**, or any key, token, email or browser profile.
3. Don't modify anything outside `video/` (except adding `video/app/` to `.gitignore` for `node_modules/` and `out/`).
4. Use only free, permissively licensed assets:
   - Google Fonts (Inter, JetBrains Mono).
   - Icons drawn in SVG yourself, or from Lucide (ISC).
   - No copyrighted characters, logos or music.
   - Source logos may appear only as plain text labels.

## Tech
- Scaffold a Remotion project in `video/app/` (TypeScript, `npx create-video@latest` blank template, or install `remotion @remotion/cli` manually). Pin exact versions and commit `package-lock.json`.
- One root composition `FrontierAtlasDemo`, 1920×1080, 30 fps, total ≈ 15:00 (27,000 frames). Build it from one `<Series>` of chapter compositions (Ch0–Ch9 from `DEMO.md`), so each chapter can also be previewed and rendered alone.
- Folder layout: `src/chapters/Ch0ColdOpen.tsx … Ch9Close.tsx`, `src/components/` (reusable pieces), `src/theme.ts` (palette and type from `DEMO.md` §2), `src/data/facts.ts` (typed import of `video/facts.json`), `public/recordings/`, `public/audio/`.
- Animate with Remotion primitives (`useCurrentFrame`, `interpolate`, `spring`, `Sequence`, `Series`, `AbsoluteFill`, `<OffthreadVideo>` for recordings, `<Audio>` for voiceover). Ease everything; nothing should move linearly unless it's a conveyor.

## Reusable components to build
- `Atlas`: the SVG chibi crawler robot (round body, one big eye, antenna, small wheels). Expressions: idle, blink, sweat (429), shocked (kill -9), cheer (verified), wave. It can carry a glowing `UrlTicket`.
- `UrlTicket`: a pill with a monospace `semantic_key`, a fencing-token badge, and an optional lease timer ring that shrinks and refreshes on heartbeat.
- `Conveyor`: the task queue belt; duplicate tickets bounce off with an "ON CONFLICT DO NOTHING" pop.
- `WorkerBot`: a variant of Atlas that claims tickets; it can be "killed" (greyed out, lightning flash) while its leases expire to amber.
- `SiteDoor`: a website door with a robots.txt sign, a traffic light for pacing, a Retry-After clock, and a locked "challenge" shield state that triggers a detour to an "RSS / API" side door.
- `Vault`: raw-store drawers labelled with sha256 prefixes.
- `SegmentCards`: a page splitting into labelled cards (`seg:0001:title`, `seg:0005:pricing`, …).
- `BudgetMeter`: stacked bar of context − reserved output − instructions − safety margin; chunks fill it.
- `GraphFlow`: LangGraph nodes (`facts.langgraph_nodes`) lighting up in order, with an animated back-edge "413 → rebudget".
- `TierGate`: one per LLM tier (`facts.llm_chain`), with a status light (green/amber/red), a quota meter that fills before each call, a cooldown clock, and a "$ cap" badge on the billed tier.
- `GroundingCheck`: an answer card (field, value, quote) plus a highlighter sweep that finds the quote inside a segment. It ends either green ✓ ("verified") or with a red "REJECTED: not supported by page" stamp. Drive it from `facts.grounded_examples`.
- `NameMagnets`: messy-name bubbles that snap into a canonical node; grey abstain bubbles; a dotted suggestion line with a similarity score labelled "suggest only".
- `FreshnessRuler`: a timeline with the cutoff and a 24 h window; interval bars for "2 hours ago", ±14 h zoneless widening, a conflict → quarantine bin, and a "new since last run" flag.
- `CounterRoll`: animated number counters (tabular figures).
- `ChapterCard`: chapter number and title with a short kinetic-type intro.
- `ProgressBar`: persistent at the top, showing chapter markers.
- `Caption`: burned-in subtitles from the narration, two lines max, bottom-centre, always on.
- `RecordingFrame`: a rounded "screen" frame around an `<OffthreadVideo>` with animated callout arrows and highlight boxes. When the file `public/recordings/R*.mp4` is missing, it shows a labelled placeholder with the exact recording instructions from `DEMO.md` §4.

## Audio
- Narration is the text in `DEMO.md` §3, one file per chapter: `public/audio/vo_ch0.mp3 … vo_ch9.mp3`.
- **Placeholder voiceover:** generate it offline so timing can be checked now. On macOS use `say -v Samantha -r 175 -o vo_chN.aiff "<text>"`, then `ffmpeg -i vo_chN.aiff vo_chN.mp3`. Measure each file's duration and **fit the chapter length to the audio**, keeping the total near 15:00. Adjust animation pacing, never the narration text.
- Captions: derive timing from the same text split into sentences, spread proportionally across each audio file. Optional improvement: if `whisper` (local) is available, use word-level timestamps.
- Music: none, or a very quiet royalty-free ambient bed, only if you can generate one locally (no downloads of unknown licence). Narration must always be clearly audible.

## Craft bar (what "different from a random video" means)
- **Story over features.** One ticket (Atlas) travels the whole pipeline. Each chapter starts where the last ended (match cuts: the ticket leaves the vault and becomes a segment card, and so on).
- **Show then prove.** Every animated concept in Ch3–Ch7 is followed by its real recording or real numbers within the next 15 s (see `DEMO.md` §4 and Ch8).
- **Kinetic typography** for key numbers: big, short, synced to the word in the narration.
- **Visual rhythm.** Something meaningful changes every 2–4 s, with a calm frame after each big reveal. No idle screens longer than 4 s.
- **Consistency.** One palette, one icon style, one easing family, and the same metaphor for the same concept every time (a lease is always a timer ring).
- **Readability.** Minimum text size 28 px; code and URLs in JetBrains Mono; safe margins of 80 px; high contrast against `#0B1020`.
- **Tone.** Confident and honest. Ch9 shows the known limits plainly.

## Deliverables
1. `video/app/` Remotion project that runs with `npm install && npx remotion studio`.
2. `video/app/out/frontier_atlas_demo.mp4` (H.264, 1080p30), plus per-chapter renders `out/ch0.mp4 … ch9.mp4`.
3. `video/app/README.md`: how to preview, re-render, swap in the real voiceover and recordings, and where each scene's data comes from.
4. A short report: final duration per chapter, which recordings were still placeholders, and any `DEMO.md` instruction you couldn't follow and why.

## Self-check before you finish
- Grep the rendered text sources: every number shown exists in `video/facts.json` or `two_worker_demo.txt`.
- Watch the full render once muted (are the captions enough?) and once at 1.5× (is the pacing tight?). Fix anything that drags.
- Total runtime is 14:30–15:30. No chapter has a frame without captions while narration plays.
- No `.env` contents, keys or personal data appear anywhere in the project or the video.
