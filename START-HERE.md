# AI Engineer internship · Cursor build kit

**Start in Cursor Agent.** Open [cursor.md](cursor.md) or paste the line below in the project chat.

> Read cursor.md and execute B0 through B9 as separate work orders. Resume at the first open gate and follow its delegation, verification and stopping rules.

For one block only, use `/one-block`. The planner, builder, verifier and reviewer inherit your selected model where supported.

| Read | Use |
| --- | --- |
| [Original assessment](docs/assessment.md) | Employer’s supplied Markdown. Source of truth for scope and submission. |
| [Implementation plan](docs/build-plan.md) | One ticket at a time, tests, failure handling, acceptance and schedule. |
| [Data contracts](docs/contracts.md) | Field mapping, evidence, identity and scale invariants shared by all tickets. |
| [Source register](docs/sources.md) | Research-backed starting URLs, acquisition paths, fallback choices and quota planning. |
| [Progress](notes/progress.md) | Pick up after class or start a fresh agent session. |
| [Agent instructions](AGENTS.md) | Ownership, review, logging and when to stop. |

**What counts at submission.** A public six-tab Google Sheet with at least 1,000 unique startups, 1,000 unique products and 1,000 unique AI papers with an evidenced repo and observed stars. Include all verified fresh news/jobs discovered in the final window, the raw-to-canonical mapping log, a reviewer-accessible repository with README and source, and a design PDF of at most three pages. The employer assigns 50% to LLM orchestration and data quality together and warns that fabricated data disqualifies the entry.

**Deadline.** The email says 23 November 2026 at 7:00 PM without specifying a timezone. Plan on India time and aim to submit by 5:00 PM IST on 23 November. Confirm the organizer’s timezone in the invitation or event listing.

**Cost control.** The zero-spend setting applies to the ingestion pipeline. Cursor agent and subagent usage follows your Cursor plan separately. Keep each agent on one ticket and one file set. If editor credits are tight, keep the implementer and use one verifier for tests followed by a separate short review session. Record which roles actually ran.

**What the gate proves.** Run `python3 tools/gate.py status` now. It should report B0. After B0 creates the project environment, use its interpreter (for example `.venv/bin/python tools/gate.py B0`). Receipts in `artifacts/gates/` record local historical runs and link to test reports and reviews. The runner binds each receipt to the build inputs it checked. It reports changes and reruns earlier tests at the next gate. Changes after final acceptance reopen B9. Run the kit’s own regression tests with `python -m unittest discover -s tools/tests -v` in an environment with pytest. A reviewer must inspect the assertions, since any green test can encode the wrong expectation. The application and its acceptance suites are the work Cursor will build.

**Working rhythm.** The startup line queues the full build and the coordinator checks state at every handoff. Reuse it to resume an interrupted session. The optional `/one-block` command stops after one accepted block. The reviewer can reopen an earlier gate when new evidence contradicts it. Engineering gates use fixtures, a temporary local database and zero external API calls. B8 introduces the first external data/provider calls. B9 is the only publication handoff. The kit contains planning files and an executable test gate. Implementation, real records and submission evidence are pending.
