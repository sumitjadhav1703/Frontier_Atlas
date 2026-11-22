# Cursor project entry point

Paste this line into Cursor Agent in the connected project folder.

> Read cursor.md and execute B0 through B9 as separate work orders. Resume at the first open gate and follow its delegation, verification and stopping rules.

The plain-text prompt works without slash-command discovery. If you want one block only, use `/one-block` or `/one-block B3` with the appropriate number. A full queue operates during the active Cursor session. After an interruption, paste the same line. The durable state determines where work resumes.

## At startup

1. Read AGENTS.md, START-HERE.md, docs/assessment.md, docs/build-plan.md, docs/contracts.md, docs/sources.md and notes/progress.md. Read the complete plan once. Then send each specialist just the relevant ticket, field rules and evidence. Employer requirements are authoritative for the deliverables.
2. From the repository root run `python3 tools/gate.py status`. Use the project interpreter once B0 creates it. Inspect existing files, receipt and review before deciding to implement anything. A prose “done” entry cannot close an open gate.
3. Inventory local terminal, workspace/search, documentation lookup, database, browser and subagent capabilities by actual availability and task relevance. Prefer local execution. Keep Astra selected if available and let the agents inherit the selected model. Use a relevant installed MCP when it improves a specific check. Do not send evidence or credentials to a tool unnecessarily.
4. For a full queue, the startup line explicitly requests ten sequential one-block jobs. At each handoff close that job, persist the receipt and check status before dispatching the next already-requested job. For a single job, stop at its handoff. If all receipts are current, summarize the existing release evidence with its cutoff and verification time; distinguish an archived result from a current publication check. Do not start another crawl.

## One work order

| Owner | Files and actions | Required handoff |
| --- | --- | --- |
| Coordinator in the main chat | Own shared contracts, dependency locks, integration, gate and ledger. Resolve routine decisions. | Block number, dependency state, scope, file ownership and success condition. |
| assessment-planner · read-only | Before code, inspect the active ticket and current implementation. Identify the weakest assumption, stable interfaces and the smallest test that could disprove the design. | A short decision with paths, contract, failure case and source. One planning pass per block unless new facts change the design. |
| assessment-builder | Implement the agreed behavior in the assigned application files. | Entry point, changed files and commands run. |
| assessment-verifier | Independently derive expected outputs from the brief and contracts. Own test files, fixtures and negative cases. May work in parallel with the builder after the interface is agreed. | Evidence that the test detects a specific broken behavior and passes on the integrated fix. |
| assessment-reviewer · read-only | Inspect the integrated diff, actual test assertions, evidence and resulting data. Trace one accepted case and one rejected case. | PASS or REOPEN, the exact inspected evidence and the remaining limitation. |

Agents work only in their assigned paths. Only the coordinator can close a gate. The planner and reviewer are separate contexts. Restrict parallel writers to the builder and verifier. A second model agreeing with the first supplies no evidence of correctness. Validate the result with a test, source observation or actual artifact.

**Close or repair.** Run targeted tests first. Fix failures with a minimal reproducer. Have the reviewer inspect the final integrated revision, capture its verdict using notes/review-template.md, then run the cumulative gate. If a gate run fails or a material change follows review, record the failure, repair it and request review of the fix. Record time, tests, commands, artifact locations and the exact next action. Never edit an assertion or gate threshold solely to obtain a pass. Reopening an earlier gate requires reconciling affected downstream outputs.

## Decisions Cursor can make itself

| Situation | Decision |
| --- | --- |
| Two compatible libraries or layouts | Prefer the simplest option already installed or pinned that passes the contract. Record a one-line reason. |
| Source shape, date, ownership or pricing uncertain | Inspect the cited source and local capture in the allowed stage. If still ambiguous, quarantine the row. Continue other work. |
| Source fails or has low yield | Choose an approved alternate from the source register, rerun its qualification and recompute ETA. Preserve the failed route and shortfall in the ledger. |
| Provider key or free quota missing | Disable that tier, try another configured free provider or local model and continue independent tasks. Record what would unlock any remaining mandatory live proof. |
| Subagents unavailable | Record the limitation. Use separate editor sessions when available or distinct local verification passes. Name the actual review mode; never invent independent sign-off. |
| Credentials, external publication or irreversible action required | Finish independent work and prepare exact handoff artifacts. Ask one narrow question only if the action still requires the applicant. Never request a secret in chat. |
| Session, editor credit or time limit reached | Persist the exact failing command or next job, evidence paths and checkpoint. Resume with the startup line. |

**Release boundary.** Offline engineering through B7 costs no provider requests. B8 may use explicitly configured free services. During B9, prepare and audit the snapshot locally; publication and submission remain with the applicant unless authorized for that session. If viewer verification awaits a published URL, mark B9 awaiting publication and list exactly what is ready. A test receipt proves what ran on its recorded inputs. It cannot promise that a remote provider, source or share permission will remain unchanged.
