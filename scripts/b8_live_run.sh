#!/usr/bin/env bash
# B8 live acquisition: one shared run for Startups / Products / Research Papers.
#
# - Never prints .env: the CLI loads it in-process (dotenv, override=False) in live mode.
# - Paid spend 0 (config/providers.yaml paid_spend_cap: 0); LLM calls bounded by --limit.
# - Expected wall time: papers listing ~5 min (~62 HF links /rows pages paced ~3.6 s/GET
#   with CloudFront 429 back-off, then ~34 OpenAlex batches at ~1/s), vendor homepages +
#   GitHub stars ~15-25 min.
# Usage: bash scripts/b8_live_run.sh            (new run)
#        RUN=<uuid> bash scripts/b8_live_run.sh (resume work/export for an existing run)
set -euo pipefail
cd "$(dirname "$0")/.."

export SIGNAL_INGEST_MODE=live
export SIGNAL_INGEST_NETWORK_GUARD=0
export SIGNAL_INGEST_DSN_LIVE=postgresql://signal:signal@127.0.0.1:5432/signal_live
export LITELLM_LOCAL_MODEL_COST_MAP=True
export LITELLM_MODE=PRODUCTION
SI=.venv/bin/signal-ingest
OUT=var/live/manifests/b8_run
mkdir -p "$OUT"
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
step() { echo "[$(ts)] $*" | tee -a "$OUT/steps.log"; }
run_id_of() { .venv/bin/python -c 'import json,sys; print(json.load(sys.stdin)["run_id"])'; }

step "init-db live"
$SI init-db

if [[ -z "${RUN:-}" ]]; then
  step "crawl bulk.yc (creates run)"
  RUN=$($SI crawl --source bulk.yc | tee "$OUT/crawl_bulk.yc.json" | run_id_of)
  for src in bulk.openrouter bulk.products bulk.papers; do
    step "crawl $src into run $RUN"
    $SI crawl --source "$src" --run "$RUN" > "$OUT/crawl_$src.json"
  done
fi
echo "$RUN" > "$OUT/run_id.txt"
step "run=$RUN"

step "work: fetch/parse/enrich_stars (drain)"
$SI work --stage any --run "$RUN" --concurrency 8 --drain --max-tasks 200000 \
  --lease-seconds 120 --worker-id b8-live > "$OUT/work_$RUN.json"
step "work exit=$?"

step "owner LLM fallback (bounded)"
$SI owner-llm --run "$RUN" --limit 40 | tee "$OUT/owner_llm.json" || step "owner-llm failed (continuing)"

step "seeds from accepted STARTUP rows"
$SI qualify --track seeds --run "$RUN" --no-update-inventory --no-init-db > "$OUT/seeds.json" || step "seeds exit=$?"

step "LLM canary lineage"
$SI qualify --track canary --run "$RUN" --no-update-inventory --no-init-db > "$OUT/canary.json" || step "canary exit=$? (2 = no live LLM success)"

AS_OF=$(ts)
step "export as-of $AS_OF"
$SI export --run "$RUN" --as-of "$AS_OF" > "$OUT/export.json"

step "blocker / acquisition recompute from export"
$SI b8-status > "$OUT/b8_status.json" || step "b8-status: blocked (see artifacts/gates/b8_blocker.json)"

step "accepted counts per class (DB)"
psql "$SIGNAL_INGEST_DSN_LIVE" -v ON_ERROR_STOP=1 -c "
  SELECT record_type, source_id, status, COUNT(DISTINCT external_id) AS n
  FROM candidates WHERE run_id = '$RUN'
  GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;" \
  -c "
  SELECT source_id, reason, COUNT(*) AS n FROM quarantines
  WHERE run_id = '$RUN' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 25;" \
  -c "
  SELECT source_id, stage, status, COUNT(*) FROM tasks
  WHERE run_id = '$RUN' GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;" | tee "$OUT/counts.txt"
.venv/bin/python -c 'import json; d=json.load(open("artifacts/gates/b8_blocker.json")); print(json.dumps({k: d.get(k) for k in ("status","proven","shortfalls")}, indent=1))'
step "done run=$RUN"
