#!/usr/bin/env bash
# Two-worker horizontal-scaling demo (fixture mode, loopback only). Re-runnable.
set -euo pipefail
cd "$(dirname "$0")/.."
export SIGNAL_INGEST_MODE=fixture SIGNAL_INGEST_NETWORK_GUARD=1
export SIGNAL_INGEST_DSN_FIXTURE="${SIGNAL_INGEST_DSN_FIXTURE:-postgresql://signal:signal@127.0.0.1:5432/signal_fixture}"
export SIGNAL_INGEST_TEST_DSN_FIXTURE="${SIGNAL_INGEST_TEST_DSN_FIXTURE:-$SIGNAL_INGEST_DSN_FIXTURE}"
mkdir -p artifacts/gates
{ echo "# two_worker_demo $(date -u +%Y-%m-%dT%H:%M:%SZ)"; .venv/bin/python -u scripts/demo_two_workers.py; } \
  2>&1 | tee artifacts/gates/two_worker_demo.txt
exit "${PIPESTATUS[0]}"
