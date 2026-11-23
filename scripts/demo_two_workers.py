"""Two-worker demo: 300 fetch tasks, two `work` processes, kill -9 one mid-run.

Fixture mode, loopback HTTP only. Proves one committed effect per task:
observations count == distinct task_id == tasks enqueued. Fresh run each time.
"""
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from signal_ingest.config import Mode, load_settings  # noqa: E402
from signal_ingest.crawl.queue import enqueue_task  # noqa: E402
from signal_ingest.storage import connect, create_run, init_db  # noqa: E402
from tests.integration.b1_http import start_b1_http_server  # noqa: E402

N_TASKS = 300
KILL_AFTER_S = 3.0
ok = False
LEASE_S = 5  # short lease so B reaps A's orphaned leases quickly

settings = load_settings(mode=Mode.FIXTURE)
init_db(settings)
srv = start_b1_http_server(slow_delay_s=0.15)
srv._httpd.handle_error = lambda *a: None  # SIGKILLed client -> BrokenPipe; expected, keep log clean
logs = Path(tempfile.mkdtemp(prefix="two_worker_demo_"))


def q(sql, *args):
    with connect(settings) as conn:
        return conn.execute(sql, args).fetchall()


def logged_outcomes(path: Path) -> int:
    return sum(1 for line in path.read_text(errors="replace").splitlines() if " outcome=" in line)


try:
    with connect(settings) as conn:
        run = create_run(conn, settings=settings, metadata={"demo": "two_workers"})
        for _ in range(N_TASKS):
            enqueue_task(conn, source_id="demo-two-workers", url=srv.url("/slow"), run_id=run,
                         external_id=f"demo2-{uuid.uuid4().hex}")
        conn.commit()
    print(f"run={run} tasks={N_TASKS} endpoint=/slow delay=0.15s lease={LEASE_S}s")

    cli = [".venv/bin/signal-ingest", "--mode", "fixture", "work", "--stage", "fetch",
           "--run", str(run), "--concurrency", "4", "--drain", "--max-tasks", "1000",
           "--lease-seconds", str(LEASE_S)]
    procs = {}
    for wid in ("A", "B"):
        err = (logs / f"{wid}.err").open("w")
        procs[wid] = subprocess.Popen(cli + ["--worker-id", wid], stdout=subprocess.PIPE,
                                      stderr=err, text=True)
    t0 = time.time()
    time.sleep(KILL_AFTER_S)
    procs["A"].kill()  # SIGKILL: no cleanup, leases left behind
    procs["A"].wait()
    print(f"kill -9 A at t={time.time() - t0:.1f}s; tasks then:",
          q("SELECT status, count(*) FROM tasks WHERE run_id=%s GROUP BY 1 ORDER BY 1", str(run)))

    b_out, _ = procs["B"].communicate(timeout=600)
    b = json.loads(b_out)
    print(f"B exited rc={procs['B'].returncode} at t={time.time() - t0:.1f}s")

    healer = None
    left = q("SELECT count(*) FROM tasks WHERE run_id=%s AND status IN ('ready','leased')", str(run))
    if left[0][0]:
        out = subprocess.run(cli + ["--worker-id", "healer"], capture_output=True, text=True,
                             timeout=600)
        healer = json.loads(out.stdout)

    b_ok = sum(1 for o in b["outcomes"] if o.get("ok") and not o.get("stale_lease"))
    print("per-worker processed:")
    print(f"  A (killed): {logged_outcomes(logs / 'A.err')} completions logged before SIGKILL")
    print(f"  B: processed={b['processed']} committed={b_ok}")
    if healer is not None:
        print(f"  healer: processed={healer['processed']}")
    else:
        print("  healer: not needed (B drained the run)")
    obs = q("SELECT count(*), count(DISTINCT task_id) FROM observations WHERE run_id=%s", str(run))
    print("observations count, distinct task_id:", obs[0])
    print("task status counts:",
          q("SELECT status, count(*) FROM tasks WHERE run_id=%s GROUP BY 1 ORDER BY 1", str(run)))
    ok = obs[0][0] == obs[0][1] == N_TASKS and q(
        "SELECT count(*) FROM tasks WHERE run_id=%s AND status='done'", str(run))[0][0] == N_TASKS
    print("RESULT:", "PASS" if ok else "FAIL")
finally:
    srv.stop()
sys.exit(0 if ok else 1)
