"""Loopback fixture benchmark: real queue + `work` loop + raw store against local HTTP servers.

Inputs are synthetic items served by loopback fixture servers. Numbers measure this
pipeline on this machine; they are not web-scrape or live-source throughput.
"""

from __future__ import annotations

import json
import os
import platform
import resource
import shutil
import statistics
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator

from signal_ingest.config.settings import Mode, Settings, load_settings
from signal_ingest.crawl.pool import HttpPool, PoolLimits
from signal_ingest.crawl.queue import enqueue_task
from signal_ingest.pipeline import work as work_mod
from signal_ingest.storage.db import connect
from signal_ingest.storage.records import create_run

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ARTIFACT = _REPO_ROOT / "artifacts" / "gates" / "b7_benchmark.json"

SIZES = (1_000, 10_000)
WORKER_COUNTS = (1, 4)
SOURCE_ID = "bench.loopback"
# Several loopback origins so the per-origin politeness pace (HttpPool default) is not
# the only thing measured; each origin is still paced exactly as in production.
DEFAULT_ORIGINS = 8
DEFAULT_LATENCY_S = 0.002
MODE_LABEL = "fixture_loopback"


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802
        srv = self.server
        if self.path == "/robots.txt":
            body, ctype, code = b"not found", "text/plain", 404
        else:
            time.sleep(srv.latency_s)  # type: ignore[attr-defined]
            if self.path.endswith(".json"):
                body = json.dumps({"id": self.path, "title": f"bench item {self.path}",
                                   "stars": len(self.path)}).encode()
                ctype = "application/json"
            else:
                body = (f"<html><head><title>bench {self.path}</title></head>"
                        f"<body><p>item {self.path}</p></body></html>").encode()
                ctype = "text/html; charset=utf-8"
            code = 200
        with srv.lock:  # type: ignore[attr-defined]
            srv.hits += 1  # type: ignore[attr-defined]
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:  # silence per-request stderr
        pass


@contextmanager
def _loopback_servers(count: int, latency_s: float) -> Iterator[list[str]]:
    servers = []
    for _ in range(count):
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        httpd.daemon_threads = True
        httpd.latency_s, httpd.lock, httpd.hits = latency_s, threading.Lock(), 0  # type: ignore[attr-defined]
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        servers.append(httpd)
    try:
        yield [f"http://127.0.0.1:{s.server_address[1]}" for s in servers]
    finally:
        for s in servers:
            s.shutdown()
            s.server_close()


@contextmanager
def _timed() -> Iterator[dict[str, list[float]]]:
    """Time real fetches (HttpPool.request, incl. pacing) and whole tasks (work._run_one).

    shortcut: process-wide wrap for the bench call only; upgrade to pool metrics hooks
    if the bench ever runs alongside other work in one process.
    """
    samples: dict[str, list[float]] = {"fetch": [], "task": []}
    orig_request, orig_run_one = HttpPool.request, work_mod._run_one

    async def request(self: HttpPool, method: str, url: str, **kw: Any) -> Any:
        t = time.perf_counter()
        try:
            return await orig_request(self, method, url, **kw)
        finally:
            if not url.endswith("/robots.txt"):
                samples["fetch"].append((time.perf_counter() - t) * 1000.0)

    async def run_one(*args: Any, **kw: Any) -> dict[str, Any]:
        t = time.perf_counter()
        try:
            return await orig_run_one(*args, **kw)
        finally:
            samples["task"].append((time.perf_counter() - t) * 1000.0)

    HttpPool.request, work_mod._run_one = request, run_one  # type: ignore[method-assign]
    try:
        yield samples
    finally:
        HttpPool.request, work_mod._run_one = orig_request, orig_run_one  # type: ignore[method-assign]


def _pct(values: list[float], q: int) -> float | None:
    if not values:
        return None
    if len(values) < 2:
        return round(values[0], 3)
    return round(statistics.quantiles(values, n=100)[q - 1], 3)


def _peak_rss_mb() -> float:
    rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return rss / (1024 * 1024) if platform.system() == "Darwin" else rss / 1024


def _bench_settings(settings: Settings | None, raw_root: Path) -> Settings:
    cfg = settings or load_settings()
    if cfg.mode is not Mode.FIXTURE:
        raise RuntimeError("benchmark runs only in fixture mode (never the live DB)")
    # Separate raw root so bench blobs never mix with the fixture replay store.
    update: dict[str, Any] = {"raw_root_fixture": raw_root}
    # Gate runs provide the disposable fixture DB here; an explicit settings object wins.
    if settings is None and os.environ.get("SIGNAL_INGEST_TEST_DSN_FIXTURE"):
        update["dsn_fixture"] = os.environ["SIGNAL_INGEST_TEST_DSN_FIXTURE"]
    return cfg.model_copy(update=update)


def _run_one(
    n: int,
    workers: int,
    *,
    settings: Settings,
    origins: list[str],
    keep_rows: bool,
) -> dict[str, Any]:
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings,
                            metadata={"bench": MODE_LABEL, "n": n, "workers": workers})
        for i in range(n):
            ext = ".json" if i % 3 == 0 else ""
            enqueue_task(conn, source_id=SOURCE_ID, run_id=run_id,
                         url=f"{origins[i % len(origins)]}/{run_id}/{i}{ext}",
                         external_id=f"{run_id}:{i}")
        conn.commit()

    rss_before = _peak_rss_mb()
    with _timed() as samples:
        t0 = time.perf_counter()
        result = work_mod.work_stage(run_id=run_id, stage="fetch", settings=settings,
                                     worker_id=f"bench-w{workers}", max_tasks=n,
                                     concurrency=workers)
        wall_s = time.perf_counter() - t0

    with connect(settings) as conn:
        obs = conn.execute(
            """SELECT count(*) FILTER (WHERE outcome = 'ok'),
                      count(*) - count(DISTINCT request_url),
                      count(*) - count(DISTINCT task_id),
                      avg(content_length)
               FROM observations WHERE run_id = %s""",
            (str(run_id),),
        ).fetchone()
        tasks = conn.execute(
            """SELECT count(*) FILTER (WHERE status <> 'done'),
                      coalesce(sum(greatest(attempt_count - 1, 0)), 0)
               FROM tasks WHERE run_id = %s""",
            (str(run_id),),
        ).fetchone()
        if not keep_rows:
            for table in ("observations", "tasks", "runs"):
                col = "id" if table == "runs" else "run_id"
                conn.execute(f"DELETE FROM {table} WHERE {col} = %s", (str(run_id),))
        conn.commit()

    committed, dup_url, dup_task, avg_bytes = obs
    unfinished, retries = tasks
    fetch_ms, task_ms = samples["fetch"], samples["task"]
    fetch_share = sum(fetch_ms) / sum(task_ms) if task_ms and sum(task_ms) else 0.0
    return {
        "n": n,
        "workers": workers,
        "mode": MODE_LABEL,
        "origins": len(origins),
        "wall_time_s": round(wall_s, 3),
        "processed": result["processed"],
        "committed_rows": int(committed),
        "committed_rows_per_s": round(int(committed) / wall_s, 2) if wall_s else 0.0,
        "tasks_per_s": round(result["processed"] / wall_s, 2) if wall_s else 0.0,
        "p50_fetch_ms": _pct(fetch_ms, 50),
        "p95_fetch_ms": _pct(fetch_ms, 95),
        "p50_processing_ms": _pct(task_ms, 50),
        "p95_processing_ms": _pct(task_ms, 95),
        "fetch_share_of_task_time": round(fetch_share, 3),
        "bytes_per_item": round(float(avg_bytes), 1) if avg_bytes is not None else None,
        "peak_rss_mb": round(max(rss_before, _peak_rss_mb()), 1),
        # All n tasks are enqueued up front; each slot holds at most one claimed task.
        "queue_depth_peak": n,
        "max_in_memory_tasks": workers,
        "duplicate_commits": int(dup_url) + int(dup_task),
        "unfinished_tasks": int(unfinished),
        "retries": int(retries),
        "recovery_time_s": None,  # no crash injected here; see B1/R1 kill -9 evidence
        "bottleneck": (
            "fetch_incl_per_origin_pace" if fetch_share >= 0.5
            else "db_claim_commit_and_blob_write"
        ),
        "input_kind": "synthetic_items_on_loopback_http",
        "not_live_throughput": True,
    }


def _db_settings(settings: Settings) -> dict[str, Any]:
    with connect(settings) as conn:
        out = {
            name: conn.execute(f"SHOW {name}").fetchone()[0]
            for name in ("server_version", "max_connections", "shared_buffers",
                         "synchronous_commit", "fsync")
        }
        conn.commit()
    out["database"] = "fixture"
    out["pending_live_evidence"] = True
    return out


def run_benchmark(
    *,
    sizes: tuple[int, ...] = SIZES,
    worker_counts: tuple[int, ...] = WORKER_COUNTS,
    settings: Settings | None = None,
    origins: int = DEFAULT_ORIGINS,
    latency_s: float = DEFAULT_LATENCY_S,
    keep_rows: bool = False,
) -> dict[str, Any]:
    raw_root = Path(tempfile.mkdtemp(prefix="signal-bench-raw-"))
    try:
        cfg = _bench_settings(settings, raw_root)
        with _loopback_servers(origins, latency_s) as urls:
            runs = [
                _run_one(n, w, settings=cfg, origins=urls, keep_rows=keep_rows)
                for n in sizes
                for w in worker_counts
            ]
        db = _db_settings(cfg)
    finally:
        shutil.rmtree(raw_root, ignore_errors=True)
    limits = PoolLimits()
    return {
        "schema_version": "b7_benchmark.v2",
        "mode": MODE_LABEL,
        "measured_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "machine": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
        },
        "db_settings": db,
        "fixture_server": {
            "origins": origins,
            "artificial_latency_ms": latency_s * 1000.0,
            "pool_limits": {
                "max_active": limits.max_active,
                "per_origin_concurrency": limits.per_origin_concurrency,
                "per_origin_min_interval_s": limits.per_origin_min_interval_s,
            },
        },
        "runs": runs,
        "projection_notes": {
            "time_lower_bound": (
                "wall >= max(stage_work / effective_throughput, "
                "upstream_rate_governed_request_time, provider_token_time)"
            ),
            "storage": (
                "raw_bytes ~= measured_compressed_bytes_per_item x candidates "
                "+ indexes + retention"
            ),
            "500k_optional": "not run; never label as web scrape",
            "measurements_pending_live": True,
        },
        "label": (
            "synthetic items, fixture_loopback: real queue + work loop + raw store "
            "against local HTTP servers; not a web scrape, separate from live throughput"
        ),
    }


def write_benchmark_artifact(
    report: dict[str, Any] | None = None,
    *,
    path: Path | None = None,
) -> Path:
    dest = path or DEFAULT_ARTIFACT
    dest.parent.mkdir(parents=True, exist_ok=True)
    payload = report if report is not None else run_benchmark()
    dest.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return dest


__all__ = [
    "DEFAULT_ARTIFACT",
    "MODE_LABEL",
    "SIZES",
    "WORKER_COUNTS",
    "run_benchmark",
    "write_benchmark_artifact",
]
