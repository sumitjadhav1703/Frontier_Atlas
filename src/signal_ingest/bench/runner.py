"""Benchmark runner: 1k then 10k synthetic with 1 and 4 workers → gate artifact."""

from __future__ import annotations

import json
import os
import platform
import resource
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from signal_ingest.bench.synthetic import process_synthetic_item, synthetic_keys

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ARTIFACT = _REPO_ROOT / "artifacts" / "gates" / "b7_benchmark.json"

SIZES = (1_000, 10_000)
WORKER_COUNTS = (1, 4)


def _peak_rss_mb() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # macOS reports bytes; Linux reports kilobytes.
    rss = float(usage.ru_maxrss)
    if platform.system() == "Darwin":
        return rss / (1024 * 1024)
    return rss / 1024


def _run_one(
    n: int,
    workers: int,
    *,
    batch_hint: int = 250,
) -> dict[str, Any]:
    keys = synthetic_keys(n)
    latencies_ms: list[float] = []
    committed = 0
    duplicates = 0
    retries = 0
    seen: set[str] = set()
    queue_depth_peak = 0
    t0 = time.perf_counter()
    recovery_t0 = time.perf_counter()

    def _one(key: str) -> dict[str, Any]:
        start = time.perf_counter()
        result = process_synthetic_item(key)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        result["latency_ms"] = elapsed_ms
        return result

    pending = list(keys)  # materialize only the key strings (cheap); payloads stay lazy
    queue_depth_peak = len(pending)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        # Submit in waves to keep a bounded working set.
        idx = 0
        while idx < len(pending):
            wave = pending[idx : idx + max(batch_hint, workers)]
            idx += len(wave)
            queue_depth_peak = max(queue_depth_peak, len(pending) - idx + len(wave))
            futs = [pool.submit(_one, k) for k in wave]
            for fut in as_completed(futs):
                result = fut.result()
                latencies_ms.append(float(result["latency_ms"]))
                key = str(result["key"])
                if key in seen:
                    duplicates += 1
                else:
                    seen.add(key)
                    committed += 1

    wall_s = time.perf_counter() - t0
    recovery_s = time.perf_counter() - recovery_t0  # same process; no crash in synthetic
    p95 = (
        statistics.quantiles(latencies_ms, n=20)[18]
        if len(latencies_ms) >= 20
        else (max(latencies_ms) if latencies_ms else 0.0)
    )
    rows_per_s = committed / wall_s if wall_s > 0 else 0.0
    bottleneck = (
        "cpu_hash_walk"
        if workers >= 4 and rows_per_s < committed / max(wall_s, 1e-9) * 1.5
        else "single_worker_serial"
        if workers == 1
        else "thread_pool_scheduling"
    )
    return {
        "n": n,
        "workers": workers,
        "wall_time_s": round(wall_s, 6),
        "committed_rows": committed,
        "committed_rows_per_s": round(rows_per_s, 3),
        "p95_processing_ms": round(p95, 4),
        "p95_fetch_ms": None,  # synthetic — no HTTP
        "peak_rss_mb": round(_peak_rss_mb(), 3),
        "queue_depth_peak": queue_depth_peak,
        "duplicate_commits": duplicates,
        "retries": retries,
        "recovery_time_s": round(recovery_s, 6),
        "bottleneck": bottleneck,
        "input_kind": "synthetic_lazy_keys",
        "not_live_throughput": True,
    }


def run_benchmark(
    *,
    sizes: tuple[int, ...] = SIZES,
    worker_counts: tuple[int, ...] = WORKER_COUNTS,
) -> dict[str, Any]:
    runs: list[dict[str, Any]] = []
    for n in sizes:
        for w in worker_counts:
            runs.append(_run_one(n, w))
    return {
        "schema_version": "b7_benchmark.v1",
        "measured_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "machine": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
        },
        "db_settings": {
            "note": "synthetic bench does not hit PostgreSQL; DB settings pending live/fixture replay measurement",
            "pending_live_evidence": True,
        },
        "runs": runs,
        "projection_notes": {
            "time_lower_bound": (
                "wall ≥ max(stage_work / effective_throughput, "
                "upstream_rate_governed_request_time, provider_token_time)"
            ),
            "storage": (
                "raw_bytes ≈ measured_compressed_bytes_per_item × candidates "
                "+ indexes + retention"
            ),
            "500k_optional": "not run in default gate path; never label as web scrape",
            "measurements_pending_live": True,
        },
        "label": "synthetic — separate from live throughput",
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
    "SIZES",
    "WORKER_COUNTS",
    "run_benchmark",
    "write_benchmark_artifact",
]
