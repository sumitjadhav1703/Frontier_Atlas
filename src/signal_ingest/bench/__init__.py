"""Loopback fixture benchmarks (1k/10k x 1/4 workers) — separate from live throughput."""

from signal_ingest.bench.runner import run_benchmark, write_benchmark_artifact

__all__ = ["run_benchmark", "write_benchmark_artifact"]
