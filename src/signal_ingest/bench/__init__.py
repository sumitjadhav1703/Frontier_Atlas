"""Synthetic load benchmarks (1k/10k) — separate from live throughput."""

from signal_ingest.bench.runner import run_benchmark, write_benchmark_artifact

__all__ = ["run_benchmark", "write_benchmark_artifact"]
