"""Import helpers for B5 fallback / quota surfaces.

Prefer package exports; fall back to submodules so gates fail clearly
when the B5 stack is incomplete.
"""

from __future__ import annotations

import importlib
from typing import Any


class MissingB5APIError(AssertionError):
    """Required B5 API is not importable."""


def _import(path: str) -> Any:
    try:
        return importlib.import_module(path)
    except ImportError as exc:
        raise MissingB5APIError(f"Cannot import {path}: {exc}") from exc


def require_attr(module: Any, *names: str) -> Any:
    tried = []
    for name in names:
        tried.append(f"{getattr(module, '__name__', module)}.{name}")
        if hasattr(module, name):
            return getattr(module, name)
    raise MissingB5APIError("Missing API: " + ", ".join(tried))


def load_fallback() -> Any:
    return _import("signal_ingest.llm.fallback")


def load_quota() -> Any:
    return _import("signal_ingest.llm.quota")


def load_circuit() -> Any:
    return _import("signal_ingest.llm.circuit")


def load_retry() -> Any:
    return _import("signal_ingest.llm.retry")


def load_transport() -> Any:
    return _import("signal_ingest.llm.http_transport")


def load_transcript() -> Any:
    return _import("signal_ingest.llm.transcript")


def load_errors() -> Any:
    return _import("signal_ingest.llm.errors")


def load_adapters() -> Any:
    return _import("signal_ingest.llm.adapters")
