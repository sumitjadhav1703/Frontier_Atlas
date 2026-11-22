"""Canonical request serialization for byte/token measurement."""

from __future__ import annotations

import json
from typing import Any


def serialize_json(obj: Any) -> bytes:
    """UTF-8 JSON with sorted keys and compact separators (stable measurement)."""
    return json.dumps(
        obj,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_default,
    ).encode("utf-8")


def _default(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump(mode="json")
    if isinstance(obj, (set, frozenset)):
        return sorted(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
