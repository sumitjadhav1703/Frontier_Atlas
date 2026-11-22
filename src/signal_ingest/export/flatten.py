"""Flatten nested payloads to dot-separated Sheet headers."""

from __future__ import annotations

import json
from typing import Any


def flatten_dict(
    data: dict[str, Any],
    *,
    prefix: str = "",
    sep: str = ".",
) -> dict[str, Any]:
    """Recursively flatten; lists/authors become JSON arrays as strings for cells."""
    out: dict[str, Any] = {}
    for key, value in data.items():
        path = f"{prefix}{sep}{key}" if prefix else str(key)
        if isinstance(value, dict):
            out.update(flatten_dict(value, prefix=path, sep=sep))
        elif isinstance(value, list):
            out[path] = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        else:
            out[path] = value
    return out


def cell_value(value: Any) -> Any:
    """Preserve numeric/bool typing; stringify datetimes; leave None as empty."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat().replace("+00:00", "Z")
    return value


def project_row(
    flat: dict[str, Any],
    headers: tuple[str, ...] | list[str],
) -> list[Any]:
    """Stable column order; missing keys → empty cell."""
    return [cell_value(flat.get(h)) for h in headers]


FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def looks_like_formula(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    return value[0] in FORMULA_PREFIXES


__all__ = [
    "FORMULA_PREFIXES",
    "cell_value",
    "flatten_dict",
    "looks_like_formula",
    "project_row",
]
