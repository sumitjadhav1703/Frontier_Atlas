"""Load dated source-qualification inventory skeletons."""

from __future__ import annotations

from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCES_DIR = _REPO_ROOT / "config" / "sources"


def load_source_inventory(directory: Path | None = None) -> list[dict[str, Any]]:
    """Return all YAML source entries (excluding README). Requires PyYAML only if present.

    B0 ships plain YAML; parsing uses a minimal subset reader when PyYAML is absent.
    """
    root = directory or DEFAULT_SOURCES_DIR
    entries: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.yaml")):
        entries.append(_parse_simple_yaml(path.read_text(encoding="utf-8"), source=path.name))
    return entries


def _parse_simple_yaml(text: str, *, source: str) -> dict[str, Any]:
    """Minimal YAML subset parser for flat key: value inventories (no nested maps)."""
    data: dict[str, Any] = {}
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            raise ValueError(f"{source}:{lineno}: expected key: value")
        key, _, value = line.partition(":")
        key = key.strip()
        value = value.strip()
        if value.startswith('"') and value.endswith('"') and len(value) >= 2:
            value = value[1:-1]
        elif value.lower() == "null":
            value = None
        elif value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False
        data[key] = value
    required = {
        "source_id",
        "canonical_publisher",
        "scope",
        "status",
        "documented_access_url",
        "policy_url",
        "review_date",
        "allowed_text_use",
        "original_time_field",
        "original_timezone",
        "pagination_stop_rule",
        "attribution",
        "domain_governor",
        "fallback",
        "reviewer",
        "probe_artifact",
    }
    missing = required - set(data)
    if missing:
        raise ValueError(f"{source}: missing keys {sorted(missing)}")
    if data.get("status") not in {"candidate", "qualified", "disabled"}:
        raise ValueError(f"{source}: invalid status {data.get('status')!r}")
    return data
