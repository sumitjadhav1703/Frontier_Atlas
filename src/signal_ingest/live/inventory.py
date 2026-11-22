"""Update config/sources/*.yaml candidate → qualified|disabled with probe fields."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from signal_ingest.config.sources_inventory import (
    DEFAULT_SOURCES_DIR,
    load_source_inventory,
)

ALLOWED_STATUS = frozenset({"candidate", "qualified", "disabled"})


def source_yaml_path(source_id: str, directory: Path | None = None) -> Path:
    root = directory or DEFAULT_SOURCES_DIR
    # Inventory files are named after source_id with underscores (news_mit.yaml).
    direct = root / f"{source_id}.yaml"
    if direct.is_file():
        return direct
    for path in root.glob("*.yaml"):
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            if line.strip().startswith("source_id:"):
                value = line.split(":", 1)[1].strip().strip('"').strip("'")
                if value == source_id:
                    return path
    raise FileNotFoundError(f"no inventory yaml for source_id={source_id!r}")


def update_source_status(
    source_id: str,
    *,
    status: str,
    probe: dict[str, Any] | None = None,
    directory: Path | None = None,
) -> Path:
    """Rewrite flat inventory YAML with new status and optional probe_* fields."""
    if status not in ALLOWED_STATUS:
        raise ValueError(f"invalid status {status!r}")
    path = source_yaml_path(source_id, directory)
    data = _read_flat_yaml(path)
    if data.get("source_id") != source_id:
        # Allow inventory id aliases (bulk_yc_startups vs bulk.yc adapter).
        pass
    data["status"] = status
    probe = probe or {}
    mapping = {
        "probe_artifact": "probe_artifact",
        "fetch_status": "probe_fetch_status",
        "schema_fingerprint": "probe_schema_fingerprint",
        "accepted_fraction": "probe_accepted_fraction",
        "sample_date": "probe_sample_date",
        "rate_limit_headers": "probe_rate_limit_headers",
        "observed_at": "probe_observed_at",
        "detail_url": "probe_detail_url",
        "error": "probe_error",
        "pilot_count": "probe_pilot_count",
    }
    for src_key, yaml_key in mapping.items():
        if src_key in probe and probe[src_key] is not None:
            data[yaml_key] = probe[src_key]
    _write_flat_yaml(path, data)
    return path


def inventory_by_scope(
    directory: Path | None = None,
) -> dict[str, list[dict[str, Any]]]:
    entries = load_source_inventory(directory)
    out: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        scope = str(entry.get("scope") or "unknown")
        out.setdefault(scope, []).append(entry)
    return out


def _read_flat_yaml(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = {}
    order: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        key = key.strip()
        value = value.strip()
        data[key] = _coerce(value)
        if key not in order:
            order.append(key)
    data["_order"] = order  # type: ignore[assignment]
    return data


def _write_flat_yaml(path: Path, data: dict[str, Any]) -> None:
    order = list(data.pop("_order", []) or [])
    preferred = [
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
        "probe_fetch_status",
        "probe_schema_fingerprint",
        "probe_accepted_fraction",
        "probe_sample_date",
        "probe_rate_limit_headers",
        "probe_observed_at",
        "probe_detail_url",
        "probe_error",
        "probe_pilot_count",
        "notes",
    ]
    keys: list[str] = []
    for key in preferred:
        if key in data and key not in keys:
            keys.append(key)
    for key in order:
        if key in data and key not in keys and not str(key).startswith("_"):
            keys.append(key)
    for key in sorted(data):
        if key not in keys and not str(key).startswith("_"):
            keys.append(key)
    lines: list[str] = []
    for key in keys:
        lines.append(f"{key}: {_format_value(data[key])}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _coerce(value: str) -> Any:
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        return value[1:-1]
    lower = value.lower()
    if lower == "null":
        return None
    if lower == "true":
        return True
    if lower == "false":
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def _format_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    text = str(value)
    if text == "" or any(ch in text for ch in ":#{}[]&*!|>%@`"):
        escaped = text.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return text
