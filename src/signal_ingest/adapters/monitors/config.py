"""Load monitor source YAML configs (news_* / jobs_*)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from signal_ingest.adapters.monitors.types import MonitorSourceConfig
from signal_ingest.config.sources_inventory import (
    DEFAULT_SOURCES_DIR,
    load_source_inventory,
)

MONITOR_SCOPES = frozenset({"news_monitor", "jobs_monitor"})


def list_monitor_source_ids(directory: Path | None = None) -> list[str]:
    return [c.source_id for c in load_monitor_configs(directory)]


def load_monitor_configs(directory: Path | None = None) -> list[MonitorSourceConfig]:
    entries = load_source_inventory(directory or DEFAULT_SOURCES_DIR)
    out: list[MonitorSourceConfig] = []
    for entry in entries:
        scope = str(entry.get("scope") or "")
        sid = str(entry.get("source_id") or "")
        if scope not in MONITOR_SCOPES and not (
            sid.startswith("news_") or sid.startswith("jobs_")
        ):
            continue
        out.append(_to_config(entry))
    return out


def load_monitor_config(
    source_id: str,
    directory: Path | None = None,
) -> MonitorSourceConfig:
    for cfg in load_monitor_configs(directory):
        if cfg.source_id == source_id:
            return cfg
    raise KeyError(f"unknown monitor source: {source_id}")


def _to_config(entry: dict[str, Any]) -> MonitorSourceConfig:
    return MonitorSourceConfig(
        source_id=str(entry["source_id"]),
        canonical_publisher=str(entry["canonical_publisher"]),
        scope=str(entry["scope"]),
        status=str(entry["status"]),
        documented_access_url=str(entry["documented_access_url"]),
        original_time_field=str(entry["original_time_field"]),
        original_timezone=str(entry["original_timezone"]),
        domain_governor=str(entry["domain_governor"]),
        pagination_stop_rule=str(entry["pagination_stop_rule"]),
        raw=dict(entry),
    )
