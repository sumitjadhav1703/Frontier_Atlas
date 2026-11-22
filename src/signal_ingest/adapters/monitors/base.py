"""Thin per-source adapter helpers wrapping shared discover/detail."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from signal_ingest.adapters.monitors.config import load_monitor_config
from signal_ingest.adapters.monitors.detail import parse_detail
from signal_ingest.adapters.monitors.discover import discover_list
from signal_ingest.adapters.monitors.types import DiscoverScanResult, MonitorParseResult
from signal_ingest.crawl.types import RawObservation


def make_discover(source_id: str):
    def _discover(
        raw: bytes | str | dict[str, Any],
        *,
        checkpoint: str | None = None,
        cutoff: datetime | None = None,
        observed_at: datetime | None = None,
        feed_ordered: bool | None = None,
        base_url: str | None = None,
    ) -> DiscoverScanResult:
        cfg = load_monitor_config(source_id)
        return discover_list(
            cfg,
            raw,
            checkpoint=checkpoint,
            cutoff=cutoff,
            observed_at=observed_at,
            feed_ordered=feed_ordered,
            base_url=base_url or cfg.documented_access_url,
        )

    return _discover


def make_parse(source_id: str):
    def _parse(
        raw: RawObservation,
        *,
        cutoff: datetime | None = None,
        list_fields: dict[str, Any] | None = None,
        board_url: str | None = None,
        employer_url: str | None = None,
        title_hint: str | None = None,
        external_id_hint: str | None = None,
        expected_start: str | None = None,
        expected_end: str | None = None,
        list_observed_at: datetime | str | None = None,
        new_since_last_run: dict[str, Any] | None = None,
    ) -> MonitorParseResult:
        cfg = load_monitor_config(source_id)
        return parse_detail(
            raw,
            cfg,
            cutoff=cutoff,
            list_fields=list_fields,
            board_url=board_url,
            employer_url=employer_url,
            title_hint=title_hint,
            external_id_hint=external_id_hint,
            expected_start=expected_start,
            expected_end=expected_end,
            list_observed_at=list_observed_at,
            new_since_last_run=new_since_last_run,
        )

    return _parse
