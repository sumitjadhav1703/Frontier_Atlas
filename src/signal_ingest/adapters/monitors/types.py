"""Shared monitor adapter result and health types."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from signal_ingest.freshness.interval import PublicationInterval


class MonitorHealth(str, Enum):
    HEALTHY = "healthy"
    HEALTHY_EMPTY = "healthy_empty"
    PARTIAL = "partial"
    RATE_LIMITED = "rate_limited"
    FAILED = "failed"


@dataclass
class MonitorParseResult:
    """Outcome of parsing one detail observation into NEWS/JOB or quarantine."""

    ok: bool
    record_type: str | None = None
    external_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    quarantine_reason: str | None = None
    quarantine_detail: dict[str, Any] = field(default_factory=dict)
    publication: PublicationInterval | None = None
    eligible: bool = False
    body_complete: bool = False
    board_url: str | None = None
    employer_url: str | None = None


@dataclass
class DiscoverScanResult:
    """List/feed discovery outcome with ordering and stop metadata."""

    items: list[Any]
    cursor: str | None = None
    ordered: bool = False
    descending_proven: bool = False
    early_stopped: bool = False
    capped_feed: bool = False
    scanned_count: int = 0
    limitation: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MonitorSourceConfig:
    source_id: str
    canonical_publisher: str
    scope: str
    status: str
    documented_access_url: str
    original_time_field: str
    original_timezone: str
    domain_governor: str
    pagination_stop_rule: str
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def is_news(self) -> bool:
        return self.scope == "news_monitor" or self.source_id.startswith("news_")

    @property
    def is_jobs(self) -> bool:
        return self.scope == "jobs_monitor" or self.source_id.startswith("jobs_")


@dataclass
class BodyExtraction:
    text: str
    start_excerpt: str
    end_excerpt: str
    complete: bool
    method: str
    evidence: dict[str, Any] = field(default_factory=dict)
