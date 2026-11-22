"""Per-source monitor run ledger and health classification."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from signal_ingest.adapters.monitors.types import MonitorHealth
from signal_ingest.config.clock import get_clock


def classify_health(
    *,
    failed: bool = False,
    rate_limited: bool = False,
    partial: bool = False,
    accepted_count: int = 0,
    limitation: str | None = None,
) -> MonitorHealth:
    """
    Distinguish healthy / healthy_empty / partial / rate_limited / failed.

    A legitimate empty scan (complete pagination, no eligible signals) is healthy_empty.
    """
    if failed:
        return MonitorHealth.FAILED
    if rate_limited:
        return MonitorHealth.RATE_LIMITED
    if partial or (limitation and "capped" in limitation):
        return MonitorHealth.PARTIAL
    if accepted_count <= 0:
        return MonitorHealth.HEALTHY_EMPTY
    return MonitorHealth.HEALTHY


def insert_monitor_run(
    conn: psycopg.Connection,
    *,
    source_id: str,
    cutoff_at: datetime,
    health: MonitorHealth | str,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    run_id: UUID | None = None,
    pagination_complete: bool = False,
    feed_ordered: bool | None = None,
    descending_proven: bool | None = None,
    capped_feed: bool = False,
    scan_lower: datetime | None = None,
    scan_upper: datetime | None = None,
    discovered_count: int = 0,
    fetched_count: int = 0,
    accepted_count: int = 0,
    rejected_count: int = 0,
    pending_count: int = 0,
    limitation: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> UUID:
    mid = uuid4()
    now = get_clock().now()
    health_value = health.value if isinstance(health, MonitorHealth) else str(health)
    conn.execute(
        """
        INSERT INTO monitor_runs (
            id, run_id, source_id, started_at, finished_at, cutoff_at, health,
            pagination_complete, feed_ordered, descending_proven, capped_feed,
            scan_lower, scan_upper,
            discovered_count, fetched_count, accepted_count, rejected_count, pending_count,
            limitation, metadata, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        """,
        (
            str(mid),
            str(run_id) if run_id else None,
            source_id,
            started_at or now,
            finished_at or now,
            cutoff_at,
            health_value,
            pagination_complete,
            feed_ordered,
            descending_proven,
            capped_feed,
            scan_lower,
            scan_upper,
            discovered_count,
            fetched_count,
            accepted_count,
            rejected_count,
            pending_count,
            limitation,
            Jsonb(metadata or {}),
            now,
        ),
    )
    return mid


def get_latest_monitor_run(
    conn: psycopg.Connection,
    *,
    source_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, run_id, source_id, started_at, finished_at, cutoff_at, health,
               pagination_complete, feed_ordered, descending_proven, capped_feed,
               scan_lower, scan_upper,
               discovered_count, fetched_count, accepted_count, rejected_count, pending_count,
               limitation, metadata, created_at
        FROM monitor_runs
        WHERE source_id = %s
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (source_id,),
    ).fetchone()
    if row is None:
        return None
    cols = [
        "id",
        "run_id",
        "source_id",
        "started_at",
        "finished_at",
        "cutoff_at",
        "health",
        "pagination_complete",
        "feed_ordered",
        "descending_proven",
        "capped_feed",
        "scan_lower",
        "scan_upper",
        "discovered_count",
        "fetched_count",
        "accepted_count",
        "rejected_count",
        "pending_count",
        "limitation",
        "metadata",
        "created_at",
    ]
    return dict(zip(cols, row, strict=True))
