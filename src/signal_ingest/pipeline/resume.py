"""Resume a run: release stale leases and continue ready work."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from signal_ingest.config.clock import get_clock
from signal_ingest.config.settings import Settings, get_settings
from signal_ingest.crawl.queue import reap_expired
from signal_ingest.pipeline.work import work_stage
from signal_ingest.storage.db import connect


def release_stale_leases(
    *,
    run_id: UUID,
    settings: Settings,
    now: datetime | None = None,
) -> int:
    """Return expired leases to ready; dead-letter the ones with no attempts left."""
    clock = now or get_clock().now()
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=timezone.utc)
    with connect(settings) as conn:
        counts = reap_expired(conn, run_id=run_id, now=clock)
        conn.execute(
            "UPDATE runs SET status = %s, metadata = metadata || %s::jsonb WHERE id = %s",
            (
                "resumed",
                json.dumps({"stale_leases_released": counts["released"], "dead_lettered": counts["failed"]}),
                str(run_id),
            ),
        )
        conn.commit()
    return counts["released"]


def resume_run(
    *,
    run_id: UUID | str,
    settings: Settings | None = None,
    worker_id: str = "worker-resume",
    stage: str = "any",
    max_tasks: int = 500,
) -> dict[str, Any]:
    """Release stale leases then drain remaining ready tasks for the run."""
    cfg = settings or get_settings()
    rid = UUID(str(run_id))
    released = release_stale_leases(run_id=rid, settings=cfg)
    work = work_stage(
        run_id=rid,
        stage=stage,
        settings=cfg,
        worker_id=worker_id,
        max_tasks=max_tasks,
    )
    return {
        "run_id": str(rid),
        "stale_leases_released": released,
        "work": work,
        "raw_root": str(cfg.assert_mode_raw_root()),
    }


__all__ = ["release_stale_leases", "resume_run"]
