"""Worker helpers: claim → fetch (no DB lock) → blob → fenced complete."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg

from signal_ingest.config.settings import Settings, get_settings
from signal_ingest.crawl.fetch import FetchPolicy, fetch_with_retries
from signal_ingest.crawl.parse import parse
from signal_ingest.crawl.pool import HttpPool
from signal_ingest.crawl.queue import (
    claim_tasks,
    complete_with_blob,
    schedule_retry,
)
from signal_ingest.crawl.store import RawStore, load_cached_body
from signal_ingest.crawl.types import ClaimedTask, FetchOutcomeKind, RawObservation


def _dispatch_bulk(task: ClaimedTask) -> bool:
    """True when this claim should use B2 bulk adapter stages."""
    if task.stage in {"parse", "enrich_stars"}:
        return True
    try:
        from signal_ingest.adapters.bulk import is_bulk_source

        return is_bulk_source(task.source_id)
    except ImportError:  # pragma: no cover - package always present in tree
        return False


async def process_claimed_task(
    conn: psycopg.Connection,
    task: ClaimedTask,
    *,
    pool: HttpPool,
    raw_store: RawStore | None = None,
    settings: Settings | None = None,
    policy: FetchPolicy | None = None,
    parse_links: bool = False,
    enqueue_successors: bool = False,
) -> dict[str, Any]:
    """
    Run fetch outside any row lock, save raw before parse, complete under fence.

    Crash after blob write / before commit: restart reclaim yields one effect via
    unique observation(task_id) + fencing token rejection for the stale worker.

    Bulk sources (B2) dispatch discover→fetch→parse(+enrich_stars) via adapters.
    """
    cfg = settings or get_settings()
    from signal_ingest.adapters.monitors.pipeline import MONITOR_SOURCES, process_monitor_task

    if task.source_id in MONITOR_SOURCES:
        from signal_ingest.adapters.monitors.config import load_monitor_config

        mcfg = load_monitor_config(task.source_id)
        if (mcfg.raw or {}).get("max_decoded_bytes"):
            pool.allow_origin_body_bytes(task.url, int(mcfg.raw["max_decoded_bytes"]))
        # Monitor fetch -> parse. Live parse judges freshness at the list observation time
        # (export re-filters at its own cutoff); fixture keeps cutoff=None semantics.
        cutoff = None
        observed = task.payload.get("list_observed_at")
        if cfg.mode.value == "live" and observed:
            cutoff = datetime.fromisoformat(str(observed))
        return await process_monitor_task(
            conn, task, pool=pool, raw_store=raw_store, settings=cfg, policy=policy, cutoff=cutoff
        )
    if _dispatch_bulk(task):
        from signal_ingest.adapters.bulk import process_bulk_task

        return await process_bulk_task(
            conn,
            task,
            pool=pool,
            raw_store=raw_store,
            settings=cfg,
            policy=policy,
        )

    store = raw_store or RawStore.from_settings(cfg)
    fetch_policy = policy or FetchPolicy(mode=cfg.mode)

    cached_etag = None
    cached_body = None
    cached_meta = None
    cached = load_cached_body(conn, store, url=task.url)
    if cached is not None:
        cached_body, cached_meta = cached
        cached_etag = cached_meta.get("etag")

    # IMPORTANT: no open transaction / row lock across HTTP.
    observation = await fetch_with_retries(
        task,
        fetch_policy,
        pool=pool,
        settings=cfg,
        cached_etag=cached_etag,
        cached_body=cached_body,
        cached_meta=cached_meta,
    )

    if observation.outcome is FetchOutcomeKind.RETRY_AT:
        ok = schedule_retry(
            conn,
            task,
            retry_at=observation.retry_at or task.lease_expires,
            error=observation.reason,
        )
        return {
            "ok": ok,
            "outcome": observation.outcome.value,
            "retry_at": observation.retry_at,
            "reason": observation.reason,
        }

    successors: list[str] = []
    if (
        parse_links
        and observation.outcome in {FetchOutcomeKind.OK, FetchOutcomeKind.NOT_MODIFIED}
        and observation.body is not None
    ):
        # Save raw before parse (content-addressed).
        from signal_ingest.crawl.store import save_raw_observation_blob

        save_raw_observation_blob(store, observation)
        candidate = parse(observation)
        if enqueue_successors:
            successors = list(candidate.links)

    committed = complete_with_blob(
        conn,
        task,
        observation,
        store,
        successor_urls=successors if enqueue_successors else None,
    )
    return {
        "ok": committed,
        "outcome": observation.outcome.value,
        "reason": observation.reason,
        "alternate_url": observation.alternate_url,
        "content_sha256": observation.content_sha256,
        "stale_lease": not committed,
    }


async def run_worker_once(
    conn: psycopg.Connection,
    *,
    worker_id: str,
    pool: HttpPool,
    source_id: str | None = None,
    stage: str = "fetch",
    settings: Settings | None = None,
    **process_kwargs: Any,
) -> dict[str, Any] | None:
    """Claim up to one task and process it. Returns None if queue empty."""
    claimed = claim_tasks(
        conn,
        worker_id=worker_id,
        limit=1,
        source_id=source_id,
        stage=stage,
    )
    if not claimed:
        return None
    task = claimed[0]
    result = await process_claimed_task(
        conn,
        task,
        pool=pool,
        settings=settings,
        **process_kwargs,
    )
    result["task_id"] = str(task.id)
    result["fencing_token"] = task.fencing_token
    return result


def recover_orphaned_blob_effect(
    conn: psycopg.Connection,
    task_id: UUID,
) -> bool:
    """True if an observation already exists for the task (one committed effect)."""
    row = conn.execute(
        "SELECT 1 FROM observations WHERE task_id = %s",
        (str(task_id),),
    ).fetchone()
    return row is not None
