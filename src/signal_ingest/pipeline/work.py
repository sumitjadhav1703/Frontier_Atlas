"""Worker loop: N concurrent slots claim → process under shared raw root.

Self-healing behavior:
- each slot owns a pooled DB connection (dropped connections are replaced);
- a heartbeat renews leases of in-flight tasks and reaps expired leases from
  dead workers (retry, or dead-letter when attempts are exhausted);
- a task that raises is rescheduled with backoff; the loop keeps going;
- SIGINT/SIGTERM stop new claims and let in-flight tasks finish;
- with drain=True the worker waits for scheduled retries instead of exiting.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from datetime import timedelta
from typing import Any
from uuid import UUID

from signal_ingest.config.clock import get_clock
from signal_ingest.config.settings import Settings, get_settings
from signal_ingest.crawl.fetch import FetchPolicy, db_retry_delay
from signal_ingest.crawl.pool import HttpPool
from signal_ingest.crawl.queue import (
    DEFAULT_LEASE_SECONDS,
    claim_tasks,
    reap_expired,
    release_lease,
    renew_lease,
    schedule_retry,
)
from signal_ingest.crawl.store import RawStore
from signal_ingest.crawl.types import ClaimedTask
from signal_ingest.crawl.worker import process_claimed_task
from signal_ingest.storage.db import connection_pool

log = logging.getLogger(__name__)

# Give up after this many DB failures in a row (~5 min of backoff): the process
# supervisor restarts us, and leases are recovered by the reaper.
MAX_CONSECUTIVE_DB_ERRORS = 12


async def _heartbeat(
    dbpool: Any,
    inflight: dict[UUID, ClaimedTask],
    *,
    run_id: UUID,
    lease_seconds: int,
) -> None:
    """Renew in-flight leases every lease/3 and reap leases of dead workers."""
    interval = max(lease_seconds / 3, 0.2)
    while True:
        await asyncio.sleep(interval)
        try:
            with dbpool.connection() as conn:
                for task in list(inflight.values()):
                    if not renew_lease(conn, task, lease_seconds=lease_seconds):
                        log.warning("lease lost task=%s source=%s", task.id, task.source_id)
                counts = reap_expired(conn, run_id=run_id)
            if any(counts.values()):
                log.info("reaped run=%s released=%d dead_lettered=%d",
                         run_id, counts["released"], counts["failed"])
        except Exception:  # keep beating; the next tick retries
            log.exception("heartbeat failed")


def _has_pending(conn: Any, run_id: UUID, stage: str | None, source_id: str | None) -> bool:
    """Same scope as the claim filter, so drain cannot wait on work it never claims."""
    sql = "SELECT EXISTS (SELECT 1 FROM tasks WHERE run_id = %s AND status IN ('ready', 'leased')"
    params: list[Any] = [str(run_id)]
    if stage is not None:
        sql += " AND stage = %s"
        params.append(stage)
    if source_id is not None:
        sql += " AND source_id = %s"
        params.append(source_id)
    row = conn.execute(sql + ")", params).fetchone()
    conn.commit()
    return bool(row[0])


async def _run_one(
    conn: Any,
    task: ClaimedTask,
    *,
    pool: HttpPool,
    store: RawStore,
    settings: Settings,
    policy: FetchPolicy,
) -> dict[str, Any]:
    try:
        result = await process_claimed_task(
            conn, task, pool=pool, raw_store=store, settings=settings, policy=policy
        )
    except asyncio.CancelledError:
        try:
            conn.rollback()
            release_lease(conn, task, error="worker cancelled")
        except Exception:  # DB gone: lease expiry + reaper recover the task
            log.exception("could not release task=%s on cancel", task.id)
        raise
    except Exception as exc:
        log.exception("task failed task=%s source=%s attempt=%d",
                      task.id, task.source_id, task.attempt_count)
        error = f"{type(exc).__name__}: {exc}"[:500]
        try:
            conn.rollback()
            retry_at = get_clock().now() + timedelta(seconds=db_retry_delay(task.attempt_count))
            schedule_retry(conn, task, retry_at=retry_at, error=error)
        except Exception:  # DB gone: lease expiry + reaper recover the task
            log.exception("could not reschedule task=%s", task.id)
        return {"ok": False, "outcome": "error", "reason": error}
    log.info("task source=%s stage=%s attempt=%d outcome=%s",
             task.source_id, task.stage, task.attempt_count, result.get("outcome"))
    return result


async def _work_loop(
    *,
    run_id: UUID,
    stage: str,
    worker_id: str,
    settings: Settings,
    max_tasks: int,
    source_id: str | None,
    concurrency: int = 1,
    drain: bool = False,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    idle_poll_s: float = 1.0,
) -> dict[str, Any]:
    store = RawStore.from_settings(settings)
    policy = FetchPolicy(mode=settings.mode)  # one robots cache per process
    stage_filter = None if stage == "any" else stage
    stop = asyncio.Event()
    inflight: dict[UUID, ClaimedTask] = {}
    outcomes: list[dict[str, Any]] = []
    budget = [max_tasks]

    loop = asyncio.get_running_loop()
    handled: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
            handled.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):
            pass  # not main thread / unsupported platform

    async def pause(seconds: float) -> None:
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def slot(dbpool: Any, pool: HttpPool, n: int) -> None:
        wid = worker_id if concurrency == 1 else f"{worker_id}/{n}"
        idle = idle_poll_s
        db_errors = 0
        while not stop.is_set() and budget[0] > 0:
            budget[0] -= 1
            claimed: list[ClaimedTask] = []
            refunded = False
            try:
                # Fresh checkout per task: the pool pings it and swaps a dead socket.
                with dbpool.connection() as conn:
                    claimed = claim_tasks(
                        conn,
                        worker_id=wid,
                        limit=1,
                        stage=stage_filter,
                        run_id=run_id,
                        source_id=source_id,
                        lease_seconds=lease_seconds,
                    )
                    if not claimed:
                        budget[0] += 1
                        refunded = True
                        if not drain or not _has_pending(conn, run_id, stage_filter, source_id):
                            return
                    else:
                        task = claimed[0]
                        inflight[task.id] = task
                        try:
                            result = await _run_one(
                                conn, task, pool=pool, store=store,
                                settings=settings, policy=policy,
                            )
                        finally:
                            inflight.pop(task.id, None)
                        result["task_id"] = str(task.id)
                        result["stage"] = task.stage
                        result["source_id"] = task.source_id
                        result["content_key"] = result.get("content_key") or result.get(
                            "content_sha256"
                        )
                        row = conn.execute(
                            "SELECT content_key FROM observations WHERE task_id = %s",
                            (str(task.id),),
                        ).fetchone()
                        if row and row[0]:
                            result["content_key"] = row[0]
                        outcomes.append(result)
            except Exception:
                # DB hiccup around claim/bookkeeping: back off and try again. A claimed
                # task left behind is recovered by lease expiry + the reaper.
                log.exception("slot %s db error; backing off", wid)
                if not claimed and not refunded:
                    budget[0] += 1
                db_errors += 1
                if db_errors >= MAX_CONSECUTIVE_DB_ERRORS:
                    raise
                await pause(idle)
                idle = min(idle * 2, 30.0)
                continue
            db_errors = 0
            if claimed:
                idle = idle_poll_s
            else:
                await pause(idle)
                idle = min(idle * 2, 30.0)

    try:
        with connection_pool(settings, size=concurrency + 1) as dbpool:
            pool = HttpPool()
            beat = asyncio.create_task(
                _heartbeat(dbpool, inflight, run_id=run_id, lease_seconds=lease_seconds)
            )
            try:
                async with asyncio.TaskGroup() as tg:
                    for n in range(concurrency):
                        tg.create_task(slot(dbpool, pool, n))
            finally:
                beat.cancel()
                await asyncio.gather(beat, return_exceptions=True)
                await pool.aclose()
            with dbpool.connection() as conn:
                conn.execute(
                    "UPDATE runs SET status = %s, metadata = metadata || %s::jsonb WHERE id = %s",
                    (
                        "working",
                        json.dumps(
                            {
                                "last_worker": worker_id,
                                "last_stage": stage,
                                "processed": len(outcomes),
                                "stopped_by_signal": stop.is_set(),
                            }
                        ),
                        str(run_id),
                    ),
                )
    finally:
        for sig in handled:
            loop.remove_signal_handler(sig)
    return {
        "run_id": str(run_id),
        "stage": stage,
        "worker_id": worker_id,
        "processed": len(outcomes),
        "raw_root": str(settings.assert_mode_raw_root()),
        "outcomes": outcomes,
    }


def work_stage(
    *,
    run_id: UUID | str,
    stage: str,
    settings: Settings | None = None,
    worker_id: str = "worker-1",
    max_tasks: int = 100,
    source_id: str | None = None,
    concurrency: int = 1,
    drain: bool = False,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> dict[str, Any]:
    """Process up to max_tasks for a run/stage using the shared raw store."""
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")
    cfg = settings or get_settings()
    rid = UUID(str(run_id))
    return asyncio.run(
        _work_loop(
            run_id=rid,
            stage=stage,
            worker_id=worker_id,
            settings=cfg,
            max_tasks=max_tasks,
            source_id=source_id,
            concurrency=concurrency,
            drain=drain,
            lease_seconds=lease_seconds,
        )
    )


__all__ = ["work_stage"]
