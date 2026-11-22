"""Task enqueue and claim/lease with fencing tokens."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Sequence
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from signal_ingest.config.clock import get_clock
from signal_ingest.crawl.store import insert_observation, save_raw_observation_blob
from signal_ingest.crawl.types import (
    TASK_STATUS_FOR_OUTCOME,
    ClaimedTask,
    FetchOutcomeKind,
    RawObservation,
)


DEFAULT_LEASE_SECONDS = 60


def semantic_key(
    source_id: str,
    stage: str,
    *,
    external_id: str | None = None,
    url: str | None = None,
    version: str = "1",
) -> str:
    """Stable unique task key. Prefer external_id; fall back to normalized URL."""
    identity = external_id if external_id else (url or "")
    return f"{source_id}|{stage}|{version}|{identity}"


def enqueue_task(
    conn: psycopg.Connection,
    *,
    source_id: str,
    url: str,
    stage: str = "fetch",
    external_id: str | None = None,
    run_id: UUID | None = None,
    parent_task_id: UUID | None = None,
    payload: dict[str, Any] | None = None,
    version: str = "1",
    max_attempts: int = 5,
    next_attempt: datetime | None = None,
    task_id: UUID | None = None,
) -> UUID | None:
    """
    Insert a ready task. Returns id on insert, None if semantic_key already exists
    (duplicate discovery is a no-op).
    """
    key = semantic_key(source_id, stage, external_id=external_id, url=url, version=version)
    tid = task_id or uuid4()
    when = next_attempt or get_clock().now()
    row = conn.execute(
        """
        INSERT INTO tasks (
            id, run_id, source_id, stage, semantic_key, external_id, url,
            status, max_attempts, next_attempt, parent_task_id, payload
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            'ready', %s, %s, %s, %s
        )
        ON CONFLICT (semantic_key) DO NOTHING
        RETURNING id
        """,
        (
            str(tid),
            str(run_id) if run_id else None,
            source_id,
            stage,
            key,
            external_id,
            url,
            max_attempts,
            when,
            str(parent_task_id) if parent_task_id else None,
            Jsonb(payload or {}),
        ),
    ).fetchone()
    return UUID(str(row[0])) if row else None


def enqueue_tasks_batch(
    conn: psycopg.Connection,
    items: Sequence[dict[str, Any]],
) -> list[UUID]:
    """Enqueue many tasks in the current transaction; skip duplicates."""
    created: list[UUID] = []
    for item in items:
        tid = enqueue_task(conn, **item)
        if tid is not None:
            created.append(tid)
    return created


def claim_tasks(
    conn: psycopg.Connection,
    *,
    worker_id: str,
    limit: int = 1,
    source_id: str | None = None,
    stage: str | None = None,
    run_id: UUID | None = None,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    now: datetime | None = None,
) -> list[ClaimedTask]:
    """
    Short-transaction claim: SELECT … FOR UPDATE SKIP LOCKED, set lease + fencing
    token, commit. Caller must not hold this connection lock across HTTP.
    """
    if limit < 1:
        return []
    clock = now or get_clock().now()
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=timezone.utc)
    expires = clock + timedelta(seconds=lease_seconds)

    filters = [
        "(status = 'ready' OR (status = 'leased' AND lease_expires < %s))",
        "next_attempt <= %s",
        "attempt_count < max_attempts",
    ]
    params: list[Any] = [clock, clock]
    if source_id is not None:
        filters.append("source_id = %s")
        params.append(source_id)
    if stage is not None:
        filters.append("stage = %s")
        params.append(stage)
    if run_id is not None:
        filters.append("run_id = %s")
        params.append(str(run_id))
    where = " AND ".join(filters)
    params.append(limit)

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            WITH picked AS (
                SELECT id
                FROM tasks
                WHERE {where}
                ORDER BY next_attempt ASC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE tasks AS t
            SET
                status = 'leased',
                lease_owner = %s,
                lease_expires = %s,
                fencing_token = t.fencing_token + 1,
                attempt_count = t.attempt_count + 1,
                updated_at = %s
            FROM picked
            WHERE t.id = picked.id
            RETURNING
                t.id, t.run_id, t.source_id, t.stage, t.semantic_key, t.external_id,
                t.url, t.attempt_count, t.max_attempts, t.fencing_token, t.lease_owner,
                t.lease_expires, t.payload, t.parent_task_id
            """,
            (*params, worker_id, expires, clock),
        )
        rows = cur.fetchall()
    conn.commit()
    return [_row_to_claimed(r, worker_id=worker_id) for r in rows]


def renew_lease(
    conn: psycopg.Connection,
    task: ClaimedTask,
    *,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    now: datetime | None = None,
) -> bool:
    """Extend lease only if owner + fencing token still match."""
    clock = now or get_clock().now()
    expires = clock + timedelta(seconds=lease_seconds)
    row = conn.execute(
        """
        UPDATE tasks
        SET lease_expires = %s, updated_at = %s
        WHERE id = %s
          AND lease_owner = %s
          AND fencing_token = %s
          AND status = 'leased'
        RETURNING id
        """,
        (expires, clock, str(task.id), task.lease_owner, task.fencing_token),
    ).fetchone()
    conn.commit()
    return row is not None


def release_lease(
    conn: psycopg.Connection,
    task: ClaimedTask,
    *,
    ready: bool = True,
    next_attempt: datetime | None = None,
    error: str | None = None,
) -> bool:
    """Release a lease back to ready/failed without completing work."""
    clock = get_clock().now()
    status = "ready" if ready else "failed"
    when = next_attempt or clock
    row = conn.execute(
        """
        UPDATE tasks
        SET
            status = %s,
            lease_owner = NULL,
            lease_expires = NULL,
            next_attempt = %s,
            last_error = COALESCE(%s, last_error),
            updated_at = %s
        WHERE id = %s
          AND lease_owner = %s
          AND fencing_token = %s
        RETURNING id
        """,
        (status, when, error, clock, str(task.id), task.lease_owner, task.fencing_token),
    ).fetchone()
    conn.commit()
    return row is not None


def schedule_retry(
    conn: psycopg.Connection,
    task: ClaimedTask,
    *,
    retry_at: datetime,
    error: str | None = None,
) -> bool:
    """
    Put task back to ready at retry_at. Releases lease so worker slots are free
    while delayed. Stale fencing tokens are rejected.
    """
    clock = get_clock().now()
    # If attempts exhausted, mark failed instead.
    if task.attempt_count >= task.max_attempts:
        row = conn.execute(
            """
            UPDATE tasks
            SET
                status = 'failed',
                lease_owner = NULL,
                lease_expires = NULL,
                last_error = COALESCE(%s, last_error),
                updated_at = %s
            WHERE id = %s
              AND lease_owner = %s
              AND fencing_token = %s
            RETURNING id
            """,
            (error, clock, str(task.id), task.lease_owner, task.fencing_token),
        ).fetchone()
    else:
        row = conn.execute(
            """
            UPDATE tasks
            SET
                status = 'ready',
                lease_owner = NULL,
                lease_expires = NULL,
                next_attempt = %s,
                last_error = COALESCE(%s, last_error),
                updated_at = %s
            WHERE id = %s
              AND lease_owner = %s
              AND fencing_token = %s
            RETURNING id
            """,
            (
                retry_at,
                error,
                clock,
                str(task.id),
                task.lease_owner,
                task.fencing_token,
            ),
        ).fetchone()
    conn.commit()
    return row is not None


def reap_expired(
    conn: psycopg.Connection,
    *,
    run_id: UUID | None = None,
    now: datetime | None = None,
) -> dict[str, int]:
    """
    Self-heal leases left by dead/hung workers, in one transaction.

    Expired leases with attempts left go back to ready. Expired leases (or stray
    ready rows) with no attempts left become failed: a dead-letter row with
    last_error, instead of sitting unclaimable forever.
    """
    clock = now or get_clock().now()
    run_filter = "AND run_id = %s" if run_id is not None else ""
    run_param: tuple[Any, ...] = (str(run_id),) if run_id is not None else ()
    with conn.transaction():
        failed = conn.execute(
            f"""
            UPDATE tasks
            SET status = 'failed', lease_owner = NULL, lease_expires = NULL,
                last_error = 'attempts exhausted (' || attempt_count || '/'
                    || max_attempts || '): ' || COALESCE(last_error, 'lease expired'),
                updated_at = %s
            WHERE attempt_count >= max_attempts
              AND (status = 'ready' OR (status = 'leased' AND lease_expires < %s))
              {run_filter}
            RETURNING id
            """,
            (clock, clock, *run_param),
        ).fetchall()
        released = conn.execute(
            f"""
            UPDATE tasks
            SET status = 'ready', lease_owner = NULL, lease_expires = NULL,
                next_attempt = LEAST(next_attempt, %s), updated_at = %s
            WHERE status = 'leased' AND lease_expires < %s
              {run_filter}
            RETURNING id
            """,
            (clock, clock, clock, *run_param),
        ).fetchall()
    return {"failed": len(failed), "released": len(released)}


def requeue_failed(conn: psycopg.Connection, *, run_id: UUID) -> int:
    """Operator action: give dead-lettered tasks of a run a fresh attempt budget.

    Only rows that exhausted their attempts; a parse reject on attempt 1 would
    just fail again.
    """
    clock = get_clock().now()
    rows = conn.execute(
        """
        UPDATE tasks
        SET status = 'ready', attempt_count = 0, next_attempt = %s, updated_at = %s
        WHERE run_id = %s AND status = 'failed' AND attempt_count >= max_attempts
        RETURNING id
        """,
        (clock, clock, str(run_id)),
    ).fetchall()
    conn.commit()
    return len(rows)


def complete_task(
    conn: psycopg.Connection,
    task: ClaimedTask,
    observation: RawObservation,
    *,
    successor_urls: Sequence[str] | None = None,
    cursor_opaque: str | None = None,
    checkpoint_stage: str = "discover",
    partition_key: str = "",
) -> bool:
    """
    Atomically write observation + mark task done (or terminal) + optional
    successors/checkpoint, only if owner+fencing_token match.

    Returns False when a stale worker is fenced out (or effect already committed).
    """
    clock = get_clock().now()
    status = TASK_STATUS_FOR_OUTCOME.get(observation.outcome, "failed")
    if observation.outcome is FetchOutcomeKind.RETRY_AT:
        # Should use schedule_retry instead.
        return schedule_retry(
            conn,
            task,
            retry_at=observation.retry_at or clock,
            error=observation.reason,
        )

    with conn.transaction():
        # Hold the row under fence for the whole commit; do not release until
        # observation (+ successors) are written so crash mid-txn rolls back.
        row = conn.execute(
            """
            SELECT id FROM tasks
            WHERE id = %s
              AND lease_owner = %s
              AND fencing_token = %s
              AND status = 'leased'
            FOR UPDATE
            """,
            (str(task.id), task.lease_owner, task.fencing_token),
        ).fetchone()
        if row is None:
            return False

        # One committed effect per task: unique(task_id) + reuse on replay.
        insert_observation(
            conn,
            task_id=task.id,
            source_id=task.source_id,
            observation=observation,
            run_id=task.run_id,
        )

        conn.execute(
            """
            UPDATE tasks
            SET
                status = %s,
                lease_owner = NULL,
                lease_expires = NULL,
                last_error = %s,
                updated_at = %s
            WHERE id = %s
              AND lease_owner = %s
              AND fencing_token = %s
            """,
            (
                status,
                observation.reason,
                clock,
                str(task.id),
                task.lease_owner,
                task.fencing_token,
            ),
        )

        if successor_urls:
            for url in successor_urls:
                enqueue_task(
                    conn,
                    source_id=task.source_id,
                    url=url,
                    stage="fetch",
                    run_id=task.run_id,
                    parent_task_id=task.id,
                    payload={"from": str(task.id)},
                )

        if cursor_opaque is not None:
            upsert_checkpoint(
                conn,
                source_id=task.source_id,
                stage=checkpoint_stage,
                partition_key=partition_key,
                cursor_opaque=cursor_opaque,
            )
    return True


def complete_with_blob(
    conn: psycopg.Connection,
    task: ClaimedTask,
    observation: RawObservation,
    raw_store: Any,
    **kwargs: Any,
) -> bool:
    """
    Save raw blob first (outside or before DB commit), then complete under fence.

    Crash after blob write / before commit: restart reclaims; refetch or reuse
    hash; unique task observation yields one committed effect.
    """
    if observation.body is not None and observation.outcome in {
        FetchOutcomeKind.OK,
        FetchOutcomeKind.NOT_MODIFIED,
        FetchOutcomeKind.BLOCKED,
    }:
        save_raw_observation_blob(raw_store, observation)
    return complete_task(conn, task, observation, **kwargs)


def upsert_checkpoint(
    conn: psycopg.Connection,
    *,
    source_id: str,
    stage: str = "discover",
    partition_key: str = "",
    cursor_opaque: str | None,
    metadata: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO source_checkpoints (
            source_id, stage, partition_key, cursor_opaque, updated_at, metadata
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_id, stage, partition_key) DO UPDATE SET
            cursor_opaque = EXCLUDED.cursor_opaque,
            updated_at = EXCLUDED.updated_at,
            metadata = EXCLUDED.metadata
        """,
        (
            source_id,
            stage,
            partition_key,
            cursor_opaque,
            get_clock().now(),
            Jsonb(metadata or {}),
        ),
    )


def get_checkpoint(
    conn: psycopg.Connection,
    *,
    source_id: str,
    stage: str = "discover",
    partition_key: str = "",
) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT source_id, stage, partition_key, cursor_opaque, updated_at, metadata
            FROM source_checkpoints
            WHERE source_id = %s AND stage = %s AND partition_key = %s
            """,
            (source_id, stage, partition_key),
        )
        return cur.fetchone()


def get_task(conn: psycopg.Connection, task_id: UUID) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM tasks WHERE id = %s", (str(task_id),))
        return cur.fetchone()


def _row_to_claimed(row: dict[str, Any], *, worker_id: str) -> ClaimedTask:
    run_id = row["run_id"]
    parent = row["parent_task_id"]
    return ClaimedTask(
        id=UUID(str(row["id"])),
        run_id=UUID(str(run_id)) if run_id else None,
        source_id=row["source_id"],
        stage=row["stage"],
        semantic_key=row["semantic_key"],
        external_id=row["external_id"],
        url=row["url"],
        attempt_count=int(row["attempt_count"]),
        max_attempts=int(row["max_attempts"]),
        fencing_token=int(row["fencing_token"]),
        lease_owner=row["lease_owner"] or worker_id,
        lease_expires=row["lease_expires"],
        payload=dict(row["payload"] or {}),
        parent_task_id=UUID(str(parent)) if parent else None,
    )
