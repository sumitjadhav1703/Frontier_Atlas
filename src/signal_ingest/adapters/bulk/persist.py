"""Persist bulk candidates, relationships, repo metrics and quarantines."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from signal_ingest.adapters.bulk.types import RepoMetricSnapshot
from signal_ingest.config.clock import get_clock
from signal_ingest.crawl.queue import semantic_key


def _aware(dt: datetime | None) -> datetime:
    clock = dt or get_clock().now()
    if clock.tzinfo is None:
        return clock.replace(tzinfo=timezone.utc)
    return clock.astimezone(timezone.utc)


def candidate_semantic_key(
    source_id: str,
    record_type: str,
    external_id: str,
    *,
    version: str = "1",
) -> str:
    return semantic_key(
        source_id,
        f"candidate:{record_type}",
        external_id=external_id,
        version=version,
    )


def upsert_candidate(
    conn: psycopg.Connection,
    *,
    source_id: str,
    record_type: str,
    external_id: str,
    payload: dict[str, Any],
    evidence: dict[str, Any],
    status: str = "pending",
    run_id: UUID | None = None,
    observation_id: UUID | None = None,
    collected_at: datetime | None = None,
    version: str = "1",
) -> UUID:
    """
    One row per logical identity (source + record_type + external_id).
    Replay updates payload/evidence/status in place.
    """
    key = candidate_semantic_key(source_id, record_type, external_id, version=version)
    when = _aware(collected_at)
    now = get_clock().now()
    row = conn.execute(
        """
        INSERT INTO candidates (
            id, run_id, source_id, record_type, external_id, semantic_key,
            status, payload, evidence, observation_id, collected_at, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (semantic_key) DO UPDATE SET
            run_id = COALESCE(EXCLUDED.run_id, candidates.run_id),
            status = EXCLUDED.status,
            payload = EXCLUDED.payload,
            evidence = EXCLUDED.evidence,
            observation_id = COALESCE(EXCLUDED.observation_id, candidates.observation_id),
            collected_at = EXCLUDED.collected_at,
            updated_at = EXCLUDED.updated_at
        RETURNING id
        """,
        (
            str(uuid4()),
            str(run_id) if run_id else None,
            source_id,
            record_type,
            external_id,
            key,
            status,
            Jsonb(payload),
            Jsonb(evidence),
            str(observation_id) if observation_id else None,
            when,
            now,
            now,
        ),
    ).fetchone()
    assert row is not None
    return UUID(str(row[0]))


def insert_quarantine(
    conn: psycopg.Connection,
    *,
    source_id: str,
    reason: str,
    record_type: str | None = None,
    external_id: str | None = None,
    detail: dict[str, Any] | None = None,
    run_id: UUID | None = None,
    observation_id: UUID | None = None,
    semantic_key_value: str | None = None,
) -> UUID:
    qid = uuid4()
    conn.execute(
        """
        INSERT INTO quarantines (
            id, run_id, source_id, record_type, external_id, reason,
            detail, observation_id, semantic_key, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(qid),
            str(run_id) if run_id else None,
            source_id,
            record_type,
            external_id,
            reason,
            Jsonb(detail or {}),
            str(observation_id) if observation_id else None,
            semantic_key_value,
            get_clock().now(),
        ),
    )
    return qid


def upsert_relationship(
    conn: psycopg.Connection,
    *,
    relation_type: str,
    from_kind: str,
    from_key: str,
    to_kind: str,
    to_key: str,
    evidence: dict[str, Any] | None = None,
    observation_id: UUID | None = None,
    is_primary: bool = False,
    status: str = "accepted",
    reason: str | None = None,
    run_id: UUID | None = None,
) -> UUID:
    rid = uuid4()
    row = conn.execute(
        """
        INSERT INTO relationships (
            id, run_id, relation_type, from_kind, from_key, to_kind, to_key,
            evidence, observation_id, is_primary, status, reason, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (relation_type, from_kind, from_key, to_kind, to_key) DO UPDATE SET
            evidence = EXCLUDED.evidence,
            observation_id = COALESCE(EXCLUDED.observation_id, relationships.observation_id),
            is_primary = EXCLUDED.is_primary,
            status = EXCLUDED.status,
            reason = EXCLUDED.reason
        RETURNING id
        """,
        (
            str(rid),
            str(run_id) if run_id else None,
            relation_type,
            from_kind,
            from_key,
            to_kind,
            to_key,
            Jsonb(evidence or {}),
            str(observation_id) if observation_id else None,
            is_primary,
            status,
            reason,
            get_clock().now(),
        ),
    ).fetchone()
    assert row is not None
    return UUID(str(row[0]))


def upsert_repo_metric(
    conn: psycopg.Connection,
    snapshot: RepoMetricSnapshot,
    *,
    source_id: str = "bulk.github",
) -> UUID:
    """
    Persist a star snapshot. Unique on (repo_id, observed_at).
    A later snapshot may legitimately have fewer stars.
    """
    mid = uuid4()
    row = conn.execute(
        """
        INSERT INTO repo_metrics (
            id, repo_id, repo_url, stars, observed_at, etag, content_sha256,
            observation_id, source_id, metadata, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (repo_id, observed_at) DO UPDATE SET
            stars = EXCLUDED.stars,
            repo_url = EXCLUDED.repo_url,
            etag = COALESCE(EXCLUDED.etag, repo_metrics.etag),
            content_sha256 = COALESCE(EXCLUDED.content_sha256, repo_metrics.content_sha256),
            observation_id = COALESCE(EXCLUDED.observation_id, repo_metrics.observation_id),
            metadata = EXCLUDED.metadata
        RETURNING id
        """,
        (
            str(mid),
            snapshot.repo_id,
            snapshot.repo_url,
            snapshot.stars,
            _aware(snapshot.observed_at),
            snapshot.etag,
            snapshot.content_sha256,
            str(snapshot.observation_id) if snapshot.observation_id else None,
            source_id,
            Jsonb(snapshot.metadata),
            get_clock().now(),
        ),
    ).fetchone()
    assert row is not None
    return UUID(str(row[0]))


def latest_repo_metric(
    conn: psycopg.Connection,
    *,
    repo_id: str | None = None,
    repo_url: str | None = None,
) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        if repo_id:
            cur.execute(
                """
                SELECT * FROM repo_metrics
                WHERE repo_id = %s
                ORDER BY observed_at DESC
                LIMIT 1
                """,
                (repo_id,),
            )
        elif repo_url:
            cur.execute(
                """
                SELECT * FROM repo_metrics
                WHERE repo_url = %s
                ORDER BY observed_at DESC
                LIMIT 1
                """,
                (repo_url,),
            )
        else:
            return None
        return cur.fetchone()


def get_candidate(
    conn: psycopg.Connection,
    *,
    source_id: str,
    record_type: str,
    external_id: str,
) -> dict[str, Any] | None:
    key = candidate_semantic_key(source_id, record_type, external_id)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM candidates WHERE semantic_key = %s", (key,))
        return cur.fetchone()


def list_candidates(
    conn: psycopg.Connection,
    *,
    source_id: str | None = None,
    record_type: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if source_id:
        filters.append("source_id = %s")
        params.append(source_id)
    if record_type:
        filters.append("record_type = %s")
        params.append(record_type)
    if status:
        filters.append("status = %s")
        params.append(status)
    where = (" WHERE " + " AND ".join(filters)) if filters else ""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(f"SELECT * FROM candidates{where} ORDER BY created_at ASC", params)
        return list(cur.fetchall())
