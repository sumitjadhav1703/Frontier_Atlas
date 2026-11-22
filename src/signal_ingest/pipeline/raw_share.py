"""Dual-worker shared raw root: reopen observations via relative content_key only."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from signal_ingest.config.settings import Settings, get_settings
from signal_ingest.crawl.store import RawStore, get_observation_for_task


def open_observation_by_content_key(
    conn: psycopg.Connection,
    *,
    content_key: str,
    settings: Settings | None = None,
    raw_root: Path | None = None,
) -> dict[str, Any]:
    """
    Resolve a blob written by another worker using only the persisted relative key.

    Both processes must share the same mode-scoped raw root (mounted directory).
    Temporary working directories may differ; the content_key is root-relative.
    """
    cfg = settings or get_settings()
    root = cfg.assert_mode_raw_root(raw_root)
    store = RawStore(root)
    body = store.read_key(content_key)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, task_id, source_id, request_url, final_url, status_code,
                   outcome, content_type, content_key, content_sha256, content_length,
                   fetched_at
            FROM observations
            WHERE content_key = %s
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (content_key,),
        )
        row = cur.fetchone()
    return {
        "content_key": content_key,
        "body": body,
        "body_length": len(body),
        "observation": dict(row) if row else None,
        "raw_root": str(root),
    }


def observation_body_for_task(
    conn: psycopg.Connection,
    task_id: UUID,
    *,
    settings: Settings | None = None,
) -> bytes | None:
    """Load raw body for a task via content_key under the shared raw root."""
    cfg = settings or get_settings()
    row = get_observation_for_task(conn, task_id)
    if row is None or not row.get("content_key"):
        return None
    store = RawStore.from_settings(cfg)
    return store.read_key(str(row["content_key"]))


__all__ = [
    "observation_body_for_task",
    "open_observation_by_content_key",
]
