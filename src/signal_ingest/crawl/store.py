"""Content-addressed raw blob store and observation persistence."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from signal_ingest.config.clock import get_clock
from signal_ingest.config.settings import Settings, get_settings
from signal_ingest.crawl.types import FetchOutcomeKind, RawObservation


def content_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def content_key_for(sha256: str) -> str:
    """Relative key: ab/cd/<sha256> under raw_root."""
    return f"{sha256[:2]}/{sha256[2:4]}/{sha256}"


class RawStore:
    """Immutable content-addressed blobs under the mode-scoped raw root."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> RawStore:
        cfg = settings or get_settings()
        root = cfg.assert_mode_raw_root()
        return cls(root)

    def path_for(self, sha256: str) -> Path:
        return self.root / content_key_for(sha256)

    def exists(self, sha256: str) -> bool:
        return self.path_for(sha256).is_file()

    def write(self, data: bytes) -> tuple[str, str]:
        """Write blob if absent. Returns (content_key, sha256). Idempotent on hash."""
        digest = content_sha256(data)
        key = content_key_for(digest)
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            tmp = path.with_suffix(path.suffix + f".{uuid4().hex}.tmp")
            try:
                tmp.write_bytes(data)
                tmp.replace(path)
            finally:
                if tmp.exists():
                    tmp.unlink(missing_ok=True)
        return key, digest

    def read(self, sha256: str) -> bytes:
        return self.path_for(sha256).read_bytes()

    def read_key(self, content_key: str) -> bytes:
        return (self.root / content_key).read_bytes()


def save_raw_observation_blob(
    store: RawStore,
    observation: RawObservation,
) -> RawObservation:
    """Persist successful body before parse/commit. Reuses existing hash on crash replay."""
    if observation.body is None:
        return observation
    key, digest = store.write(observation.body)
    observation.content_key = key
    observation.content_sha256 = digest
    return observation


def insert_observation(
    conn: psycopg.Connection,
    *,
    task_id: UUID,
    source_id: str,
    observation: RawObservation,
    run_id: UUID | None = None,
    observation_id: UUID | None = None,
) -> UUID | None:
    """
    Insert observation row. Returns id on insert, None if task already has one
    (unique task_id → one committed effect).
    """
    oid = observation_id or uuid4()
    fetched_at = observation.fetched_at or get_clock().now()
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    content_length = len(observation.body) if observation.body is not None else None
    row = conn.execute(
        """
        INSERT INTO observations (
            id, task_id, run_id, source_id, request_url, final_url,
            status_code, outcome, content_type, content_key, content_sha256,
            content_length, fetched_at, etag, last_modified, headers, metadata
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (task_id) DO NOTHING
        RETURNING id
        """,
        (
            str(oid),
            str(task_id),
            str(run_id) if run_id else None,
            source_id,
            observation.request_url,
            observation.final_url,
            observation.status_code,
            observation.outcome.value,
            observation.content_type,
            observation.content_key,
            observation.content_sha256,
            content_length,
            fetched_at,
            observation.etag,
            observation.last_modified,
            Jsonb(observation.headers),
            Jsonb(observation.metadata),
        ),
    ).fetchone()
    return UUID(str(row[0])) if row else None


def get_observation_for_task(
    conn: psycopg.Connection,
    task_id: UUID,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, task_id, source_id, request_url, final_url, status_code, outcome,
               content_type, content_key, content_sha256, content_length, fetched_at,
               etag, last_modified, headers, metadata
        FROM observations WHERE task_id = %s
        """,
        (str(task_id),),
    ).fetchone()
    if row is None:
        return None
    cols = [
        "id",
        "task_id",
        "source_id",
        "request_url",
        "final_url",
        "status_code",
        "outcome",
        "content_type",
        "content_key",
        "content_sha256",
        "content_length",
        "fetched_at",
        "etag",
        "last_modified",
        "headers",
        "metadata",
    ]
    return dict(zip(cols, row, strict=True))


def load_cached_body(
    conn: psycopg.Connection,
    store: RawStore,
    *,
    url: str,
    etag: str | None = None,
) -> tuple[bytes, dict[str, Any]] | None:
    """Find a prior OK observation for URL (optional etag match) and load body."""
    if etag:
        row = conn.execute(
            """
            SELECT content_key, content_sha256, etag, last_modified, content_type
            FROM observations
            WHERE final_url = %s AND outcome = 'ok' AND etag = %s
              AND content_key IS NOT NULL
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (url, etag),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT content_key, content_sha256, etag, last_modified, content_type
            FROM observations
            WHERE final_url = %s AND outcome = 'ok' AND content_key IS NOT NULL
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (url,),
        ).fetchone()
    if row is None:
        return None
    content_key, sha, cached_etag, last_mod, ctype = row
    try:
        body = store.read_key(content_key)
    except FileNotFoundError:
        return None
    meta = {
        "content_key": content_key,
        "content_sha256": sha,
        "etag": cached_etag,
        "last_modified": last_mod,
        "content_type": ctype,
    }
    return body, meta


def observation_from_row(row: dict[str, Any], body: bytes | None = None) -> RawObservation:
    return RawObservation(
        request_url=row["request_url"],
        final_url=row["final_url"],
        status_code=row["status_code"],
        outcome=FetchOutcomeKind(row["outcome"]),
        body=body,
        content_type=row.get("content_type"),
        etag=row.get("etag"),
        last_modified=row.get("last_modified"),
        headers=dict(row.get("headers") or {}),
        fetched_at=row.get("fetched_at"),
        content_key=row.get("content_key"),
        content_sha256=row.get("content_sha256"),
        metadata=dict(row.get("metadata") or {}),
    )


def dumps_meta(data: dict[str, Any]) -> str:
    return json.dumps(data, sort_keys=True, default=str)
