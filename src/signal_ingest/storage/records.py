"""Persist validated records with durable lineage mode."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from signal_ingest.config.settings import Mode, Settings, get_settings
from signal_ingest.models.records import AnyRecord, RecordType, parse_record
from signal_ingest.storage.lineage import assert_live_admission, lineage_mode_for


def create_run(
    conn: psycopg.Connection,
    *,
    mode: Mode | str | None = None,
    settings: Settings | None = None,
    cutoff_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> UUID:
    cfg = settings or get_settings()
    run_mode = Mode(mode) if mode is not None else cfg.mode
    if run_mode is not cfg.mode:
        raise ValueError(
            f"run mode {run_mode.value!r} does not match settings mode {cfg.mode.value!r}"
        )
    run_id = uuid.uuid4()
    conn.execute(
        """
        INSERT INTO runs (id, mode, status, cutoff_at, metadata)
        VALUES (%s, %s, 'initialized', %s, %s)
        """,
        (
            run_id,
            run_mode.value,
            cutoff_at,
            Jsonb(metadata or {}),
        ),
    )
    return run_id


def persist_record(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    record_type: RecordType | str,
    payload: dict[str, Any] | AnyRecord,
    settings: Settings | None = None,
) -> UUID:
    """Validate and insert a typed payload; lineage_mode is taken from the run."""
    cfg = settings or get_settings()
    rtype = RecordType(record_type) if isinstance(record_type, str) else record_type
    if isinstance(payload, dict):
        model = parse_record(rtype, payload)
        data = model.model_dump(mode="json")
    else:
        data = payload.model_dump(mode="json")

    row = conn.execute(
        "SELECT mode FROM runs WHERE id = %s",
        (run_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"run not found: {run_id}")
    run_mode = Mode(row[0])
    lineage = lineage_mode_for(run_mode)

    # Live runs must not ingest fixture-marked payloads (defense in depth).
    payload_lineage = data.get("_lineage_mode") or data.get("lineage_mode")
    if payload_lineage:
        assert_live_admission(lineage_mode=str(payload_lineage), target_mode=run_mode)

    if run_mode is Mode.LIVE and cfg.mode is Mode.FIXTURE:
        assert_live_admission(lineage_mode=Mode.FIXTURE.value, target_mode=Mode.LIVE)

    record_id = uuid.uuid4()
    conn.execute(
        """
        INSERT INTO records (id, run_id, record_type, payload, lineage_mode)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (record_id, run_id, rtype.value, Jsonb(data), lineage),
    )
    return record_id


def load_record_lineage(conn: psycopg.Connection, record_id: UUID) -> str:
    row = conn.execute(
        "SELECT lineage_mode FROM records WHERE id = %s",
        (record_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"record not found: {record_id}")
    return str(row[0])
