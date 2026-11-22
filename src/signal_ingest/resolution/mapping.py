"""Idempotent Entity Mapping Log helpers."""

from __future__ import annotations

import hashlib
import uuid
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from signal_ingest.resolution.types import (
    MappingLogRecord,
    ResolveDecision,
)
from signal_ingest.resolution.versions import RULE_VERSION


def mapping_observation_key(
    *,
    source_item_id: str = "",
    field: str = "entityName",
    raw_value: str | None = None,
    raw_name: str | None = None,
    rule_version: str = RULE_VERSION,
) -> str:
    """Idempotent key = source item + field + raw value + rule version."""
    raw = raw_value if raw_value is not None else (raw_name or "")
    material = "\0".join(
        [
            str(source_item_id or ""),
            str(field or ""),
            str(raw),
            str(rule_version or RULE_VERSION),
        ]
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f"map:{digest}"


def mapping_event_key(**kwargs: Any) -> str:
    """Adapter alias for mapping_observation_key (accepts raw_name)."""
    return mapping_observation_key(
        source_item_id=str(kwargs.get("source_item_id") or ""),
        field=str(kwargs.get("field") or "entityName"),
        raw_value=kwargs.get("raw_value"),
        raw_name=kwargs.get("raw_name"),
        rule_version=str(kwargs.get("rule_version") or RULE_VERSION),
    )


def decision_to_mapping_log(
    decision: ResolveDecision,
    *,
    run_id: str | None = None,
) -> MappingLogRecord:
    """Project a ResolveDecision onto the contracts.md Entity Mapping Log shape."""
    return MappingLogRecord(
        entityType=decision.namespace.value,
        rawName=decision.raw_name,
        canonicalName=decision.canonical_name,
        canonicalID=decision.canonical_id,
        method=decision.method,
        decision=decision.decision.value,
        evidenceURL=decision.evidence_url,
        runID=run_id,
        ruleVersion=decision.rule_version,
    )


def record_resolution_event(
    conn: psycopg.Connection,
    decision: ResolveDecision,
    *,
    run_id: UUID | str | None = None,
    payload: dict[str, Any] | None = None,
) -> UUID:
    """Insert a resolution_events row; identical observation_key is a no-op.

    Returns the event id (existing or newly inserted).
    """
    obs_key = decision.observation_key or mapping_observation_key(
        source_item_id="",
        field="entityName",
        raw_value=decision.raw_name,
        rule_version=decision.rule_version,
    )
    existing = conn.execute(
        "SELECT id FROM resolution_events WHERE observation_key = %s",
        (obs_key,),
    ).fetchone()
    if existing is not None:
        return UUID(str(existing[0]))

    event_id = uuid.uuid4()
    run_uuid: UUID | None
    if run_id is None:
        run_uuid = None
    elif isinstance(run_id, UUID):
        run_uuid = run_id
    else:
        # Non-UUID run ids (test strings) live in payload; FK column stays null.
        try:
            run_uuid = UUID(str(run_id))
        except ValueError:
            run_uuid = None
            payload = {**(payload or {}), "run_id": str(run_id)}

    conn.execute(
        """
        INSERT INTO resolution_events (
            id, observation_key, run_id, entity_type, raw_name,
            canonical_name, canonical_id, method, decision,
            evidence_url, rule_version, candidate_ids, comparison_key,
            reason, payload
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (observation_key) DO NOTHING
        """,
        (
            event_id,
            obs_key,
            run_uuid,
            decision.namespace.value,
            decision.raw_name,
            decision.canonical_name,
            decision.canonical_id,
            decision.method,
            decision.decision.value,
            decision.evidence_url,
            decision.rule_version,
            Jsonb(list(decision.candidate_ids)),
            decision.comparison_key,
            decision.reason,
            Jsonb(payload or {}),
        ),
    )
    row = conn.execute(
        "SELECT id FROM resolution_events WHERE observation_key = %s",
        (obs_key,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"failed to persist resolution event for {obs_key}")
    return UUID(str(row[0]))


def record_mapping_event(
    decision: ResolveDecision | Any,
    *,
    source_item_id: str | None = None,
    field: str = "entityName",
    raw_name: str | None = None,
    rule_version: str | None = None,
    run_id: str | UUID | None = None,
    evidence_url: str | None = None,
    settings: Any = None,
    conn: psycopg.Connection | None = None,
    **_ignored: Any,
) -> UUID:
    """High-level idempotent mapping-log writer (opens DB when settings given)."""
    from signal_ingest.storage.db import connect

    if not isinstance(decision, ResolveDecision):
        # Best-effort adapt from duck-typed decisions.
        from signal_ingest.resolution.types import DecisionKind, Namespace

        decision = ResolveDecision(
            decision=DecisionKind(str(getattr(decision, "decision", decision.get("decision")))),
            canonical_id=getattr(decision, "canonical_id", None)
            if not isinstance(decision, dict)
            else decision.get("canonical_id"),
            canonical_name=getattr(decision, "canonical_name", None)
            if not isinstance(decision, dict)
            else decision.get("canonical_name"),
            method=str(
                getattr(decision, "method", None)
                if not isinstance(decision, dict)
                else decision.get("method")
                or "unknown"
            ),
            rule_version=str(
                rule_version
                or (
                    getattr(decision, "rule_version", None)
                    if not isinstance(decision, dict)
                    else decision.get("rule_version")
                )
                or RULE_VERSION
            ),
            candidate_ids=tuple(
                getattr(decision, "candidate_ids", ())
                if not isinstance(decision, dict)
                else decision.get("candidate_ids")
                or ()
            ),
            raw_name=str(
                raw_name
                or (
                    getattr(decision, "raw_name", None)
                    if not isinstance(decision, dict)
                    else decision.get("raw_name")
                )
                or ""
            ),
            comparison_key=str(
                getattr(decision, "comparison_key", "")
                if not isinstance(decision, dict)
                else decision.get("comparison_key")
                or ""
            ),
            namespace=Namespace(
                str(
                    getattr(decision, "namespace", "startup")
                    if not isinstance(decision, dict)
                    else decision.get("namespace")
                    or "startup"
                )
            ),
            reason=getattr(decision, "reason", None)
            if not isinstance(decision, dict)
            else decision.get("reason"),
            evidence_url=evidence_url
            or (
                getattr(decision, "evidence_url", None)
                if not isinstance(decision, dict)
                else decision.get("evidence_url")
            ),
            observation_key=getattr(decision, "observation_key", None)
            if not isinstance(decision, dict)
            else decision.get("observation_key"),
        )

    obs_key = mapping_observation_key(
        source_item_id=source_item_id or "",
        field=field,
        raw_name=raw_name or decision.raw_name,
        rule_version=rule_version or decision.rule_version or RULE_VERSION,
    )
    decision = decision.model_copy(
        update={
            "observation_key": obs_key,
            "evidence_url": evidence_url or decision.evidence_url,
            "rule_version": rule_version or decision.rule_version,
        }
    )

    owns_conn = conn is None
    if conn is None:
        if settings is None:
            raise TypeError("record_mapping_event requires conn or settings")
        conn = connect(settings)
    try:
        event_id = record_resolution_event(conn, decision, run_id=run_id)
        if owns_conn:
            conn.commit()
        return event_id
    finally:
        if owns_conn and conn is not None:
            conn.close()


__all__ = [
    "decision_to_mapping_log",
    "mapping_event_key",
    "mapping_observation_key",
    "record_mapping_event",
    "record_resolution_event",
]
