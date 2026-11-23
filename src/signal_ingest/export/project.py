"""Build a consistent six-tab export snapshot from DB state."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from signal_ingest.config.settings import Mode, Settings, get_settings
from signal_ingest.export.flatten import flatten_dict
from signal_ingest.export.tabs import (
    RECORD_TYPE_TO_TAB,
    TAB_ENTITY_MAPPING_LOG,
    TAB_HEADERS,
    TAB_NAMES,
    TAB_NEWS,
    TAB_JOBS,
    TAB_RESEARCH_PAPERS,
)
from signal_ingest.freshness.interval import PublicationInterval
from signal_ingest.freshness.window import in_freshness_window
from signal_ingest.resolution.mapping import decision_to_mapping_log
from signal_ingest.resolution.types import (
    DecisionKind,
    Namespace,
    ResolveDecision,
)


_OPTIONAL_COLUMNS = (
    (TAB_ENTITY_MAPPING_LOG, "suggested_candidates"),
    (TAB_NEWS, "nearDuplicateOf"),
)


def _parse_as_of(as_of: str | datetime) -> datetime:
    if isinstance(as_of, datetime):
        dt = as_of
    else:
        text = str(as_of).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _row_sort_key(flat: dict[str, Any], tab: str) -> tuple[str, ...]:
    if tab == TAB_ENTITY_MAPPING_LOG:
        return (
            str(flat.get("entityType") or ""),
            str(flat.get("rawName") or ""),
            str(flat.get("ruleVersion") or ""),
            str(flat.get("runID") or ""),
        )
    return (
        str(flat.get("durableIdentity") or flat.get("source.url") or ""),
        str(flat.get("collectedAt") or ""),
        str(flat.get("content.title") or flat.get("content.entityName") or ""),
    )


def _durable_id(candidate: dict[str, Any]) -> str:
    external = candidate.get("external_id") or ""
    semantic = candidate.get("semantic_key") or ""
    return str(external or semantic)


def _payload_as_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        return json.loads(payload)
    return dict(payload or {})


def _candidate_to_flat(
    candidate: dict[str, Any],
    *,
    as_of: datetime,
    lineage_mode: str,
    run_id: str,
) -> dict[str, Any] | None:
    if candidate.get("status") != "accepted":
        return None
    record_type = str(candidate.get("record_type") or "")
    tab = RECORD_TYPE_TO_TAB.get(record_type)
    if tab is None:
        return None
    payload = _payload_as_dict(candidate.get("payload"))
    evidence = _payload_as_dict(candidate.get("evidence"))
    flat = flatten_dict(payload)
    content_in = payload.get("content") or {}
    meta = payload.get("meta") or {}
    # Adapters keep repo ID / star observation time under meta; project them.
    if tab == TAB_RESEARCH_PAPERS:
        repo_id = (meta.get("repo_association") or {}).get("repo_id") or meta.get("repoId")
        if repo_id is not None:
            flat.setdefault("content.repoId", str(repo_id))
        if meta.get("starsObservedAt"):
            flat.setdefault("content.starsObservedAt", meta["starsObservedAt"])
    # Optional evidence-backed extras for Jobs/News body provenance.
    if tab in {TAB_JOBS, TAB_NEWS}:
        evidence_body = evidence.get("body")
        body = str(
            content_in.get("text")
            or content_in.get("description")
            or (evidence_body if isinstance(evidence_body, str) else "")
            or ""
        )
        if body and "content.bodyHash" not in flat:
            flat["content.bodyHash"] = hashlib.sha256(body.encode("utf-8")).hexdigest()
            flat["content.bodyLength"] = len(body)
        if evidence.get("date_extraction_method"):
            flat.setdefault(
                "content.dateExtractionMethod",
                evidence.get("date_extraction_method"),
            )
        listing = content_in.get("listing_url") or evidence.get("listing_url")
        if listing:
            flat.setdefault("content.listingUrl", listing)
        employer = (
            content_in.get("original_employer_job_url")
            or evidence.get("original_employer_job_url")
            or evidence.get("employer_job_url")
        )
        if employer:
            flat.setdefault("content.employerJobUrl", employer)
    if tab == TAB_NEWS and meta.get("nearDuplicateOf"):
        flat["nearDuplicateOf"] = str(meta["nearDuplicateOf"])
    # Freshness filter for Jobs/News at export time.
    if tab in {TAB_JOBS, TAB_NEWS}:
        content = payload.get("content") or {}
        # Judge the whole stored interval: content.date is only its lower bound,
        # and a wide interval (day-only, "2 days ago") must not slip in.
        stored = content.get("publication") or payload.get("publication") or {}
        lower = stored.get("lower") if isinstance(stored, dict) else None
        upper = stored.get("upper") if isinstance(stored, dict) else None
        if lower is None and upper is None:
            lower = upper = content.get("date")
        if lower is None or upper is None:
            return None
        try:
            bounds = []
            for value in (lower, upper):
                dt = (
                    datetime.fromisoformat(value.replace("Z", "+00:00"))
                    if isinstance(value, str)
                    else value
                )
                bounds.append(dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc))
            interval = PublicationInterval(
                lower=bounds[0],
                upper=bounds[1],
                precision=str(stored.get("precision") or "instant") if isinstance(stored, dict) else "instant",
                method="export_publication_interval",
            )
            if not in_freshness_window(interval, cutoff=as_of):
                return None
        except (TypeError, ValueError):
            return None
    flat["durableIdentity"] = _durable_id(candidate)
    flat["asOf"] = as_of.isoformat().replace("+00:00", "Z")
    flat["lineageMode"] = lineage_mode
    flat["runId"] = run_id
    flat["_tab"] = tab
    return flat


def _mapping_rows(
    conn: psycopg.Connection,
    *,
    run_id: UUID | None,
    as_of: datetime,
    lineage_mode: str,
) -> list[dict[str, Any]]:
    params: list[Any] = []
    where = ""
    if run_id is not None:
        where = " WHERE run_id = %s OR run_id IS NULL"
        params.append(str(run_id))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT entity_type, raw_name, canonical_name, canonical_id, method,
                   decision, evidence_url, run_id, rule_version,
                   payload->>'suggested_candidates' AS suggested_candidates
            FROM resolution_events
            {where}
            ORDER BY entity_type ASC, raw_name ASC, rule_version ASC, created_at ASC
            """,
            params,
        )
        rows = list(cur.fetchall())
    out: list[dict[str, Any]] = []
    for row in rows:
        decision = ResolveDecision(
            decision=DecisionKind(str(row["decision"])),
            canonical_id=row.get("canonical_id"),
            canonical_name=row.get("canonical_name"),
            method=str(row.get("method") or "unknown"),
            rule_version=str(row.get("rule_version") or ""),
            raw_name=str(row.get("raw_name") or ""),
            comparison_key="",
            namespace=Namespace(str(row.get("entity_type") or "startup")),
            evidence_url=row.get("evidence_url"),
        )
        log = decision_to_mapping_log(
            decision,
            run_id=str(row["run_id"]) if row.get("run_id") else (str(run_id) if run_id else None),
        )
        flat = log.model_dump(mode="json")
        if row.get("suggested_candidates"):
            flat["suggested_candidates"] = row["suggested_candidates"]
        flat["asOf"] = as_of.isoformat().replace("+00:00", "Z")
        flat["lineageMode"] = lineage_mode
        flat["_tab"] = TAB_ENTITY_MAPPING_LOG
        out.append(flat)
    return out


def build_export_snapshot(
    conn: psycopg.Connection,
    *,
    run_id: UUID | str,
    as_of: str | datetime,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """
    Consistent read snapshot: accepted candidates + mapping log, stable sort.
    Fixture mode never claims live admission.
    """
    cfg = settings or get_settings()
    rid = UUID(str(run_id))
    cutoff = _parse_as_of(as_of)

    run_row = conn.execute(
        "SELECT id, mode, status, cutoff_at, metadata FROM runs WHERE id = %s",
        (str(rid),),
    ).fetchone()
    if run_row is None:
        raise LookupError(f"run not found: {rid}")
    run_mode = Mode(str(run_row[1]))
    if run_mode is not cfg.mode:
        raise ValueError(
            f"export mode {cfg.mode.value!r} does not match run mode {run_mode.value!r}"
        )
    lineage = run_mode.value

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM candidates
            WHERE (run_id = %s OR run_id IS NULL)
              AND status = 'accepted'
            ORDER BY record_type ASC, semantic_key ASC
            """,
            (str(rid),),
        )
        candidates = list(cur.fetchall())

    # Prefer run-scoped; if empty, still allow records table fallback for B0 fixtures.
    flats: list[dict[str, Any]] = []
    for cand in candidates:
        flat = _candidate_to_flat(
            cand,
            as_of=cutoff,
            lineage_mode=lineage,
            run_id=str(rid),
        )
        if flat is not None:
            flats.append(flat)

    if not any(f.get("_tab") != TAB_ENTITY_MAPPING_LOG for f in flats):
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, record_type, payload, lineage_mode
                FROM records
                WHERE run_id = %s
                ORDER BY record_type ASC, created_at ASC
                """,
                (str(rid),),
            )
            for row in cur.fetchall():
                payload = _payload_as_dict(row["payload"])
                flat = flatten_dict(payload)
                tab = RECORD_TYPE_TO_TAB.get(str(row["record_type"]))
                if tab is None:
                    continue
                flat["durableIdentity"] = str(row["id"])
                flat["asOf"] = cutoff.isoformat().replace("+00:00", "Z")
                flat["lineageMode"] = str(row["lineage_mode"])
                flat["runId"] = str(rid)
                flat["_tab"] = tab
                flats.append(flat)

    flats.extend(
        _mapping_rows(conn, run_id=rid, as_of=cutoff, lineage_mode=lineage)
    )

    tabs: dict[str, list[dict[str, Any]]] = {name: [] for name in TAB_NAMES}
    for flat in flats:
        tab = str(flat.get("_tab") or "")
        if tab in tabs:
            tabs[tab].append(flat)

    for tab_name, rows in tabs.items():
        rows.sort(key=lambda r: _row_sort_key(r, tab_name))

    headers = {name: list(TAB_HEADERS[name]) for name in TAB_NAMES}
    # R4 columns appear only once embeddings produced a value: no-embedding exports stay byte-stable.
    for tab_name, column in _OPTIONAL_COLUMNS:
        if any(r.get(column) for r in tabs[tab_name]):
            headers[tab_name].append(column)

    return {
        "run_id": str(rid),
        "as_of": cutoff.isoformat().replace("+00:00", "Z"),
        "mode": cfg.mode.value,
        "lineage_mode": lineage,
        "tabs": tabs,
        "tab_names": list(TAB_NAMES),
        "headers": headers,
        "counts": {name: len(rows) for name, rows in tabs.items()},
    }


__all__ = ["build_export_snapshot"]
