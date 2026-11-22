"""Monitor pipeline: discover → fetch → parse on the B1 queue + ledger."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg

from signal_ingest.adapters.bulk.persist import insert_quarantine, upsert_candidate
from signal_ingest.adapters.monitors import (
    jobs_aicommunity,
    jobs_aijobs,
    jobs_aiworkportal,
    jobs_inferencejobs,
    jobs_moaijobs,
    news_arstechnica,
    news_guardian,
    news_mit,
    news_sciencedaily,
    news_techcrunch,
)
from signal_ingest.adapters.monitors.config import load_monitor_config
from signal_ingest.adapters.monitors.detail import parse_detail
from signal_ingest.adapters.monitors.discover import discover_list
from signal_ingest.adapters.monitors.ledger import classify_health, insert_monitor_run
from signal_ingest.adapters.monitors.types import MonitorParseResult
from signal_ingest.config.clock import get_clock
from signal_ingest.config.settings import Settings, get_settings
from signal_ingest.crawl.discover import persist_discovery_page
from signal_ingest.crawl.fetch import FetchPolicy, fetch_with_retries
from signal_ingest.crawl.pool import HttpPool
from signal_ingest.crawl.queue import (
    claim_tasks,
    complete_with_blob,
    enqueue_task,
    schedule_retry,
    upsert_checkpoint,
)
from signal_ingest.crawl.store import RawStore, get_observation_for_task
from signal_ingest.crawl.types import ClaimedTask, FetchOutcomeKind, RawObservation

STAGE_FETCH = "fetch"
STAGE_PARSE = "parse"

ADAPTERS: dict[str, Any] = {
    news_mit.SOURCE_ID: news_mit,
    news_sciencedaily.SOURCE_ID: news_sciencedaily,
    news_techcrunch.SOURCE_ID: news_techcrunch,
    news_arstechnica.SOURCE_ID: news_arstechnica,
    news_guardian.SOURCE_ID: news_guardian,
    jobs_aijobs.SOURCE_ID: jobs_aijobs,
    jobs_moaijobs.SOURCE_ID: jobs_moaijobs,
    jobs_inferencejobs.SOURCE_ID: jobs_inferencejobs,
    jobs_aiworkportal.SOURCE_ID: jobs_aiworkportal,
    jobs_aicommunity.SOURCE_ID: jobs_aicommunity,
}

MONITOR_SOURCES = frozenset(ADAPTERS)


def get_adapter(source_id: str) -> Any:
    return ADAPTERS.get(source_id)


def is_monitor_source(source_id: str) -> bool:
    return source_id in MONITOR_SOURCES or source_id.startswith(("news_", "jobs_"))


def discover_and_enqueue(
    conn: psycopg.Connection,
    *,
    source_id: str,
    payload: Any,
    run_id: UUID | None = None,
    checkpoint: str | None = None,
    cutoff: datetime | None = None,
    observed_at: datetime | None = None,
    feed_ordered: bool | None = None,
    base_url: str | None = None,
    partition_key: str = "monitor",
) -> dict[str, Any]:
    """Adapter discover_list → persist_discovery_page; record scan metadata on checkpoint."""
    cfg = load_monitor_config(source_id)
    adapter = get_adapter(source_id)
    when = observed_at or get_clock().now()
    freeze = cutoff or when
    if adapter is not None and hasattr(adapter, "discover_list"):
        scan = adapter.discover_list(
            payload,
            checkpoint=checkpoint,
            cutoff=freeze,
            observed_at=when,
            feed_ordered=feed_ordered,
            base_url=base_url,
        )
    else:
        scan = discover_list(
            cfg,
            payload,
            checkpoint=checkpoint,
            cutoff=freeze,
            observed_at=when,
            feed_ordered=feed_ordered,
            base_url=base_url,
        )

    _drop_seen_new_since_flags(conn, source_id=source_id, items=scan.items)
    result = persist_discovery_page(
        conn,
        source_id=source_id,
        items=scan.items,
        run_id=run_id,
        stage=STAGE_FETCH,
        checkpoint_stage="discover",
        partition_key=partition_key,
        normalize=True,
    )
    upsert_checkpoint(
        conn,
        source_id=source_id,
        stage="discover",
        partition_key=partition_key,
        cursor_opaque=scan.cursor or result.get("cursor"),
        metadata={
            "ordered": scan.ordered,
            "descending_proven": scan.descending_proven,
            "early_stopped": scan.early_stopped,
            "capped_feed": scan.capped_feed,
            "limitation": scan.limitation,
            "scanned_count": scan.scanned_count,
            "cutoff": freeze.isoformat(),
            **dict(scan.metadata),
        },
    )
    conn.commit()
    return {
        **result,
        "discovered": len(scan.items),
        "scan": {
            "ordered": scan.ordered,
            "descending_proven": scan.descending_proven,
            "early_stopped": scan.early_stopped,
            "capped_feed": scan.capped_feed,
            "limitation": scan.limitation,
            "scanned_count": scan.scanned_count,
        },
        "cutoff": freeze,
        "source_id": source_id,
    }


def _drop_seen_new_since_flags(
    conn: psycopg.Connection,
    *,
    source_id: str,
    items: list[Any],
) -> None:
    """new_since_last_run also requires the GUID/URL to be unseen in earlier monitor runs."""
    flagged = [i for i in items if (i.metadata or {}).get("new_since_last_run")]
    if not flagged:
        return
    ids = [str(i.external_id or i.url) for i in flagged]
    urls = [str(i.url) for i in flagged]
    rows = conn.execute(
        """
        SELECT external_id, url FROM tasks
        WHERE source_id = %s AND (external_id = ANY(%s) OR url = ANY(%s))
        """,
        (source_id, ids, urls),
    ).fetchall()
    seen = {str(v) for row in rows for v in row if v is not None}
    for item in flagged:
        if str(item.external_id or item.url) in seen or str(item.url) in seen:
            item.metadata.pop("new_since_last_run", None)


async def process_monitor_task(
    conn: psycopg.Connection,
    task: ClaimedTask,
    *,
    pool: HttpPool,
    raw_store: RawStore | None = None,
    settings: Settings | None = None,
    policy: FetchPolicy | None = None,
    cutoff: datetime | None = None,
) -> dict[str, Any]:
    if task.stage == STAGE_PARSE:
        return process_parse_stage(conn, task, cutoff=cutoff)
    return await process_fetch_stage(
        conn,
        task,
        pool=pool,
        raw_store=raw_store,
        settings=settings,
        policy=policy,
    )


async def process_fetch_stage(
    conn: psycopg.Connection,
    task: ClaimedTask,
    *,
    pool: HttpPool,
    raw_store: RawStore | None = None,
    settings: Settings | None = None,
    policy: FetchPolicy | None = None,
) -> dict[str, Any]:
    cfg = settings or get_settings()
    store = raw_store or RawStore.from_settings(cfg)
    fetch_policy = policy or FetchPolicy(mode=cfg.mode)

    if cfg.mode.value == "fixture" and (
        task.payload.get("row") is not None or task.payload.get("body") is not None
    ):
        body_obj = task.payload.get("row")
        if body_obj is None:
            body = task.payload.get("body")
            if isinstance(body, str):
                raw_body = body.encode("utf-8")
                ctype = task.payload.get("content_type") or "text/html"
            elif isinstance(body, (bytes, bytearray)):
                raw_body = bytes(body)
                ctype = task.payload.get("content_type") or "application/octet-stream"
            else:
                raw_body = json.dumps(body).encode("utf-8")
                ctype = "application/json"
        else:
            raw_body = json.dumps({"row": body_obj, "_synthetic": True}).encode("utf-8")
            ctype = "application/json"
        observation = RawObservation(
            request_url=task.url,
            final_url=task.url,
            status_code=200,
            outcome=FetchOutcomeKind.OK,
            body=raw_body,
            content_type=ctype,
            fetched_at=get_clock().now(),
            metadata={"fixture_loopback": True, "adapter": task.source_id},
        )
    else:
        observation = await fetch_with_retries(
            task,
            fetch_policy,
            pool=pool,
            settings=cfg,
        )

    if observation.outcome is FetchOutcomeKind.RETRY_AT:
        ok = schedule_retry(
            conn,
            task,
            retry_at=observation.retry_at or task.lease_expires,
            error=observation.reason,
        )
        return {"ok": ok, "outcome": observation.outcome.value, "stage": STAGE_FETCH}

    committed = complete_with_blob(conn, task, observation, store)
    if not committed:
        return {
            "ok": False,
            "outcome": observation.outcome.value,
            "stage": STAGE_FETCH,
            "stale_lease": True,
        }

    parse_id = None
    if observation.outcome in {FetchOutcomeKind.OK, FetchOutcomeKind.NOT_MODIFIED}:
        parse_id = enqueue_task(
            conn,
            source_id=task.source_id,
            url=task.url,
            stage=STAGE_PARSE,
            external_id=task.external_id,
            run_id=task.run_id,
            parent_task_id=task.id,
            payload={
                **dict(task.payload),
                "fetch_task_id": str(task.id),
                "adapter": task.source_id,
            },
        )
        conn.commit()

    return {
        "ok": True,
        "outcome": observation.outcome.value,
        "stage": STAGE_FETCH,
        "parse_task_id": str(parse_id) if parse_id else None,
        "content_sha256": observation.content_sha256,
        "rate_limited": observation.status_code == 429,
    }


def process_parse_stage(
    conn: psycopg.Connection,
    task: ClaimedTask,
    *,
    cutoff: datetime | None = None,
) -> dict[str, Any]:
    parent_id = task.parent_task_id
    fetch_task_id = task.payload.get("fetch_task_id")
    if parent_id is None and fetch_task_id:
        parent_id = UUID(str(fetch_task_id))
    if parent_id is None:
        _fail_task(conn, task, "missing_parent_fetch")
        return {"ok": False, "stage": STAGE_PARSE, "reason": "missing_parent_fetch"}

    obs_row = get_observation_for_task(conn, parent_id)
    if obs_row is None:
        _fail_task(conn, task, "missing_observation")
        return {"ok": False, "stage": STAGE_PARSE, "reason": "missing_observation"}

    raw = _raw_from_observation_row(conn, obs_row, task=task)
    result = dispatch_parse(task.source_id, raw, task.payload, cutoff=cutoff)

    with conn.transaction():
        obs_id = UUID(str(obs_row["id"])) if obs_row.get("id") else None
        if not result.ok:
            insert_quarantine(
                conn,
                source_id=task.source_id,
                reason=result.quarantine_reason or "parse_failed",
                record_type=result.record_type,
                external_id=result.external_id or task.external_id,
                detail=result.quarantine_detail,
                run_id=task.run_id,
                observation_id=obs_id,
                semantic_key_value=task.semantic_key,
            )
            _complete_parse_task(conn, task, status="done", error=result.quarantine_reason)
            return {
                "ok": True,
                "stage": STAGE_PARSE,
                "quarantined": True,
                "reason": result.quarantine_reason,
            }

        assert result.record_type and result.external_id
        status = "accepted" if result.eligible else "pending"
        cand_id = upsert_candidate(
            conn,
            source_id=task.source_id,
            record_type=result.record_type,
            external_id=result.external_id,
            payload=result.payload,
            evidence=result.evidence,
            status=status,
            run_id=task.run_id,
            observation_id=obs_id,
        )
        _complete_parse_task(conn, task, status="done")

    return {
        "ok": True,
        "stage": STAGE_PARSE,
        "candidate_id": str(cand_id),
        "eligible": result.eligible,
        "external_id": result.external_id,
        "body_complete": result.body_complete,
    }


def dispatch_parse(
    source_id: str,
    raw: RawObservation,
    payload: dict[str, Any],
    *,
    cutoff: datetime | None = None,
) -> MonitorParseResult:
    cfg = load_monitor_config(source_id)
    adapter = get_adapter(source_id)
    freeze = cutoff
    if freeze is None and payload.get("cutoff"):
        freeze = datetime.fromisoformat(str(payload["cutoff"]))
    kwargs = {
        "cutoff": freeze,
        "list_fields": payload.get("list_fields") or (payload.get("row") or {}).get("fields"),
        "board_url": payload.get("board_url"),
        "employer_url": payload.get("employer_url"),
        "title_hint": payload.get("title"),
        "external_id_hint": payload.get("external_id") or None,
        "expected_start": payload.get("expected_start"),
        "expected_end": payload.get("expected_end"),
        "list_observed_at": payload.get("list_observed_at"),
        "new_since_last_run": payload.get("new_since_last_run"),
    }
    if adapter is not None and hasattr(adapter, "parse_detail"):
        return adapter.parse_detail(raw, **kwargs)
    return parse_detail(raw, cfg, **kwargs)


async def drain_monitor_once(
    conn: psycopg.Connection,
    *,
    source_id: str,
    pool: HttpPool,
    worker_id: str = "monitor-worker",
    limit: int = 20,
    raw_store: RawStore | None = None,
    settings: Settings | None = None,
    cutoff: datetime | None = None,
) -> dict[str, Any]:
    """Claim and process up to `limit` ready monitor tasks for one source."""
    claimed = claim_tasks(
        conn,
        worker_id=worker_id,
        source_id=source_id,
        limit=limit,
    )
    results = []
    for task in claimed:
        results.append(
            await process_monitor_task(
                conn,
                task,
                pool=pool,
                raw_store=raw_store,
                settings=settings,
                cutoff=cutoff,
            )
        )
    return {"claimed": len(claimed), "results": results}


def finalize_monitor_run(
    conn: psycopg.Connection,
    *,
    source_id: str,
    cutoff_at: datetime,
    discover_meta: dict[str, Any] | None = None,
    run_id: UUID | None = None,
    fetched_count: int = 0,
    accepted_count: int = 0,
    rejected_count: int = 0,
    pending_count: int = 0,
    failed: bool = False,
    rate_limited: bool = False,
    started_at: datetime | None = None,
) -> UUID:
    meta = discover_meta or {}
    scan = meta.get("scan") or meta
    pagination_complete = bool(
        meta.get("pagination_complete", not scan.get("capped_feed", False))
    )
    limitation = scan.get("limitation")
    partial = bool(scan.get("capped_feed")) or bool(limitation)
    health = classify_health(
        failed=failed,
        rate_limited=rate_limited,
        partial=partial and not failed and not rate_limited,
        accepted_count=accepted_count,
        limitation=limitation,
    )
    return insert_monitor_run(
        conn,
        source_id=source_id,
        cutoff_at=cutoff_at,
        health=health,
        started_at=started_at,
        run_id=run_id,
        pagination_complete=pagination_complete,
        feed_ordered=scan.get("ordered"),
        descending_proven=scan.get("descending_proven"),
        capped_feed=bool(scan.get("capped_feed")),
        discovered_count=int(meta.get("discovered") or scan.get("scanned_count") or 0),
        fetched_count=fetched_count,
        accepted_count=accepted_count,
        rejected_count=rejected_count,
        pending_count=pending_count,
        limitation=limitation,
        metadata=meta,
    )


def _raw_from_observation_row(
    conn: psycopg.Connection,
    obs_row: dict[str, Any],
    *,
    task: ClaimedTask,
) -> RawObservation:
    body = None
    content_key = obs_row.get("content_key")
    content_sha = obs_row.get("content_sha256")
    if content_key or content_sha:
        store = RawStore.from_settings(get_settings())
        try:
            if content_sha and store.exists(content_sha):
                body = store.read(content_sha)
            elif content_key:
                body = store.read_key(content_key)
        except FileNotFoundError:
            body = None
    if body is None and task.payload.get("row") is not None:
        body = json.dumps({"row": task.payload["row"], "_synthetic": True}).encode("utf-8")
    elif body is None and task.payload.get("body") is not None:
        payload_body = task.payload["body"]
        if isinstance(payload_body, str):
            body = payload_body.encode("utf-8")
        elif isinstance(payload_body, (bytes, bytearray)):
            body = bytes(payload_body)
        else:
            body = json.dumps(payload_body).encode("utf-8")
    outcome = FetchOutcomeKind(str(obs_row.get("outcome") or "ok"))
    return RawObservation(
        request_url=str(obs_row.get("request_url") or task.url),
        final_url=str(obs_row.get("final_url") or task.url),
        status_code=obs_row.get("status_code"),
        outcome=outcome,
        body=body,
        content_type=obs_row.get("content_type"),
        etag=obs_row.get("etag"),
        last_modified=obs_row.get("last_modified"),
        headers=dict(obs_row.get("headers") or {}),
        fetched_at=obs_row.get("fetched_at") or get_clock().now(),
        content_key=content_key,
        content_sha256=content_sha,
        metadata=dict(obs_row.get("metadata") or {}),
    )


def _complete_parse_task(
    conn: psycopg.Connection,
    task: ClaimedTask,
    *,
    status: str,
    error: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE tasks
        SET status = %s,
            last_error = %s,
            lease_owner = NULL,
            lease_expires = NULL,
            updated_at = %s
        WHERE id = %s
          AND lease_owner = %s
          AND fencing_token = %s
        """,
        (
            status,
            error,
            get_clock().now(),
            str(task.id),
            task.lease_owner,
            task.fencing_token,
        ),
    )


def _fail_task(conn: psycopg.Connection, task: ClaimedTask, reason: str) -> None:
    _complete_parse_task(conn, task, status="failed", error=reason)
    conn.commit()
