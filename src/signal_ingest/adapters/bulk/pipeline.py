"""Bulk pipeline: discover → fetch → parse (+ enrich_stars) on the B1 queue."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence
from urllib.parse import urlparse
from uuid import UUID

import psycopg

from signal_ingest.adapters.bulk import arxiv as arxiv_adapter
from signal_ingest.adapters.bulk import github as github_adapter
from signal_ingest.adapters.bulk import openrouter as openrouter_adapter
from signal_ingest.adapters.bulk import papers as papers_adapter
from signal_ingest.adapters.bulk import products as products_adapter
from signal_ingest.adapters.bulk import yc as yc_adapter
from signal_ingest.adapters.bulk.eligibility import (
    paper_eligible_from_candidate,
    paper_join_complete,
)
from signal_ingest.adapters.bulk.mapping import github_owner_repo, normalize_github_url
from signal_ingest.adapters.bulk.papers import apply_stars_to_paper_payload
from signal_ingest.adapters.bulk.persist import (
    get_candidate,
    insert_quarantine,
    upsert_candidate,
    upsert_relationship,
)
from signal_ingest.adapters.bulk.types import BulkParseResult
from signal_ingest.config.clock import get_clock
from signal_ingest.config.settings import Settings, get_settings
from signal_ingest.crawl.discover import persist_discovery_page
from signal_ingest.crawl.fetch import FetchPolicy, fetch_with_retries
from signal_ingest.crawl.pool import HttpPool
from signal_ingest.crawl.queue import (
    claim_tasks,
    complete_with_blob,
    enqueue_task,
    get_checkpoint,
    schedule_retry,
    upsert_checkpoint,
)
from signal_ingest.crawl.store import RawStore, get_observation_for_task, load_cached_body
from signal_ingest.crawl.types import ClaimedTask, DiscoveredItem, FetchOutcomeKind, RawObservation
from signal_ingest.live.settings import github_token_from_env

SOURCE_YC = yc_adapter.SOURCE_ID
SOURCE_PRODUCTS = products_adapter.SOURCE_ID
SOURCE_PAPERS = papers_adapter.SOURCE_ID
SOURCE_ARXIV = arxiv_adapter.SOURCE_ID
SOURCE_GITHUB = github_adapter.SOURCE_ID
SOURCE_OPENROUTER = openrouter_adapter.SOURCE_ID

BULK_SOURCES = frozenset(
    {SOURCE_YC, SOURCE_PRODUCTS, SOURCE_PAPERS, SOURCE_ARXIV, SOURCE_GITHUB, SOURCE_OPENROUTER}
)

STAGE_FETCH = "fetch"
STAGE_PARSE = "parse"
STAGE_ENRICH_STARS = "enrich_stars"


def get_adapter(source_id: str) -> Any:
    return {
        SOURCE_YC: yc_adapter,
        SOURCE_PRODUCTS: products_adapter,
        SOURCE_PAPERS: papers_adapter,
        SOURCE_ARXIV: arxiv_adapter,
        SOURCE_GITHUB: github_adapter,
        SOURCE_OPENROUTER: openrouter_adapter,
    }.get(source_id)


def is_bulk_source(source_id: str) -> bool:
    return source_id in BULK_SOURCES or source_id.startswith("bulk.")


def discover_and_enqueue(
    conn: psycopg.Connection,
    *,
    source_id: str,
    payload: Any,
    partition: str = "default",
    run_id: UUID | None = None,
    revision: str | None = None,
    base_url: str | None = None,
) -> dict[str, Any]:
    """
    Adapter discover → persist_discovery_page with revision-bound checkpoint.
    Replay of the same logical IDs is a no-op via semantic_key uniqueness.
    """
    items, rev, partition_key = _discover_items(
        source_id,
        payload,
        partition=partition,
        revision=revision,
        base_url=base_url,
    )
    prior = get_checkpoint(
        conn,
        source_id=source_id,
        stage="discover",
        partition_key=partition_key,
    )
    # Also check same partition under a different revision → new partition_key.
    result = persist_discovery_page(
        conn,
        source_id=source_id,
        items=items,
        run_id=run_id,
        stage=STAGE_FETCH,
        checkpoint_stage="discover",
        partition_key=partition_key,
        normalize=True,
    )
    # Stamp revision onto checkpoint metadata (persist_discovery_page stores cursor only).
    upsert_checkpoint(
        conn,
        source_id=source_id,
        stage="discover",
        partition_key=partition_key,
        cursor_opaque=result.get("cursor"),
        metadata={"revision": rev, "partition": partition, "content_revision": rev},
    )
    conn.commit()
    checkpoint = get_checkpoint(
        conn,
        source_id=source_id,
        stage="discover",
        partition_key=partition_key,
    )
    return {
        **result,
        "checkpoint": checkpoint,
        "revision": rev,
        "partition_key": partition_key,
        "prior_checkpoint": prior,
        "discovered": len(items),
    }


def _discover_items(
    source_id: str,
    payload: Any,
    *,
    partition: str,
    revision: str | None,
    base_url: str | None,
) -> tuple[list[DiscoveredItem], str, str]:
    if source_id == SOURCE_YC:
        kwargs: dict[str, Any] = {"partition": partition, "revision": revision}
        if base_url:
            kwargs["base_url"] = base_url
        return yc_adapter.discover_from_payload(payload, **kwargs)
    if source_id == SOURCE_PRODUCTS:
        return products_adapter.discover_from_payload(
            payload, partition=partition, revision=revision
        )
    if source_id == SOURCE_PAPERS:
        return papers_adapter.discover_from_payload(
            payload, partition=partition, revision=revision
        )
    if source_id == SOURCE_OPENROUTER:
        return openrouter_adapter.discover_from_payload(
            payload, partition=partition, revision=revision
        )
    if source_id == SOURCE_ARXIV:
        ids = payload if isinstance(payload, list) else list(payload.get("ids") or [])
        return arxiv_adapter.discover_ids(ids, partition=partition, revision=revision)
    if source_id == SOURCE_GITHUB:
        repos = payload if isinstance(payload, list) else list(payload.get("repos") or [])
        return github_adapter.discover_repos(repos, partition=partition, revision=revision)
    raise ValueError(f"unknown bulk source: {source_id}")


async def process_bulk_task(
    conn: psycopg.Connection,
    task: ClaimedTask,
    *,
    pool: HttpPool,
    raw_store: RawStore | None = None,
    settings: Settings | None = None,
    policy: FetchPolicy | None = None,
) -> dict[str, Any]:
    """Dispatch bulk stages. Fixture-mode loopback only for HTTP."""
    if task.stage == STAGE_PARSE:
        return process_parse_stage(conn, task, settings=settings)
    if task.stage == STAGE_ENRICH_STARS:
        return await process_enrich_stars_stage(
            conn,
            task,
            pool=pool,
            raw_store=raw_store,
            settings=settings,
            policy=policy,
        )
    # Default: fetch then enqueue parse.
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

    row = task.payload.get("row")
    listing = row.get("_listing") if isinstance(row, dict) else None
    # Row shortcut: the row was already fetched in a listing (live) or is a labeled
    # fixture. Products with a listing still GET the vendor homepage for owner evidence.
    shortcut = row is not None and (
        (listing is not None and task.source_id != SOURCE_PRODUCTS)
        or (listing is None and cfg.mode.value == "fixture")
    )
    if shortcut:
        body = json.dumps({"row": row, "repo_edges": task.payload.get("repo_edges")}).encode(
            "utf-8"
        )
        if listing is not None:
            # Evidence points at the fetched listing page; the item URL is never requested.
            metadata = {
                "listing_row_shortcut": True,
                "listing": listing,
                "item_url": task.url,
                "adapter": task.payload.get("adapter"),
            }
            fetched_at = _listing_time(listing) or get_clock().now()
            request_url = str(listing.get("url") or task.url)
        else:
            metadata = {"fixture_loopback": True, "adapter": task.payload.get("adapter")}
            fetched_at = get_clock().now()
            request_url = task.url
        observation = RawObservation(
            request_url=request_url,
            final_url=request_url,
            status_code=200,
            outcome=FetchOutcomeKind.OK,
            body=body,
            content_type="application/json",
            fetched_at=fetched_at,
            metadata=metadata,
        )
    else:
        cached_etag = None
        cached_body = None
        cached_meta = None
        cached = load_cached_body(conn, store, url=task.url)
        if cached is not None:
            cached_body, cached_meta = cached
            cached_etag = cached_meta.get("etag")
        observation = await fetch_with_retries(
            task,
            fetch_policy,
            pool=pool,
            settings=cfg,
            headers=_fetch_headers(task.url),
            cached_etag=cached_etag,
            cached_body=cached_body,
            cached_meta=cached_meta,
        )
        observation = _github_rate_limit_as_retry(observation)

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
                "adapter": task.payload.get("adapter") or task.source_id,
            },
        )
        conn.commit()

    return {
        "ok": True,
        "outcome": observation.outcome.value,
        "stage": STAGE_FETCH,
        "parse_task_id": str(parse_id) if parse_id else None,
        "content_sha256": observation.content_sha256,
    }


def process_parse_stage(
    conn: psycopg.Connection, task: ClaimedTask, *, settings: Settings | None = None
) -> dict[str, Any]:
    """Load parent fetch observation and run the source adapter parse."""
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

    raw = _raw_from_observation_row(conn, obs_row, task=task, settings=settings)
    result = dispatch_parse(task.source_id, raw, task.payload)
    row = task.payload.get("row")
    if result.ok and isinstance(row, dict) and row.get("_listing"):
        result.evidence.setdefault("listing", row["_listing"])

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
            if result.relations:
                _persist_relations(conn, result.relations, run_id=task.run_id, observation_id=obs_id)
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
        if result.relations:
            _persist_relations(conn, result.relations, run_id=task.run_id, observation_id=obs_id)

        enrich_id = None
        if result.enrich_stars and not result.eligible:
            enrich_id = _enqueue_enrich_stars(
                conn,
                task,
                enrich=result.enrich_stars,
                candidate_external_id=result.external_id,
            )
        _complete_parse_task(conn, task, status="done")

    return {
        "ok": True,
        "stage": STAGE_PARSE,
        "candidate_id": str(cand_id),
        "eligible": result.eligible,
        "enrich_task_id": str(enrich_id) if enrich_id else None,
        "external_id": result.external_id,
    }


async def process_enrich_stars_stage(
    conn: psycopg.Connection,
    task: ClaimedTask,
    *,
    pool: HttpPool,
    raw_store: RawStore | None = None,
    settings: Settings | None = None,
    policy: FetchPolicy | None = None,
) -> dict[str, Any]:
    """Fetch GitHub metrics and complete paper eligibility."""
    cfg = settings or get_settings()
    store = raw_store or RawStore.from_settings(cfg)
    fetch_policy = policy or FetchPolicy(mode=cfg.mode)

    paper_external_id = task.payload.get("paper_external_id") or task.external_id
    paper_source_id = task.payload.get("paper_source_id") or SOURCE_PAPERS

    # Fixture-mode loopback only: never hit public GitHub from fixture runs.
    fixture_repo = _fixture_repo_payload(task)
    if cfg.mode.value == "fixture" and fixture_repo is not None:
        if fixture_repo.get("_not_found") or fixture_repo.get("private") is True:
            observation = RawObservation(
                request_url=task.url,
                final_url=task.url,
                status_code=404 if fixture_repo.get("_not_found") else 200,
                outcome=FetchOutcomeKind.NOT_FOUND
                if fixture_repo.get("_not_found")
                else FetchOutcomeKind.OK,
                body=None
                if fixture_repo.get("_not_found")
                else json.dumps(fixture_repo).encode("utf-8"),
                content_type="application/json",
                fetched_at=get_clock().now(),
                metadata={"fixture_loopback": True},
            )
            if observation.outcome is FetchOutcomeKind.OK and fixture_repo.get("private"):
                observation.body = json.dumps(fixture_repo).encode("utf-8")
        else:
            observation = RawObservation(
                request_url=task.url,
                final_url=task.url,
                status_code=200,
                outcome=FetchOutcomeKind.OK,
                body=json.dumps(fixture_repo).encode("utf-8"),
                content_type="application/json",
                fetched_at=get_clock().now(),
                etag=fixture_repo.get("etag"),
                metadata={"fixture_loopback": True},
            )
    elif cfg.mode.value == "fixture":
        # No synthetic stars invented: quarantine path via missing body.
        observation = RawObservation(
            request_url=task.url,
            final_url=task.url,
            status_code=None,
            outcome=FetchOutcomeKind.INVALID,
            body=None,
            reason="fixture_enrich_requires_stargazers_count",
            fetched_at=get_clock().now(),
            metadata={"fixture_loopback": True},
        )
    else:
        cached_etag = None
        cached_body = None
        cached_meta = None
        cached = load_cached_body(conn, store, url=task.url)
        if cached is not None:
            cached_body, cached_meta = cached
            cached_etag = cached_meta.get("etag")
        observation = await fetch_with_retries(
            task,
            fetch_policy,
            pool=pool,
            settings=cfg,
            headers=_fetch_headers(task.url),
            cached_etag=cached_etag,
            cached_body=cached_body,
            cached_meta=cached_meta,
        )
        observation = _github_rate_limit_as_retry(observation)

    if observation.outcome is FetchOutcomeKind.RETRY_AT:
        ok = schedule_retry(
            conn,
            task,
            retry_at=observation.retry_at or task.lease_expires,
            error=observation.reason,
        )
        return {"ok": ok, "stage": STAGE_ENRICH_STARS, "outcome": observation.outcome.value}

    committed = complete_with_blob(conn, task, observation, store)
    if not committed:
        return {"ok": False, "stage": STAGE_ENRICH_STARS, "stale_lease": True}

    obs_row = get_observation_for_task(conn, task.id)
    obs_id = UUID(str(obs_row["id"])) if obs_row and obs_row.get("id") else None
    parsed = github_adapter.parse_observation(observation, observation_id=obs_id)

    with conn.transaction():
        if not parsed.ok:
            insert_quarantine(
                conn,
                source_id=SOURCE_GITHUB,
                reason=parsed.quarantine_reason or "github_failed",
                record_type="RESEARCH_PAPER",
                external_id=str(paper_external_id),
                detail=parsed.quarantine_detail,
                run_id=task.run_id,
                observation_id=obs_id,
            )
            # Mark paper candidate ineligible / quarantined.
            existing = get_candidate(
                conn,
                source_id=paper_source_id,
                record_type="RESEARCH_PAPER",
                external_id=str(paper_external_id),
            )
            if existing:
                upsert_candidate(
                    conn,
                    source_id=paper_source_id,
                    record_type="RESEARCH_PAPER",
                    external_id=str(paper_external_id),
                    payload=existing["payload"],
                    evidence=existing["evidence"],
                    status="quarantined",
                    run_id=task.run_id,
                    observation_id=obs_id,
                )
            return {
                "ok": True,
                "stage": STAGE_ENRICH_STARS,
                "quarantined": True,
                "reason": parsed.quarantine_reason,
            }

        metric_id = github_adapter.persist_metric_from_parse(conn, parsed)
        stars = int(parsed.payload["github_stars"])
        observed_at = parsed.payload["starsObservedAt"]
        repo_id = str(parsed.payload["repo_id"])
        repo_url = parsed.payload["repo_url"]

        existing = get_candidate(
            conn,
            source_id=paper_source_id,
            record_type="RESEARCH_PAPER",
            external_id=str(paper_external_id),
        )
        if existing is None:
            insert_quarantine(
                conn,
                source_id=paper_source_id,
                reason="paper_candidate_missing_for_stars",
                record_type="RESEARCH_PAPER",
                external_id=str(paper_external_id),
                detail={"repo_id": repo_id, "repo_url": repo_url},
                run_id=task.run_id,
            )
            return {
                "ok": True,
                "stage": STAGE_ENRICH_STARS,
                "metric_id": str(metric_id) if metric_id else None,
                "paper_missing": True,
            }

        new_payload = apply_stars_to_paper_payload(
            dict(existing["payload"]),
            stars=stars,
            stars_observed_at=observed_at,
            repo_id=repo_id,
        )
        evidence = dict(existing["evidence"] or {})
        evidence["stars"] = parsed.evidence
        eligible = paper_eligible_from_candidate(new_payload, primary_repo_ok=True)
        # Defense: require join predicate explicitly.
        content = new_payload.get("content") or {}
        eligible = eligible and paper_join_complete(
            title=content.get("title"),
            authors=content.get("authors"),
            paper_url=content.get("paper_url"),
            published_date=content.get("published_date"),
            github_url=content.get("github_url") or repo_url,
            github_stars=stars,
            primary_repo_ok=True,
        )
        upsert_candidate(
            conn,
            source_id=paper_source_id,
            record_type="RESEARCH_PAPER",
            external_id=str(paper_external_id),
            payload=new_payload,
            evidence=evidence,
            status="accepted" if eligible else "pending",
            run_id=task.run_id,
            observation_id=obs_id,
        )

    return {
        "ok": True,
        "stage": STAGE_ENRICH_STARS,
        "metric_id": str(metric_id) if metric_id else None,
        "stars": stars,
        "eligible": eligible,
        "repo_id": repo_id,
        "not_modified": observation.outcome is FetchOutcomeKind.NOT_MODIFIED,
    }


def dispatch_parse(
    source_id: str,
    raw: RawObservation,
    payload: dict[str, Any],
) -> BulkParseResult:
    row = payload.get("row")
    if source_id == SOURCE_YC:
        return yc_adapter.parse_observation(raw, row=row)
    if source_id == SOURCE_PRODUCTS:
        return products_adapter.parse_observation(raw, row=row)
    if source_id == SOURCE_PAPERS:
        return papers_adapter.parse_observation(
            raw,
            row=row,
            repo_edges=payload.get("repo_edges"),
            arxiv_meta=payload.get("arxiv_meta"),
        )
    if source_id == SOURCE_OPENROUTER:
        return openrouter_adapter.parse_observation(raw, row=row)
    if source_id == SOURCE_ARXIV:
        return arxiv_adapter.parse_observation(raw)
    if source_id == SOURCE_GITHUB:
        return github_adapter.parse_observation(raw)
    return BulkParseResult(
        ok=False,
        quarantine_reason="unknown_bulk_source",
        quarantine_detail={"source_id": source_id},
    )


def _enqueue_enrich_stars(
    conn: psycopg.Connection,
    task: ClaimedTask,
    *,
    enrich: dict[str, Any],
    candidate_external_id: str,
) -> UUID | None:
    repo_url = normalize_github_url(enrich.get("repo_url"))
    if not repo_url:
        return None
    # One star lookup per (repo, paper) per run: papers sharing a repo each get
    # completed, and a later run refreshes the snapshot (semantic_key has no run part).
    repo_id = enrich.get("repo_id")
    external = f"{repo_id if repo_id is not None else repo_url}|paper:{candidate_external_id}"
    api_url = enrich.get("api_url")
    if not api_url:
        pair = github_owner_repo(repo_url)
        api_url = (
            f"https://api.github.com/repos/{pair[0]}/{pair[1]}"
            if pair
            else repo_url
        )
    fixture_repo = enrich.get("fixture_repo")
    if fixture_repo is None and enrich.get("stargazers_count") is not None:
        pair = github_owner_repo(repo_url)
        fixture_repo = {
            "id": int(repo_id) if repo_id is not None and str(repo_id).isdigit() else abs(hash(repo_url)) % (10**9),
            "html_url": repo_url,
            "full_name": f"{pair[0]}/{pair[1]}" if pair else repo_url,
            "stargazers_count": int(enrich["stargazers_count"]),
            "private": False,
        }
    return enqueue_task(
        conn,
        source_id=SOURCE_GITHUB,
        url=str(api_url),
        stage=STAGE_ENRICH_STARS,
        external_id=external,
        run_id=task.run_id,
        parent_task_id=task.id,
        payload={
            "paper_external_id": candidate_external_id,
            "paper_source_id": task.source_id,
            "repo_url": repo_url,
            "repo_id": repo_id,
            "fixture_repo": fixture_repo,
            "stargazers_count": enrich.get("stargazers_count"),
            "simulate": enrich.get("simulate"),
            "adapter": SOURCE_GITHUB,
        },
        version=f"run:{task.run_id}",
    )


def _listing_time(listing: dict[str, Any]) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(listing.get("fetched_at")).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# The shared pool inflates only gzip/deflate under its byte caps; never advertise br/zstd.
_ACCEPT_ENCODING = {"Accept-Encoding": "gzip, deflate"}


def _fetch_headers(url: str) -> dict[str, str]:
    return {**_ACCEPT_ENCODING, **(_github_headers(url) or {})}


def _github_headers(url: str) -> dict[str, str] | None:
    """Bearer token from env for api.github.com only. Never logged or persisted."""
    if (urlparse(url).hostname or "").lower() != "api.github.com":
        return None
    headers = {"Accept": "application/vnd.github+json"}
    token = github_token_from_env()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _github_rate_limit_as_retry(observation: RawObservation) -> RawObservation:
    """GitHub 403/429 with x-ratelimit-remaining: 0 (or retry-after) is a scheduled retry.

    A plain 403 without those headers stays DENIED.
    """
    if observation.outcome is not FetchOutcomeKind.DENIED or observation.status_code not in {403, 429}:
        return observation
    hdrs = {str(k).lower(): str(v) for k, v in (observation.headers or {}).items()}
    now = get_clock().now()
    retry_at = None
    if hdrs.get("x-ratelimit-remaining", "").strip() == "0":
        reset = hdrs.get("x-ratelimit-reset", "").strip()
        retry_at = (
            datetime.fromtimestamp(int(reset), tz=timezone.utc)
            if reset.isdigit()
            else now + timedelta(hours=1)
        )
    elif hdrs.get("retry-after", "").strip().isdigit():
        retry_at = now + timedelta(seconds=int(hdrs["retry-after"].strip()))
    if retry_at is None:
        return observation
    return replace(
        observation,
        outcome=FetchOutcomeKind.RETRY_AT,
        retry_at=max(retry_at, now),
        reason="github_rate_limited",
    )


def _fixture_repo_payload(task: ClaimedTask) -> dict[str, Any] | None:
    repo = task.payload.get("fixture_repo")
    if isinstance(repo, dict) and (
        "stargazers_count" in repo or repo.get("_not_found") or repo.get("private") is True
    ):
        return dict(repo)
    if task.payload.get("stargazers_count") is not None:
        repo_url = normalize_github_url(task.payload.get("repo_url") or task.url) or task.url
        pair = github_owner_repo(repo_url)
        repo_id = task.payload.get("repo_id")
        return {
            "id": int(repo_id)
            if repo_id is not None and str(repo_id).isdigit()
            else abs(hash(repo_url)) % (10**9),
            "html_url": repo_url,
            "full_name": f"{pair[0]}/{pair[1]}" if pair else repo_url,
            "stargazers_count": int(task.payload["stargazers_count"]),
            "private": False,
            "etag": task.payload.get("etag"),
        }
    if task.payload.get("simulate") in {"not_found", "private", "deleted"}:
        kind = task.payload["simulate"]
        if kind in {"not_found", "deleted"}:
            return {"_not_found": True}
        return {
            "id": 1,
            "html_url": task.payload.get("repo_url") or task.url,
            "full_name": "private/repo",
            "stargazers_count": 0,
            "private": True,
        }
    return None


async def drain_bulk_once(
    conn: psycopg.Connection,
    *,
    worker_id: str,
    pool: HttpPool,
    source_id: str | None = None,
    stage: str | None = None,
    settings: Settings | None = None,
) -> dict[str, Any] | None:
    """Claim and process one bulk-related task (any stage unless filtered)."""
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
    result = await process_bulk_task(
        conn,
        task,
        pool=pool,
        settings=settings,
    )
    result["task_id"] = str(task.id)
    result["stage_claimed"] = task.stage
    result["source_id"] = task.source_id
    return result


def _persist_relations(
    conn: psycopg.Connection,
    relations: Sequence[dict[str, Any]],
    *,
    run_id: UUID | None,
    observation_id: UUID | None,
) -> None:
    for rel in relations:
        if not rel.get("to_key"):
            continue
        upsert_relationship(
            conn,
            relation_type=str(rel.get("relation_type") or "IMPLEMENTED_BY"),
            from_kind=str(rel.get("from_kind") or "paper"),
            from_key=str(rel.get("from_key")),
            to_kind=str(rel.get("to_kind") or "repository"),
            to_key=str(rel.get("to_key")),
            evidence=dict(rel.get("evidence") or {}),
            observation_id=observation_id,
            is_primary=bool(rel.get("is_primary")),
            status=str(rel.get("status") or "accepted"),
            reason=rel.get("reason"),
            run_id=run_id,
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
            error,
            get_clock().now(),
            str(task.id),
            task.lease_owner,
            task.fencing_token,
        ),
    )


def _fail_task(conn: psycopg.Connection, task: ClaimedTask, error: str) -> None:
    with conn.transaction():
        _complete_parse_task(conn, task, status="failed", error=error)


def _raw_from_observation_row(
    conn: psycopg.Connection,
    obs_row: dict[str, Any],
    *,
    task: ClaimedTask,
    settings: Settings | None = None,
) -> RawObservation:
    body = None
    content_key = obs_row.get("content_key")
    content_sha = obs_row.get("content_sha256")
    if content_key or content_sha:
        store = RawStore.from_settings(settings or get_settings())
        try:
            if content_sha and store.exists(content_sha):
                body = store.read(content_sha)
            elif content_key:
                body = store.read_key(content_key)
        except FileNotFoundError:
            body = None
    # Fixture: reconstruct from task payload when blob missing.
    if body is None and task.payload.get("row") is not None:
        body = json.dumps(
            {"row": task.payload["row"], "repo_edges": task.payload.get("repo_edges")}
        ).encode("utf-8")
    outcome = FetchOutcomeKind(obs_row["outcome"])
    return RawObservation(
        request_url=obs_row["request_url"],
        final_url=obs_row["final_url"],
        status_code=obs_row.get("status_code"),
        outcome=outcome,
        body=body,
        content_type=obs_row.get("content_type"),
        etag=obs_row.get("etag"),
        last_modified=obs_row.get("last_modified"),
        headers=dict(obs_row.get("headers") or {}),
        fetched_at=obs_row.get("fetched_at"),
        content_key=content_key,
        content_sha256=content_sha,
        metadata=dict(obs_row.get("metadata") or {}),
    )
