"""GitHub repository metrics adapter."""

from __future__ import annotations

import json
from datetime import timezone
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import psycopg

from signal_ingest.adapters.bulk.mapping import (
    checkpoint_partition_key,
    content_revision,
    github_owner_repo,
    normalize_github_url,
)
from signal_ingest.adapters.bulk.persist import upsert_repo_metric
from signal_ingest.adapters.bulk.types import BulkParseResult, RepoMetricSnapshot
from signal_ingest.config.clock import get_clock
from signal_ingest.crawl.types import DiscoveredItem, FetchOutcomeKind, RawObservation

SOURCE_ID = "bulk.github"
SOURCE_NAME = "GitHub"


def discover_repos(
    repos: list[dict[str, Any] | str],
    *,
    partition: str = "default",
    revision: str | None = None,
    api_base: str = "https://api.github.com/repos",
) -> tuple[list[DiscoveredItem], str, str]:
    """
    Deduplicate by stable repo ID when known, else normalized URL.
    """
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for item in repos:
        if isinstance(item, str):
            row = {"repo_url": item}
        else:
            row = dict(item)
        key = _dedupe_key(row)
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(row)

    rev = revision or content_revision(unique)
    partition_key = checkpoint_partition_key(revision=rev, partition=partition)
    items: list[DiscoveredItem] = []
    for idx, row in enumerate(unique):
        url = _api_url(row, api_base=api_base)
        external_id = _dedupe_key(row) or url
        items.append(
            DiscoveredItem(
                url=url,
                external_id=str(external_id),
                cursor=f"{rev}:{partition}:{idx}",
                metadata={
                    "adapter": SOURCE_ID,
                    "partition": partition,
                    "revision": rev,
                    "row": row,
                    "repo_url": normalize_github_url(
                        row.get("repo_url") or row.get("html_url") or row.get("url")
                    ),
                    "repo_id": row.get("repo_id") or row.get("id"),
                },
            )
        )
    return items, rev, partition_key


def parse_observation(
    raw: RawObservation,
    *,
    observation_id: UUID | None = None,
) -> BulkParseResult:
    """
    Parse GitHub repo JSON.
    True zero stars is valid. Private/deleted → quarantine (no fabricated zero).
    304 with cached body revalidates via RawObservation body reuse upstream.
    """
    if raw.outcome is FetchOutcomeKind.NOT_FOUND:
        return BulkParseResult(
            ok=False,
            quarantine_reason="repo_private_or_deleted",
            quarantine_detail={
                "status_code": raw.status_code,
                "url": raw.final_url or raw.request_url,
            },
        )
    if raw.outcome in {FetchOutcomeKind.DENIED, FetchOutcomeKind.BLOCKED}:
        return BulkParseResult(
            ok=False,
            quarantine_reason="repo_access_denied",
            quarantine_detail={"outcome": raw.outcome.value, "reason": raw.reason},
        )
    if raw.body is None:
        return BulkParseResult(
            ok=False,
            quarantine_reason="missing_repo_body",
            quarantine_detail={"outcome": raw.outcome.value},
        )

    try:
        data = json.loads(raw.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return BulkParseResult(
            ok=False,
            quarantine_reason="repo_parse_drift",
            quarantine_detail={"error": str(exc)},
        )

    if not isinstance(data, dict):
        return BulkParseResult(
            ok=False,
            quarantine_reason="repo_parse_drift",
            quarantine_detail={"type": type(data).__name__},
        )

    if data.get("private") is True or data.get("message") in {
        "Not Found",
        "Moved Permanently",
    }:
        # Anonymous 404 may already be NOT_FOUND; private flag on body also quarantines.
        return BulkParseResult(
            ok=False,
            quarantine_reason="repo_private_or_deleted",
            quarantine_detail={"private": data.get("private"), "message": data.get("message")},
        )

    repo_id = data.get("id")
    if repo_id is None:
        return BulkParseResult(
            ok=False,
            quarantine_reason="missing_repo_id",
            quarantine_detail={},
        )
    html_url = normalize_github_url(
        data.get("html_url") or data.get("url") or raw.final_url
    )
    if not html_url:
        return BulkParseResult(
            ok=False,
            quarantine_reason="missing_repo_url",
            quarantine_detail={},
        )

    if "stargazers_count" not in data:
        return BulkParseResult(
            ok=False,
            quarantine_reason="missing_stargazers_count",
            quarantine_detail={"keys": sorted(data.keys())},
        )
    stars = data["stargazers_count"]
    if not isinstance(stars, int) or isinstance(stars, bool) or stars < 0:
        return BulkParseResult(
            ok=False,
            quarantine_reason="invalid_stargazers_count",
            quarantine_detail={"stargazers_count": stars},
        )

    observed = raw.fetched_at or get_clock().now()
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)

    renamed_from = None
    # Follow renames: compare requested owner/repo with the API's full_name.
    pair = requested_owner_repo(raw.request_url)
    full_name = str(data.get("full_name") or "").strip().lower()
    current = tuple(full_name.split("/", 1)) if "/" in full_name else github_owner_repo(html_url)
    if pair and current and pair != current:
        renamed_from = f"{pair[0]}/{pair[1]}"

    snapshot = RepoMetricSnapshot(
        repo_id=str(repo_id),
        repo_url=html_url,
        stars=stars,
        observed_at=observed,
        etag=raw.etag,
        content_sha256=raw.content_sha256,
        observation_id=observation_id,
        renamed_from=renamed_from,
        metadata={
            "full_name": data.get("full_name"),
            "not_modified": raw.outcome is FetchOutcomeKind.NOT_MODIFIED,
            "renamed_from": renamed_from,
        },
    )
    return BulkParseResult(
        ok=True,
        record_type="RESEARCH_PAPER",
        external_id=str(repo_id),
        payload={
            "repo_id": str(repo_id),
            "repo_url": html_url,
            "github_stars": stars,
            "starsObservedAt": observed.isoformat().replace("+00:00", "Z"),
            "full_name": data.get("full_name"),
            "renamed_from": renamed_from,
        },
        evidence={
            "request_url": raw.request_url,
            "final_url": raw.final_url,
            "content_sha256": raw.content_sha256,
            "etag": raw.etag,
            "outcome": raw.outcome.value,
            "stargazers_count": {"path": "$.stargazers_count", "value": stars},
        },
        eligible=True,
        enrich_stars={"snapshot": snapshot},
    )


def requested_owner_repo(url: str | None) -> tuple[str, str] | None:
    """Lowercased owner/repo from an api.github.com/repos/<o>/<r> or github.com URL."""
    if not url:
        return None
    parsed = urlparse(str(url).strip())
    if (parsed.hostname or "").lower() == "api.github.com":
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 3 and parts[0] == "repos":
            return parts[1].lower(), parts[2].lower()
        return None
    return github_owner_repo(url)


def persist_metric_from_parse(
    conn: psycopg.Connection,
    result: BulkParseResult,
) -> UUID | None:
    if not result.ok or not result.enrich_stars:
        return None
    snap = result.enrich_stars.get("snapshot")
    if not isinstance(snap, RepoMetricSnapshot):
        return None
    return upsert_repo_metric(conn, snap, source_id=SOURCE_ID)


def _dedupe_key(row: dict[str, Any]) -> str | None:
    if row.get("repo_id") is not None:
        return f"id:{row['repo_id']}"
    if row.get("id") is not None and not row.get("repo_url"):
        return f"id:{row['id']}"
    url = normalize_github_url(
        row.get("repo_url") or row.get("html_url") or row.get("url") or row.get("github_url")
    )
    return f"url:{url}" if url else None


def _api_url(row: dict[str, Any], *, api_base: str) -> str:
    if row.get("api_url"):
        return str(row["api_url"])
    pair = github_owner_repo(
        row.get("repo_url") or row.get("html_url") or row.get("url") or row.get("github_url")
    )
    if pair:
        return f"{api_base.rstrip('/')}/{pair[0]}/{pair[1]}"
    if row.get("full_name"):
        return f"{api_base.rstrip('/')}/{row['full_name']}"
    return str(row.get("url") or api_base)
