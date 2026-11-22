"""YC OSS-shaped startup bulk adapter."""

from __future__ import annotations

import json
from typing import Any, Sequence

from signal_ingest.adapters.bulk.eligibility import startup_eligible
from signal_ingest.adapters.bulk.mapping import (
    checkpoint_partition_key,
    content_revision,
    is_explicit_ai,
    parse_employee_count,
)
from signal_ingest.adapters.bulk.types import BulkParseResult
from signal_ingest.config.clock import get_clock
from signal_ingest.crawl.types import DiscoveredItem, RawObservation

SOURCE_ID = "bulk.yc"
SOURCE_NAME = "YC OSS"


def discover_from_payload(
    payload: Any,
    *,
    partition: str = "default",
    base_url: str = "https://www.ycombinator.com/companies/",
    revision: str | None = None,
) -> tuple[list[DiscoveredItem], str, str]:
    """
    Stream companies from a YC-shaped JSON array/object.
    Returns (items, revision, partition_key).
    """
    rows = _companies(payload)
    rev = revision or content_revision(rows)
    partition_key = checkpoint_partition_key(revision=rev, partition=partition)
    items: list[DiscoveredItem] = []
    for idx, row in enumerate(rows):
        external_id = _company_id(row)
        slug = str(row.get("slug") or row.get("id") or external_id)
        url = str(row.get("url") or row.get("profile_url") or f"{base_url.rstrip('/')}/{slug}")
        cursor = f"{rev}:{partition}:{idx}"
        items.append(
            DiscoveredItem(
                url=url,
                external_id=external_id,
                cursor=cursor,
                metadata={
                    "adapter": SOURCE_ID,
                    "partition": partition,
                    "revision": rev,
                    "row": row,
                    "synthetic": bool(row.get("_synthetic", True)),
                },
            )
        )
    return items, rev, partition_key


def parse_observation(
    raw: RawObservation,
    *,
    row: dict[str, Any] | None = None,
) -> BulkParseResult:
    """Map one company observation to STARTUP or quarantine."""
    data = row
    if data is None:
        data = _row_from_body(raw)
    if data is None:
        return BulkParseResult(
            ok=False,
            quarantine_reason="parse_drift",
            quarantine_detail={"error": "missing_company_json"},
        )

    external_id = _company_id(data)
    name = (
        data.get("name")
        or data.get("entityName")
        or data.get("company_name")
        or data.get("title")
    )
    if not name:
        return BulkParseResult(
            ok=False,
            record_type="STARTUP",
            external_id=external_id,
            quarantine_reason="missing_entity_name",
            quarantine_detail={"keys": sorted(data.keys())},
        )

    raw_team = data.get("team_size", data.get("employeeCount", data.get("employees")))
    employee_count = parse_employee_count(raw_team)
    ai_included = is_explicit_ai(
        tags=data.get("tags") or data.get("industries") or data.get("tags_highlighted"),
        categories=data.get("categories"),
        description=data.get("one_liner") or data.get("description"),
        industry=data.get("industry"),
    )
    if not ai_included:
        return BulkParseResult(
            ok=False,
            record_type="STARTUP",
            external_id=external_id,
            quarantine_reason="ai_inclusion_not_explicit",
            quarantine_detail={
                "tags": data.get("tags"),
                "industries": data.get("industries"),
            },
        )

    source_url = (
        data.get("url")
        or data.get("profile_url")
        or raw.final_url
        or raw.request_url
    )
    collected = get_clock().now()
    payload = {
        "schemaVersion": "1.0",
        "source": {"name": SOURCE_NAME, "url": source_url},
        "content": {
            "entityName": str(name),
            "data": {"employeeCount": employee_count},
        },
        "collectedAt": collected.isoformat().replace("+00:00", "Z"),
        "meta": {
            "ai_included": True,
            "team_size_raw": raw_team,
            "yc_id": external_id,
            "batch": data.get("batch"),
        },
    }
    evidence = {
        "request_url": raw.request_url,
        "final_url": raw.final_url,
        "content_sha256": raw.content_sha256,
        "employeeCount": {
            "path": "$.team_size",
            "raw": raw_team,
            "parsed": employee_count,
        },
        "ai_inclusion": {
            "path": "$.tags",
            "tags": data.get("tags") or data.get("industries"),
        },
    }
    eligible = startup_eligible(payload, ai_included=True)
    return BulkParseResult(
        ok=True,
        record_type="STARTUP",
        external_id=external_id,
        payload=payload,
        evidence=evidence,
        eligible=eligible,
    )


def _companies(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("companies", "results", "items", "data"):
            val = payload.get(key)
            if isinstance(val, list):
                return [r for r in val if isinstance(r, dict)]
        if "id" in payload or "name" in payload or "slug" in payload:
            return [payload]
    return []


def _company_id(row: dict[str, Any]) -> str:
    for key in ("id", "uuid", "slug", "company_id"):
        if row.get(key) is not None:
            return str(row[key])
    name = row.get("name") or row.get("entityName") or "unknown"
    return f"name:{name}".lower()


def _row_from_body(raw: RawObservation) -> dict[str, Any] | None:
    if raw.body is None:
        return None
    try:
        data = json.loads(raw.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if isinstance(data, dict) and "row" in data and isinstance(data["row"], dict):
        return data["row"]
    rows = _companies(data)
    return rows[0] if len(rows) == 1 else (data if isinstance(data, dict) else None)


def discover_pages(
    pages: Sequence[Any],
    *,
    partition: str = "default",
) -> tuple[list[DiscoveredItem], str, str]:
    """Flatten multiple pages; revision hashes the concatenated payload."""
    all_rows: list[dict[str, Any]] = []
    for page in pages:
        all_rows.extend(_companies(page))
    return discover_from_payload(all_rows, partition=partition)
