"""Ashby public Job Postings API boards of AI companies -> JOB.

API: GET https://api.ashbyhq.com/posting-api/job-board/<org>?includeCompensation=false
(no auth). Every field comes from the listing row, so the fetch stage uses the
listing-row shortcut (no per-posting GET). Only isListed postings are kept.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from signal_ingest.adapters.monitors.types import DiscoverScanResult, MonitorParseResult
from signal_ingest.crawl.types import DiscoveredItem, RawObservation
from signal_ingest.freshness.interval import PublicationInterval
from signal_ingest.freshness.window import in_freshness_window

SOURCE_ID = "jobs_ashby_ai"
SOURCE_NAME = "Ashby job boards (AI companies)"
API_URL = "https://api.ashbyhq.com/posting-api/job-board/{org}?includeCompensation=false"

# Controlled role families: first match on title, then department/team (versioned rules).
ROLE_RULE_VERSION = "role-family-v1"
_ROLE_RULES: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (family, re.compile(rx, re.IGNORECASE))
    for family, rx in (
        ("research", r"\bresearch(er)?\b|\bscientist\b|\bphd\b"),
        ("ml_engineering", r"\b(ml|machine learning|ai|llm|inference|training|model)s?\b.*\bengineer"
         r"|\bengineer\b.*\b(ml|machine learning|ai|llm|inference)\b"),
        ("data", r"\bdata\b|\banalytics?\b|\banalyst\b"),
        ("engineering", r"\bengineer(ing)?\b|\bdeveloper\b|\bsre\b|\binfrastructure\b|\bsecurity\b|\barchitect\b"),
        ("product", r"\bproduct (manager|management|lead)\b|\bprogram manager\b|\bpm\b"),
        ("design", r"\bdesign(er)?\b|\bux\b|\bui\b"),
        ("sales", r"\bsales\b|\baccount (executive|manager)\b|\bbusiness development\b|\bsdr\b|\bbdr\b|\brevenue\b|\bgo[- ]to[- ]market\b|\bgtm\b"),
        ("marketing", r"\bmarketing\b|\bcommunications?\b|\bbrand\b|\bcontent\b|\bgrowth\b"),
        ("customer_success", r"\bcustomer\b|\bsupport\b|\bsuccess\b|\bsolutions?\b"),
        ("people", r"\brecruit(er|ing)\b|\btalent\b|\bpeople\b|\bhr\b"),
        ("legal", r"\blegal\b|\bcounsel\b|\bpolicy\b|\bcompliance\b"),
        ("finance", r"\bfinance\b|\baccount(ing|ant)\b|\btax\b|\btreasury\b"),
        ("operations", r"\boperations?\b|\bops\b|\bworkplace\b|\bfacilities\b"),
    )
)


def role_family(title: str | None, department: str | None, team: str | None) -> tuple[str, str]:
    """(family, evidence field) from title first, then department/team; 'other' if none."""
    for field, text in (("title", title), ("department", department), ("team", team)):
        for family, rx in _ROLE_RULES:
            if text and rx.search(text):
                return family, field
    return "other", "none"


def remote_flag(posting: dict[str, Any]) -> tuple[bool | None, str]:
    """contracts.md: remote = explicitly fully remote; explicit on-site/hybrid = False.

    Ashby sets isRemote=True on Hybrid roles, so workplaceType decides when present.
    """
    wt = str(posting.get("workplaceType") or "").strip().lower()
    if wt == "remote":
        return True, "workplaceType"
    if wt in {"onsite", "on-site", "hybrid"}:
        return False, "workplaceType"
    if isinstance(posting.get("isRemote"), bool):
        return posting["isRemote"], "isRemote"
    return None, "none"


def rows_from_board(
    payload: dict[str, Any], *, org: str, company: str, listing: dict[str, Any]
) -> list[dict[str, Any]]:
    """Listed postings from one board response, stamped with listing provenance."""
    out = []
    for job in payload.get("jobs") or []:
        if not isinstance(job, dict) or job.get("isListed") is not True or not job.get("id"):
            continue
        row = {k: job.get(k) for k in (
            "id", "title", "department", "team", "publishedAt", "isRemote", "workplaceType",
            "location", "jobUrl", "applyUrl", "descriptionPlain", "employmentType",
        )}
        out.append({**row, "org": org, "company": company, "_listing": listing})
    return out


def discover_list(
    payload: Any,
    *,
    checkpoint: str | None = None,
    cutoff: datetime | None = None,
    observed_at: datetime | None = None,
    feed_ordered: bool | None = None,
    base_url: str | None = None,
) -> DiscoverScanResult:
    """payload = {"items": [rows_from_board rows]} -> one task per posting (no pre-filter)."""
    rows = payload.get("items") if isinstance(payload, dict) else payload
    items = []
    for row in rows or []:
        if not isinstance(row, dict) or not row.get("id") or not row.get("org"):
            continue
        items.append(
            DiscoveredItem(
                url=str(row.get("jobUrl") or (row.get("_listing") or {}).get("url")),
                external_id=f"{row['org']}:{row['id']}",
                cursor=None,
                metadata={
                    "adapter": SOURCE_ID,
                    "row": row,
                    "title": row.get("title"),
                    "list_observed_at": observed_at.isoformat() if observed_at else None,
                    "board_url": (row.get("_listing") or {}).get("url"),
                    "synthetic": bool(row.get("_synthetic", False)),
                },
            )
        )
    return DiscoverScanResult(
        items=items, ordered=False, scanned_count=len(items),
        metadata={"format": "ashby_posting_api", "boards": sorted({str(r.get("org")) for r in rows or [] if isinstance(r, dict)})},
    )


def _quarantine(reason: str, external_id: str | None, **detail: Any) -> MonitorParseResult:
    return MonitorParseResult(
        ok=False, record_type="JOB", external_id=external_id,
        quarantine_reason=reason, quarantine_detail=detail,
    )


def parse_detail(
    raw: RawObservation,
    *,
    cutoff: datetime | None = None,
    llm_remote: dict[str, Any] | None = None,
    **_: Any,
) -> MonitorParseResult:
    """llm_remote: an ACCEPTED, quote-grounded is_remote from extract(), used only when the
    posting carries no source flag ({value, quote, segment_ids, provider, model})."""
    try:
        body = json.loads((raw.body or b"{}").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _quarantine("parse_drift", None)
    row = body.get("row") if isinstance(body, dict) else None
    if not isinstance(row, dict) or not row.get("id"):
        return _quarantine("parse_drift", None)
    external_id = f"{row.get('org')}:{row['id']}"
    listing = row.get("_listing") or {}
    try:
        published = datetime.fromisoformat(str(row.get("publishedAt")).replace("Z", "+00:00"))
    except ValueError:
        return _quarantine("missing_publication_evidence", external_id)
    if published.tzinfo is None:
        return _quarantine("missing_publication_evidence", external_id, raw=row.get("publishedAt"))
    published = published.astimezone(timezone.utc)
    interval = PublicationInterval(
        lower=published, upper=published, precision="instant",
        method="ashby_publishedAt", raw=str(row.get("publishedAt")), tz="UTC",
    )
    text = str(row.get("descriptionPlain") or "").strip()
    title = str(row.get("title") or "").strip()
    if not title:
        return _quarantine("missing_title", external_id)
    if not text:
        return _quarantine("incomplete_body", external_id)
    is_remote, remote_basis = remote_flag(row)
    if is_remote is None and llm_remote and isinstance(llm_remote.get("value"), bool):
        is_remote, remote_basis = llm_remote["value"], "llm"
    if is_remote is None:
        return _quarantine("remote_unspecified", external_id, workplaceType=row.get("workplaceType"))
    family, family_basis = role_family(title, row.get("department"), row.get("team"))
    job_url = str(row.get("jobUrl") or "")
    date = published.isoformat().replace("+00:00", "Z")
    publication = interval.to_dict()
    payload = {
        "schemaVersion": "1.0",
        "source": {"name": SOURCE_NAME, "url": str(listing.get("url") or raw.request_url)},
        "content": {
            "company": row.get("company"),
            "date": date,
            "date_precision": "instant",
            "is_remote": is_remote,
            "role_family": family,
            "title": title,
            "listing_url": str(listing.get("url") or raw.request_url),
            "original_employer_job_url": job_url,
            "description": text,
        },
        "collectedAt": (raw.fetched_at or datetime.now(timezone.utc)).isoformat(),
        "publication": publication,
        "board_url": listing.get("url"),
        "employer_url": job_url,
    }
    evidence = {
        "publication": publication,
        "date_extraction_method": "ashby_publishedAt",
        "date_precision": "instant",
        "is_remote": {"value": is_remote, "basis": remote_basis, "workplaceType": row.get("workplaceType"), "isRemote": row.get("isRemote"),
                      **({"method": "llm", **{k: llm_remote.get(k) for k in ("quote", "segment_ids", "provider", "model")}} if remote_basis == "llm" and llm_remote else {})},
        "role_family": {"value": family, "basis": family_basis, "rule_version": ROLE_RULE_VERSION},
        "body": {
            "start_excerpt": text[:240], "end_excerpt": text[-240:], "complete": True,
            "method": "ashby_descriptionPlain", "length": len(text),
        },
        "listing": listing,
        "listing_url": listing.get("url"),
        "employer_job_url": job_url,
        "observation": {"request_url": raw.request_url, "content_sha256": raw.content_sha256},
        "source_id": SOURCE_ID,
        "synthetic": bool(row.get("_synthetic", False)),
    }
    return MonitorParseResult(
        ok=True, record_type="JOB", external_id=external_id, payload=payload, evidence=evidence,
        publication=interval, eligible=bool(cutoff and in_freshness_window(interval, cutoff)),
        body_complete=True, board_url=listing.get("url"), employer_url=job_url,
    )
