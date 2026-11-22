"""Shared detail parse for NEWS and JOB monitors."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from signal_ingest.adapters.monitors.body import (
    extract_body,
    extract_json_ld_blocks,
    extract_page_dates,
)
from signal_ingest.adapters.monitors.types import MonitorParseResult, MonitorSourceConfig
from signal_ingest.config.clock import get_clock
from signal_ingest.crawl.types import RawObservation
from signal_ingest.freshness import (
    PublicationInterval,
    in_freshness_window,
    is_non_proof_field,
    is_relative_text,
    parse_publication_value,
    select_publication_interval,
)


def parse_detail(
    raw: RawObservation,
    source_cfg: MonitorSourceConfig,
    *,
    cutoff: datetime | None = None,
    list_fields: dict[str, Any] | None = None,
    board_url: str | None = None,
    employer_url: str | None = None,
    title_hint: str | None = None,
    external_id_hint: str | None = None,
    expected_start: str | None = None,
    expected_end: str | None = None,
    list_observed_at: datetime | str | None = None,
    new_since_last_run: dict[str, Any] | None = None,
) -> MonitorParseResult:
    """
    Parse a detail page/feed item into NEWS or JOB.

    Prefer original publication fields; never updated/lastmod/first_seen as proof.
    Require full body with start and end evidence. Jobs keep board + employer URLs.
    Relative listing dates are anchored to the listing fetch time, never the detail's.
    cutoff=None never admits (eligible=False). A dateless row flagged new_since_last_run
    by discovery is kept observed-only (precision=heuristic), never date-proven fresh.
    """
    observed_at = raw.fetched_at or get_clock().now()
    record_type = "NEWS" if source_cfg.is_news else "JOB"
    list_fields = _anchor_list_fields(
        list_fields,
        list_observed_at=list_observed_at or (list_fields or {}).get("list_observed_at"),
        source_timezone=source_cfg.original_timezone,
    )

    body_bytes = raw.body or b""
    ctype = (raw.content_type or "").lower()
    structured = _maybe_json(body_bytes, ctype)

    if structured is not None:
        return _parse_structured(
            structured,
            raw=raw,
            source_cfg=source_cfg,
            record_type=record_type,
            cutoff=cutoff,
            observed_at=observed_at,
            list_fields=list_fields,
            board_url=board_url,
            employer_url=employer_url,
            title_hint=title_hint,
            external_id_hint=external_id_hint,
            new_since_last_run=new_since_last_run,
        )

    html = body_bytes.decode("utf-8", errors="replace")
    body = extract_body(
        html,
        expected_start=expected_start,
        expected_end=expected_end,
    )
    fields = _collect_html_fields(
        html,
        list_fields=list_fields,
        source_cfg=source_cfg,
        page_url=raw.final_url or raw.request_url,
    )
    # Strip non-proof keys explicitly.
    fields = {k: v for k, v in fields.items() if not is_non_proof_field(k)}

    interval, conflict = select_publication_interval(
        fields,
        observed_at=observed_at,
        source_timezone=source_cfg.original_timezone,
        prefer_keys=(source_cfg.original_time_field,)
        + (
            "datePublished",
            "datePosted",
            "published",
            "pubDate",
            "labeled_publication_time",
            "visible_publication_time",
        ),
    )
    if conflict == "conflicting_publication_evidence" or (conflict and not new_since_last_run):
        return MonitorParseResult(
            ok=False,
            record_type=record_type,
            external_id=external_id_hint or raw.final_url,
            quarantine_reason=conflict,
            quarantine_detail={"fields": sorted(fields.keys())},
        )
    if interval is None and not new_since_last_run:
        return MonitorParseResult(
            ok=False,
            record_type=record_type,
            external_id=external_id_hint or raw.final_url,
            quarantine_reason="missing_publication_evidence",
            quarantine_detail={"fields": sorted(fields.keys())},
        )
    pub = _publication_record(interval, new_since_last_run)

    if not body.complete or not body.text:
        return MonitorParseResult(
            ok=False,
            record_type=record_type,
            external_id=external_id_hint or raw.final_url,
            publication=interval,
            quarantine_reason="incomplete_body",
            quarantine_detail=dict(body.evidence),
            body_complete=False,
        )

    title = (
        title_hint
        or fields.get("headline")
        or fields.get("title")
        or body.evidence.get("title")
        or _title_from_html(html)
    )
    if not title:
        return MonitorParseResult(
            ok=False,
            record_type=record_type,
            external_id=external_id_hint or raw.final_url,
            publication=interval,
            quarantine_reason="missing_title",
            body_complete=True,
        )

    fresh = _is_fresh(interval, cutoff)
    article_url = raw.final_url or raw.request_url
    board = board_url or (list_fields or {}).get("board_url")
    employer = employer_url or fields.get("employer_url") or fields.get("original_url")

    if record_type == "NEWS":
        payload = {
            "schemaVersion": "1.0",
            "source": {
                "name": source_cfg.canonical_publisher,
                "url": article_url,
            },
            "content": {
                "title": title,
                "date": pub["date"],
                "date_precision": pub["precision"],
                "text": body.text,
                "article_url": article_url,
            },
            "collectedAt": observed_at.isoformat(),
            "publication": pub["publication"],
        }
        external_id = external_id_hint or _stable_id(article_url, fields.get("id"))
    else:
        company = fields.get("company") or fields.get("hiringOrganization") or "unknown"
        if company == "unknown":
            return MonitorParseResult(
                ok=False,
                record_type=record_type,
                external_id=external_id_hint or article_url,
                publication=interval,
                quarantine_reason="missing_company",
                body_complete=True,
            )
        is_remote = fields.get("is_remote")
        if is_remote is None:
            is_remote = _infer_remote(body.text, fields)
        if is_remote is None:
            return MonitorParseResult(
                ok=False,
                record_type=record_type,
                external_id=external_id_hint or article_url,
                publication=interval,
                quarantine_reason="remote_unspecified",
                quarantine_detail={"needs_review": True},
                body_complete=True,
                board_url=board,
                employer_url=employer or article_url,
            )
        role_family = fields.get("role_family") or fields.get("department") or "other"
        listing_url = article_url
        original_employer = employer or fields.get("application_url") or listing_url
        payload = {
            "schemaVersion": "1.0",
            "source": {
                "name": source_cfg.canonical_publisher,
                "url": listing_url,
            },
            "content": {
                "company": company,
                "date": pub["date"],
                "date_precision": pub["precision"],
                "is_remote": bool(is_remote),
                "role_family": role_family,
                "title": title,
                "listing_url": listing_url,
                "original_employer_job_url": original_employer,
                "description": body.text,
            },
            "collectedAt": observed_at.isoformat(),
            "publication": pub["publication"],
            "board_url": board or source_cfg.documented_access_url,
            "employer_url": original_employer,
        }
        external_id = external_id_hint or _stable_id(
            original_employer,
            fields.get("requisition_id") or fields.get("id"),
        )

    evidence = {
        "publication": pub["publication"],
        "date_extraction_method": pub["method"],
        "date_precision": pub["precision"],
        "body": {
            "start_excerpt": body.start_excerpt,
            "end_excerpt": body.end_excerpt,
            "complete": body.complete,
            "method": body.method,
            "paragraph_count": body.evidence.get("paragraph_count"),
        },
        "observation": {
            "request_url": raw.request_url,
            "final_url": raw.final_url,
            "fetched_at": observed_at.isoformat(),
            "content_sha256": raw.content_sha256,
        },
        "source_id": source_cfg.source_id,
    }
    if record_type == "JOB":
        evidence["board_url"] = board or source_cfg.documented_access_url
        evidence["employer_url"] = payload.get("employer_url")

    return MonitorParseResult(
        ok=True,
        record_type=record_type,
        external_id=external_id,
        payload=payload,
        evidence=evidence,
        publication=interval,
        eligible=fresh,
        body_complete=True,
        board_url=board,
        employer_url=employer if record_type == "JOB" else None,
    )


def _parse_structured(
    data: dict[str, Any],
    *,
    raw: RawObservation,
    source_cfg: MonitorSourceConfig,
    record_type: str,
    cutoff: datetime | None,
    observed_at: datetime,
    list_fields: dict[str, Any] | None,
    board_url: str | None,
    employer_url: str | None,
    title_hint: str | None,
    external_id_hint: str | None,
    new_since_last_run: dict[str, Any] | None = None,
) -> MonitorParseResult:
    """Fixture / JSON detail path with explicit body paragraphs and date fields."""
    row = data.get("row") if isinstance(data.get("row"), dict) else data
    fields = dict(list_fields or {})
    for key, value in row.items():
        if key in {"text", "body", "description", "paragraphs", "content"}:
            continue
        fields[key] = value
    fields = {k: v for k, v in fields.items() if not is_non_proof_field(k)}

    # Body from paragraphs or text.
    paragraphs = row.get("paragraphs")
    if isinstance(paragraphs, list) and paragraphs:
        text = "\n\n".join(str(p) for p in paragraphs)
        start_excerpt = str(paragraphs[0])[:240]
        end_excerpt = str(paragraphs[-1])[:240]
        complete = not bool(row.get("truncated") or row.get("teaser"))
        if row.get("missing_last_paragraph"):
            complete = False
    else:
        text = str(row.get("text") or row.get("body") or row.get("description") or "")
        start_excerpt = text[:240]
        end_excerpt = text[-240:] if text else ""
        complete = bool(text) and not bool(row.get("truncated") or row.get("teaser"))

    interval, conflict = select_publication_interval(
        fields,
        observed_at=observed_at,
        source_timezone=source_cfg.original_timezone,
        prefer_keys=(source_cfg.original_time_field,)
        + ("datePublished", "datePosted", "published", "pubDate"),
    )
    # Explicit republish trap: old original + fresh modified must not admit via modified.
    if row.get("dateModified") or row.get("updated") or row.get("modified"):
        if interval is None and not any(
            k in fields
            for k in (
                "datePublished",
                "datePosted",
                "published",
                source_cfg.original_time_field,
            )
        ):
            return MonitorParseResult(
                ok=False,
                record_type=record_type,
                external_id=external_id_hint or raw.final_url,
                quarantine_reason="non_proof_modified_only",
                quarantine_detail={
                    "modified": row.get("dateModified") or row.get("updated"),
                },
            )

    if conflict == "conflicting_publication_evidence" or (conflict and not new_since_last_run):
        return MonitorParseResult(
            ok=False,
            record_type=record_type,
            external_id=external_id_hint or str(row.get("id") or raw.final_url),
            quarantine_reason=conflict,
            quarantine_detail={"fields": sorted(fields.keys())},
        )
    if interval is None and not new_since_last_run:
        reason = "missing_publication_evidence"
        if row.get("first_seen") and not any(
            k in row
            for k in ("datePublished", "datePosted", "published", source_cfg.original_time_field)
        ):
            reason = "first_seen_or_non_proof_only"
        return MonitorParseResult(
            ok=False,
            record_type=record_type,
            external_id=external_id_hint or str(row.get("id") or raw.final_url),
            quarantine_reason=reason,
            quarantine_detail={"fields": sorted(fields.keys())},
        )

    if not complete or not text:
        return MonitorParseResult(
            ok=False,
            record_type=record_type,
            external_id=external_id_hint or str(row.get("id") or raw.final_url),
            publication=interval,
            quarantine_reason="incomplete_body",
            quarantine_detail={
                "teaser": bool(row.get("teaser")),
                "truncated": bool(row.get("truncated")),
                "missing_last_paragraph": bool(row.get("missing_last_paragraph")),
            },
            body_complete=False,
        )

    title = title_hint or row.get("title") or row.get("headline")
    if not title:
        return MonitorParseResult(
            ok=False,
            record_type=record_type,
            external_id=external_id_hint or raw.final_url,
            publication=interval,
            quarantine_reason="missing_title",
            body_complete=True,
        )

    pub = _publication_record(interval, new_since_last_run)
    fresh = _is_fresh(interval, cutoff)
    article_url = str(row.get("article_url") or row.get("url") or raw.final_url)
    board = board_url or row.get("board_url") or source_cfg.documented_access_url
    employer = employer_url or row.get("employer_url") or row.get("original_url")

    if record_type == "NEWS":
        payload = {
            "schemaVersion": "1.0",
            "source": {"name": source_cfg.canonical_publisher, "url": article_url},
            "content": {
                "title": title,
                "date": pub["date"],
                "date_precision": pub["precision"],
                "text": text,
                "article_url": article_url,
            },
            "collectedAt": observed_at.isoformat(),
            "publication": pub["publication"],
        }
        external_id = external_id_hint or str(row.get("id") or article_url)
    else:
        company = row.get("company") or row.get("hiringOrganization")
        if not company:
            return MonitorParseResult(
                ok=False,
                record_type=record_type,
                external_id=external_id_hint or article_url,
                publication=interval,
                quarantine_reason="missing_company",
                body_complete=True,
            )
        is_remote = row.get("is_remote")
        if is_remote is None:
            return MonitorParseResult(
                ok=False,
                record_type=record_type,
                external_id=external_id_hint or article_url,
                publication=interval,
                quarantine_reason="remote_unspecified",
                body_complete=True,
                board_url=board,
                employer_url=employer or article_url,
            )
        original_employer = employer or row.get("original_employer_job_url") or article_url
        payload = {
            "schemaVersion": "1.0",
            "source": {"name": source_cfg.canonical_publisher, "url": article_url},
            "content": {
                "company": company,
                "date": pub["date"],
                "date_precision": pub["precision"],
                "is_remote": bool(is_remote),
                "role_family": row.get("role_family") or "other",
                "title": title,
                "listing_url": article_url,
                "original_employer_job_url": original_employer,
                "description": text,
            },
            "collectedAt": observed_at.isoformat(),
            "publication": pub["publication"],
            "board_url": board,
            "employer_url": original_employer,
        }
        external_id = external_id_hint or str(
            row.get("requisition_id") or row.get("id") or original_employer
        )

    evidence = {
        "publication": pub["publication"],
        "date_extraction_method": pub["method"],
        "date_precision": pub["precision"],
        "body": {
            "start_excerpt": start_excerpt,
            "end_excerpt": end_excerpt,
            "complete": True,
            "method": "structured_fixture",
        },
        "observation": {
            "request_url": raw.request_url,
            "final_url": raw.final_url,
            "fetched_at": observed_at.isoformat(),
            "content_sha256": raw.content_sha256,
        },
        "source_id": source_cfg.source_id,
        "synthetic": bool(row.get("_synthetic", True)),
    }
    if record_type == "JOB":
        evidence["board_url"] = board
        evidence["employer_url"] = payload.get("employer_url")

    return MonitorParseResult(
        ok=True,
        record_type=record_type,
        external_id=external_id,
        payload=payload,
        evidence=evidence,
        publication=interval,
        eligible=fresh,
        body_complete=True,
        board_url=board if record_type == "JOB" else None,
        employer_url=employer if record_type == "JOB" else None,
    )


def _collect_html_fields(
    html: str,
    *,
    list_fields: dict[str, Any] | None,
    source_cfg: MonitorSourceConfig,
    page_url: str | None = None,
) -> dict[str, Any]:
    fields: dict[str, Any] = dict(list_fields or {})
    page = extract_page_dates(html)
    meta = page["meta"]  # name/property/itemprop -> content, either attribute order
    for name in ("article:published_time", "og:article:published_time", "datepublished"):
        if meta.get(name):
            fields.setdefault("datePublished", meta[name])
            break
    if meta.get("dateposted"):
        fields.setdefault("datePosted", meta["dateposted"])
    for name in ("article:modified_time", "og:updated_time"):
        if meta.get(name):
            fields.setdefault("dateModified", meta[name])  # non-proof; kept for conflict checks only
            break
    if meta.get("og:title"):
        fields.setdefault("title", meta["og:title"])
    if page["itemprops"].get("datepublished"):
        fields.setdefault("datePublished", page["itemprops"]["datepublished"])
    if page["itemprops"].get("dateposted"):
        fields.setdefault("datePosted", page["itemprops"]["dateposted"])
    # <time datetime>: only publication-semantic or inside article/main (never sidebar/nav).
    times = page["times"]
    chosen = next((t for t in times if t["semantic"]), None) or next(
        (t for t in times if t["in_region"]), None
    )
    if chosen is not None:
        fields.setdefault("labeled_publication_time", chosen["datetime"])

    board_host = _host(page_url) or _host(source_cfg.documented_access_url)
    for block in extract_json_ld_blocks(html):
        types = block.get("@type")
        type_list = types if isinstance(types, list) else [types]
        type_names = {str(t).lower() for t in type_list if t}
        if any(n.endswith("article") or n in {"blogposting", "liveblogposting"} for n in type_names):
            if block.get("datePublished"):
                fields.setdefault("datePublished", block["datePublished"])
            if block.get("headline"):
                fields.setdefault("title", block["headline"])
        if "jobposting" in type_names:
            if block.get("datePosted"):
                fields.setdefault("datePosted", block["datePosted"])
            if block.get("title"):
                fields.setdefault("title", block["title"])
            org = block.get("hiringOrganization")
            if isinstance(org, dict) and org.get("name"):
                fields.setdefault("company", org["name"])
            elif isinstance(org, str):
                fields.setdefault("company", org)
            # JobPosting.url is usually the board's own listing; accept it only off-board.
            job_url = block.get("url")
            if isinstance(job_url, str) and _host(job_url) and not _same_site(
                _host(job_url), board_host, source_cfg.domain_governor
            ):
                fields.setdefault("employer_url", job_url)
            elif isinstance(org, dict):
                org_url = org.get("sameAs") or org.get("url")
                if isinstance(org_url, list):
                    org_url = next((u for u in org_url if isinstance(u, str)), None)
                if isinstance(org_url, str) and org_url:
                    fields.setdefault("employer_url", org_url)
            if block.get("jobLocationType"):
                fields.setdefault("jobLocationType", block["jobLocationType"])
            location = _location_text(block.get("jobLocation"))
            if location:
                fields.setdefault("location", location)

    # Source-native field alias from config
    native = source_cfg.original_time_field
    if native not in fields:
        for key in list(fields):
            if key.lower().replace(" ", "_") == native.lower():
                fields[native] = fields[key]
                break
    return fields


def _anchor_list_fields(
    list_fields: dict[str, Any] | None,
    *,
    list_observed_at: datetime | str | None,
    source_timezone: str,
) -> dict[str, Any] | None:
    """Resolve relative listing dates at the listing's fetch time; drop them if unknown."""
    if not list_fields:
        return list_fields
    anchor = list_observed_at
    if isinstance(anchor, str):
        try:
            anchor = datetime.fromisoformat(anchor)
        except ValueError:
            anchor = None
    if isinstance(anchor, datetime) and anchor.tzinfo is None:
        anchor = None
    out: dict[str, Any] = {}
    for key, value in list_fields.items():
        if is_relative_text(value):
            interval = (
                parse_publication_value(
                    value,
                    field_name=key,
                    observed_at=anchor,
                    source_timezone=source_timezone,
                )
                if anchor is not None
                else None
            )
            if interval is not None:
                out[key] = interval
            continue  # never re-anchor a listing's relative date to the detail fetch time
        out[key] = value
    return out


def _publication_record(
    interval: PublicationInterval | None,
    new_since_last_run: dict[str, Any] | None,
) -> dict[str, Any]:
    """content.date + precision; heuristic rows carry no date (observed-only)."""
    if interval is not None:
        return {
            "date": interval.lower.isoformat(),
            "precision": interval.precision,
            "method": interval.method,
            "publication": interval.to_dict(),
        }
    heuristic = dict(new_since_last_run or {})
    return {
        "date": None,
        "precision": "heuristic",
        "method": "new_since_last_run",
        "publication": {
            "lower": None,
            "upper": None,
            "precision": "heuristic",
            "basis": "new_since_last_run",
            "observed_only": True,
            "evidence": heuristic,
        },
    }


def _is_fresh(interval: PublicationInterval | None, cutoff: datetime | None) -> bool:
    # No cutoff or no date evidence never proves freshness.
    if interval is None or cutoff is None:
        return False
    return in_freshness_window(interval, cutoff)


def _host(url: str | None) -> str | None:
    if not url:
        return None
    host = urlparse(url).hostname
    return host.lower() if host else None


def _same_site(host: str | None, board_host: str | None, governor: str | None) -> bool:
    if host is None:
        return False
    for site in (board_host, (governor or "").lower() or None):
        if site and (host == site or host.endswith("." + site.removeprefix("www."))):
            return True
    return False


def _location_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, list):
        parts = [p for p in (_location_text(v) for v in value) if p]
        return "; ".join(parts) or None
    if isinstance(value, dict):
        address = value.get("address")
        if isinstance(address, dict):
            parts = [
                str(address.get(k))
                for k in ("addressLocality", "addressRegion", "addressCountry")
                if address.get(k)
            ]
            if parts:
                return ", ".join(parts)
        elif isinstance(address, str) and address.strip():
            return address.strip()
        name = value.get("name")
        return str(name).strip() if name else None
    return None


def _maybe_json(body: bytes, ctype: str) -> dict[str, Any] | None:
    if not body:
        return None
    if "json" not in ctype:
        sample = body.lstrip()[:1]
        if sample not in (b"{", b"["):
            return None
    try:
        data = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if isinstance(data, dict):
        return data
    return None


def _title_from_html(html: str) -> str | None:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", m.group(1))).strip() or None


def _infer_remote(text: str, fields: dict[str, Any]) -> bool | None:
    if "is_remote" in fields:
        return bool(fields["is_remote"])
    loc_type = fields.get("jobLocationType")
    loc_types = loc_type if isinstance(loc_type, list) else [loc_type]
    if any("telecommute" in str(v).lower() for v in loc_types if v):
        return True
    location = fields.get("location")
    if (
        isinstance(location, str)
        and re.match(r"^\s*(fully\s+)?remote\b", location, re.IGNORECASE)
        and not re.search(r"hybrid|on[- ]?site|in[- ]office", location, re.IGNORECASE)
    ):
        return True
    lower = text.lower()
    if re.search(r"\b(fully remote|remote[- ]friendly|work from home)\b", lower):
        return True
    if re.search(r"\b(on[- ]site only|onsite only|hybrid only)\b", lower):
        return False
    return None


def _stable_id(url: str, alt: Any) -> str:
    if alt:
        return str(alt)
    return url
