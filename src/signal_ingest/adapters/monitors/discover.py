"""Feed/list discovery with ordered vs unordered stop rules."""

from __future__ import annotations

import calendar
import json
from datetime import datetime, timezone
from typing import Any, Sequence
from urllib.parse import urljoin

import feedparser

from signal_ingest.adapters.monitors.body import extract_list_links
from signal_ingest.adapters.monitors.types import DiscoverScanResult, MonitorSourceConfig
from signal_ingest.crawl.types import DiscoveredItem
from signal_ingest.freshness import (
    PublicationInterval,
    in_freshness_window,
    parse_publication_value,
    select_publication_interval,
)


def discover_list(
    source_cfg: MonitorSourceConfig,
    raw: bytes | str | dict[str, Any] | Sequence[dict[str, Any]],
    *,
    checkpoint: str | None = None,
    cutoff: datetime | None = None,
    observed_at: datetime | None = None,
    base_url: str | None = None,
    feed_ordered: bool | None = None,
) -> DiscoverScanResult:
    """
    Discover detail URLs from an RSS/Atom feed, HTML list, or fixture JSON.

    Unordered feeds: never early-stop on an old row.
    Ordered feeds: prove descending publication order before cutoff stop.
    """
    payload = _coerce_payload(raw)
    base = base_url or source_cfg.documented_access_url

    if isinstance(payload, dict) and (
        "items" in payload or payload.get("format") in {"fixture_list", "json_list"}
    ):
        return _discover_from_json(
            source_cfg,
            payload,
            checkpoint=checkpoint,
            cutoff=cutoff,
            observed_at=observed_at,
            base_url=base,
            feed_ordered=feed_ordered,
        )

    if isinstance(payload, list):
        return _discover_from_json(
            source_cfg,
            {"items": payload, "ordered": bool(feed_ordered)},
            checkpoint=checkpoint,
            cutoff=cutoff,
            observed_at=observed_at,
            base_url=base,
            feed_ordered=feed_ordered,
        )

    assert isinstance(payload, (bytes, str))
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
    sample = text.lstrip()[:200].lower()
    if "<rss" in sample or "<feed" in sample or "<rdf" in sample or "xmlns:atom" in sample:
        return _discover_from_feed(
            source_cfg,
            text,
            checkpoint=checkpoint,
            cutoff=cutoff,
            observed_at=observed_at,
            base_url=base,
            feed_ordered=feed_ordered if feed_ordered is not None else True,
        )
    return _discover_from_html(
        source_cfg,
        text,
        checkpoint=checkpoint,
        cutoff=cutoff,
        observed_at=observed_at,
        base_url=base,
        feed_ordered=False if feed_ordered is None else feed_ordered,
    )


def prove_descending(intervals: Sequence[PublicationInterval | None]) -> bool:
    """True when consecutive non-null intervals are non-increasing by upper bound."""
    prev: datetime | None = None
    seen = 0
    for interval in intervals:
        if interval is None:
            continue
        seen += 1
        if prev is not None and interval.upper > prev:
            return False
        prev = interval.upper
    return seen >= 2


def _discover_from_json(
    source_cfg: MonitorSourceConfig,
    payload: dict[str, Any],
    *,
    checkpoint: str | None,
    cutoff: datetime | None,
    observed_at: datetime | None,
    base_url: str,
    feed_ordered: bool | None,
) -> DiscoverScanResult:
    rows = list(payload.get("items") or [])
    ordered = (
        bool(feed_ordered)
        if feed_ordered is not None
        else bool(payload.get("ordered", False))
    )
    capped = bool(payload.get("capped_feed", False))
    return _scan_rows(
        source_cfg,
        rows,
        ordered=ordered,
        capped_feed=capped,
        checkpoint=checkpoint,
        cutoff=cutoff,
        observed_at=observed_at,
        base_url=base_url,
        limitation=payload.get("limitation"),
        metadata={"format": "json_list", "synthetic": bool(payload.get("_synthetic", False))},
    )


def _discover_from_feed(
    source_cfg: MonitorSourceConfig,
    text: str,
    *,
    checkpoint: str | None,
    cutoff: datetime | None,
    observed_at: datetime | None,
    base_url: str,
    feed_ordered: bool,
) -> DiscoverScanResult:
    parsed = feedparser.parse(text)
    rows: list[dict[str, Any]] = []
    for entry in parsed.entries:
        link = getattr(entry, "link", None) or ""
        external_id = (
            getattr(entry, "id", None)
            or getattr(entry, "guid", None)
            or link
        )
        published = None
        if getattr(entry, "published", None):
            published = entry.published
        elif getattr(entry, "updated", None):
            # Feed updated is not original proof — keep for metadata only.
            published = None
        fields: dict[str, Any] = {}
        if published:
            fields["published"] = published
        # structured_time from feedparser
        if getattr(entry, "published_parsed", None):
            try:
                # feedparser struct_time is UTC; mktime would read it as machine-local.
                ts = calendar.timegm(entry.published_parsed)
                fields["datePublished"] = datetime.fromtimestamp(ts, tz=timezone.utc)
            except (OverflowError, ValueError, TypeError, OSError):
                pass
        rows.append(
            {
                "url": link,
                "external_id": str(external_id) if external_id else link,
                "title": getattr(entry, "title", None),
                "fields": fields,
                "summary": getattr(entry, "summary", None),
            }
        )
    page_size = source_cfg.raw.get("feed_page_size")
    # shortcut: without a configured feed_page_size, assume the common 50-item cap.
    capped = len(rows) >= int(page_size or 50)
    return _scan_rows(
        source_cfg,
        rows,
        ordered=feed_ordered,
        capped_feed=capped,
        checkpoint=checkpoint,
        cutoff=cutoff,
        observed_at=observed_at,
        base_url=base_url,
        metadata={
            "format": "rss_atom",
            "feed_title": getattr(parsed.feed, "title", None),
            "capped_basis": "configured_page_size" if page_size else "default_50_unconfigured",
        },
    )


def _discover_from_html(
    source_cfg: MonitorSourceConfig,
    text: str,
    *,
    checkpoint: str | None,
    cutoff: datetime | None,
    observed_at: datetime | None,
    base_url: str,
    feed_ordered: bool,
) -> DiscoverScanResult:
    links = extract_list_links(text)
    rows = [
        {
            "url": item["url"],
            "external_id": item["url"],
            "title": item.get("title"),
            "fields": {},
        }
        for item in links
    ]
    return _scan_rows(
        source_cfg,
        rows,
        ordered=feed_ordered,
        capped_feed=False,
        checkpoint=checkpoint,
        cutoff=cutoff,
        observed_at=observed_at,
        base_url=base_url,
        metadata={"format": "html_list"},
    )


def _scan_rows(
    source_cfg: MonitorSourceConfig,
    rows: Sequence[dict[str, Any]],
    *,
    ordered: bool,
    capped_feed: bool,
    checkpoint: str | None,
    cutoff: datetime | None,
    observed_at: datetime | None,
    base_url: str,
    limitation: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> DiscoverScanResult:
    watermark = decode_watermark(checkpoint)

    intervals: list[PublicationInterval | None] = []
    # Pre-pass intervals for order proof when ordered.
    for row in rows:
        intervals.append(
            _row_interval(row, source_cfg=source_cfg, observed_at=observed_at)
        )
    descending_proven = prove_descending(intervals) if ordered else False

    # Newest row seen this scan becomes the next run's watermark (feeds prepend new items).
    new_cursor = checkpoint
    for idx, row in enumerate(rows):
        if row.get("url"):
            new_cursor = encode_watermark(
                external_id=str(row.get("external_id") or urljoin(base_url, str(row["url"]))),
                url=urljoin(base_url, str(row["url"])),
                interval=intervals[idx],
            )
            break

    items: list[DiscoveredItem] = []
    early_stopped = False
    watermark_reached = False
    scanned = 0
    wm_published = _parse_iso(watermark.get("published")) if watermark else None
    for idx, row in enumerate(rows):
        interval = intervals[idx]
        url = str(row.get("url") or "")
        if not url:
            scanned += 1
            continue
        absolute = urljoin(base_url, url)
        external_id = str(row.get("external_id") or absolute)
        if watermark and (external_id == watermark.get("id") or absolute == watermark.get("url")):
            watermark_reached = True  # everything below was seen by a previous run
            break
        if (
            watermark
            and ordered
            and descending_proven
            and wm_published is not None
            and interval is not None
            and interval.upper < wm_published
        ):
            watermark_reached = True  # watermark row vanished; older rows were already seen
            break
        scanned += 1

        # Ordered early cutoff only after descending proof.
        if (
            ordered
            and descending_proven
            and cutoff is not None
            and interval is not None
            and not in_freshness_window(interval, cutoff)
            and interval.upper < freshness_lower(cutoff)
        ):
            early_stopped = True
            break

        # Unordered: never stop for old rows; still enqueue for detail corroboration.
        items.append(
            DiscoveredItem(
                url=absolute,
                external_id=external_id,
                cursor=new_cursor,
                metadata={
                    "adapter": source_cfg.source_id,
                    "title": row.get("title"),
                    "list_fields": dict(row.get("fields") or {}),
                    "list_observed_at": observed_at.isoformat() if observed_at else None,
                    "publication": interval.to_dict() if interval else None,
                    "board_url": base_url,
                    "employer_url": row.get("employer_url") or row.get("original_url"),
                    "row": row,
                    "synthetic": bool(row.get("_synthetic", metadata and metadata.get("synthetic"))),
                },
            )
        )

    if watermark_reached:
        # Heuristic for dateless rows: above the previous run's watermark = new since last run.
        for position, item in enumerate(items):
            item.metadata["new_since_last_run"] = {
                "precision": "heuristic",
                "basis": "new_since_last_run",
                "watermark": watermark,
                "position_above_watermark": position,
                "observed_at": observed_at.isoformat() if observed_at else None,
            }

    lim = limitation
    if capped_feed and not early_stopped and not watermark_reached:
        lim = lim or "capped_feed_no_overlap_proof"
    if watermark and not watermark_reached and not early_stopped:
        lim = lim or "watermark_not_found_gap_possible"
    return DiscoverScanResult(
        items=items,
        cursor=new_cursor,
        ordered=ordered,
        descending_proven=descending_proven,
        early_stopped=early_stopped,
        capped_feed=capped_feed,
        scanned_count=scanned,
        limitation=lim,
        metadata={**(metadata or {}), "watermark_reached": watermark_reached},
    )


_WATERMARK_PREFIX = "wm:"


def encode_watermark(
    *,
    external_id: str,
    url: str,
    interval: PublicationInterval | None,
) -> str:
    """Opaque resume cursor: newest seen GUID/URL plus its publication upper bound."""
    return _WATERMARK_PREFIX + json.dumps(
        {
            "id": external_id,
            "url": url,
            "published": interval.upper.isoformat() if interval else None,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def decode_watermark(checkpoint: str | None) -> dict[str, Any] | None:
    """Legacy `idx:N` cursors are ignored (full rescan; dedupe is by semantic key)."""
    if not checkpoint or not checkpoint.startswith(_WATERMARK_PREFIX):
        return None
    try:
        data = json.loads(checkpoint[len(_WATERMARK_PREFIX):])
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return dt if dt.tzinfo else None


def freshness_lower(cutoff: datetime) -> datetime:
    from signal_ingest.freshness.window import freshness_bounds

    lower, _ = freshness_bounds(cutoff)
    return lower


def _row_interval(
    row: dict[str, Any],
    *,
    source_cfg: MonitorSourceConfig,
    observed_at: datetime | None,
) -> PublicationInterval | None:
    fields = dict(row.get("fields") or {})
    for key in (
        "datePublished",
        "datePosted",
        "published",
        source_cfg.original_time_field,
        "publication",
    ):
        if key in row and key not in fields:
            fields[key] = row[key]
    # Never lift modified/updated into proof.
    for bad in ("updated", "modified", "dateModified", "lastmod", "first_seen"):
        fields.pop(bad, None)
    interval, _reason = select_publication_interval(
        fields,
        observed_at=observed_at,
        source_timezone=source_cfg.original_timezone,
        prefer_keys=(source_cfg.original_time_field,)
        + (
            "datePublished",
            "datePosted",
            "published",
            "pubDate",
        ),
    )
    if interval is not None:
        return interval
    # Single scalar
    for key in ("published", "date", source_cfg.original_time_field):
        if key in row:
            return parse_publication_value(
                row[key],
                field_name=key,
                observed_at=observed_at,
                source_timezone=source_cfg.original_timezone,
            )
    return None


def _coerce_payload(
    raw: bytes | str | dict[str, Any] | Sequence[dict[str, Any]],
) -> bytes | str | dict[str, Any] | list[dict[str, Any]]:
    if isinstance(raw, (dict, list)):
        return raw  # type: ignore[return-value]
    if isinstance(raw, (bytes, str)):
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        stripped = text.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                data = json.loads(text)
                return data
            except json.JSONDecodeError:
                pass
        return raw
    raise TypeError(f"unsupported discover payload type: {type(raw)!r}")
