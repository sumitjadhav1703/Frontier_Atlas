"""Extract PublicationInterval from structured/visible fields; reject non-proof."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from signal_ingest.freshness.interval import PublicationInterval

# Never treat these as original publication proof.
NON_PROOF_KEYS = frozenset(
    {
        "updated",
        "updated_at",
        "updatedAt",
        "lastmod",
        "last_mod",
        "lastModified",
        "last_modified",
        "dateModified",
        "datemodified",
        "modified",
        "modification_date",
        "collectedAt",
        "collected_at",
        "cache",
        "cache_time",
        "cached_at",
        "first_seen",
        "firstSeen",
        "first_seen_at",
        "new_badge",
        "is_new",
    }
)

# Preference order for original publication evidence.
PROOF_KEYS = (
    "datePublished",
    "date_published",
    "datePosted",
    "date_posted",
    "original_publication",
    "original_publication_time",
    "original_publication_date",
    "original_timestamp",
    "original_posting_date",
    "original_posting_time",
    "article_publication_time",
    "published",
    "published_at",
    "publishedAt",
    "pubDate",
    "publication_date",
    "publication_time",
    "labeled_publication_time",
    "visible_publication_time",
)

_RELATIVE_HOURS = re.compile(
    r"^\s*(\d+)\s*hours?\s+ago\s*$",
    re.IGNORECASE,
)
_RELATIVE_MINUTES = re.compile(
    r"^\s*(\d+)\s*minutes?\s+ago\s*$",
    re.IGNORECASE,
)
_YESTERDAY = re.compile(r"^\s*yesterday\s*$", re.IGNORECASE)
_TODAY = re.compile(r"^\s*today\s*$", re.IGNORECASE)
_DATE_ONLY = re.compile(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$")
# Visible labels before a date. "Updated/Modified" labels are non-proof and rejected.
_PROOF_PREFIX = re.compile(r"^\s*(?:posted|published|first\s+published)(?:\s+on)?\s*:?\s*", re.IGNORECASE)
_NON_PROOF_PREFIX = re.compile(r"^\s*(?:last\s+)?(?:updated|modified|edited|refreshed)\b", re.IGNORECASE)
_EPOCH = re.compile(r"^\s*(\d{10}|\d{13})\s*$")
_JUST_NOW = re.compile(r"^\s*(?:just\s+now|moments?\s+ago|now)\s*$", re.IGNORECASE)
# Count + unit relative forms; the unit is the display precision.
_RELATIVE_UNIT = re.compile(
    r"^\s*(\d+|an?|one)\s*(s|secs?|seconds?|m|mins?|minutes?|h|hrs?|hours?|d|days?|w|wks?|weeks?)\b\s*(?:ago)?\s*$",
    re.IGNORECASE,
)
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def is_non_proof_field(name: str | None) -> bool:
    if not name:
        return False
    return name in NON_PROOF_KEYS or name.lower() in {k.lower() for k in NON_PROOF_KEYS}


def relative_hour_floor_interval(
    hours: int,
    observed_at: datetime,
    *,
    raw: str | None = None,
    evidence: dict[str, Any] | None = None,
) -> PublicationInterval:
    """
    Source floors relative hours at observation time F.

    \"2 hours ago\" → conservative [F−3h, F−2h].
    """
    if hours < 0:
        raise ValueError("hours must be nonnegative")
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    f = observed_at.astimezone(timezone.utc)
    # n hours ago (floored) means age ∈ [n, n+1) hours → [F-(n+1)h, F-n h]
    lower = f - timedelta(hours=hours + 1)
    upper = f - timedelta(hours=hours)
    return PublicationInterval(
        lower=lower,
        upper=upper,
        precision="relative_hour_floor",
        method="relative_hour_floor",
        raw=raw or f"{hours} hours ago",
        tz="UTC",
        evidence=evidence or {"observed_at": f.isoformat(), "hours": hours},
    )


def parse_publication_value(
    value: Any,
    *,
    field_name: str | None = None,
    observed_at: datetime | None = None,
    source_timezone: str | None = None,
    evidence: dict[str, Any] | None = None,
) -> PublicationInterval | None:
    """
    Map one candidate value to an interval.

    Returns None when the field is non-proof or the value is unusable.
    Conflicting multi-field selection is handled by `select_publication_interval`.
    """
    if field_name and is_non_proof_field(field_name):
        return None
    if value is None or value == "":
        return None

    base_evidence = dict(evidence or {})
    if field_name:
        base_evidence["field"] = field_name

    if isinstance(value, datetime):
        naive = value.tzinfo is None
        if naive:
            value = value.replace(tzinfo=_zone(source_timezone or "UTC"))
        instant = _instant_interval(
            value,
            method=field_name or "datetime",
            raw=value.isoformat(),
            evidence=base_evidence,
        )
        if naive and source_timezone is None:
            return _widen_unknown_zone(instant, cap=observed_at)
        return instant
    if isinstance(value, date) and not isinstance(value, datetime):
        day = date_only_interval(
            value,
            source_timezone=source_timezone or "UTC",
            method=field_name or "date_only",
            evidence=base_evidence,
        )
        return _widen_unknown_zone(day, cap=observed_at) if source_timezone is None else day

    if isinstance(value, PublicationInterval):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        value = str(int(value))

    text = str(value).strip()
    if not text:
        return None
    if _NON_PROOF_PREFIX.match(text):
        return None
    text = _PROOF_PREFIX.sub("", text, count=1).strip()
    if not text:
        return None

    m_epoch = _EPOCH.match(text)
    if m_epoch:
        digits = m_epoch.group(1)
        seconds = int(digits) / 1000 if len(digits) == 13 else int(digits)
        return _instant_interval(
            datetime.fromtimestamp(seconds, tz=timezone.utc),
            method=field_name or "epoch",
            raw=text,
            evidence={**base_evidence, "epoch_unit": "ms" if len(digits) == 13 else "s"},
        )

    # Relative forms need observation time F.
    m_h = _RELATIVE_HOURS.match(text)
    if m_h:
        if observed_at is None:
            return None
        return relative_hour_floor_interval(
            int(m_h.group(1)),
            observed_at,
            raw=text,
            evidence=base_evidence,
        )
    m_m = _RELATIVE_MINUTES.match(text)
    if m_m:
        if observed_at is None:
            return None
        return relative_minute_interval(
            int(m_m.group(1)),
            observed_at,
            raw=text,
            evidence=base_evidence,
        )
    if _YESTERDAY.match(text):
        if observed_at is None:
            return None
        day = yesterday_interval(
            observed_at,
            source_timezone=source_timezone or "UTC",
            raw=text,
            evidence=base_evidence,
        )
        return _widen_unknown_zone(day, cap=observed_at) if source_timezone is None else day
    if _TODAY.match(text):
        if observed_at is None:
            return None
        day = today_interval(
            observed_at,
            source_timezone=source_timezone or "UTC",
            raw=text,
            evidence=base_evidence,
        )
        return _widen_unknown_zone(day, cap=observed_at) if source_timezone is None else day

    m_d = _DATE_ONLY.match(text)
    if m_d:
        d = date(int(m_d.group(1)), int(m_d.group(2)), int(m_d.group(3)))
        day = date_only_interval(
            d,
            source_timezone=source_timezone or "UTC",
            method=field_name or "date_only",
            raw=text,
            evidence=base_evidence,
        )
        return _widen_unknown_zone(day, cap=observed_at) if source_timezone is None else day

    relative = _relative_interval(text, observed_at, evidence=base_evidence)
    if relative is not False:
        return relative  # interval, or None when relative but unanchored/unknown unit

    instant = _parse_instant(text, source_timezone=source_timezone)
    if instant is None:
        natural = _natural_interval(
            text,
            observed_at=observed_at,
            source_timezone=source_timezone or "UTC",
            method=field_name or "natural_date",
            evidence=base_evidence,
        )
        return _widen_unknown_zone(natural, cap=observed_at) if source_timezone is None else natural
    interval = _instant_interval(
        instant,
        method=field_name or "parsed_instant",
        raw=text,
        evidence=base_evidence,
        tz=source_timezone,
    )
    if source_timezone is None and not _has_explicit_zone(text):
        return _widen_unknown_zone(interval, cap=observed_at)
    return interval


def _has_explicit_zone(text: str) -> bool:
    try:
        parsed = datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
        return parsed.tzinfo is not None
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(text.strip())
    except (TypeError, ValueError, IndexError):
        return False
    return parsed.tzinfo is not None or re.search(r"-0000\s*$", text.strip()) is not None


def _widen_unknown_zone(
    interval: PublicationInterval | None, *, cap: datetime | None = None
) -> PublicationInterval | None:
    """Wall-clock time/day with no zone (none in the text, none configured): the
    real instant can be anywhere from UTC-12 to UTC+14 of the value read as UTC.
    Nothing is published after it was observed, so `cap` bounds the upper end."""
    if interval is None:
        return None
    upper = interval.upper + timedelta(hours=12)
    if cap is not None:
        upper = min(upper, cap.astimezone(timezone.utc))
    return PublicationInterval(
        lower=interval.lower - timedelta(hours=14),
        upper=max(upper, interval.lower - timedelta(hours=14)),
        precision="unknown_zone",
        method=interval.method,
        raw=interval.raw,
        tz=None,
        evidence={**(interval.evidence or {}), "zone": "unknown"},
    )


def select_publication_interval(
    fields: dict[str, Any],
    *,
    observed_at: datetime | None = None,
    source_timezone: str | None = None,
    prefer_keys: tuple[str, ...] | None = None,
) -> tuple[PublicationInterval | None, str | None]:
    """
    Prefer original publication fields; never use updated/lastmod/first_seen.

    Returns (interval, quarantine_reason). Quarantine on conflicting primary proofs.
    """
    order = prefer_keys or PROOF_KEYS
    chosen: list[tuple[str, PublicationInterval]] = []
    for key in order:
        if key not in fields:
            continue
        if is_non_proof_field(key):
            continue
        interval = parse_publication_value(
            fields[key],
            field_name=key,
            observed_at=observed_at,
            source_timezone=source_timezone,
        )
        if interval is not None:
            chosen.append((key, interval))

    # Also scan any explicitly labeled proof-like keys present but not in order.
    for key, value in fields.items():
        if key in order or is_non_proof_field(key):
            continue
        lower = key.lower()
        if lower in {"first_seen", "firstseen"}:
            continue
        if any(
            token in lower
            for token in (
                "datepublished",
                "dateposted",
                "published",
                "pubdate",
                "original",
            )
        ) and not any(
            token in lower for token in ("updated", "modified", "lastmod", "first_seen")
        ):
            interval = parse_publication_value(
                value,
                field_name=key,
                observed_at=observed_at,
                source_timezone=source_timezone,
            )
            if interval is not None and all(k != key for k, _ in chosen):
                chosen.append((key, interval))

    if not chosen:
        # first_seen alone → not proven publication
        if any(is_non_proof_field(k) or k.lower() in {"first_seen", "firstseen"} for k in fields):
            return None, "first_seen_or_non_proof_only"
        return None, "missing_publication_evidence"

    # Conflict: any two publication proofs that disagree (beyond 1s) quarantine the
    # row. Picking the first key would let one fresh-looking field win silently.
    primary = chosen[0][1]
    for _key, other in chosen[1:]:
        if (
            abs((other.lower - primary.lower).total_seconds()) > 1
            or abs((other.upper - primary.upper).total_seconds()) > 1
        ):
            return None, "conflicting_publication_evidence"
    return primary, None


def date_only_interval(
    day: date,
    *,
    source_timezone: str,
    method: str = "date_only",
    raw: str | None = None,
    evidence: dict[str, Any] | None = None,
) -> PublicationInterval:
    """Full local calendar day in the documented source timezone."""
    tz = _zone(source_timezone)
    start = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
    end = datetime(day.year, day.month, day.day, 23, 59, 59, 999999, tzinfo=tz)
    return PublicationInterval(
        lower=start.astimezone(timezone.utc),
        upper=end.astimezone(timezone.utc),
        precision="day",
        method=method,
        raw=raw or day.isoformat(),
        tz=source_timezone,
        evidence=evidence or {},
    )


def yesterday_interval(
    observed_at: datetime,
    *,
    source_timezone: str,
    raw: str | None = None,
    evidence: dict[str, Any] | None = None,
) -> PublicationInterval:
    tz = _zone(source_timezone)
    local = observed_at.astimezone(tz)
    day = (local.date() - timedelta(days=1))
    return date_only_interval(
        day,
        source_timezone=source_timezone,
        method="yesterday",
        raw=raw or "yesterday",
        evidence={**(evidence or {}), "observed_at": observed_at.astimezone(timezone.utc).isoformat()},
    )


def today_interval(
    observed_at: datetime,
    *,
    source_timezone: str,
    raw: str | None = None,
    evidence: dict[str, Any] | None = None,
) -> PublicationInterval:
    tz = _zone(source_timezone)
    local = observed_at.astimezone(tz)
    # Bound by observation if publication by F is established.
    start = datetime(local.year, local.month, local.day, 0, 0, 0, tzinfo=tz)
    end = local
    return PublicationInterval(
        lower=start.astimezone(timezone.utc),
        upper=end.astimezone(timezone.utc),
        precision="day_partial",
        method="today",
        raw=raw or "today",
        tz=source_timezone,
        evidence={**(evidence or {}), "observed_at": observed_at.astimezone(timezone.utc).isoformat()},
    )


def relative_minute_interval(
    minutes: int,
    observed_at: datetime,
    *,
    raw: str | None = None,
    evidence: dict[str, Any] | None = None,
) -> PublicationInterval:
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    f = observed_at.astimezone(timezone.utc)
    # Floor minutes similarly: n minutes ago → [F-(n+1)m, F-n m]
    lower = f - timedelta(minutes=minutes + 1)
    upper = f - timedelta(minutes=minutes)
    return PublicationInterval(
        lower=lower,
        upper=upper,
        precision="relative_minute_floor",
        method="relative_minute_floor",
        raw=raw or f"{minutes} minutes ago",
        tz="UTC",
        evidence=evidence or {"observed_at": f.isoformat(), "minutes": minutes},
    )


def _instant_interval(
    value: datetime,
    *,
    method: str,
    raw: str | None,
    evidence: dict[str, Any],
    tz: str | None = None,
) -> PublicationInterval:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    instant = value.astimezone(timezone.utc)
    return PublicationInterval(
        lower=instant,
        upper=instant,
        precision="instant",
        method=method,
        raw=raw,
        tz=tz or "UTC",
        evidence=evidence,
    )


def _parse_instant(text: str, *, source_timezone: str | None) -> datetime | None:
    cleaned = text.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        dt = None
    if dt is None:
        # RFC 822 / feed dates: locale-independent and honours GMT/offset zones.
        try:
            dt = parsedate_to_datetime(cleaned)
        except (TypeError, ValueError, IndexError):
            return None
        if dt.tzinfo is None and re.search(r"-0000\s*$", cleaned):
            dt = dt.replace(tzinfo=timezone.utc)  # RFC 5322: -0000 is UTC, local zone unknown
    if dt.tzinfo is None:
        tz = _zone(source_timezone or "UTC")
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(timezone.utc)


def is_relative_text(value: Any) -> bool:
    """True when the value is a relative form that needs an observation anchor."""
    if not isinstance(value, str):
        return False
    text = _PROOF_PREFIX.sub("", value.strip(), count=1).strip()
    lower = text.lower()
    return bool(
        _RELATIVE_HOURS.match(text)
        or _RELATIVE_MINUTES.match(text)
        or _RELATIVE_UNIT.match(text)
        or _JUST_NOW.match(text)
        or _YESTERDAY.match(text)
        or _TODAY.match(text)
        or lower.endswith(" ago")
    )


def _dateparser_settings(base: datetime, source_timezone: str | None = None) -> dict[str, Any]:
    settings: dict[str, Any] = {
        "RELATIVE_BASE": base,
        "RETURN_AS_TIMEZONE_AWARE": True,
        "TO_TIMEZONE": "UTC",
        "PREFER_DATES_FROM": "past",
        "RETURN_TIME_AS_PERIOD": True,
    }
    if source_timezone:
        settings["TIMEZONE"] = source_timezone  # never the machine-local zone
    return settings


def _relative_interval(
    text: str,
    observed_at: datetime | None,
    *,
    evidence: dict[str, Any],
) -> PublicationInterval | None | bool:
    """
    Relative count+unit forms ("an hour ago", "1h", "2 days ago", "just now").

    Returns False when the text is not relative. Otherwise an interval one display
    unit wide ([F-(n+1)u, F-nu]), or None when unanchored / unparseable.
    """
    m = _RELATIVE_UNIT.match(text)
    just_now = _JUST_NOW.match(text)
    if not m and not just_now and not text.lower().endswith(" ago"):
        return False
    if observed_at is None or observed_at.tzinfo is None:
        return None
    f = observed_at.astimezone(timezone.utc)
    if just_now:
        # shortcut: "just now" assumed within the last hour; quarantine-free but conservative.
        return PublicationInterval(
            lower=f - timedelta(hours=1),
            upper=f,
            precision="relative_just_now",
            method="relative_just_now",
            raw=text,
            tz="UTC",
            evidence={**evidence, "observed_at": f.isoformat()},
        )
    if not m:
        return None  # "... ago" with a unit we do not bound (months/years) -> no proof
    import dateparser

    count_text = m.group(1).lower()
    count = 1 if count_text in {"a", "an", "one"} else int(count_text)
    unit_key = m.group(2).lower()[0]
    unit = timedelta(seconds=_UNIT_SECONDS[unit_key])
    normalized = f"{count} {_UNIT_NAMES[unit_key]} ago"
    anchor = dateparser.parse(normalized, settings=_dateparser_settings(f))
    if anchor is None:
        return None
    upper = anchor.astimezone(timezone.utc)
    return PublicationInterval(
        lower=upper - unit,
        upper=upper,
        precision=f"relative_{_UNIT_NAMES[unit_key].rstrip('s')}_floor",
        method="relative_dateparser",
        raw=text,
        tz="UTC",
        evidence={**evidence, "observed_at": f.isoformat(), "count": count, "unit": _UNIT_NAMES[unit_key]},
    )


_UNIT_NAMES = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}


def _natural_interval(
    text: str,
    *,
    observed_at: datetime | None,
    source_timezone: str,
    method: str,
    evidence: dict[str, Any],
) -> PublicationInterval | None:
    """Natural absolute forms ("Nov 20, 2026", "November 20, 2026 10:00 AM") via dateparser."""
    if not re.search(r"\d", text):
        return None  # no digits: not an absolute date we can bound
    from dateparser.date import DateDataParser

    base = (observed_at or datetime(2000, 1, 1, tzinfo=timezone.utc)).astimezone(timezone.utc)
    try:
        data = DateDataParser(
            languages=["en"],
            settings=_dateparser_settings(base, source_timezone),
        ).get_date_data(text)
    except (ValueError, TypeError, OverflowError):
        return None
    if data is None or data.date_obj is None:
        return None
    if observed_at is None and not re.search(r"\b\d{4}\b", text):
        return None  # year implied from an anchor we do not have
    if data.period == "time":
        return _instant_interval(
            data.date_obj, method=method, raw=text, evidence={**evidence, "parser": "dateparser"}, tz=source_timezone
        )
    if data.period == "day":
        local_day = data.date_obj.astimezone(_zone(source_timezone)).date()
        return date_only_interval(
            local_day,
            source_timezone=source_timezone,
            method=method,
            raw=text,
            evidence={**evidence, "parser": "dateparser"},
        )
    return None  # week/month/year precision is too coarse to prove anything


def _zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        if name.upper() in {"UTC", "GMT", "Z"}:
            return ZoneInfo("UTC")
        raise
