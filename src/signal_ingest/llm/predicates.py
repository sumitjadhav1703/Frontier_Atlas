"""Deterministic domain predicates + entity-aware quote grounding."""

from __future__ import annotations

import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlparse

from signal_ingest.models.records import PricingModel
from signal_ingest.llm.types import Segment

_REMOTE_TRUE = re.compile(
    r"\b(fully\s+remote|remote[- ]first|work\s+from\s+anywhere|100%\s*remote)\b",
    re.IGNORECASE,
)
_REMOTE_FALSE = re.compile(
    r"\b(on[- ]site\s+only|hybrid\s+only|in[- ]office\s+only|no\s+remote)\b",
    re.IGNORECASE,
)
_DATE_ISO = re.compile(r"\b(\d{4}-\d{2}-\d{2})([Tt ]\d{2}:\d{2}(:\d{2})?(Z|[+-]\d{2}:?\d{2})?)?\b")
_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_NAME_OK = re.compile(r"^[\w][\w .,&'’\-+/()]{0,200}$", re.UNICODE)


_QUOTE_CHARS = str.maketrans({"\u2018": "'", "\u2019": "'", "\u201c": '"', "\u201d": '"'})
_ZERO_WIDTH = re.compile("[\u200b-\u200d\ufeff]")
_WS = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    """NFKC, straight quotes, no zero-width chars, single spaces (nbsp → space)."""
    text = unicodedata.normalize("NFKC", text).translate(_QUOTE_CHARS)
    return _WS.sub(" ", _ZERO_WIDTH.sub("", text)).strip()


def quote_in_segment(quote: str, segment: Segment) -> bool:
    if not quote or not quote.strip():
        return False
    return normalize_text(quote) in normalize_text(segment.text)


def locate_quote(quote: str, segment: Segment) -> int | None:
    """Offset of the quote in the *normalized* segment text."""
    if not quote:
        return None
    idx = normalize_text(segment.text).find(normalize_text(quote))
    return idx if idx >= 0 else None


def entity_window(segment: Segment, quote: str, *, radius: int = 160) -> str:
    """Local context around the quote inside the (normalized) segment."""
    text = normalize_text(segment.text)
    idx = locate_quote(quote, segment)
    if idx is None:
        return text
    start = max(0, idx - radius)
    end = min(len(text), idx + len(normalize_text(quote)) + radius)
    return text[start:end]


_EXTRACTIVE_TEXT_FIELDS = frozenset(
    {"entityName", "startupName", "productName", "company", "owner", "title", "role_title"}
)
_NUMERIC_FIELDS = frozenset({"price", "employeeCount", "github_stars"})
_URL_FIELDS = frozenset({"article_url", "product_url", "paper_url", "github_url"})
_NUMBER = re.compile(r"\d+(?:[.,]\d+)*")


def _numbers(text: str) -> set[Decimal]:
    out: set[Decimal] = set()
    for n in _NUMBER.findall(text):
        try:
            out.add(Decimal(n.replace(",", "")).normalize())  # 1,299.00 == 1299
        except InvalidOperation:
            continue
    return out


def _contains_phrase(text: str, phrase: str, *, url: bool = False) -> bool:
    """Whole-phrase match: "Ace" is not in "Acer", a repo URL is not its parent."""
    phrase = normalize_text(phrase).casefold().rstrip(".,;:")
    if not phrase:
        return False
    # URL: next char must not continue the path; a sentence-ending "." is fine.
    tail = r"(?![\w/\-])(?!\.\w)" if url else r"(?!\w)"
    return re.search(r"(?<!\w)" + re.escape(phrase) + tail, text) is not None


_REMOTE_ANY = re.compile(
    r"\b(remote|work from (home|anywhere)|telecommute|distributed (team|company))\b", re.I
)
_ONSITE_ANY = re.compile(r"\b(on[- ]?site|in[- ]office|office[- ]based|hybrid|relocat\w*)\b", re.I)
_MONTHS = (
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
)
_PRICING_EVIDENCE = {
    "FREE": re.compile(r"\bfree\b", re.I),
    "FREEMIUM": re.compile(r"\bfreemium\b|\bfree (tier|plan)\b", re.I),
    "PAID": re.compile(r"[$€£]\s?\d|\bpaid\b|\bper (month|year|seat|user)\b|/mo\b", re.I),
    "ENTERPRISE": re.compile(r"\benterprise\b|\bcontact sales\b", re.I),
}


def _date_in_quote(value: str, q: str) -> bool:
    m = _DATE_ISO.search(value)
    if not m:
        return False
    iso = m.group(1)
    if iso in q:
        return True
    year, month, day = iso.split("-")
    month_name = _MONTHS[int(month) - 1]  # full name; abbreviation = first 3
    # "Nov 21, 2026" / "21 November 2026": year, month name and day all present.
    return (
        year in q
        # "Nov", "Nov." or "November" as a word; "novel" does not count.
        and re.search(rf"\b({month_name[:3]}|{month_name})\b", q) is not None
        and re.search(rf"(?<!\d)0?{int(day)}(?!\d)", q) is not None
    )


def value_supported_by_quote(field: str, value: Any, quote: str) -> bool:
    """The cited quote must actually evidence the value.

    Extractive fields need the value itself in the quote (whole phrase / same
    number). Label fields need wording that supports the label: remote flags,
    dates, pricing models. role_family stays a free classification of the quote.
    """
    q = normalize_text(quote).casefold()
    if field == "authors":
        names = value if isinstance(value, list) else [value]
        return all(_contains_phrase(q, str(n)) for n in names)
    if field in _NUMERIC_FIELDS:
        wanted = _numbers(str(value))
        if wanted:
            return wanted <= _numbers(q)
        return _contains_phrase(q, str(value))
    if field in _URL_FIELDS:
        bare = re.sub(r"^https?://", "", str(value).strip(), flags=re.I).rstrip("/")
        return _contains_phrase(q, bare, url=True)
    if field in _EXTRACTIVE_TEXT_FIELDS:
        return _contains_phrase(q, str(value))
    if field == "is_remote":
        return bool(_REMOTE_ANY.search(q) if value else _ONSITE_ANY.search(q))
    if field in {"date", "published_date"}:
        return _date_in_quote(str(value), q)
    if field == "pricingModel":
        evidence = _PRICING_EVIDENCE.get(str(value))
        return bool(evidence and evidence.search(q))
    return True


def entity_mentions(text: str, names: tuple[str, ...]) -> list[tuple[str, int]]:
    hits: list[tuple[str, int]] = []
    lower = text.lower()
    for name in names:
        if not name:
            continue
        pos = lower.find(name.lower())
        if pos >= 0:
            hits.append((name, pos))
    return hits


def quote_describes_entity(
    *,
    quote: str,
    segment: Segment,
    target_entity: str | None,
    target_aliases: tuple[str, ...] = (),
    known_entities: tuple[str, ...] = (),
) -> tuple[bool, str | None]:
    """Reject correct-quote-wrong-entity: quote must be about the target entity.

    Rules:
    - Quote must occur in the segment.
    - If a target is set, the local window around the quote must mention the target
      (or alias), OR no competing entity may be closer to the quote than the target.
    - If another known entity on the page/segment is nearer to the quote and the
      target is absent from the window → reject.
    """
    if not quote_in_segment(quote, segment):
        return False, "quote_not_in_segment"

    if not target_entity:
        return True, None

    window = entity_window(segment, quote)
    target_names = tuple(dict.fromkeys([target_entity, *target_aliases]))
    target_hits = entity_mentions(window, target_names)
    if target_hits:
        return True, None

    target_lower = {t.lower() for t in target_names}
    others = tuple(
        dict.fromkeys(
            [
                n
                for n in (*segment.entity_hints, *known_entities)
                if n and n.lower() not in target_lower
            ]
        )
    )
    other_hits = entity_mentions(window, others) if others else []
    if other_hits:
        return False, "correct_quote_wrong_entity"

    # Broader segment check: if target never appears in segment but others do near quote.
    seg_target = entity_mentions(normalize_text(segment.text), target_names)
    if not seg_target:
        if others and entity_mentions(normalize_text(segment.text), others):
            return False, "correct_quote_wrong_entity"
        # No entity evidence at all for a targeted claim → abstain/reject.
        return False, "target_entity_not_in_evidence"

    # Target appears somewhere in segment but not near quote — require proximity.
    qidx = locate_quote(quote, segment) or 0
    nearest_target = min(abs(pos - qidx) for _, pos in seg_target)
    other_positions = entity_mentions(normalize_text(segment.text), others)
    if other_positions:
        nearest_other = min(abs(pos - qidx) for _, pos in other_positions)
        if nearest_other + 8 < nearest_target:
            return False, "correct_quote_wrong_entity"
    if nearest_target > 220:
        return False, "quote_not_about_target_entity"
    return True, None


def predicate_name(value: Any) -> tuple[bool, str | None]:
    if value is None:
        return True, None
    if not isinstance(value, str) or not value.strip():
        return False, "invalid_name"
    if not _NAME_OK.match(value.strip()):
        return False, "invalid_name_charset"
    return True, None


def predicate_owner(value: Any) -> tuple[bool, str | None]:
    return predicate_name(value)


def predicate_price(value: Any) -> tuple[bool, str | None]:
    if value is None:
        return True, None
    if isinstance(value, (int, float)) and value >= 0:
        return True, None
    if isinstance(value, str) and value.strip():
        return True, None
    return False, "invalid_price"


def predicate_pricing_model(value: Any) -> tuple[bool, str | None]:
    if value is None:
        return True, None
    try:
        PricingModel(value)
    except ValueError:
        return False, "invalid_pricing_model"
    return True, None


def predicate_remote(value: Any, *, quote: str | None = None) -> tuple[bool, str | None]:
    if value is None:
        return True, None
    if not isinstance(value, bool):
        return False, "remote_must_be_bool"
    if quote:
        if value is True and _REMOTE_FALSE.search(quote) and not _REMOTE_TRUE.search(quote):
            return False, "remote_true_contradicts_quote"
        if value is False and _REMOTE_TRUE.search(quote) and not _REMOTE_FALSE.search(quote):
            return False, "remote_false_contradicts_quote"
    return True, None


def predicate_url(value: Any) -> tuple[bool, str | None]:
    if value is None:
        return True, None
    if not isinstance(value, str):
        return False, "invalid_url"
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False, "invalid_url"
    return True, None


def predicate_date(value: Any) -> tuple[bool, str | None]:
    if value is None:
        return True, None
    if isinstance(value, str) and _DATE_ISO.search(value):
        return True, None
    return False, "invalid_date"


def predicate_authors(value: Any) -> tuple[bool, str | None]:
    if value is None:
        return True, None
    if isinstance(value, list) and all(isinstance(x, str) and x.strip() for x in value):
        return True, None
    if isinstance(value, str) and value.strip():
        return True, None
    return False, "invalid_authors"


_FIELD_PREDICATES = {
    "entityName": predicate_name,
    "startupName": predicate_name,
    "productName": predicate_name,
    "company": predicate_name,
    "owner": predicate_owner,
    "price": predicate_price,
    "pricingModel": predicate_pricing_model,
    "is_remote": predicate_remote,
    "article_url": predicate_url,
    "product_url": predicate_url,
    "paper_url": predicate_url,
    "github_url": predicate_url,
    "date": predicate_date,
    "published_date": predicate_date,
    "authors": predicate_authors,
    "title": predicate_name,
    "role_title": predicate_name,
    "role_family": predicate_name,
}


def apply_field_predicate(
    field: str,
    value: Any,
    *,
    quote: str | None = None,
) -> tuple[bool, str | None]:
    fn = _FIELD_PREDICATES.get(field)
    if fn is None:
        # Unknown field: allow null abstention only.
        if value is None:
            return True, None
        return True, None
    if field == "is_remote":
        return predicate_remote(value, quote=quote)
    return fn(value)
