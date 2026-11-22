"""HTML → labeled segments with stable IDs, offsets, and semantic kinds."""

from __future__ import annotations

import hashlib
import json
import re
from html.parser import HTMLParser
from typing import Any

from signal_ingest.llm.types import Segment, SegmentKind
from signal_ingest.llm.versions import SEGMENTER_VERSION

_SKIP_TAGS = frozenset(
    {
        "script",
        "style",
        "nav",
        "aside",
        "footer",
        "header",
        "noscript",
        "iframe",
        "form",
        "svg",
        "button",
    }
)

# Class/role tokens that mark page chrome. Token match, so "unavailable" or
# "navy" never trigger it.
_CHROME_CLASS = re.compile(r"^(nav|navbar|navigation|nav-.+|.*sidebar.*|menu|breadcrumbs?)$")

_BLOCK_TAGS = frozenset(
    {"p", "div", "section", "article", "main", "li", "h1", "h2", "h3", "h4", "h5", "h6", "pre", "blockquote", "tr"}
)

_PRIORITY = {
    SegmentKind.TITLE: 100,
    SegmentKind.DATE: 90,
    SegmentKind.PRICING: 85,
    SegmentKind.OWNER: 80,
    SegmentKind.CODE: 75,
    SegmentKind.JSON_LD: 70,
    SegmentKind.META: 60,
    SegmentKind.BODY: 40,
    SegmentKind.OTHER: 20,
}

_PRICE_RE = re.compile(
    r"(\$|usd|eur|price|pricing|freemium|free tier|paid plan|enterprise|subscription|/\$\d)",
    re.IGNORECASE,
)
_OWNER_RE = re.compile(
    r"\b(owned by|founder[s]?|founded by|operated by|employer:|company:)\b",
    re.IGNORECASE,
)
_DATE_RE = re.compile(
    r"\b(published|posted|datePublished|datePosted|\d{4}-\d{2}-\d{2}|ago)\b",
    re.IGNORECASE,
)
_CODE_RE = re.compile(r"(```|github\.com/|def |class |import |npm |pip )", re.IGNORECASE)


class _SegmentParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.blocks: list[tuple[str, str, dict[str, str]]] = []
        self._chunks: list[str] = []
        self._skip_depth = 0
        # Chrome element skipped by class/role: its tag and same-tag nesting count.
        self._chrome_tag: str | None = None
        self._chrome_open = 0
        self._tag_stack: list[str] = []
        self._attrs_stack: list[dict[str, str]] = []
        self.title: str | None = None
        self._in_title = False
        self.json_ld: list[str] = []
        self._in_ld_json = False
        self._ld_chunks: list[str] = []
        self.meta: dict[str, str] = {}
        self._in_pre = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        ad = {k.lower(): (v or "") for k, v in attrs}
        if tag == "title":
            self._in_title = True
        if tag == "meta":
            name = (ad.get("name") or ad.get("property") or ad.get("itemprop") or "").lower()
            content = ad.get("content") or ""
            if name and content:
                self.meta[name] = content
            return
        if tag == "script" and "ld+json" in (ad.get("type") or "").lower():
            self._in_ld_json = True
            self._ld_chunks = []
            return
        if self._chrome_tag is not None:
            if tag == self._chrome_tag:
                self._chrome_open += 1
            return
        if tag in _SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        classes = (ad.get("class") or "").lower().split()
        role = (ad.get("role") or "").lower()
        if role == "navigation" or any(_CHROME_CLASS.match(c) for c in classes):
            self._chrome_tag, self._chrome_open = tag, 1
            return
        if tag == "pre" or tag == "code":
            self._in_pre = True
        self._tag_stack.append(tag)
        self._attrs_stack.append(ad)
        if tag == "br":
            self._flush("br", {})

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        if tag == "script" and self._in_ld_json:
            self._in_ld_json = False
            self.json_ld.append("".join(self._ld_chunks))
            self._ld_chunks = []
            return
        if self._chrome_tag is not None:
            if tag == self._chrome_tag:
                self._chrome_open -= 1
                if self._chrome_open == 0:
                    self._chrome_tag = None
            return
        if tag in _SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if tag in {"pre", "code"}:
            self._in_pre = False
        if tag in _BLOCK_TAGS:
            attrs = self._attrs_stack[-1] if self._attrs_stack else {}
            self._flush(tag, attrs)
        if self._tag_stack and self._tag_stack[-1] == tag:
            self._tag_stack.pop()
            if self._attrs_stack:
                self._attrs_stack.pop()

    def handle_data(self, data: str) -> None:
        if self._in_title:
            t = data.strip()
            if t and self.title is None:
                self.title = t
        if self._in_ld_json:
            self._ld_chunks.append(data)
            return
        if self._skip_depth or self._chrome_tag is not None:
            return
        if self._in_pre:
            if data:
                self._chunks.append(data)
            return
        text = data.strip()
        if text:
            self._chunks.append(text)

    def _flush(self, tag: str, attrs: dict[str, str]) -> None:
        if not self._chunks:
            return
        if self._in_pre or tag == "pre":
            text = "".join(self._chunks).strip("\n")
        else:
            text = " ".join(self._chunks).strip()
        self._chunks = []
        if text:
            self.blocks.append((tag, text, dict(attrs)))


def html_to_segments(html: str | bytes, *, entity_hints: tuple[str, ...] = ()) -> list[Segment]:
    """Parse HTML into stable labeled segments; strip nav/scripts/boilerplate."""
    if isinstance(html, bytes):
        text = html.decode("utf-8", errors="replace")
    else:
        text = html
    parser = _SegmentParser()
    parser.feed(text)
    parser.close()

    segments: list[Segment] = []
    cursor = 0
    seq = 0

    def _add(kind: SegmentKind, body: str, hints: tuple[str, ...] = ()) -> None:
        nonlocal cursor, seq
        if not body:
            return
        seq += 1
        start = cursor
        end = start + len(body)
        cursor = end + 1  # account for join separator in concatenated view
        seg_id = f"seg:{seq:04d}:{kind.value}"
        segments.append(
            Segment(
                segment_id=seg_id,
                kind=kind,
                text=body,
                start_offset=start,
                end_offset=end,
                priority=_PRIORITY[kind],
                entity_hints=hints or entity_hints,
            )
        )

    if parser.title:
        _add(SegmentKind.TITLE, parser.title, entity_hints)

    for name, content in sorted(parser.meta.items()):
        kind = SegmentKind.META
        if "date" in name or name in {"article:published_time", "pubdate"}:
            kind = SegmentKind.DATE
        _add(kind, f"{name}: {content}", entity_hints)

    for block in parser.json_ld:
        pretty = block.strip()
        if pretty:
            _add(SegmentKind.JSON_LD, pretty, entity_hints)
            # Promote structured pricing/owner clues from JSON-LD keys.
            lower = pretty.lower()
            if "price" in lower or "offers" in lower:
                _add(SegmentKind.PRICING, pretty, entity_hints)
            if "author" in lower or "founder" in lower or "organization" in lower:
                _add(SegmentKind.OWNER, pretty, entity_hints)

    for tag, body, attrs in parser.blocks:
        kind = _classify_block(tag, body, attrs)
        hints = _entity_hints_from_text(body, entity_hints)
        _add(kind, body, hints)

    return segments


def segments_hash(segments: list[Segment]) -> str:
    payload = [
        {
            "id": s.segment_id,
            "kind": s.kind.value,
            "text": s.text,
            "start": s.start_offset,
            "end": s.end_offset,
        }
        for s in segments
    ]
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def raw_content_hash(html: str | bytes) -> str:
    if isinstance(html, bytes):
        data = html
    else:
        data = html.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def segment_index(segments: list[Segment]) -> dict[str, Segment]:
    return {s.segment_id: s for s in segments}


def concatenated_text(segments: list[Segment]) -> str:
    return "\n".join(s.text for s in segments)


def segmenter_version() -> str:
    return SEGMENTER_VERSION


def _classify_block(tag: str, body: str, attrs: dict[str, str]) -> SegmentKind:
    classes = (attrs.get("class") or "").lower()
    itemprop = (attrs.get("itemprop") or "").lower()
    if tag in {"h1", "h2"} or itemprop == "headline":
        return SegmentKind.TITLE
    if tag in {"pre", "code"} or "code" in classes or _CODE_RE.search(body):
        return SegmentKind.CODE
    if itemprop in {"datepublished", "dateposted"} or _DATE_RE.search(body):
        return SegmentKind.DATE
    if "price" in classes or "pricing" in classes or _PRICE_RE.search(body):
        return SegmentKind.PRICING
    if "author" in classes or "owner" in classes or itemprop in {"author", "founder"} or _OWNER_RE.search(body):
        return SegmentKind.OWNER
    if tag in {"p", "div", "section", "li", "article", "main"}:
        return SegmentKind.BODY
    return SegmentKind.OTHER


def _entity_hints_from_text(body: str, seed: tuple[str, ...]) -> tuple[str, ...]:
    found: list[str] = []
    lower = body.lower()
    for name in seed:
        if name and name.lower() in lower:
            found.append(name)
    # Keep unique order.
    out: list[str] = []
    for n in found:
        if n not in out:
            out.append(n)
    return tuple(out)


def load_segments_from_plain_text(
    text: str,
    *,
    entity_hints: tuple[str, ...] = (),
) -> list[Segment]:
    """Split plain text into paragraph segments (no HTML)."""
    parts = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    if not parts and text.strip():
        parts = [text.strip()]
    segments: list[Segment] = []
    cursor = 0
    for i, part in enumerate(parts, start=1):
        kind = _classify_block("p", part, {})
        start = cursor
        end = start + len(part)
        cursor = end + 1
        segments.append(
            Segment(
                segment_id=f"seg:{i:04d}:{kind.value}",
                kind=kind,
                text=part,
                start_offset=start,
                end_offset=end,
                priority=_PRIORITY[kind],
                entity_hints=_entity_hints_from_text(part, entity_hints),
            )
        )
    return segments
