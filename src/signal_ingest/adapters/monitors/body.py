"""Full-body extraction with start/end evidence; strip nav/sidebar boilerplate."""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Any

from signal_ingest.adapters.monitors.types import BodyExtraction

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
    }
)
# Token-based chrome classes (same pattern as llm/segments.py); "unavailable" is not "nav".
_CHROME_CLASS = re.compile(
    r"^(nav|navbar|navigation|nav-.+|.+-nav|.*sidebar.*|menu|breadcrumbs?|related(-.+)?)$"
)
_TEASER_CLASSES = frozenset({"teaser", "summary-only", "paywall-teaser"})
_BLOCK_TAGS = frozenset(
    {"p", "div", "section", "li", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "pre", "tr"}
)
# Visible end-of-body cut markers ("…", "...", "[truncated]", "Read more").
_TRUNCATED_END = re.compile(r"(…|\.\.\.|\[truncated\]|read more|continue reading)\s*$", re.IGNORECASE)


class _ArticleBodyParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.paragraphs: list[tuple[str, bool]] = []  # (text, inside article/main/articleBody)
        self._chunks: list[str] = []
        self._skip_depth = 0
        # Element skipped by class/role: its tag and same-tag nesting count.
        self._chrome_tag: str | None = None
        self._chrome_open = 0
        self._in_article = 0
        self._in_main = 0
        # itemprop=articleBody root: its tag and same-tag nesting count.
        self._body_tag: str | None = None
        self._body_open = 0
        self._saw_prefer = False
        self.title: str | None = None
        self._in_title = False
        self.json_ld: list[str] = []
        self._in_ld_json = False
        self._ld_chunks: list[str] = []
        self.meta: dict[str, str] = {}
        self.itemprops: dict[str, str] = {}
        self.times: list[dict[str, Any]] = []
        self._markers: list[tuple[str, bool]] = []  # (teaser|truncated, in_region)

    @property
    def in_region(self) -> bool:
        return bool(self._in_article or self._in_main or self._body_tag)

    def _marker_hit(self, kind: str) -> bool:
        return any(k == kind and (inside or not self._saw_prefer) for k, inside in self._markers)

    @property
    def teaser_only(self) -> bool:
        return self._marker_hit("teaser")

    @property
    def truncated_marker(self) -> bool:
        return self._marker_hit("truncated")

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        ad = {k.lower(): (v or "") for k, v in attrs}
        if tag == "title":
            self._in_title = True
        itemprop = (ad.get("itemprop") or "").lower()
        if tag == "meta":
            name = (ad.get("name") or ad.get("property") or ad.get("itemprop") or "").lower()
            content = ad.get("content") or ""
            if name and content:
                self.meta.setdefault(name, content)
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
        if role in {"navigation", "complementary"} or any(_CHROME_CLASS.match(c) for c in classes):
            self._chrome_tag, self._chrome_open = tag, 1
            return
        if tag == "article":
            self._in_article += 1
            self._saw_prefer = True
        if tag == "main":
            self._in_main += 1
            self._saw_prefer = True
        if self._body_tag is not None and tag == self._body_tag:
            self._body_open += 1
        elif self._body_tag is None and itemprop in {"articlebody", "text"}:
            self._body_tag, self._body_open = tag, 1
            self._saw_prefer = True
        if itemprop in {"datepublished", "dateposted"}:
            value = ad.get("content") or ad.get("datetime")
            if value:
                self.itemprops.setdefault(itemprop, value)
        if tag == "time" and ad.get("datetime"):
            self.times.append(
                {
                    "datetime": ad["datetime"],
                    "in_region": self.in_region,
                    "semantic": itemprop in {"datepublished", "dateposted"}
                    or "pubdate" in ad
                    or any(c in {"published", "pubdate", "entry-date"} for c in classes),
                }
            )
        if any(c in _TEASER_CLASSES for c in classes):
            self._markers.append(("teaser", self.in_region))
        if "truncated" in classes or ad.get("data-truncated") == "true":
            self._markers.append(("truncated", self.in_region))
        if tag == "br":
            self._flush_para()

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
        if tag in _BLOCK_TAGS or tag in {"article", "main"} or tag == self._body_tag:
            self._flush_para()
        if tag == "article" and self._in_article:
            self._in_article -= 1
        if tag == "main" and self._in_main:
            self._in_main -= 1
        if self._body_tag is not None and tag == self._body_tag:
            self._body_open -= 1
            if self._body_open == 0:
                self._body_tag = None

    def handle_data(self, data: str) -> None:
        if self._in_title and self.title is None:
            t = data.strip()
            if t:
                self.title = t
        if self._in_ld_json:
            self._ld_chunks.append(data)
            return
        if self._skip_depth or self._chrome_tag is not None:
            return
        text = data.strip()
        if text:
            self._chunks.append(text)

    def _flush_para(self) -> None:
        if not self._chunks:
            return
        para = " ".join(self._chunks).strip()
        self._chunks = []
        if para:
            self.paragraphs.append((para, self.in_region))

    def close(self) -> None:
        super().close()
        self._flush_para()

    def body_paragraphs(self) -> list[str]:
        # Once an article/main/articleBody region exists, ignore prose outside it.
        return [p for p, inside in self.paragraphs if inside or not self._saw_prefer]


def _parse(html: str | bytes) -> _ArticleBodyParser:
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    parser = _ArticleBodyParser()
    parser.feed(text)
    parser.close()
    return parser


def _trafilatura_paragraphs(html: str) -> list[str]:
    try:
        import trafilatura

        out = trafilatura.extract(html, include_comments=False)
    except Exception:  # noqa: BLE001 - extractor failure falls back to the local parser
        return []
    if not out:
        return []
    return [line.strip() for line in out.splitlines() if line.strip()]


def _body_checks(
    paragraphs: list[str],
    *,
    expected_start: str | None,
    expected_end: str | None,
    min_paragraphs: int,
) -> list[str]:
    body = "\n\n".join(paragraphs)
    reasons: list[str] = []
    if expected_start and paragraphs and expected_start[:40] not in paragraphs[0]:
        if expected_start[:40] not in body:
            reasons.append("missing_start")
    if expected_end:
        if not paragraphs or expected_end[:40] not in paragraphs[-1]:
            if expected_end[:40] not in body:
                reasons.append("missing_end")
    if len(paragraphs) < min_paragraphs:
        reasons.append("too_few_paragraphs")
    return reasons


def extract_body(
    html: str | bytes,
    *,
    expected_start: str | None = None,
    expected_end: str | None = None,
    min_paragraphs: int = 1,
) -> BodyExtraction:
    """
    Extract clean article/job paragraphs with start and end evidence.

    Prefers trafilatura full text; falls back to the local region parser when
    trafilatura is empty or misses the expected start/end evidence.
    Marks incomplete when teaser/truncated markers appear or expected end missing.
    """
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    parser = _parse(text)
    candidates = [
        ("trafilatura", _trafilatura_paragraphs(text)),
        ("html_article_paragraphs", parser.body_paragraphs()),
    ]
    candidates = [(m, paras) for m, paras in candidates if paras]
    method, paragraphs, reasons = "html_article_paragraphs", [], ["too_few_paragraphs"]
    for cand_method, cand_paras in candidates:
        cand_reasons = _body_checks(
            cand_paras,
            expected_start=expected_start,
            expected_end=expected_end,
            min_paragraphs=min_paragraphs,
        )
        if not paragraphs or not cand_reasons:
            method, paragraphs, reasons = cand_method, cand_paras, cand_reasons
        if not cand_reasons:
            break
    body = "\n\n".join(paragraphs).strip()
    markers = ["teaser"] if parser.teaser_only else []
    if parser.truncated_marker or (paragraphs and _TRUNCATED_END.search(paragraphs[-1])):
        markers.append("truncated")
    reasons = markers + reasons
    complete = not reasons
    start_excerpt = paragraphs[0][:240] if paragraphs else ""
    end_excerpt = paragraphs[-1][:240] if paragraphs else ""
    return BodyExtraction(
        text=body,
        start_excerpt=start_excerpt,
        end_excerpt=end_excerpt,
        complete=complete and bool(body),
        method=method,
        evidence={
            "paragraph_count": len(paragraphs),
            "start_excerpt": start_excerpt,
            "end_excerpt": end_excerpt,
            "incomplete_reasons": reasons,
            "title": parser.title,
            "meta": dict(parser.meta),
            "json_ld_blocks": len(parser.json_ld),
        },
    )


def _flatten_json_ld(data: Any) -> list[dict[str, Any]]:
    """Expand top-level lists and Yoast-style @graph containers into node dicts."""
    out: list[dict[str, Any]] = []
    if isinstance(data, list):
        for item in data:
            out.extend(_flatten_json_ld(item))
    elif isinstance(data, dict):
        out.append(data)
        graph = data.get("@graph")
        if isinstance(graph, (list, dict)):
            out.extend(_flatten_json_ld(graph))
    return out


def extract_json_ld_blocks(html: str | bytes) -> list[dict[str, Any]]:
    import json

    out: list[dict[str, Any]] = []
    for block in _parse(html).json_ld:
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue
        out.extend(_flatten_json_ld(data))
    return out


def extract_page_dates(html: str | bytes) -> dict[str, Any]:
    """Meta (either attribute order), itemprop dates and <time> candidates with context."""
    parser = _parse(html)
    return {"meta": dict(parser.meta), "itemprops": dict(parser.itemprops), "times": list(parser.times)}


_HREF_RE = re.compile(
    r"""<a\s+[^>]*href=["']([^"']+)["'][^>]*>(.*?)</a>""",
    re.IGNORECASE | re.DOTALL,
)


def extract_list_links(html: str | bytes, *, base_hint: str | None = None) -> list[dict[str, str]]:
    """Lightweight listing link harvest for HTML indexes."""
    if isinstance(html, bytes):
        text = html.decode("utf-8", errors="replace")
    else:
        text = html
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in _HREF_RE.finditer(text):
        href = match.group(1).strip()
        title = re.sub(r"<[^>]+>", "", match.group(2)).strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        if href in seen:
            continue
        seen.add(href)
        items.append({"url": href, "title": title})
    return items
