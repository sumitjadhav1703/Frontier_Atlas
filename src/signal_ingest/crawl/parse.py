"""Minimal B1 parse: candidates only after raw is saved."""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

from signal_ingest.crawl.types import ParseCandidate, RawObservation


class _LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []
        self.title: str | None = None
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        ad = {k: v for k, v in attrs}
        if tag == "a" and ad.get("href"):
            self.hrefs.append(ad["href"])
        if tag == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title and self.title is None:
            self.title = data.strip() or None


def parse(raw: RawObservation) -> ParseCandidate:
    """
    Produce a minimal candidate from a saved raw observation.
    Does not invent field defaults. Full HTML/JSON/feed adapters are B2+.
    """
    if raw.body is None:
        return ParseCandidate(url=raw.final_url, fields={}, evidence={})

    ctype = (raw.content_type or "").lower()
    evidence: dict[str, Any] = {
        "request_url": raw.request_url,
        "final_url": raw.final_url,
        "content_sha256": raw.content_sha256,
        "content_key": raw.content_key,
        "status_code": raw.status_code,
    }

    if "json" in ctype:
        return _parse_json(raw, evidence)
    if "html" in ctype or _looks_like_html(raw.body):
        return _parse_html(raw, evidence)
    if "xml" in ctype or "rss" in ctype or "atom" in ctype:
        return _parse_xml_links(raw, evidence)

    return ParseCandidate(
        url=raw.final_url,
        fields={"bytes": len(raw.body)},
        evidence=evidence,
        links=[],
    )


def _looks_like_html(body: bytes) -> bool:
    sample = body[:256].lstrip().lower()
    return sample.startswith(b"<!doctype html") or sample.startswith(b"<html")


def _parse_json(raw: RawObservation, evidence: dict[str, Any]) -> ParseCandidate:
    assert raw.body is not None
    try:
        data = json.loads(raw.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return ParseCandidate(
            url=raw.final_url,
            fields={},
            evidence={**evidence, "parse_error": str(exc)},
        )
    links: list[str] = []
    if isinstance(data, dict):
        for key in ("url", "next", "next_url", "href"):
            val = data.get(key)
            if isinstance(val, str) and val.startswith("http"):
                links.append(val)
    return ParseCandidate(
        url=raw.final_url,
        fields={"json": data} if isinstance(data, (dict, list)) else {},
        evidence=evidence,
        links=links,
    )


def _parse_html(raw: RawObservation, evidence: dict[str, Any]) -> ParseCandidate:
    assert raw.body is not None
    text = raw.body.decode("utf-8", errors="replace")
    parser = _LinkExtractor()
    try:
        parser.feed(text)
    except Exception as exc:  # pragma: no cover - defensive
        return ParseCandidate(
            url=raw.final_url,
            fields={},
            evidence={**evidence, "parse_error": str(exc)},
        )
    links = []
    seen: set[str] = set()
    for href in parser.hrefs:
        abs_url = urljoin(raw.final_url, href)
        if abs_url not in seen:
            seen.add(abs_url)
            links.append(abs_url)
    fields: dict[str, Any] = {}
    if parser.title:
        fields["title"] = parser.title
    return ParseCandidate(
        url=raw.final_url,
        fields=fields,
        evidence=evidence,
        links=links,
    )


_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)


def _parse_xml_links(raw: RawObservation, evidence: dict[str, Any]) -> ParseCandidate:
    assert raw.body is not None
    text = raw.body.decode("utf-8", errors="replace")
    links = []
    seen: set[str] = set()
    for match in _HREF_RE.findall(text):
        abs_url = urljoin(raw.final_url, match)
        if abs_url not in seen:
            seen.add(abs_url)
            links.append(abs_url)
    # Also <link>...</link> text nodes for Atom
    for match in re.findall(r"<link>([^<]+)</link>", text, flags=re.IGNORECASE):
        abs_url = urljoin(raw.final_url, match.strip())
        if abs_url not in seen:
            seen.add(abs_url)
            links.append(abs_url)
    return ParseCandidate(
        url=raw.final_url,
        fields={},
        evidence=evidence,
        links=links,
    )
