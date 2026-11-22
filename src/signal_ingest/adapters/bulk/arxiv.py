"""arXiv metadata adapter: versionless ID + v1 published_date."""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from typing import Any

from signal_ingest.adapters.bulk.mapping import (
    arxiv_version,
    checkpoint_partition_key,
    content_revision,
    parse_aware_datetime,
    versionless_arxiv_id,
)
from signal_ingest.adapters.bulk.types import BulkParseResult
from signal_ingest.crawl.types import DiscoveredItem, RawObservation

SOURCE_ID = "bulk.arxiv"
SOURCE_NAME = "arXiv"
_ATOM_NS = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}


def discover_ids(
    arxiv_ids: list[str],
    *,
    partition: str = "default",
    revision: str | None = None,
    api_base: str = "http://export.arxiv.org/api/query",
) -> tuple[list[DiscoveredItem], str, str]:
    cleaned = [versionless_arxiv_id(x) or x for x in arxiv_ids]
    cleaned = [c for c in cleaned if c]
    rev = revision or content_revision(cleaned)
    partition_key = checkpoint_partition_key(revision=rev, partition=partition)
    items: list[DiscoveredItem] = []
    for idx, aid in enumerate(cleaned):
        url = f"{api_base}?id_list={aid}"
        items.append(
            DiscoveredItem(
                url=url,
                external_id=aid,
                cursor=f"{rev}:{partition}:{idx}",
                metadata={
                    "adapter": SOURCE_ID,
                    "partition": partition,
                    "revision": rev,
                    "arxiv_id": aid,
                },
            )
        )
    return items, rev, partition_key


def parse_observation(raw: RawObservation) -> BulkParseResult:
    """Parse arXiv Atom/JSON into metadata used to ground paper publication."""
    meta = parse_metadata_bytes(raw.body or b"", content_type=raw.content_type)
    if meta is None:
        return BulkParseResult(
            ok=False,
            quarantine_reason="arxiv_parse_failed",
            quarantine_detail={"content_type": raw.content_type},
        )
    aid = meta.get("arxiv_id")
    if not aid:
        return BulkParseResult(
            ok=False,
            quarantine_reason="missing_arxiv_id",
            quarantine_detail=meta,
        )
    return BulkParseResult(
        ok=True,
        record_type="RESEARCH_PAPER",
        external_id=aid,
        payload={"arxiv": meta},
        evidence={
            "request_url": raw.request_url,
            "final_url": raw.final_url,
            "content_sha256": raw.content_sha256,
            "v1_published_date": meta.get("v1_published_date"),
        },
        eligible=False,
    )


def parse_metadata_bytes(
    body: bytes,
    *,
    content_type: str | None = None,
) -> dict[str, Any] | None:
    if not body:
        return None
    ctype = (content_type or "").lower()
    text = body.decode("utf-8", errors="replace")
    if "json" in ctype or text.lstrip().startswith("{") or text.lstrip().startswith("["):
        return _parse_json_meta(text)
    return _parse_atom_meta(text)


def merge_arxiv_into_paper_row(
    paper: dict[str, Any],
    arxiv_meta: dict[str, Any],
) -> dict[str, Any]:
    """Overlay versionless id, authors, title, and v1 published_date onto a paper row."""
    out = dict(paper)
    aid = arxiv_meta.get("arxiv_id") or versionless_arxiv_id(
        paper.get("arxiv_id") or paper.get("paper_arxiv_id")
    )
    if aid:
        out["arxiv_id"] = aid
    if arxiv_meta.get("title") and not out.get("title") and not out.get("paper_title"):
        out["title"] = arxiv_meta["title"]
    if arxiv_meta.get("authors") and not out.get("authors"):
        out["authors"] = arxiv_meta["authors"]
    v1 = arxiv_meta.get("v1_published_date") or arxiv_meta.get("published_date")
    if v1:
        out["published_date"] = v1
    if arxiv_meta.get("paper_url"):
        out.setdefault("paper_url", arxiv_meta["paper_url"])
    return out


def _parse_json_meta(text: str) -> dict[str, Any] | None:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(data, list) and data:
        data = data[0]
    if not isinstance(data, dict):
        return None
    # Nested entry
    if "entry" in data and isinstance(data["entry"], dict):
        data = data["entry"]
    aid = versionless_arxiv_id(
        data.get("arxiv_id") or data.get("id") or data.get("paper_id")
    )
    versions = data.get("versions") or []
    v1_date = None
    for ver in versions:
        if isinstance(ver, dict) and str(ver.get("version") or ver.get("v") or "") in {
            "1",
            "v1",
        }:
            v1_date = parse_aware_datetime(ver.get("created") or ver.get("published"))
            break
    if v1_date is None and data.get("v1_published_date"):
        v1_date = parse_aware_datetime(data["v1_published_date"])
    published = parse_aware_datetime(data.get("published") or data.get("published_date"))
    # If the payload is a specific version > v1 and no v1 date, keep published but flag.
    ver = arxiv_version(str(data.get("id") or data.get("arxiv_id") or ""))
    authors = data.get("authors") or []
    if isinstance(authors, str):
        authors = [a.strip() for a in authors.split(",") if a.strip()]
    return {
        "arxiv_id": aid,
        "version": ver,
        "title": data.get("title"),
        "authors": authors,
        "published_date": (v1_date or published).isoformat().replace("+00:00", "Z")
        if (v1_date or published)
        else None,
        "v1_published_date": v1_date.isoformat().replace("+00:00", "Z") if v1_date else (
            published.isoformat().replace("+00:00", "Z") if published and (ver in (None, 1)) else None
        ),
        "paper_url": data.get("paper_url")
        or (f"https://arxiv.org/abs/{aid}" if aid else None),
    }


def _parse_atom_meta(text: str) -> dict[str, Any] | None:
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return None
    entry = root.find("atom:entry", _ATOM_NS)
    if entry is None:
        # Sometimes the document is a bare entry
        if root.tag.endswith("entry"):
            entry = root
        else:
            entry = root.find("entry")
    if entry is None:
        return None

    def _text(path: str) -> str | None:
        node = entry.find(path, _ATOM_NS)
        if node is None:
            node = entry.find(path.split(":")[-1])
        if node is None or node.text is None:
            return None
        return re.sub(r"\s+", " ", node.text).strip()

    raw_id = _text("atom:id") or _text("id") or ""
    aid = versionless_arxiv_id(raw_id)
    ver = arxiv_version(raw_id)
    published = parse_aware_datetime(_text("atom:published") or _text("published"))
    # arXiv Atom published is the current version time; look for journal_ref / versions
    # In Atom API, v1 time is often in <arxiv:version version="v1"> or first published.
    v1_date = None
    for ver_el in entry.findall("arxiv:version", _ATOM_NS):
        vattr = ver_el.attrib.get("version") or ""
        if vattr in {"v1", "1"}:
            v1_date = parse_aware_datetime((ver_el.text or "").strip()) or parse_aware_datetime(
                ver_el.attrib.get("date")
            )
    if v1_date is None and ver in (None, 1):
        v1_date = published

    authors: list[str] = []
    for author in entry.findall("atom:author", _ATOM_NS) or entry.findall("author"):
        name_el = author.find("atom:name", _ATOM_NS)
        if name_el is None:
            name_el = author.find("name")
        if name_el is not None and name_el.text:
            authors.append(name_el.text.strip())

    title = _text("atom:title") or _text("title")
    return {
        "arxiv_id": aid,
        "version": ver,
        "title": title,
        "authors": authors,
        "published_date": (v1_date or published).isoformat().replace("+00:00", "Z")
        if (v1_date or published)
        else None,
        "v1_published_date": v1_date.isoformat().replace("+00:00", "Z") if v1_date else None,
        "paper_url": f"https://arxiv.org/abs/{aid}" if aid else None,
    }
