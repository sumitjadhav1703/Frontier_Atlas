"""Papers-with-Code-shaped paper + repo-edge adapter."""

from __future__ import annotations

import json
import re
from typing import Any, Sequence

from signal_ingest.adapters.bulk.eligibility import paper_join_complete
from signal_ingest.adapters.bulk.mapping import (
    checkpoint_partition_key,
    content_revision,
    normalize_github_url,
    parse_aware_datetime,
    select_primary_repo,
    versionless_arxiv_id,
)
from signal_ingest.adapters.bulk.types import BulkParseResult
from signal_ingest.config.clock import get_clock
from signal_ingest.crawl.types import DiscoveredItem, RawObservation

SOURCE_ID = "bulk.papers"
_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SOURCE_NAME = "Papers with Code"


def discover_from_payload(
    payload: Any,
    *,
    partition: str = "default",
    revision: str | None = None,
) -> tuple[list[DiscoveredItem], str, str]:
    """
    Discover papers. Payload may be:
    - list of paper objects (with optional repo edges embedded)
    - {"papers": [...], "links": [...]} joined by paper id/url/arxiv
    """
    papers, links = _split_papers_links(payload)
    by_paper = _index_links(links)
    rev = revision or content_revision({"papers": papers, "links": links})
    partition_key = checkpoint_partition_key(revision=rev, partition=partition)
    items: list[DiscoveredItem] = []
    for idx, paper in enumerate(papers):
        external_id = paper_identity(paper)
        url = str(
            paper.get("paper_url")
            or paper.get("url")
            or paper.get("abs_url")
            or f"https://paperswithcode.example/paper/{external_id}"
        )
        edges = list(paper.get("repos") or paper.get("code") or [])
        edges.extend(by_paper.get(external_id, []))
        # Also match by arxiv / raw paper url keys
        aid = versionless_arxiv_id(
            paper.get("arxiv_id") or paper.get("paper_arxiv_id") or paper.get("arxiv")
        )
        if aid:
            edges.extend(by_paper.get(f"arxiv:{aid}", []))
        items.append(
            DiscoveredItem(
                url=url,
                external_id=external_id,
                cursor=f"{rev}:{partition}:{idx}",
                metadata={
                    "adapter": SOURCE_ID,
                    "partition": partition,
                    "revision": rev,
                    "row": paper,
                    "repo_edges": edges,
                    "synthetic": bool(paper.get("_synthetic", True)),
                },
            )
        )
    return items, rev, partition_key


def paper_identity(paper: dict[str, Any]) -> str:
    """Versionless arXiv ID preferred; else stable paper id/url."""
    aid = versionless_arxiv_id(
        paper.get("arxiv_id")
        or paper.get("paper_arxiv_id")
        or paper.get("arxiv")
        or paper.get("paper_url")
        or paper.get("url")
    )
    if aid:
        return aid
    for key in ("paper_id", "id", "paper_title", "title"):
        if paper.get(key):
            if key in {"paper_title", "title"}:
                return f"title:{str(paper[key]).strip().lower()}"
            return str(paper[key])
    return "unknown"


def parse_observation(
    raw: RawObservation,
    *,
    row: dict[str, Any] | None = None,
    repo_edges: list[dict[str, Any]] | None = None,
    arxiv_meta: dict[str, Any] | None = None,
) -> BulkParseResult:
    """
    Build a RESEARCH_PAPER candidate.
    Stars are filled later by enrich_stars; eligible stays false until join complete.
    """
    data = row or _row_from_body(raw)
    if data is None:
        return BulkParseResult(
            ok=False,
            quarantine_reason="parse_drift",
            quarantine_detail={"error": "missing_paper_json"},
        )

    external_id = paper_identity(data)
    title = data.get("title") or data.get("paper_title")
    authors = _authors(data)
    paper_url = (
        data.get("paper_url")
        or data.get("url")
        or data.get("abs_url")
        or raw.final_url
    )
    published = None
    published_raw = None
    for candidate_raw in (
        data.get("published_date") or data.get("date"),
        (arxiv_meta or {}).get("published_date"),
        (arxiv_meta or {}).get("published"),
    ):
        published = parse_aware_datetime(candidate_raw)
        if published is not None:
            published_raw = candidate_raw
            break
    # Prefer v1 publication date when arxiv meta supplies it.
    if arxiv_meta and arxiv_meta.get("v1_published_date"):
        v1 = parse_aware_datetime(arxiv_meta["v1_published_date"])
        if v1 is not None:
            published, published_raw = v1, arxiv_meta["v1_published_date"]
    # A date-only source value is a day, not an instant at midnight UTC.
    date_precision = (
        "day" if isinstance(published_raw, str) and _DATE_ONLY_RE.match(published_raw.strip()) else "instant"
    )

    if not title or not authors or not paper_url or published is None:
        return BulkParseResult(
            ok=False,
            record_type="RESEARCH_PAPER",
            external_id=external_id,
            quarantine_reason="missing_title_authors_or_publication",
            quarantine_detail={
                "title": title,
                "authors": authors,
                "paper_url": paper_url,
                "published_date": data.get("published_date"),
            },
        )

    edges = list(repo_edges or data.get("repos") or data.get("code") or [])
    primary, reject = select_primary_repo(edges)
    if primary is None:
        reason = reject or "no_primary_repo"
        # no-code / dead / ambiguous / dependency
        if reason == "no_repo_edge":
            reason = "no_code_repo"
        return BulkParseResult(
            ok=False,
            record_type="RESEARCH_PAPER",
            external_id=external_id,
            quarantine_reason=reason,
            quarantine_detail={"edges": edges},
            relations=[
                {
                    "relation_type": "IMPLEMENTED_BY",
                    "from_kind": "paper",
                    "from_key": external_id,
                    "to_kind": "repository",
                    "to_key": str(e.get("repo_url") or e.get("url")),
                    "status": "rejected",
                    "reason": reason,
                    "is_primary": False,
                }
                for e in edges
                if e.get("repo_url") or e.get("url")
            ],
        )

    github_url = normalize_github_url(
        primary.get("repo_url") or primary.get("github_url") or primary.get("url")
    )
    assert github_url is not None

    # Pre-joined archive stars count only with an explicit observation time;
    # otherwise the GitHub enrich stage observes them.
    stars = primary.get("stars")
    if stars is None:
        stars = primary.get("stargazers_count")
    if stars is None:
        stars = data.get("github_stars")
    if stars is None:
        stars = data.get("stargazers_count")
    stars_observed = None
    for holder in (primary, data):
        for key in ("starsObservedAt", "stars_observed_at"):
            stars_observed = stars_observed or parse_aware_datetime(holder.get(key))
    stars_i = (
        int(stars)
        if isinstance(stars, int) and not isinstance(stars, bool) and stars >= 0
        and stars_observed is not None
        else None
    )

    collected = get_clock().now()
    payload = {
        "schemaVersion": "1.0",
        "source": {"name": SOURCE_NAME, "url": str(paper_url)},
        "content": {
            "title": str(title),
            "authors": authors,
            "paper_url": str(paper_url),
            "github_url": github_url,
            # Stars filled on enrich; omit until known so join_complete fails.
            **({"github_stars": stars_i} if stars_i is not None else {}),
            "published_date": published.isoformat().replace("+00:00", "Z"),
        },
        "collectedAt": collected.isoformat().replace("+00:00", "Z"),
        "meta": {
            "arxiv_id": versionless_arxiv_id(
                data.get("arxiv_id")
                or data.get("paper_arxiv_id")
                or external_id
            ),
            "primary_repo_ok": True,
            "published_date_precision": date_precision,
            "published_date_raw": published_raw,
            **(
                {"starsObservedAt": stars_observed.isoformat().replace("+00:00", "Z")}
                if stars_i is not None and stars_observed is not None
                else {}
            ),
            "repo_association": {
                "is_official": bool(primary.get("is_official") or primary.get("official")),
                "repo_url": github_url,
                "repo_id": primary.get("repo_id") or primary.get("github_id"),
            },
        },
    }
    evidence = {
        "request_url": raw.request_url,
        "final_url": raw.final_url,
        "content_sha256": raw.content_sha256,
        "title": {"path": "$.title", "value": title},
        "authors": {"path": "$.authors", "value": authors},
        "published_date": {
            "path": "$.published_date|arxiv.v1",
            "value": payload["content"]["published_date"],
        },
        "primary_repo": {
            "url": github_url,
            "official": bool(primary.get("is_official") or primary.get("official")),
        },
    }
    relations = [
        {
            "relation_type": "IMPLEMENTED_BY",
            "from_kind": "paper",
            "from_key": external_id,
            "to_kind": "repository",
            "to_key": github_url,
            "is_primary": True,
            "status": "accepted",
            "evidence": evidence["primary_repo"],
        }
    ]
    eligible = paper_join_complete(
        title=str(title),
        authors=authors,
        paper_url=str(paper_url),
        published_date=published,
        github_url=github_url,
        github_stars=stars_i,
        primary_repo_ok=True,
    )
    enrich = None
    if stars_i is None:
        enrich = {
            "repo_url": github_url,
            "repo_id": primary.get("repo_id") or primary.get("github_id"),
            "paper_external_id": external_id,
            "paper_source_id": SOURCE_ID,
            # Fixture-only hints for enrich_stars loopback (never invent live stars).
            "stargazers_count": primary.get("stargazers_count"),
            "fixture_repo": primary.get("fixture_repo"),
            "simulate": primary.get("simulate"),
        }
    return BulkParseResult(
        ok=True,
        record_type="RESEARCH_PAPER",
        external_id=external_id,
        payload=payload,
        evidence=evidence,
        relations=relations,
        enrich_stars=enrich,
        eligible=eligible,
    )


def apply_stars_to_paper_payload(
    payload: dict[str, Any],
    *,
    stars: int,
    stars_observed_at: str,
    repo_id: str | None = None,
) -> dict[str, Any]:
    """Complete the paper→repo→stars join on an existing candidate payload."""
    content = dict(payload.get("content") or {})
    content["github_stars"] = int(stars)
    meta = dict(payload.get("meta") or {})
    meta["primary_repo_ok"] = True
    meta["starsObservedAt"] = stars_observed_at
    if repo_id:
        assoc = dict(meta.get("repo_association") or {})
        assoc["repo_id"] = str(repo_id)
        meta["repo_association"] = assoc
    out = dict(payload)
    out["content"] = content
    out["meta"] = meta
    return out


def _authors(data: dict[str, Any]) -> list[str]:
    raw = data.get("authors") or data.get("author")
    if raw is None:
        return []
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return parts
    if isinstance(raw, list):
        out: list[str] = []
        for item in raw:
            if isinstance(item, str) and item.strip():
                out.append(item.strip())
            elif isinstance(item, dict):
                name = item.get("name") or item.get("author")
                if name:
                    out.append(str(name).strip())
        return out
    return []


def _split_papers_links(payload: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)], []
    if isinstance(payload, dict):
        papers = payload.get("papers") or payload.get("items") or payload.get("data")
        links = payload.get("links") or payload.get("repo_edges") or payload.get("code_links") or []
        if isinstance(papers, list):
            return [r for r in papers if isinstance(r, dict)], [
                r for r in links if isinstance(r, dict)
            ]
        return [payload], [r for r in links if isinstance(r, dict)]
    return [], []


def _index_links(links: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    indexed: dict[str, list[dict[str, Any]]] = {}
    for link in links:
        keys: list[str] = []
        for k in ("paper_id", "paper_url", "paper_title"):
            if link.get(k):
                keys.append(str(link[k]))
        aid = versionless_arxiv_id(
            link.get("arxiv_id") or link.get("paper_arxiv_id") or link.get("paper_url")
        )
        if aid:
            keys.append(aid)
            keys.append(f"arxiv:{aid}")
        for key in keys:
            indexed.setdefault(key, []).append(link)
    return indexed


def _row_from_body(raw: RawObservation) -> dict[str, Any] | None:
    if raw.body is None:
        return None
    try:
        data = json.loads(raw.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if isinstance(data, dict) and isinstance(data.get("row"), dict):
        return data["row"]
    if isinstance(data, dict):
        return data
    return None


def discover_pages(
    pages: Sequence[Any],
    *,
    partition: str = "default",
) -> tuple[list[DiscoveredItem], str, str]:
    papers: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []
    for page in pages:
        p, l = _split_papers_links(page)
        papers.extend(p)
        links.extend(l)
    return discover_from_payload({"papers": papers, "links": links}, partition=partition)
