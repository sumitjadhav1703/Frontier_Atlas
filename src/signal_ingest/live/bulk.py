"""Bulk-source pilots (30 items per class) and full live listing fetch for acquisition."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from signal_ingest.adapters.bulk import github as github_adapter
from signal_ingest.adapters.bulk import papers as papers_adapter
from signal_ingest.adapters.bulk import products as products_adapter
from signal_ingest.adapters.bulk import yc as yc_adapter
from signal_ingest.config.clock import get_clock
from signal_ingest.adapters.bulk.mapping import normalize_github_url, versionless_arxiv_id
from signal_ingest.config.settings import Mode, Settings
from signal_ingest.crawl.fetch import FetchPolicy, fetch_with_retries
from signal_ingest.crawl.pool import HttpPool, PoolLimits
from signal_ingest.crawl.types import FetchOutcomeKind
from signal_ingest.live.fetch_probe import observation_summary, probe_url
from signal_ingest.live.inventory import update_source_status
from signal_ingest.live.manifests import fingerprint_bytes, write_probe_artifact, write_qualification_manifest
from signal_ingest.live.settings import github_token_from_env

log = logging.getLogger(__name__)

# Documented pilot endpoints (desk research / sources.md). Failures recorded honestly.
# Pilots only: the acquisition path (fetch_bulk_listing) has no item cap.
PILOT_LIMIT = 30

# Inventory source_id → (adapter_id, fetch_urls tried in order)
BULK_PILOTS: dict[str, dict[str, Any]] = {
    "bulk_yc_startups": {
        "adapter": "bulk.yc",
        "class": "startups",
        "urls": [
            # Documented yc-oss AI tag partition (sources.md / yc-oss README).
            "https://yc-oss.github.io/api/tags/ai.json",
            "https://raw.githubusercontent.com/yc-oss/api/main/tags/ai.json",
            "https://yc-oss.github.io/api/tags/artificial-intelligence.json",
            "https://yc-oss.github.io/api/tags/generative-ai.json",
        ],
    },
    "bulk_agipicks_products": {
        "adapter": "bulk.products",
        "class": "products",
        "urls": [
            # Documented open dataset on agipicks.com/data/ (CC BY 4.0 tools.json).
            "https://agipicks.com/data/tools.json",
        ],
    },
    "bulk_papers_with_code": {
        "adapter": "bulk.papers",
        "class": "papers",
        # HuggingFace datasets-server rows API (documented pwc-archive; robots-safe).
        # export.arxiv.org Disallow:/ blocks our crawler UA despite public Atom API.
        "hf_rows": (
            "https://datasets-server.huggingface.co/rows"
            "?dataset=pwc-archive/papers-with-abstracts"
            "&config=default&split=train&offset=0&length=30"
        ),
        "hf_links": (
            "https://datasets-server.huggingface.co/rows"
            "?dataset=pwc-archive/links-between-paper-and-code"
            "&config=default&split=train&offset=0&length=30"
        ),
    },
    "bulk_github_metrics": {
        "adapter": "bulk.github",
        "class": "repo_metrics",
        "repo": "openai/whisper",
    },
}


async def qualify_bulk_pilots(
    settings: Settings,
    *,
    update_inventory: bool = True,
    limit: int = PILOT_LIMIT,
) -> dict[str, Any]:
    pool = HttpPool(
        PoolLimits(
            user_agent="signal-ingest/0.1 (+live-bulk-pilot)",
            # Bulk JSON pilots (YC dump) can exceed default decoded caps.
            max_compressed_bytes=12_000_000,
            max_decoded_bytes=40_000_000,
            read_timeout_s=60.0,
            overall_timeout_s=90.0,
        )
    )
    results: list[dict[str, Any]] = []
    try:
        for inventory_id, spec in BULK_PILOTS.items():
            if spec["adapter"] == "bulk.github":
                results.append(
                    await _pilot_github(settings, inventory_id, spec, pool=pool)
                )
            elif inventory_id == "bulk_papers_with_code":
                results.append(
                    await _pilot_papers(settings, inventory_id, spec, pool=pool, limit=limit)
                )
            else:
                results.append(
                    await _pilot_json_class(
                        settings, inventory_id, spec, pool=pool, limit=limit
                    )
                )
    finally:
        await pool.aclose()

    if update_inventory:
        for row in results:
            update_source_status(
                row["source_id"],
                status=str(row["status"]),
                probe=row.get("probe") or {},
            )

    by_class = {r["class"]: r.get("pilot_count", 0) for r in results}
    manifest = {
        "track": "bulk_pilots",
        "pilot_limit": limit,
        "classes": by_class,
        "sources": results,
        "note": "pilot_count is observed fetch/parse yield; not a fabricated 1000",
        "fabricated_1000": False,
    }
    path = write_qualification_manifest(settings, name="bulk_pilots", payload=manifest)
    manifest["manifest_path"] = str(path)
    return manifest


async def _pilot_json_class(
    settings: Settings,
    inventory_id: str,
    spec: dict[str, Any],
    *,
    pool: HttpPool,
    limit: int,
) -> dict[str, Any]:
    last_error = None
    for url in spec.get("urls") or []:
        obs = await probe_url(url, settings=settings, pool=pool)
        summary = observation_summary(obs)
        probe_path = write_probe_artifact(
            settings,
            source_id=inventory_id,
            kind="bulk_discovery",
            body=obs.body,
            meta=summary,
        )
        if obs.outcome is not FetchOutcomeKind.OK or not obs.body:
            last_error = obs.reason or summary["outcome"]
            continue
        try:
            payload = json.loads(obs.body.decode("utf-8", errors="replace"))
        except json.JSONDecodeError as exc:
            last_error = f"json_decode: {exc}"
            continue
        items, rev, partition_key = _discover(spec["adapter"], payload)
        pilot_items = items[:limit]
        status = "qualified" if pilot_items else "disabled"
        sample_rows = []
        for item in pilot_items[: min(50, limit)]:
            row = dict((item.metadata or {}).get("row") or {})
            sample_rows.append(
                {
                    "id": item.external_id,
                    "slug": row.get("slug") or item.external_id,
                    "name": row.get("name") or row.get("company_name") or item.external_id,
                    "url": item.url,
                    "website": row.get("website") or row.get("url"),
                }
            )
        probe = {
            "probe_artifact": str(probe_path),
            "fetch_status": summary["outcome"],
            "schema_fingerprint": fingerprint_bytes(obs.body),
            "accepted_fraction": None,
            "sample_date": get_clock().now().isoformat(),
            "rate_limit_headers": summary.get("headers") or {},
            "observed_at": get_clock().now().isoformat(),
            "error": None if status == "qualified" else "zero_items",
            "pilot_count": len(pilot_items),
        }
        return {
            "source_id": inventory_id,
            "adapter": spec["adapter"],
            "class": spec["class"],
            "status": status,
            "fetch_url": url,
            "revision": rev,
            "partition_key": partition_key,
            "pilot_count": len(pilot_items),
            "sample_ids": [i.external_id for i in pilot_items[:10]],
            "sample_urls": [i.url for i in pilot_items[:5]],
            "sample_rows": sample_rows,
            "discovery": summary,
            "probe": probe,
        }
    probe = {
        "probe_artifact": None,
        "fetch_status": "failed",
        "error": last_error or "all_urls_failed",
        "observed_at": get_clock().now().isoformat(),
        "pilot_count": 0,
    }
    return {
        "source_id": inventory_id,
        "adapter": spec["adapter"],
        "class": spec["class"],
        "status": "disabled",
        "pilot_count": 0,
        "error": last_error,
        "probe": probe,
    }


async def _pilot_papers(
    settings: Settings,
    inventory_id: str,
    spec: dict[str, Any],
    *,
    pool: HttpPool,
    limit: int,
) -> dict[str, Any]:
    """Bounded 30-item pilot from pwc-archive via HuggingFace datasets-server."""
    url = str(spec.get("hf_rows") or "")
    obs = await probe_url(url, settings=settings, pool=pool)
    summary = observation_summary(obs)
    probe_path = write_probe_artifact(
        settings,
        source_id=inventory_id,
        kind="bulk_pwc_hf_pilot",
        body=obs.body,
        meta=summary,
    )
    if obs.outcome is not FetchOutcomeKind.OK or not obs.body:
        return {
            "source_id": inventory_id,
            "adapter": spec["adapter"],
            "class": spec["class"],
            "status": "disabled",
            "pilot_count": 0,
            "discovery": summary,
            "probe": {
                "probe_artifact": str(probe_path),
                "fetch_status": summary["outcome"],
                "error": obs.reason or summary["outcome"],
                "observed_at": get_clock().now().isoformat(),
                "pilot_count": 0,
            },
        }

    try:
        payload = json.loads(obs.body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        return {
            "source_id": inventory_id,
            "adapter": spec["adapter"],
            "class": spec["class"],
            "status": "disabled",
            "pilot_count": 0,
            "discovery": summary,
            "probe": {
                "probe_artifact": str(probe_path),
                "fetch_status": "invalid",
                "error": f"json_decode: {exc}",
                "observed_at": get_clock().now().isoformat(),
                "pilot_count": 0,
            },
        }

    papers = _hf_rows_to_papers(payload, limit=limit)
    links: list[dict[str, Any]] = []
    links_url = str(spec.get("hf_links") or "")
    if links_url:
        links_obs = await probe_url(links_url, settings=settings, pool=pool)
        if links_obs.outcome is FetchOutcomeKind.OK and links_obs.body:
            try:
                links_payload = json.loads(links_obs.body.decode("utf-8"))
                links = _hf_rows_to_links(links_payload, limit=limit)
            except json.JSONDecodeError:
                links = []

    items, rev, partition_key = papers_adapter.discover_from_payload(
        {"papers": papers, "links": links}
    )
    pilot_items = items[:limit]
    status = "qualified" if pilot_items else "disabled"
    return {
        "source_id": inventory_id,
        "adapter": spec["adapter"],
        "class": spec["class"],
        "status": status,
        "fetch_url": url,
        "revision": rev,
        "partition_key": partition_key,
        "pilot_count": len(pilot_items),
        "sample_ids": [i.external_id for i in pilot_items[:10]],
        "sample_urls": [i.url for i in pilot_items[:5]],
        "discovery": summary,
        "links_joined": len(links),
        "alternate_note": "pwc-archive via huggingface datasets-server rows API",
        "probe": {
            "probe_artifact": str(probe_path),
            "fetch_status": summary["outcome"],
            "schema_fingerprint": fingerprint_bytes(obs.body),
            "sample_date": get_clock().now().isoformat(),
            "observed_at": get_clock().now().isoformat(),
            "error": None if status == "qualified" else "zero_items",
            "pilot_count": len(pilot_items),
        },
    }


async def _pilot_github(
    settings: Settings,
    inventory_id: str,
    spec: dict[str, Any],
    *,
    pool: HttpPool,
) -> dict[str, Any]:
    repo = str(spec.get("repo") or "openai/whisper")
    api_url = f"https://api.github.com/repos/{repo}"
    headers: dict[str, str] = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "signal-ingest-live-pilot",
    }
    token = github_token_from_env()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    obs = await probe_url(api_url, settings=settings, pool=pool, headers=headers)
    summary = observation_summary(obs)
    # Never persist Authorization header material.
    summary["headers"] = {
        k: v
        for k, v in (summary.get("headers") or {}).items()
        if "auth" not in k.lower() and "token" not in k.lower()
    }
    probe_path = write_probe_artifact(
        settings,
        source_id=inventory_id,
        kind="repo_metrics",
        body=obs.body,
        meta={**summary, "repo": repo, "auth_used": bool(token)},
    )
    stars = None
    repo_id = None
    if obs.outcome is FetchOutcomeKind.OK and obs.body:
        try:
            payload = json.loads(obs.body.decode("utf-8"))
            stars = payload.get("stargazers_count")
            repo_id = payload.get("id")
        except json.JSONDecodeError:
            payload = None
    else:
        payload = None

    # One distinct repo check qualifies the metrics route when stars are present.
    status = "qualified" if isinstance(stars, int) else "disabled"
    items, rev, partition_key = github_adapter.discover_repos(
        [{"repo_url": f"https://github.com/{repo}", "repo_id": repo_id, "stars": stars}]
    )
    return {
        "source_id": inventory_id,
        "adapter": spec["adapter"],
        "class": spec["class"],
        "status": status,
        "repo": repo,
        "repo_id": repo_id,
        "stargazers_count": stars,
        "auth_used": bool(token),
        "pilot_count": 1 if status == "qualified" else 0,
        "revision": rev,
        "partition_key": partition_key,
        "discovery": summary,
        "sample_ids": [i.external_id for i in items[:1]],
        "probe": {
            "probe_artifact": str(probe_path),
            "fetch_status": summary["outcome"],
            "schema_fingerprint": fingerprint_bytes(obs.body),
            "sample_date": get_clock().now().isoformat(),
            "rate_limit_headers": {
                k: v
                for k, v in (obs.headers or {}).items()
                if "ratelimit" in k.lower() or "retry" in k.lower()
            },
            "observed_at": get_clock().now().isoformat(),
            "error": None if status == "qualified" else (obs.reason or summary["outcome"]),
            "pilot_count": 1 if status == "qualified" else 0,
        },
    }


def _discover(adapter: str, payload: Any):
    if adapter == "bulk.yc":
        return yc_adapter.discover_from_payload(payload)
    if adapter == "bulk.products":
        return products_adapter.discover_from_payload(payload)
    if adapter == "bulk.papers":
        return papers_adapter.discover_from_payload(payload)
    raise ValueError(f"unsupported bulk adapter {adapter}")


def _hf_rows_to_papers(payload: dict[str, Any], *, limit: int) -> list[dict[str, Any]]:
    papers: list[dict[str, Any]] = []
    for entry in (payload.get("rows") or [])[:limit]:
        row = entry.get("row") if isinstance(entry, dict) else None
        if not isinstance(row, dict):
            continue
        authors = row.get("authors") or []
        if isinstance(authors, str):
            authors = [authors]
        papers.append(
            {
                "arxiv_id": row.get("arxiv_id"),
                "title": row.get("title"),
                "authors": list(authors) if isinstance(authors, list) else [],
                "paper_url": row.get("paper_url") or row.get("url_abs"),
                "published": row.get("date"),
                "_synthetic": False,
                "_lineage": "live_pwc_hf_rows_pilot",
            }
        )
    return papers


def _hf_rows_to_links(payload: dict[str, Any], *, limit: int) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    for entry in (payload.get("rows") or [])[:limit]:
        row = entry.get("row") if isinstance(entry, dict) else None
        if not isinstance(row, dict):
            continue
        links.append(
            {
                "paper_url": row.get("paper_url"),
                "paper_arxiv_id": row.get("paper_arxiv_id"),
                "repo_url": row.get("repo_url"),
                "is_official": row.get("is_official"),
                "framework": row.get("framework"),
                "_synthetic": False,
            }
        )
    return links


# ---------------------------------------------------------------------------
# B8 acquisition: one listing GET per source (papers: paged HF /rows + local join)
# ---------------------------------------------------------------------------

HF_ROWS_URL = (
    "https://datasets-server.huggingface.co/rows?dataset=pwc-archive/{dataset}"
    "&config=default&split=train&offset={offset}&length={length}"
)
HF_LINKS = "links-between-paper-and-code"
# Paper metadata: OpenAlex works API (CC0, robots Allow: /), by arXiv DOI 10.48550/arxiv.<id>.
OPENALEX_WORKS = "https://api.openalex.org/works"
OPENALEX_SELECT = "doi,title,publication_date,authorships,ids,locations"
OPENALEX_BATCH = 50
HF_PAGE = 100  # datasets-server maximum page length
HF_429_RETRIES = 6
HF_429_BACKOFF_S = 75.0

# adapter id -> (inventory id, default listing URL, rows key in the JSON body)
LISTINGS: dict[str, tuple[str, str, str | None]] = {
    "bulk.yc": ("bulk_yc_startups", "https://yc-oss.github.io/api/tags/ai.json", None),
    "bulk.products": ("bulk_agipicks_products", "https://agipicks.com/data/tools.json", "tools"),
    "bulk.openrouter": ("bulk_openrouter_products", "https://openrouter.ai/api/v1/models", "data"),
    "bulk.papers": ("bulk_papers_with_code", HF_ROWS_URL, "rows"),
}

LISTING_LIMITS = dict(
    user_agent="signal-ingest/0.1 (+assessment)",
    max_compressed_bytes=12_000_000,
    max_decoded_bytes=40_000_000,
    read_timeout_s=60.0,
    overall_timeout_s=90.0,
)


async def _get_listing(
    url: str, settings: Settings, pool: HttpPool, policy: FetchPolicy
) -> tuple[Any, dict[str, Any], Any]:
    """GET one listing page -> (json, _listing provenance, observation). Raises on failure."""
    obs = await fetch_with_retries(
        url, policy, pool=pool, settings=settings, sleep=True,
        headers={"Accept-Encoding": "gzip, deflate"},  # pool cannot inflate br/zstd
    )
    if obs.outcome is not FetchOutcomeKind.OK or not obs.body:
        raise RuntimeError(f"listing fetch failed {url}: {obs.outcome.value} {obs.reason or obs.status_code}")
    payload = json.loads(obs.body.decode("utf-8"))
    listing = {
        "url": obs.final_url or url,
        "sha256": fingerprint_bytes(obs.body),
        "fetched_at": (obs.fetched_at or get_clock().now()).isoformat(),
    }
    return payload, listing, obs


def _probe(settings: Settings, source_id: str, kind: str, obs: Any, meta: dict[str, Any]) -> str | None:
    if settings.mode is not Mode.LIVE:
        return None
    inventory = LISTINGS[source_id][0]
    return str(
        write_probe_artifact(
            settings,
            source_id=inventory,
            kind=kind,
            body=obs.body,
            meta={**observation_summary(obs), **meta},
        )
    )


async def fetch_bulk_listing(
    source_id: str,
    settings: Settings,
    *,
    url: str | None = None,
    pool: HttpPool | None = None,
    link_pages: int = 300,
    target_papers: int = 1700,
    concurrency: int = 1,
    hf_interval_s: float = 3.2,
    hf_rows_url: str = HF_ROWS_URL,
    openalex_url: str = OPENALEX_WORKS,
    openalex_interval_s: float = 1.0,
) -> dict[str, Any]:
    """Fetch a documented bulk listing; every row carries _listing={url, sha256, fetched_at}.

    yc/products/openrouter: one GET of the listing URL. papers: HF datasets-server
    /rows pages of official paper->code links (arXiv papers only), then OpenAlex
    metadata by arXiv DOI in batches of OPENALEX_BATCH.
    Returns {"rows": [...], ...}; probe artifacts are written in live mode only.
    """
    if source_id not in LISTINGS:
        raise ValueError(f"no documented listing for {source_id!r}; known: {sorted(LISTINGS)}")
    own_pool = pool is None
    if pool is None and source_id == "bulk.papers":
        # datasets-server sits behind a CloudFront rate rule (~100 GETs / 5 min observed
        # 2026-11-22, 429 without Retry-After): one request at a time, paced.
        pool = HttpPool(PoolLimits(**LISTING_LIMITS, per_origin_concurrency=1, per_origin_min_interval_s=hf_interval_s))
    pool = pool or HttpPool(PoolLimits(**LISTING_LIMITS))
    policy = FetchPolicy(mode=settings.mode, max_retries=4, retry_cap_s=30.0)
    try:
        if source_id == "bulk.papers":
            oa_pool = HttpPool(
                PoolLimits(**LISTING_LIMITS, per_origin_concurrency=1, per_origin_min_interval_s=openalex_interval_s)
            ) if own_pool else pool
            try:
                return await _fetch_papers_listing(
                    settings, pool, policy, oa_pool=oa_pool,
                    link_pages=link_pages, target_papers=target_papers, concurrency=concurrency,
                    hf_rows_url=hf_rows_url, openalex_url=openalex_url,
                )
            finally:
                if oa_pool is not pool:
                    await oa_pool.aclose()
        inventory, default_url, key = LISTINGS[source_id]
        target = url or default_url
        payload, listing, obs = await _get_listing(target, settings, pool, policy)
        rows = payload.get(key) if key and isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            raise RuntimeError(f"listing drift at {target}: no row list under {key!r}")
        stamped = [{**r, "_listing": listing} for r in rows if isinstance(r, dict)]
        extra = {k: payload.get(k) for k in ("license", "licenseUrl", "attribution", "generatedAt", "count")} if isinstance(payload, dict) else {}
        probe = _probe(settings, source_id, "listing", obs, {"listing": listing, "row_count": len(stamped)})
        return {"rows": stamped, "listing": listing, "row_count": len(stamped), "probe_artifact": probe, "attribution": extra}
    finally:
        if own_pool:
            await pool.aclose()


async def _hf_pages(
    dataset: str, offsets: list[int], settings: Settings, pool: HttpPool, policy: FetchPolicy, concurrency: int,
    rows_url: str = HF_ROWS_URL,
) -> list[tuple[int, list[dict[str, Any]], dict[str, Any] | None, Any, str | None]]:
    sem = asyncio.Semaphore(concurrency)

    async def one(off: int):
        url = rows_url.format(dataset=dataset, offset=off, length=HF_PAGE)
        async with sem:
            err = None
            for attempt in range(HF_429_RETRIES + 1):
                try:
                    payload, listing, obs = await _get_listing(url, settings, pool, policy)
                    err = None
                    break
                except Exception as exc:  # recorded, page skipped; never padded
                    err = f"{type(exc).__name__}: {exc}"[:300]
                    if "429" not in err or attempt == HF_429_RETRIES or settings.mode is not Mode.LIVE:
                        break
                    log.warning("hf rate limited offset=%s; backing off %ss", off, HF_429_BACKOFF_S)
                    await asyncio.sleep(HF_429_BACKOFF_S)
            if err:
                return off, [], None, None, err
        rows = [e["row"] for e in payload.get("rows") or [] if isinstance(e, dict) and isinstance(e.get("row"), dict)]
        return off, rows, listing, obs, None

    return list(await asyncio.gather(*(one(o) for o in offsets)))


def openalex_batch_url(base: str, arxiv_ids: list[str]) -> str:
    """works?filter=doi:<d1>|<d2>... (no mailto / api key: no personal data in the URL)."""
    dois = "|".join(f"10.48550/arxiv.{aid}" for aid in arxiv_ids)
    return f"{base}?filter=doi:{dois}&select={OPENALEX_SELECT}&per-page=100"


def _openalex_aids(work: dict[str, Any]) -> set[str]:
    """arXiv ids a work carries. OpenAlex merges a preprint into its published version, so
    the primary DOI may be the journal's; the arXiv DOI / abs URL survive in locations."""
    urls = [str(work.get("doi") or ""), str((work.get("ids") or {}).get("doi") or "")]
    for loc in work.get("locations") or []:
        if isinstance(loc, dict):
            urls += [str(loc.get("landing_page_url") or ""), str(loc.get("pdf_url") or "")]
    out: set[str] = set()
    for u in urls:
        low = u.lower()
        if "10.48550/arxiv." in low:
            aid = versionless_arxiv_id(low.split("10.48550/arxiv.", 1)[1])
        elif "arxiv.org/abs/" in low or "arxiv.org/pdf/" in low:
            aid = versionless_arxiv_id(u)
        else:
            aid = None
        if aid:
            out.add(aid)
    return out


def paper_row_from_openalex(
    aid: str, work: dict[str, Any] | None, listing: dict[str, Any], links: list[dict[str, Any]]
) -> dict[str, Any]:
    """Papers-adapter row. Missing work/title/authors/date -> _metadata_missing (quarantined)."""
    authors = [
        " ".join(str((a.get("author") or {}).get("display_name") or "").split())
        for a in (work or {}).get("authorships") or []
        if isinstance(a, dict)
    ]
    authors = [a for a in authors if a]
    row: dict[str, Any] = {
        "arxiv_id": aid,
        "paper_url": f"https://arxiv.org/abs/{aid}",
        "title": (work or {}).get("title"),
        "authors": authors,
        "date": (work or {}).get("publication_date"),
        "openalex_id": ((work or {}).get("ids") or {}).get("openalex"),
        "_synthetic": False,
        "_lineage": "live_openalex_by_arxiv_doi",
        "_listing": listing,
        "repo_association_listings": sorted({str((l.get("_listing") or {}).get("url")) for l in links}),
    }
    if not (work and row["title"] and authors and row["date"]):
        row["_metadata_missing"] = True
    return row


async def _fetch_papers_listing(
    settings: Settings,
    pool: HttpPool,
    policy: FetchPolicy,
    *,
    oa_pool: HttpPool,
    link_pages: int,
    target_papers: int,
    concurrency: int,
    hf_rows_url: str = HF_ROWS_URL,
    openalex_url: str = OPENALEX_WORKS,
) -> dict[str, Any]:
    # /filter answered 500 "dataset index is loading" on 2026-11-22/23, so page
    # links /rows; papers-with-abstracts is not joined (random order, no locality).
    by_aid: dict[str, list[dict[str, Any]]] = {}
    errors: list[dict[str, Any]] = []
    first_links: tuple[dict[str, Any], Any] | None = None
    pages_read = 0
    done = False
    for start in range(0, link_pages, 10):
        offsets = [i * HF_PAGE for i in range(start, min(start + 10, link_pages))]
        for off, rows, listing, obs, err in await _hf_pages(
            HF_LINKS, offsets, settings, pool, policy, concurrency, rows_url=hf_rows_url
        ):
            if err:
                errors.append({"dataset": HF_LINKS, "offset": off, "error": err})
                continue
            pages_read += 1
            if first_links is None:
                first_links = (listing, obs)
            if not rows:
                done = True  # end of dataset
            for row in rows:
                aid = versionless_arxiv_id(row.get("paper_arxiv_id"))
                if row.get("is_official") is True and aid and normalize_github_url(row.get("repo_url")):
                    by_aid.setdefault(aid, []).append({**row, "_synthetic": False, "_listing": listing})
        if done or len(by_aid) >= target_papers:
            break
    aids = list(by_aid)[:target_papers]
    papers: list[dict[str, Any]] = []
    first_oa: tuple[dict[str, Any], Any] | None = None
    for i in range(0, len(aids), OPENALEX_BATCH):
        chunk = aids[i : i + OPENALEX_BATCH]
        url = openalex_batch_url(openalex_url, chunk)
        try:
            payload, listing, obs = await _get_listing(url, settings, oa_pool, policy)
        except Exception as exc:  # whole batch unresolved: not enqueued, recorded
            errors.append({"dataset": "openalex", "offset": i, "error": f"{type(exc).__name__}: {exc}"[:300]})
            continue
        if first_oa is None:
            first_oa = (listing, obs)
        works: dict[str, dict[str, Any]] = {}
        wanted = set(chunk)
        for work in payload.get("results") or []:
            if isinstance(work, dict):
                for key in _openalex_aids(work) & wanted:
                    works.setdefault(key, work)
        papers.extend(paper_row_from_openalex(aid, works.get(aid), listing, by_aid[aid]) for aid in chunk)
    links = [link for p in papers for link in by_aid[str(p["arxiv_id"])]]
    complete = sum(1 for p in papers if not p.get("_metadata_missing"))
    summary = {
        "link_pages_read": pages_read,
        "official_arxiv_papers": len(by_aid),
        "papers_with_metadata_request": len(papers),
        "papers_metadata_complete": complete,
        "papers_metadata_missing": len(papers) - complete,
        "page_errors": len(errors),
        "join_key": "versionless arXiv id (links.paper_arxiv_id = OpenAlex DOI 10.48550/arxiv.<id>)",
        "access_path": "datasets-server /rows (links) + api.openalex.org/works (metadata)",
    }
    probe = None
    for kind, first in (("hf_links_page", first_links), ("openalex_batch", first_oa)):
        if first is not None:
            probe = _probe(settings, "bulk.papers", kind, first[1], {"listing": first[0], **summary, "errors": errors[:20]}) or probe
    if not papers and errors:
        raise RuntimeError(f"papers listing failed: {errors[:3]}")
    return {"rows": papers, "papers": papers, "links": links, "errors": errors, "probe_artifact": probe, **summary}
