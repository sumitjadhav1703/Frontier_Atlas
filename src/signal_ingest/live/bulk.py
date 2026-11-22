"""Bulk-source pilots: 30 items per class + one distinct repo metrics check."""

from __future__ import annotations

import json
from typing import Any

from signal_ingest.adapters.bulk import github as github_adapter
from signal_ingest.adapters.bulk import papers as papers_adapter
from signal_ingest.adapters.bulk import products as products_adapter
from signal_ingest.adapters.bulk import yc as yc_adapter
from signal_ingest.config.clock import get_clock
from signal_ingest.config.settings import Settings
from signal_ingest.crawl.pool import HttpPool, PoolLimits
from signal_ingest.crawl.types import FetchOutcomeKind
from signal_ingest.live.fetch_probe import observation_summary, probe_url
from signal_ingest.live.inventory import update_source_status
from signal_ingest.live.manifests import fingerprint_bytes, write_probe_artifact, write_qualification_manifest
from signal_ingest.live.settings import github_token_from_env

# Documented pilot endpoints (desk research / sources.md). Failures recorded honestly.
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
