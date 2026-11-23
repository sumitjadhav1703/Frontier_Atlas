"""Crawl: discover + enqueue (fixture payloads, or live documented listings)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import UUID

from signal_ingest.adapters.bulk import is_bulk_source
from signal_ingest.adapters.bulk.pipeline import discover_and_enqueue as bulk_discover
from signal_ingest.adapters.monitors.pipeline import (
    discover_and_enqueue as monitor_discover,
)
from signal_ingest.adapters.monitors.pipeline import is_monitor_source
from signal_ingest.config.settings import Mode, Settings, get_settings
from signal_ingest.storage.db import connect
from signal_ingest.storage.records import create_run

_REPO_ROOT = Path(__file__).resolve().parents[3]
_FIXTURE_BULK = _REPO_ROOT / "tests" / "fixtures" / "bulk"
_FIXTURE_MONITORS = _REPO_ROOT / "tests" / "fixtures" / "monitors"

# Map CLI source IDs → fixture files / adapter source ids.
SOURCE_FIXTURES: dict[str, tuple[str, Path]] = {
    "bulk.yc": ("bulk.yc", _FIXTURE_BULK / "yc_companies.json"),
    "bulk.products": ("bulk.products", _FIXTURE_BULK / "products.json"),
    "bulk.papers": ("bulk.papers", _FIXTURE_BULK / "papers.json"),
    "bulk.arxiv": ("bulk.arxiv", _FIXTURE_BULK / "arxiv_2401_00001.json"),
    "bulk.github": ("bulk.github", _FIXTURE_BULK / "github_repos.json"),
}


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_bulk_fixture(adapter_id: str, payload: Any) -> Any:
    """Unwrap labeled fixture envelopes into adapter-shaped payloads."""
    if not isinstance(payload, dict):
        return payload
    # YC / products / papers fixtures often use page_* arrays under _meta.
    page_rows: list[Any] = []
    for key, val in payload.items():
        if key.startswith("_"):
            continue
        if key.startswith("page_") and isinstance(val, list):
            page_rows.extend(r for r in val if isinstance(r, dict))
    if page_rows:
        return page_rows
    if adapter_id == "bulk.github" and "repos" in payload:
        return payload.get("repos") or []
    if adapter_id == "bulk.arxiv" and "ids" in payload:
        return payload
    if adapter_id == "bulk.papers":
        for key in ("papers", "items", "results", "data"):
            if isinstance(payload.get(key), list):
                return payload[key]
    if adapter_id == "bulk.products":
        for key in ("products", "items", "results", "data"):
            if isinstance(payload.get(key), list):
                return payload[key]
    return payload


def resolve_fixture_payload(source_id: str) -> tuple[str, Any]:
    """Return (adapter_source_id, payload) for fixture crawl."""
    if source_id in SOURCE_FIXTURES:
        adapter_id, path = SOURCE_FIXTURES[source_id]
        return adapter_id, _normalize_bulk_fixture(adapter_id, _load_json(path))
    # Allow short aliases used in README.
    aliases = {
        "yc": "bulk.yc",
        "products": "bulk.products",
        "papers": "bulk.papers",
        "arxiv": "bulk.arxiv",
        "github": "bulk.github",
    }
    if source_id in aliases:
        return resolve_fixture_payload(aliases[source_id])
    monitor_path = _FIXTURE_MONITORS / "sources" / f"{source_id}.json"
    if monitor_path.is_file():
        return source_id, _load_json(monitor_path)
    raise FileNotFoundError(
        f"no fixture payload for source={source_id!r}; "
        f"known bulk: {sorted(SOURCE_FIXTURES)}"
    )


def crawl_source(
    *,
    source_id: str,
    settings: Settings | None = None,
    run_id: UUID | None = None,
    partition: str = "default",
) -> dict[str, Any]:
    """
    Discovery into the shared queue/DB.

    Fixture mode reads labeled local fixtures. Live mode fetches the documented
    bulk listing once (live.bulk.fetch_bulk_listing) and enqueues into `run_id`
    so several sources share one run.
    """
    cfg = settings or get_settings()
    listing_meta: dict[str, Any] = {}
    if cfg.mode is Mode.LIVE and is_monitor_source(source_id):
        if run_id is None:
            raise ValueError("live monitor crawl needs --run (enqueue into the acquisition run)")
        from signal_ingest.live.monitors import crawl_monitor_live

        return {
            "run_id": str(run_id),
            "source_id": source_id,
            "mode": cfg.mode.value,
            "discover": asyncio.run(crawl_monitor_live(cfg, source_id, run_id=str(run_id))),
            "raw_root": str(cfg.assert_mode_raw_root()),
        }
    if cfg.mode is Mode.LIVE:
        from signal_ingest.live.bulk import LISTINGS, fetch_bulk_listing

        aliases = {"yc": "bulk.yc", "products": "bulk.products", "papers": "bulk.papers", "openrouter": "bulk.openrouter"}
        adapter_id = aliases.get(source_id, source_id)
        if adapter_id not in LISTINGS:
            raise ValueError(f"live crawl supports {sorted(LISTINGS)}; got {source_id!r}")
        fetched = asyncio.run(fetch_bulk_listing(adapter_id, cfg))
        payload = (
            {"papers": fetched["papers"], "links": fetched["links"]}
            if adapter_id == "bulk.papers"
            else fetched["rows"]
        )
        listing_meta = {k: v for k, v in fetched.items() if k not in {"rows", "papers", "links"}}
        from signal_ingest.live.inventory import update_source_status

        update_source_status(
            LISTINGS[adapter_id][0],
            status="qualified" if fetched["rows"] else "disabled",
            probe={
                "probe_artifact": fetched.get("probe_artifact"),
                "fetch_status": "ok" if fetched["rows"] else "empty",
                "observed_at": (fetched.get("listing") or {}).get("fetched_at"),
                "pilot_count": len(fetched["rows"]),
            },
        )
        if adapter_id == "bulk.papers":
            complete = int(fetched.get("papers_metadata_complete") or 0)
            update_source_status(
                "bulk_openalex_paper_metadata",
                status="qualified" if complete else "disabled",
                probe={
                    "probe_artifact": fetched.get("probe_artifact"),
                    "fetch_status": "ok" if complete else "empty",
                    "pilot_count": complete,
                },
            )
    else:
        adapter_id, payload = resolve_fixture_payload(source_id)
    live = cfg.mode is Mode.LIVE
    with connect(cfg) as conn:
        rid = run_id or create_run(
            conn,
            settings=cfg,
            metadata={
                "command": "crawl",
                "source_id": adapter_id,
                "fixture": not live,
                "not_for_live_export": not live,
            },
        )
        if is_bulk_source(adapter_id):
            result = bulk_discover(
                conn,
                source_id=adapter_id,
                payload=payload,
                partition=partition,
                run_id=rid,
            )
        elif is_monitor_source(adapter_id):
            result = monitor_discover(
                conn,
                source_id=adapter_id,
                payload=payload,
                run_id=rid,
            )
        else:
            raise ValueError(f"unknown source adapter: {adapter_id}")
        conn.execute(
            "UPDATE runs SET status = %s, metadata = metadata || %s::jsonb WHERE id = %s",
            (
                "crawling",
                json.dumps(
                    {"last_source": adapter_id, **({f"listing:{adapter_id}": listing_meta} if live else {})},
                    default=str,
                ),
                str(rid),
            ),
        )
        conn.commit()
    return {
        "run_id": str(rid),
        "source_id": adapter_id,
        "mode": cfg.mode.value,
        "discover": {k: v for k, v in result.items() if k != "checkpoint"} if live else result,
        **({"listing": listing_meta} if live else {}),
        "raw_root": str(cfg.assert_mode_raw_root()),
    }


__all__ = ["SOURCE_FIXTURES", "crawl_source", "resolve_fixture_payload"]
