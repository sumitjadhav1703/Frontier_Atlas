"""Qualify ten monitor sources: one discovery + one detail probe each."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from signal_ingest.adapters.monitors.config import load_monitor_configs
from signal_ingest.adapters.monitors.detail import parse_detail
from signal_ingest.adapters.monitors.discover import discover_list
from signal_ingest.config.clock import get_clock
from signal_ingest.config.settings import Settings
from signal_ingest.crawl.pool import HttpPool, PoolLimits
from signal_ingest.crawl.types import FetchOutcomeKind, RawObservation
from signal_ingest.live.fetch_probe import observation_summary, probe_url
from signal_ingest.live.inventory import update_source_status
from signal_ingest.live.manifests import fingerprint_bytes, write_probe_artifact, write_qualification_manifest


async def qualify_monitors(
    settings: Settings,
    *,
    update_inventory: bool = True,
) -> dict[str, Any]:
    """
    Run one discovery+detail pair per configured news/jobs monitor.

    A successful probe may yield zero fresh signals — still records health.
    """
    configs = load_monitor_configs()
    pool = HttpPool(PoolLimits(user_agent="signal-ingest/0.1 (+live-monitor-qualify)"))
    results: list[dict[str, Any]] = []
    try:
        for cfg in configs:
            results.append(
                await _qualify_one(settings, cfg.source_id, cfg.documented_access_url, pool=pool)
            )
    finally:
        await pool.aclose()

    qualified = [r for r in results if r.get("status") == "qualified"]
    disabled = [r for r in results if r.get("status") == "disabled"]
    if update_inventory:
        for row in results:
            update_source_status(
                row["source_id"],
                status=str(row["status"]),
                probe=row.get("probe") or {},
            )

    manifest = {
        "track": "monitors",
        "sources_total": len(results),
        "qualified_count": len(qualified),
        "disabled_count": len(disabled),
        "sources": results,
        "note": "success may include zero fresh signals; counts are observed not fabricated",
    }
    path = write_qualification_manifest(settings, name="monitors_qualification", payload=manifest)
    manifest["manifest_path"] = str(path)
    return manifest


# When HTML topic indexes return 405/challenge, try publisher feeds (same origin).
_DISCOVERY_FALLBACKS: dict[str, tuple[str, ...]] = {
    "news_arstechnica": (
        "https://feeds.arstechnica.com/arstechnica/index",
        "https://arstechnica.com/feed/",
    ),
}


async def _qualify_one(
    settings: Settings,
    source_id: str,
    discovery_url: str,
    *,
    pool: HttpPool,
) -> dict[str, Any]:
    from signal_ingest.adapters.monitors.config import load_monitor_config

    cfg = load_monitor_config(source_id)
    now = get_clock().now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    candidates = (discovery_url, *(_DISCOVERY_FALLBACKS.get(source_id) or ()))
    discovery = None
    for url in candidates:
        discovery = await probe_url(url, settings=settings, pool=pool)
        if discovery.outcome is FetchOutcomeKind.OK and discovery.body:
            discovery_url = url
            break
    assert discovery is not None
    disc_summary = observation_summary(discovery)
    disc_path = write_probe_artifact(
        settings,
        source_id=source_id,
        kind="discovery",
        body=discovery.body,
        meta=disc_summary,
    )

    if discovery.outcome is not FetchOutcomeKind.OK or not discovery.body:
        probe = {
            "probe_artifact": str(disc_path),
            "fetch_status": disc_summary["outcome"],
            "schema_fingerprint": fingerprint_bytes(discovery.body),
            "sample_date": None,
            "rate_limit_headers": _rate_headers(discovery),
            "observed_at": now.isoformat(),
            "error": discovery.reason or f"discovery_{disc_summary['outcome']}",
            "pilot_count": 0,
        }
        return {
            "source_id": source_id,
            "status": "disabled",
            "discovery": disc_summary,
            "detail": None,
            "discovered_count": 0,
            "fresh_eligible": 0,
            "probe": probe,
        }

    scan = discover_list(
        cfg,
        discovery.body,
        cutoff=now,
        observed_at=now,
        base_url=discovery.final_url or discovery_url,
    )
    detail_summary = None
    detail_parse_ok = False
    detail_url = None
    sample_date = None
    fresh_eligible = 0
    if scan.items:
        item = scan.items[0]
        detail_url = item.url
        detail = await probe_url(detail_url, settings=settings, pool=pool)
        detail_summary = observation_summary(detail)
        write_probe_artifact(
            settings,
            source_id=source_id,
            kind="detail",
            body=detail.body,
            meta={**detail_summary, "list_external_id": item.external_id},
        )
        if detail.outcome is FetchOutcomeKind.OK and detail.body is not None:
            parsed = parse_detail(
                detail,
                cfg,
                cutoff=now,
                list_fields=dict(item.metadata or {}),
                board_url=discovery_url,
                title_hint=(item.metadata or {}).get("title"),
                external_id_hint=item.external_id,
            )
            detail_parse_ok = bool(parsed.ok)
            if parsed.publication is not None:
                sample_date = parsed.publication.lower.isoformat()
            if parsed.ok and parsed.eligible:
                fresh_eligible = 1
        else:
            detail_parse_ok = False

    # Qualify when discovery fetched+parsed and (no items OR detail fetched).
    # Empty discovery with OK fetch is still a successful monitor scan.
    ok = discovery.outcome is FetchOutcomeKind.OK
    if scan.items and not detail_parse_ok and detail_summary:
        # Detail fetch/parse failed — still qualify discovery shape if body parsed.
        ok = True  # discovery worked; record detail failure in probe
    status = "qualified" if ok else "disabled"
    schema_fp = hashlib.sha256(
        f"{source_id}:{scan.ordered}:{scan.scanned_count}:{len(scan.items)}".encode()
    ).hexdigest()
    probe = {
        "probe_artifact": str(disc_path),
        "fetch_status": "ok" if ok else disc_summary["outcome"],
        "schema_fingerprint": schema_fp,
        "accepted_fraction": None,
        "sample_date": sample_date,
        "rate_limit_headers": _rate_headers(discovery),
        "observed_at": now.isoformat(),
        "detail_url": detail_url,
        "error": None if status == "qualified" else (discovery.reason or "probe_failed"),
        "pilot_count": len(scan.items),
    }
    return {
        "source_id": source_id,
        "status": status,
        "discovery": disc_summary,
        "detail": detail_summary,
        "discovered_count": len(scan.items),
        "scanned_count": scan.scanned_count,
        "fresh_eligible": fresh_eligible,
        "ordered": scan.ordered,
        "limitation": scan.limitation,
        "detail_parse_ok": detail_parse_ok,
        "probe": probe,
    }


def _rate_headers(obs: RawObservation) -> dict[str, str]:
    keys = (
        "retry-after",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-reset",
        "x-rate-limit-limit",
        "x-rate-limit-remaining",
        "x-rate-limit-reset",
    )
    out: dict[str, str] = {}
    for key, value in (obs.headers or {}).items():
        if key.lower() in keys:
            out[key] = value
    return out


# ---------------------------------------------------------------------------
# B9 prerequisite: live monitor discovery into a run (detail fetch/parse via `work`)
# ---------------------------------------------------------------------------


async def _fetch_discovery(
    settings: Settings, source_id: str, cfg: Any, pool: HttpPool
) -> tuple[RawObservation, str, list[dict[str, Any]]]:
    """Publisher feed (inventory feed_url) first, then the documented page, then fallbacks. No challenge bypass:
    a challenge/denied outcome just moves on to the next documented URL."""
    tried: list[dict[str, Any]] = []
    obs: RawObservation | None = None
    feed = (cfg.raw or {}).get("feed_url")
    urls = ((feed,) if feed else ()) + (cfg.documented_access_url, *(_DISCOVERY_FALLBACKS.get(source_id) or ()))
    for url in urls:
        obs = await probe_url(url, settings=settings, pool=pool)
        tried.append({"url": url, "outcome": obs.outcome.value, "status": obs.status_code, "reason": obs.reason})
        if obs.outcome is FetchOutcomeKind.OK and obs.body:
            return obs, url, tried
    assert obs is not None
    return obs, str(cfg.documented_access_url), tried


def _exact_fresh(items: list[Any], cutoff: datetime) -> int:
    from signal_ingest.freshness.interval import PublicationInterval
    from signal_ingest.freshness.window import in_freshness_window

    n = 0
    for item in items:
        pub = (item.metadata or {}).get("publication")
        if not pub or pub.get("precision") != "instant":
            continue
        interval = PublicationInterval(
            lower=datetime.fromisoformat(pub["lower"]),
            upper=datetime.fromisoformat(pub["upper"]),
            precision=pub["precision"],
            method=pub.get("method") or "list",
        )
        n += in_freshness_window(interval, cutoff)
    return n


async def crawl_monitor_live(
    settings: Settings,
    source_id: str,
    *,
    run_id: str,
    pool: HttpPool | None = None,
) -> dict[str, Any]:
    """Live discovery for one qualified monitor, enqueued into `run_id` (fetch stage).

    Watermark: the prior discover checkpoint cursor feeds discover_list (R3 heuristic).
    Detail fetch + parse run through `signal-ingest work`; parse eligibility uses the
    list observation time as cutoff, and export re-filters at its own as-of.
    """
    from uuid import UUID

    from signal_ingest.adapters.monitors.config import load_monitor_config
    from signal_ingest.adapters.monitors.pipeline import discover_and_enqueue, finalize_monitor_run
    from signal_ingest.crawl.queue import get_checkpoint
    from signal_ingest.live.paths import assert_live_mode
    from signal_ingest.storage.db import connect

    assert_live_mode(settings)
    cfg = load_monitor_config(source_id)
    own = pool is None
    pool = pool or HttpPool(PoolLimits(user_agent="signal-ingest/0.1 (+assessment)"))
    try:
        obs, used_url, tried = await _fetch_discovery(settings, source_id, cfg, pool)
    finally:
        if own:
            await pool.aclose()
    now = get_clock().now()
    now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    probe = write_probe_artifact(
        settings, source_id=source_id, kind="crawl_discovery", body=obs.body,
        meta={**observation_summary(obs), "tried": tried, "run_id": run_id},
    )
    with connect(settings) as conn:
        if obs.outcome is not FetchOutcomeKind.OK or not obs.body:
            finalize_monitor_run(
                conn, source_id=source_id, cutoff_at=now, run_id=UUID(run_id), failed=True,
                rate_limited=obs.status_code == 429,
                discover_meta={"tried": tried, "error": obs.reason or obs.outcome.value},
            )
            conn.commit()
            return {"source_id": source_id, "ok": False, "tried": tried, "probe_artifact": str(probe)}
        prior = get_checkpoint(conn, source_id=source_id, stage="discover", partition_key="monitor")
        result = discover_and_enqueue(
            conn,
            source_id=source_id,
            payload=obs.body,
            run_id=UUID(run_id),
            checkpoint=(prior or {}).get("cursor_opaque"),
            cutoff=now,
            observed_at=now,
            base_url=obs.final_url or used_url,
        )
        finalize_monitor_run(
            conn, source_id=source_id, cutoff_at=now, run_id=UUID(run_id),
            discover_meta={k: v for k, v in result.items() if k in {"discovered", "scan", "created", "skipped"}},
            rate_limited=obs.status_code == 429,
        )
        conn.commit()
    return {
        "source_id": source_id,
        "ok": True,
        "discovery_url": used_url,
        "discovered": result.get("discovered"),
        "enqueued": result.get("created"),
        "skipped_seen": result.get("skipped"),
        "scan": result.get("scan"),
        "probe_artifact": str(probe),
    }


async def crawl_all_monitors_live(settings: Settings, *, run_id: str) -> dict[str, Any]:
    """Crawl every inventory monitor whose status is qualified into one run."""
    from signal_ingest.adapters.monitors.config import load_monitor_configs
    from signal_ingest.config.sources_inventory import load_source_inventory

    status = {str(e.get("source_id")): str(e.get("status")) for e in load_source_inventory()}
    pool = HttpPool(PoolLimits(user_agent="signal-ingest/0.1 (+assessment)"))
    out: list[dict[str, Any]] = []
    try:
        for cfg in load_monitor_configs():
            if status.get(cfg.source_id) != "qualified":
                out.append({"source_id": cfg.source_id, "ok": False, "skipped": "not_qualified"})
                continue
            try:
                out.append(await crawl_monitor_live(settings, cfg.source_id, run_id=run_id, pool=pool))
            except Exception as exc:  # one source failing must not stop the others
                out.append({"source_id": cfg.source_id, "ok": False, "error": f"{type(exc).__name__}: {exc}"[:300]})
    finally:
        await pool.aclose()
    summary = {
        "track": "monitor_crawl",
        "run_id": run_id,
        "sources": out,
        "ok_count": sum(1 for r in out if r.get("ok")),
        "enqueued_total": sum(int(r.get("enqueued") or 0) for r in out),
    }
    path = write_qualification_manifest(settings, name="monitor_crawl", payload=summary)
    summary["manifest_path"] = str(path)
    return summary


async def dry_probe_monitors(settings: Settings) -> list[dict[str, Any]]:
    """Discovery + one detail per monitor; no DB writes, no enqueue."""
    from signal_ingest.adapters.monitors.config import load_monitor_configs

    now = get_clock().now()
    now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    pool = HttpPool(PoolLimits(user_agent="signal-ingest/0.1 (+assessment)"))
    rows: list[dict[str, Any]] = []
    try:
        for cfg in load_monitor_configs():
            obs, used_url, tried = await _fetch_discovery(settings, cfg.source_id, cfg, pool)
            row: dict[str, Any] = {"source_id": cfg.source_id, "discovery": obs.outcome.value, "url": used_url}
            if obs.outcome is FetchOutcomeKind.OK and obs.body:
                scan = discover_list(cfg, obs.body, cutoff=now, observed_at=now, base_url=obs.final_url or used_url)
                row["discovered"] = len(scan.items)
                row["exact_fresh_24h"] = _exact_fresh(scan.items, now)
                if scan.items:
                    item = scan.items[0]
                    det = await probe_url(item.url, settings=settings, pool=pool)
                    parsed = (
                        parse_detail(
                            det, cfg, cutoff=now, list_fields=dict(item.metadata or {}).get("list_fields"),
                            board_url=used_url, title_hint=(item.metadata or {}).get("title"),
                            external_id_hint=item.external_id,
                            list_observed_at=now.isoformat(),
                        )
                        if det.outcome is FetchOutcomeKind.OK and det.body
                        else None
                    )
                    row["detail"] = det.outcome.value
                    row["detail_parse_ok"] = bool(parsed and parsed.ok)
                    row["detail_eligible"] = bool(parsed and parsed.eligible)
                    row["detail_reason"] = getattr(parsed, "quarantine_reason", None)
            rows.append(row)
    finally:
        await pool.aclose()
    return rows
