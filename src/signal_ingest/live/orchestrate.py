"""Coordinate independent B8 qualification tracks (monitors / bulk / canary / seeds)."""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from signal_ingest.config.settings import Settings
from signal_ingest.live.bulk import qualify_bulk_pilots
from signal_ingest.live.canary import run_llm_canary
from signal_ingest.live.manifests import write_qualification_manifest
from signal_ingest.live.monitors import qualify_monitors
from signal_ingest.live.paths import assert_live_mode, ensure_live_dirs
from signal_ingest.live.seeds import (
    assert_fixture_seeds_rejected_in_live,
    build_live_seeds_from_startup_rows,
    ensure_live_seed_stub,
    write_live_seed_register,
)
from signal_ingest.live.settings import load_live_settings
from signal_ingest.storage.db import init_db

Track = Literal["all", "monitors", "bulk", "canary", "seeds"]


def run_live_qualification(
    *,
    settings: Settings | None = None,
    track: Track = "all",
    update_inventory: bool = True,
    init_database: bool = True,
) -> dict[str, Any]:
    """
    Run selected live tracks. Missing LLM keys cannot stall monitors/bulk.

    Never fabricates 1000 counts or live LLM success.
    """
    cfg = settings or load_live_settings()
    assert_live_mode(cfg)
    dirs = ensure_live_dirs(cfg)

    db_note: dict[str, Any] = {"init_attempted": init_database}
    if init_database:
        try:
            applied = init_db(cfg)
            db_note["ok"] = True
            db_note["migrations_applied"] = applied
        except Exception as exc:  # noqa: BLE001 — record real DB blocker
            db_note["ok"] = False
            db_note["error"] = f"{type(exc).__name__}: {exc}"

    result: dict[str, Any] = {
        "mode": cfg.mode.value,
        "track": track,
        "live_root": str(dirs["root"]),
        "db": db_note,
        "live_llm_success": False,
        "fabricated_1000": False,
    }

    async def _run() -> dict[str, Any]:
        out = dict(result)
        if track in {"all", "monitors"}:
            out["monitors"] = await qualify_monitors(
                cfg, update_inventory=update_inventory
            )
        if track in {"all", "bulk"}:
            out["bulk"] = await qualify_bulk_pilots(
                cfg, update_inventory=update_inventory
            )
            _maybe_write_seeds_from_bulk(cfg, out)
        if track in {"all", "seeds"}:
            assert_fixture_seeds_rejected_in_live()
            stub = ensure_live_seed_stub(cfg)
            out["seeds"] = {
                "fixture_rejected": True,
                "stub_path": str(stub),
                "note": "complete ≥50 live identities still required for resolution gate",
            }
        if track in {"all", "canary"}:
            out["canary"] = await run_llm_canary(cfg)
            out["live_llm_success"] = bool(out["canary"].get("live_llm_success"))
        return out

    summary = asyncio.run(_run())
    path = write_qualification_manifest(
        cfg,
        name="qualification_summary",
        payload={
            "tracks": {
                k: summary.get(k)
                for k in ("monitors", "bulk", "canary", "seeds")
                if k in summary
            },
            "db": summary.get("db"),
            "live_llm_success": summary.get("live_llm_success", False),
            "fabricated_1000": False,
            "blocker_path": (summary.get("canary") or {}).get("blocker_path"),
        },
    )
    summary["summary_manifest_path"] = str(path)
    return summary


def _maybe_write_seeds_from_bulk(settings: Settings, summary: dict[str, Any]) -> None:
    """If YC pilot yielded rows, persist a pending live seed register from them."""
    bulk = summary.get("bulk") or {}
    yc = None
    for row in bulk.get("sources") or []:
        if row.get("source_id") == "bulk_yc_startups" and row.get("status") == "qualified":
            yc = row
            break
    if not yc:
        ensure_live_seed_stub(settings)
        return
    rows = list(yc.get("sample_rows") or [])
    if not rows:
        sample_ids = yc.get("sample_ids") or []
        rows = [
            {
                "id": sid,
                "slug": sid,
                "name": sid,
                "url": f"https://www.ycombinator.com/companies/{sid}",
            }
            for sid in sample_ids
            if sid
        ]
    identities = build_live_seeds_from_startup_rows(rows, limit=50)
    if not identities:
        ensure_live_seed_stub(settings)
        return
    path = write_live_seed_register(
        identities,
        settings=settings,
        revision=f"live-yc-pilot-{yc.get('revision') or 'unknown'}",
    )
    summary["seeds"] = {
        "path": str(path),
        "identity_count": len(identities),
        "from_yc_pilot": True,
        "pending_qualification": len(identities) < 50,
        "note": "pilot-derived; gate still requires ≥50 distinct live seeds",
    }
