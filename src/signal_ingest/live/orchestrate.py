"""Coordinate independent B8 qualification tracks (monitors / bulk / canary / seeds)."""

from __future__ import annotations

import asyncio
from pathlib import Path
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
    run_id: str | None = None,
) -> dict[str, Any]:
    """
    Run selected live tracks. With run_id, canary/seeds use that acquisition run. Missing LLM keys cannot stall monitors/bulk.

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
        if track in {"all", "seeds"} and run_id:
            assert_fixture_seeds_rejected_in_live()
            from signal_ingest.live.seeds import build_live_seeds_from_run

            path = build_live_seeds_from_run(cfg, run_id=run_id)
            import json as _json

            doc = _json.loads(path.read_text(encoding="utf-8"))
            out["seeds"] = {
                "path": str(path),
                "identity_count": doc.get("identity_count"),
                "from_run": run_id,
                "fixture_rejected": True,
            }
        elif track in {"all", "seeds"}:
            assert_fixture_seeds_rejected_in_live()
            stub = ensure_live_seed_stub(cfg)
            out["seeds"] = {
                "fixture_rejected": True,
                "stub_path": str(stub),
                "note": "complete ≥50 live identities still required for resolution gate",
            }
        if track in {"all", "canary"} and run_id:
            from signal_ingest.live.canary import run_canary_from_run

            out["canary"] = await run_canary_from_run(cfg, run_id=run_id)
            out["live_llm_success"] = bool(out["canary"].get("live_llm_success"))
        elif track in {"all", "canary"}:
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
            # The success flag is only accepted with the canary's evidence path (manifests guard).
            "provider_trace_path": (summary.get("canary") or {}).get("provider_trace_path"),
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


# ---------------------------------------------------------------------------
# B8: LLM owner fallback for products with no deterministic homepage owner
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[3]


def load_env_keys() -> None:
    """Load free-tier provider keys from .env into this process only (never logged)."""
    from dotenv import load_dotenv

    load_dotenv(_REPO_ROOT / ".env", override=False)


def _observation_raw(conn: Any, store: Any, observation_id: str) -> Any:
    from psycopg.rows import dict_row

    from signal_ingest.crawl.types import FetchOutcomeKind, RawObservation

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM observations WHERE id = %s", (observation_id,))
        row = cur.fetchone()
    if row is None or not row.get("content_sha256"):
        return None
    try:
        body = store.read(row["content_sha256"])
    except FileNotFoundError:
        return None
    return RawObservation(
        request_url=row["request_url"],
        final_url=row["final_url"],
        status_code=row.get("status_code"),
        outcome=FetchOutcomeKind(row["outcome"]),
        body=body,
        content_type=row.get("content_type"),
        fetched_at=row.get("fetched_at"),
        content_key=row.get("content_key"),
        content_sha256=row.get("content_sha256"),
        metadata=dict(row.get("metadata") or {}),
    )


async def run_owner_llm_fallback(
    settings: Settings,
    *,
    run_id: str,
    limit: int = 40,
    max_consecutive_failures: int = 3,
    provider: Any = None,
) -> dict[str, Any]:
    """Grounded LLM owner for quarantined products (missing_ownership_evidence).

    Bounded by `limit` provider-backed extracts; zero paid spend (providers.yaml cap).
    Accepted owners re-run the deterministic product parse with llm_owner and upsert.
    """
    from uuid import UUID

    from signal_ingest.adapters.bulk import products as products_adapter
    from signal_ingest.adapters.bulk.persist import upsert_candidate
    from signal_ingest.adapters.bulk.pipeline import _persist_relations
    from signal_ingest.crawl.store import RawStore
    from signal_ingest.storage.db import connect

    assert_live_mode(settings)
    if provider is None:
        load_env_keys()
        from signal_ingest.llm import build_chain

        provider = build_chain(connect=lambda: connect(settings))
    store = RawStore.from_settings(settings)
    outcomes: list[dict[str, Any]] = []
    with connect(settings) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT ON (q.external_id) q.external_id, q.observation_id, t.payload
            FROM quarantines q
            JOIN tasks t ON t.semantic_key = q.semantic_key
            WHERE q.run_id = %s AND q.source_id = 'bulk.products'
              AND q.reason = 'missing_ownership_evidence' AND q.observation_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM candidates c
                WHERE c.source_id = 'bulk.products' AND c.record_type = 'PRODUCT'
                  AND c.external_id = q.external_id AND c.status = 'accepted')
            ORDER BY q.external_id, q.created_at DESC
            """,
            (run_id,),
        ).fetchall()
        conn.commit()
        failures = 0
        calls = 0
        for external_id, obs_id, payload in rows:
            if calls >= limit or failures >= max_consecutive_failures:
                break
            row = (payload or {}).get("row") or {}
            raw = _observation_raw(conn, store, str(obs_id))
            if raw is None or not products_adapter._is_html(raw):
                outcomes.append({"external_id": external_id, "outcome": "no_html"})
                continue
            calls += 1
            try:
                owner = await products_adapter.llm_owner_fallback(raw, provider=provider, row=row)
            except Exception as exc:  # provider/quota failure: record, stop after N in a row
                failures += 1
                outcomes.append({"external_id": external_id, "outcome": "error", "error": f"{type(exc).__name__}"[:80]})
                continue
            failures = 0
            if owner is None:
                outcomes.append({"external_id": external_id, "outcome": "abstain"})
                continue
            result = products_adapter.parse_observation(raw, row=row, llm_owner=owner)
            if not result.ok:
                outcomes.append({"external_id": external_id, "outcome": "quarantined", "reason": result.quarantine_reason})
                continue
            if row.get("_listing"):
                result.evidence.setdefault("listing", row["_listing"])
            with conn.transaction():
                upsert_candidate(
                    conn,
                    source_id=products_adapter.SOURCE_ID,
                    record_type="PRODUCT",
                    external_id=str(result.external_id),
                    payload=result.payload,
                    evidence=result.evidence,
                    status="accepted" if result.eligible else "pending",
                    run_id=UUID(str(run_id)),
                    observation_id=UUID(str(obs_id)),
                )
                _persist_relations(conn, result.relations, run_id=UUID(str(run_id)), observation_id=UUID(str(obs_id)))
            outcomes.append({
                "external_id": external_id, "outcome": "accepted", "owner": owner["value"],
                "provider": owner.get("provider"), "model": owner.get("model"),
            })
    summary = {
        "track": "owner_llm_fallback",
        "run_id": str(run_id),
        "candidates_considered": len(rows),
        "provider_calls": calls,
        "accepted": sum(1 for o in outcomes if o["outcome"] == "accepted"),
        "outcomes": outcomes,
        "paid_spend_cap": 0,
    }
    path = write_qualification_manifest(settings, name="owner_llm", payload=summary)
    summary["manifest_path"] = str(path)
    return summary
