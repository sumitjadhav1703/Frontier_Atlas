"""LLM canary: real provider only; write b8_blocker.json when unavailable."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from signal_ingest.config.clock import get_clock
from signal_ingest.config.settings import Settings
from signal_ingest.live.manifests import write_json, write_qualification_manifest
from signal_ingest.live.paths import B8_BLOCKER_PATH, ensure_live_dirs
from signal_ingest.live.settings import provider_keys_present
from signal_ingest.llm.providers_config import is_live_provider, load_providers_config

OLLAMA_TAGS_URL = "http://127.0.0.1:11434/api/tags"
CANARY_ATTEMPT_CEILING = 10


def _utc_now() -> datetime:
    now = get_clock().now()
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def probe_ollama(timeout_s: float = 2.0) -> dict[str, Any]:
    """Check local Ollama without claiming LLM success."""
    try:
        with httpx.Client(timeout=timeout_s) as client:
            resp = client.get(OLLAMA_TAGS_URL)
        models: list[str] = []
        if resp.status_code == 200:
            payload = resp.json()
            for row in payload.get("models") or []:
                name = row.get("name") or row.get("model")
                if name:
                    models.append(str(name))
        return {
            "reachable": resp.status_code == 200,
            "status_code": resp.status_code,
            "models": models,
            "endpoint": OLLAMA_TAGS_URL,
        }
    except (httpx.HTTPError, OSError, json.JSONDecodeError) as exc:
        return {
            "reachable": False,
            "status_code": None,
            "models": [],
            "endpoint": OLLAMA_TAGS_URL,
            "error": f"{type(exc).__name__}: {exc}",
        }


def write_b8_blocker(
    *,
    reason: str,
    unlock: dict[str, Any],
    path: Path | None = None,
    extra: dict[str, Any] | None = None,
) -> Path:
    """
    Persist BLOCKED canary state. Never sets live_llm_success true.
    """
    target = path or B8_BLOCKER_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    body = {
        "block": "b8_llm_canary",
        "status": "BLOCKED",
        "written_at": _utc_now().isoformat(),
        "reason": reason,
        "live_llm_success": False,
        "unlock": unlock,
        "attempt_ceiling": CANARY_ATTEMPT_CEILING,
        "note": (
            "Monitors/bulk pilots may still proceed independently. "
            "Do not treat this file as live LLM evidence."
        ),
    }
    if extra:
        body["context"] = extra
    write_json(target, body)
    return target


def available_live_routes() -> dict[str, Any]:
    keys = provider_keys_present()
    ollama = probe_ollama()
    cfg = load_providers_config()
    providers = cfg.get("providers") or {}
    routes: list[dict[str, Any]] = []
    for name in ("gemini", "groq", "ollama"):
        block = providers.get(name) or {}
        env_name = block.get("api_key_env")
        has_key = bool(os.environ.get(str(env_name), "").strip()) if env_name else False
        if name == "ollama":
            ready = bool(ollama.get("reachable")) and bool(ollama.get("models"))
        else:
            ready = has_key
        routes.append(
            {
                "provider": name,
                "is_live_name": is_live_provider(name),
                "config_enabled": bool(block.get("enabled", False)),
                "model": block.get("model"),
                "api_key_env": env_name,
                "credentials_present": has_key if name != "ollama" else None,
                "ready": ready,
            }
        )
    any_ready = any(r["ready"] for r in routes)
    return {
        "keys": keys,
        "ollama": ollama,
        "routes": routes,
        "any_ready": any_ready,
    }


async def run_llm_canary(
    settings: Settings,
    *,
    force_blocker_if_unready: bool = True,
) -> dict[str, Any]:
    """
    Attempt a real-provider canary. Without keys/Ollama, write blocker and stop.

    Does not invent secrets. Does not claim success on fake/replay providers.
    """
    dirs = ensure_live_dirs(settings)
    inventory = available_live_routes()
    if not inventory["any_ready"]:
        unlock = {
            "env_keys_needed": ["GEMINI_API_KEY", "GROQ_API_KEY"],
            "or_local": {
                "service": "ollama",
                "endpoint": OLLAMA_TAGS_URL,
                "status": "unreachable"
                if not inventory["ollama"].get("reachable")
                else "no_models",
                "observed": inventory["ollama"],
            },
            "steps": [
                "Export GEMINI_API_KEY and/or GROQ_API_KEY for free-tier accounts",
                "Or start Ollama locally with an instruction model (ollama pull <model>)",
                "Re-run: signal-ingest qualify --track canary --mode live",
            ],
        }
        blocker = write_b8_blocker(
            reason="no live free provider credentials or reachable Ollama with models",
            unlock=unlock,
            extra={"routes": inventory["routes"]},
        )
        payload = {
            "track": "llm_canary",
            "live_llm_success": False,
            "status": "BLOCKED",
            "blocker_path": str(blocker),
            "provider_inventory": inventory,
            "attempts": 0,
            "attempt_ceiling": CANARY_ATTEMPT_CEILING,
        }
        path = write_qualification_manifest(settings, name="llm_canary", payload=payload)
        payload["manifest_path"] = str(path)
        # Also drop a canary note under var/live/canary
        write_json(dirs["canary"] / "status.json", payload)
        return payload

    if force_blocker_if_unready:
        # Ready path reserved for a follow-up canary call with real extract();
        # this scaffolding records readiness without fabricating a trace.
        pass

    # A ready route exists — still do not fabricate a successful extract here.
    # Coordinator/operator must run extract against hand-labeled public text.
    ready = [r for r in inventory["routes"] if r["ready"]]
    payload = {
        "track": "llm_canary",
        "live_llm_success": False,
        "status": "READY_PENDING_CANARY_EXTRACT",
        "ready_routes": ready,
        "provider_inventory": inventory,
        "attempts": 0,
        "attempt_ceiling": CANARY_ATTEMPT_CEILING,
        "note": (
            "Credentials/Ollama available but canary extract not executed in this "
            "scaffolding pass; do not claim live LLM success until a redacted "
            "real-provider trace is written."
        ),
    }
    # Remove stale blocker if we became ready (credentials appeared).
    if B8_BLOCKER_PATH.is_file():
        # Leave blocker in place until an actual successful canary overwrites it —
        # readiness alone is not success.
        payload["blocker_still_present"] = True
        payload["blocker_path"] = str(B8_BLOCKER_PATH)
    path = write_qualification_manifest(settings, name="llm_canary", payload=payload)
    payload["manifest_path"] = str(path)
    write_json(dirs["canary"] / "status.json", payload)
    return payload


# ---------------------------------------------------------------------------
# B8: canary lineage from an accepted live product + blocker recompute
# ---------------------------------------------------------------------------

LLM_LINEAGE_NAME = "llm_lineage"
BULK_TABS = {"STARTUP": "Startups", "PRODUCT": "Products", "RESEARCH_PAPER": "Research Papers"}
BULK_MINIMUM = 1000
LIVE_SEEDS_TARGET = 50


def _canonical_org_id(owner: str) -> str:
    from signal_ingest.resolution.normalize import comparison_key

    return f"organization:{comparison_key(owner) or owner.strip().lower()}"


async def run_canary_from_run(
    settings: Settings,
    *,
    run_id: str,
    provider: Any = None,
    max_extracts: int = 5,
) -> dict[str, Any]:
    """Write llm_lineage.json from an accepted live PRODUCT whose owner came from the LLM.

    If no such row exists, run extract() on fetched homepages of accepted products
    (≤ max_extracts calls) and take the first grounded owner that equals the row owner.
    """
    from psycopg.rows import dict_row

    from signal_ingest.storage.db import connect

    dirs = ensure_live_dirs(settings)
    chosen: dict[str, Any] | None = None
    with connect(settings) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT external_id, payload, evidence, observation_id FROM candidates
            WHERE run_id = %s AND record_type = 'PRODUCT' AND status = 'accepted'
              AND source_id = 'bulk.products' AND evidence->'owner'->>'method' = 'llm'
            ORDER BY external_id LIMIT 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        if row:
            chosen = {**row, "owner_evidence": row["evidence"]["owner"], "role": "extraction",
                      "row_owner_method": "llm"}
        else:
            cur.execute(
                """
                SELECT c.external_id, c.payload, c.evidence, c.observation_id, t.payload AS task_payload
                FROM candidates c JOIN tasks t
                  ON t.source_id = c.source_id AND t.external_id = c.external_id AND t.stage = 'parse'
                WHERE c.run_id = %s AND c.record_type = 'PRODUCT' AND c.status = 'accepted'
                  AND c.source_id = 'bulk.products' AND c.observation_id IS NOT NULL
                ORDER BY c.external_id LIMIT %s
                """,
                (run_id, max_extracts),
            )
            rows = cur.fetchall()
            conn.commit()
            if rows:
                from signal_ingest.adapters.bulk import products as products_adapter
                from signal_ingest.crawl.store import RawStore
                from signal_ingest.live.orchestrate import _observation_raw, load_env_keys

                if provider is None:
                    load_env_keys()
                    from signal_ingest.llm import build_chain

                    provider = build_chain(connect=lambda: connect(settings))
                store = RawStore.from_settings(settings)
                for cand in rows:
                    raw = _observation_raw(conn, store, str(cand["observation_id"]))
                    if raw is None:
                        continue
                    owner = await products_adapter.llm_owner_fallback(
                        raw, provider=provider, row=(cand["task_payload"] or {}).get("row")
                    )
                    row_owner = cand["payload"]["content"]["startupName"]
                    if owner and owner["value"].strip().casefold() == str(row_owner).strip().casefold():
                        # The LLM only corroborates an owner a deterministic rule already set.
                        chosen = {
                            **cand, "owner_evidence": owner, "role": "corroboration",
                            "row_owner_method": (cand["evidence"].get("owner") or {}).get("method"),
                        }
                        break
    if chosen is None:
        payload = {
            "track": "llm_canary", "live_llm_success": False, "status": "BLOCKED",
            "reason": "no accepted live product with grounded LLM owner evidence",
            "run_id": str(run_id), "paid_spend_cap": 0,
        }
        write_qualification_manifest(settings, name="llm_canary", payload=payload)
        return payload
    ev = chosen["owner_evidence"]
    owner = str(chosen["payload"]["content"]["startupName"])
    lineage_path = dirs["manifests"] / f"{LLM_LINEAGE_NAME}.json"
    lineage = {
        "mode": "live",
        "manifest": LLM_LINEAGE_NAME,
        "written_at": _utc_now().isoformat(),
        "live_llm_success": True,
        "status": "accepted",
        "role": chosen["role"],
        "row_owner_method": chosen["row_owner_method"],
        "provider": ev.get("provider"),
        "model": ev.get("model"),
        "run_id": str(run_id),
        "durable_identity": str(chosen["external_id"]),
        "export_row_id": str(chosen["external_id"]),
        "canonical_entity_id": _canonical_org_id(owner),
        "accepted_field_evidence": {
            "owner": {
                "value": ev.get("value"),
                "quote": ev.get("quote"),
                "segment_ids": ev.get("segment_ids"),
                "url": ev.get("url"),
                "content_sha256": ev.get("content_sha256"),
                "decision": "accepted",
                "method": "llm",
            }
        },
        "transcript": {
            "provider": ev.get("provider"),
            "model": ev.get("model"),
            "attempts": ev.get("attempts") or [],
            "redacted": True,
            "note": "dispatch records only; prompts, keys and headers are not stored",
        },
        "paid_spend_cap": 0,
        "_meta": {"label": "live", "not_for_live_export": False},
    }
    write_json(lineage_path, lineage)
    payload = {
        "track": "llm_canary",
        "live_llm_success": True,
        "status": "success",
        "provider": ev.get("provider"),
        "model": ev.get("model"),
        "provider_trace_path": str(lineage_path),
        "lineage_path": str(lineage_path),
        "durable_identity": lineage["durable_identity"],
        "run_id": str(run_id),
        "paid_spend_cap": 0,
    }
    path = write_qualification_manifest(settings, name="llm_canary", payload=payload)
    write_json(dirs["canary"] / "status.json", payload)
    payload["manifest_path"] = str(path)
    return payload


def _latest_export_dir(settings: Settings) -> Path | None:
    root = settings.assert_mode_export_root()
    manifests = sorted(root.glob("*/*/manifest.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return manifests[0].parent if manifests else None


def recompute_export_counts(export_dir: Path) -> dict[str, int]:
    """Unique live durableIdentity per bulk class, from the export CSVs."""
    import csv

    counts: dict[str, int] = {}
    for record_type, tab in BULK_TABS.items():
        path = export_dir / f"{tab.replace(' ', '_')}.csv"
        ids: set[str] = set()
        if path.is_file():
            with path.open(newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    if (row.get("lineageMode") or "").strip().lower() != "live":
                        continue
                    key = (row.get("durableIdentity") or row.get("source.url") or "").strip()
                    if key:
                        ids.add(key)
        counts[record_type] = len(ids)
    return counts


def write_b8_status(settings: Settings) -> dict[str, Any]:
    """Recompute counts from the newest live export; rewrite acquisition.json + b8_blocker.json."""
    import csv

    dirs = ensure_live_dirs(settings)
    export_dir = _latest_export_dir(settings)
    counts = recompute_export_counts(export_dir) if export_dir else {k: 0 for k in BULK_TABS}
    shortfalls: list[str] = []
    if export_dir is None:
        shortfalls.append("missing_live_export")
    for cls, n in counts.items():
        if n < BULK_MINIMUM:
            shortfalls.append(f"bulk_{cls.lower()}_unique={n}<{BULK_MINIMUM}")
    lineage_path = dirs["manifests"] / f"{LLM_LINEAGE_NAME}.json"
    llm_ok = False
    if lineage_path.is_file() and export_dir is not None:
        lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
        want = str(lineage.get("durable_identity") or "")
        products = export_dir / "Products.csv"
        if lineage.get("live_llm_success") is True and want and products.is_file():
            with products.open(newline="", encoding="utf-8") as fh:
                llm_ok = any(
                    r.get("durableIdentity") == want and (r.get("lineageMode") or "").lower() == "live"
                    for r in csv.DictReader(fh)
                )
    if not llm_ok:
        shortfalls.append("missing_real_llm_lineage_into_live_export")
    seeds_n = 0
    seeds_path = Path(__file__).resolve().parents[3] / "config" / "seeds.live.json"
    if seeds_path.is_file():
        seeds = json.loads(seeds_path.read_text(encoding="utf-8"))
        seeds_n = int(seeds.get("identity_count") or len(seeds.get("identities") or []))
    if seeds_n < LIVE_SEEDS_TARGET:
        shortfalls.append(f"live_seeds={seeds_n}<{LIVE_SEEDS_TARGET}")
    proven = {
        "counts": counts,
        "export_dir": str(export_dir) if export_dir else None,
        "llm_lineage_linked": llm_ok,
        "live_seeds": seeds_n,
        "live_seeds_target": LIVE_SEEDS_TARGET,
    }
    acquisition = {
        "track": "acquisition",
        "counts": counts,
        "export_dir": proven["export_dir"],
        "recomputed_from": "export CSV durableIdentity (lineageMode=live)",
        "fabricated_1000": False,
        "paid_spend_cap": 0,
    }
    write_qualification_manifest(settings, name="acquisition", payload=acquisition)
    blocked = bool(shortfalls)
    body: dict[str, Any] = {
        "block": "B8",
        "written_at": _utc_now().isoformat(),
        "blocked": blocked,
        "gate_complete": not blocked,
        "status": "BLOCKED" if blocked else "COMPLETE",
        "live_llm_success": llm_ok,
        "proven": proven,
        "paid_spend_cap": 0,
        "shortfalls": (
            shortfalls
            + [
                "export_counts " + ",".join(f"{k}={v}" for k, v in counts.items()) + f" (need >={BULK_MINIMUM} each)",
                f"llm_lineage_linked={llm_ok}",
                f"live_seeds={seeds_n}/{LIVE_SEEDS_TARGET}",
            ]
            if blocked
            else []
        ),
    }
    if blocked:
        body["unlock"] = "rerun scripts/b8_live_run.sh after resolving the shortfalls above"
    write_json(B8_BLOCKER_PATH, body)
    return body
