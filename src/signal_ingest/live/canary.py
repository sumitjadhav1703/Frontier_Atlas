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
