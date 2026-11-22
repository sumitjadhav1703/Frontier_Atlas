"""Write/read mode-scoped live qualification manifests (never fabricate counts)."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from signal_ingest.config.clock import get_clock
from signal_ingest.config.settings import Settings
from signal_ingest.live.paths import ensure_live_dirs


def _utc_now() -> datetime:
    now = get_clock().now()
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    path.write_text(text + "\n", encoding="utf-8")
    return path


def fingerprint_bytes(body: bytes | None) -> str | None:
    if body is None:
        return None
    return hashlib.sha256(body).hexdigest()


def redact_headers(headers: dict[str, Any] | None) -> dict[str, Any]:
    """Keep rate-limit / cache headers; drop Authorization and cookies."""
    if not headers:
        return {}
    out: dict[str, Any] = {}
    deny = {"authorization", "cookie", "set-cookie", "proxy-authorization", "x-api-key"}
    for key, value in headers.items():
        lk = str(key).lower()
        if lk in deny or "token" in lk or "secret" in lk or "key" in lk:
            out[key] = "[redacted]"
            continue
        out[key] = value
    return out


def write_qualification_manifest(
    settings: Settings,
    *,
    name: str,
    payload: dict[str, Any],
) -> Path:
    dirs = ensure_live_dirs(settings)
    body = {
        "mode": settings.mode.value,
        "manifest": name,
        "written_at": _utc_now().isoformat(),
        **payload,
    }
    # Never allow a fabricated success flag to slip in without evidence keys.
    if body.get("live_llm_success") is True and not body.get("provider_trace_path"):
        raise ValueError("refusing live_llm_success=true without provider_trace_path")
    path = dirs["manifests"] / f"{name}.json"
    return write_json(path, body)


def write_probe_artifact(
    settings: Settings,
    *,
    source_id: str,
    kind: str,
    body: bytes | None,
    meta: dict[str, Any],
) -> Path:
    dirs = ensure_live_dirs(settings)
    stamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
    base = dirs["probes"] / source_id
    base.mkdir(parents=True, exist_ok=True)
    meta_path = base / f"{kind}_{stamp}.json"
    body_path = base / f"{kind}_{stamp}.bin"
    record = {
        "source_id": source_id,
        "kind": kind,
        "written_at": _utc_now().isoformat(),
        "body_sha256": fingerprint_bytes(body),
        "body_bytes": len(body) if body is not None else 0,
        "body_path": str(body_path.relative_to(dirs["root"])) if body is not None else None,
        **meta,
    }
    if body is not None:
        # Cap stored probe bodies to 512 KiB to keep var/live manageable.
        body_path.write_bytes(body[: 512 * 1024])
    write_json(meta_path, record)
    return meta_path
