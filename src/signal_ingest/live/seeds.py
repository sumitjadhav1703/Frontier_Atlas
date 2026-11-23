"""Live seed register loader — rejects fixture lineage and fixture IDs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from signal_ingest.config.settings import Mode, Settings
from signal_ingest.live.manifests import write_json
from signal_ingest.live.paths import ensure_live_dirs
from signal_ingest.resolution.seeds import (
    assert_not_fixture_seed_id,
    load_seed_document,
    load_seeds_for_mode,
    seed_document_to_dict,
)
from signal_ingest.resolution.types import (
    FixtureSeedRejectedError,
    SeedRegisterDocument,
    SeedRegisterError,
)
from signal_ingest.resolution.versions import RULE_VERSION, SEED_FORMAT_VERSION

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LIVE_SEEDS_PATH = _REPO_ROOT / "config" / "seeds.live.json"
DEFAULT_FIXTURE_SEEDS_PATH = _REPO_ROOT / "config" / "seeds.fixture.json"
LIVE_SEED_TARGET = 50


def load_live_seed_register(
    path: Path | None = None,
    *,
    settings: Settings | None = None,
    require_complete: bool = True,
) -> SeedRegisterDocument:
    """
    Load the live seed register.

    Rejects fixture lineage / fixture paths / fixture-scoped IDs.
    When require_complete=True (default), enforces ≥50 identities via schema.
    """
    target = path or DEFAULT_LIVE_SEEDS_PATH
    if target.resolve() == DEFAULT_FIXTURE_SEEDS_PATH.resolve():
        raise FixtureSeedRejectedError(
            f"refusing fixture seed path for live loader: {target}"
        )
    if settings is not None and settings.mode is not Mode.LIVE:
        raise SeedRegisterError(
            f"load_live_seed_register requires mode=live (got {settings.mode.value})"
        )
    if not target.is_file():
        raise FileNotFoundError(
            f"live seed register missing: {target}; "
            "write via write_live_seed_register after acquiring live observations"
        )

    # Fast-path fixture rejection before full schema (empty pending files).
    raw = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SeedRegisterError("seed register root must be an object")
    lineage = str(raw.get("lineage_mode") or "")
    if lineage == "fixture":
        raise FixtureSeedRejectedError(
            f"mode=live rejects fixture seed register (path={target})"
        )
    if lineage and lineage != "live":
        raise SeedRegisterError(f"unexpected lineage_mode={lineage!r}")

    identities = raw.get("identities") or []
    for item in identities:
        if isinstance(item, dict) and item.get("id"):
            assert_not_fixture_seed_id(str(item["id"]), mode="live")

    if require_complete:
        doc = load_seeds_for_mode("live", path=target)
        if doc.lineage_mode != "live":
            raise FixtureSeedRejectedError(
                f"live seed register must have lineage_mode=live (got {doc.lineage_mode!r})"
            )
        for identity in doc.identities:
            assert_not_fixture_seed_id(identity.id, mode="live")
        return doc

    # Pending scaffolding: do not claim a complete register.
    if len(identities) < LIVE_SEED_TARGET:
        raise SeedRegisterError(
            f"live seed register incomplete: {len(identities)}/{LIVE_SEED_TARGET} "
            f"(revision={raw.get('revision')!r}); not admitted for live resolution"
        )
    return load_seeds_for_mode("live", path=target)


def write_live_seed_register(
    identities: list[dict[str, Any]],
    *,
    settings: Settings,
    revision: str,
    path: Path | None = None,
) -> Path:
    """Persist a live-lineage seed register; rejects fixture ids. May be <50 (pending)."""
    for row in identities:
        assert_not_fixture_seed_id(str(row.get("id") or ""), mode="live")
        for alias in row.get("aliases") or []:
            assert_not_fixture_seed_id(str(alias), mode="live")

    complete = len(identities) >= LIVE_SEED_TARGET
    payload: dict[str, Any] = {
        "format_version": SEED_FORMAT_VERSION,
        "revision": revision,
        "lineage_mode": "live",
        "rule_version": RULE_VERSION,
        "identities": identities,
        "pending_qualification": not complete,
        "identity_count": len(identities),
        "target_count": LIVE_SEED_TARGET,
    }
    if complete:
        # Validate full schema only when complete.
        doc = SeedRegisterDocument.model_validate(
            {k: v for k, v in payload.items() if k in {
                "format_version", "revision", "lineage_mode", "rule_version", "identities"
            }}
        )
        payload = seed_document_to_dict(doc)
        payload["pending_qualification"] = False
        payload["identity_count"] = len(doc.identities)
        payload["target_count"] = LIVE_SEED_TARGET

    target = path or DEFAULT_LIVE_SEEDS_PATH
    if target.resolve() == DEFAULT_FIXTURE_SEEDS_PATH.resolve():
        raise FixtureSeedRejectedError("refusing to overwrite fixture seed register")
    write_json(target, payload)
    dirs = ensure_live_dirs(settings)
    write_json(dirs["seeds"] / "seeds.live.json", payload)
    write_json(
        dirs["seeds"] / "manifest.json",
        {
            "mode": "live",
            "revision": revision,
            "identity_count": len(identities),
            "target_count": LIVE_SEED_TARGET,
            "pending_qualification": not complete,
            "path": str(target),
            "lineage_mode": "live",
            "fixture_rejected": True,
        },
    )
    return target


def build_live_seeds_from_startup_rows(
    rows: list[dict[str, Any]],
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Map pilot/acquired startup rows into seed identity dicts (live lineage)."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if len(out) >= limit:
            break
        external = str(
            row.get("id")
            or row.get("external_id")
            or row.get("slug")
            or row.get("company_id")
            or ""
        ).strip()
        if not external:
            continue
        entity_id = f"live.startup.{external}"
        if entity_id in seen:
            continue
        assert_not_fixture_seed_id(entity_id, mode="live")
        name = str(row.get("name") or row.get("company_name") or external)
        url = str(
            row.get("url")
            or row.get("profile_url")
            or row.get("website")
            or f"https://www.ycombinator.com/companies/{external}"
        )
        aliases = []
        for key in ("slug", "name", "company_name"):
            val = row.get(key)
            if val and str(val) != name:
                aliases.append(str(val))
        out.append(
            {
                "id": entity_id,
                "namespace": "startup",
                "canonical_name": name,
                "aliases": aliases[:5],
                "domains": [],
                "source_ids": {"yc": external},
                "evidence_url": url,
                "metadata": {"origin": "live_bulk_pilot", "synthetic": False},
            }
        )
        seen.add(entity_id)
    return out


def ensure_live_seed_stub(settings: Settings) -> Path:
    """Create empty/pending live seed register if missing (not a 50-seed claim)."""
    if DEFAULT_LIVE_SEEDS_PATH.is_file():
        raw = json.loads(DEFAULT_LIVE_SEEDS_PATH.read_text(encoding="utf-8"))
        if str(raw.get("lineage_mode")) == "fixture":
            raise FixtureSeedRejectedError("existing seeds.live.json has fixture lineage")
        return DEFAULT_LIVE_SEEDS_PATH
    return write_live_seed_register(
        [],
        settings=settings,
        revision="live-empty-pending-qualification",
        path=DEFAULT_LIVE_SEEDS_PATH,
    )


def assert_fixture_seeds_rejected_in_live() -> None:
    """Demonstrate live boundary: fixture register must raise."""
    try:
        load_seeds_for_mode("live", path=DEFAULT_FIXTURE_SEEDS_PATH)
    except FixtureSeedRejectedError:
        return
    raise AssertionError("expected FixtureSeedRejectedError for fixture seeds in live mode")


def build_live_seeds_from_run(settings: Settings, *, run_id: str, limit: int = LIVE_SEED_TARGET) -> Path:
    """Seed register from accepted live STARTUP candidates of one run (stable id order)."""
    from signal_ingest.storage.db import connect

    if settings.mode is not Mode.LIVE:
        raise SeedRegisterError("build_live_seeds_from_run requires mode=live")
    with connect(settings) as conn:
        rows = conn.execute(
            """
            SELECT external_id, payload FROM candidates
            WHERE run_id = %s AND record_type = 'STARTUP' AND status = 'accepted'
            ORDER BY external_id
            """,
            (run_id,),
        ).fetchall()
    startup_rows = [
        {
            "id": external_id,
            "name": (payload.get("content") or {}).get("entityName"),
            "url": (payload.get("source") or {}).get("url"),
        }
        for external_id, payload in rows
    ]
    # Distinct startups can share a display name; keep only unambiguous names as seeds.
    names: dict[str, int] = {}
    for r in startup_rows:
        key = str(r["name"] or "").strip().casefold()
        names[key] = names.get(key, 0) + 1
    startup_rows = [r for r in startup_rows if r["name"] and names[str(r["name"]).strip().casefold()] == 1]
    identities = build_live_seeds_from_startup_rows(startup_rows, limit=limit)
    for ident in identities:
        ident["metadata"] = {"origin": "live_bulk_acquisition", "synthetic": False, "run_id": str(run_id)}
    return write_live_seed_register(identities, settings=settings, revision=f"live-run-{run_id}")
