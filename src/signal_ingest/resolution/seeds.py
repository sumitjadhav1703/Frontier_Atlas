"""Load and validate labeled seed registers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from signal_ingest.resolution.types import (
    FixtureSeedRejectedError,
    Namespace,
    SeedIdentity,
    SeedRegisterDocument,
    SeedRegisterError,
)
from signal_ingest.resolution.versions import RULE_VERSION, SEED_FORMAT_VERSION

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_FIXTURE_SEEDS_PATH = _REPO_ROOT / "config" / "seeds.fixture.json"


def load_seed_document(path: Path | None = None) -> SeedRegisterDocument:
    """Parse production or test seed JSON into SeedRegisterDocument."""
    target = path or DEFAULT_FIXTURE_SEEDS_PATH
    if not target.is_file():
        raise FileNotFoundError(f"seed register not found: {target}")
    raw = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SeedRegisterError("seed register root must be an object")
    normalized = _normalize_seed_payload(raw)
    try:
        doc = SeedRegisterDocument.model_validate(normalized)
    except Exception as exc:  # noqa: BLE001
        raise SeedRegisterError(str(exc)) from exc
    _validate_identity_graph(doc.identities)
    return doc


def load_seeds_for_mode(
    mode: str,
    *,
    path: Path | None = None,
) -> SeedRegisterDocument:
    """Load seeds admissible for the given run mode.

    Live mode refuses fixture-lineage seed files entirely (B6/B8 boundary).
    """
    doc = load_seed_document(path)
    mode_val = getattr(mode, "value", mode)
    if mode_val == "live" and doc.lineage_mode == "fixture":
        raise FixtureSeedRejectedError(
            "mode=live rejects fixture seed register "
            f"(revision={doc.revision!r}, path={path or DEFAULT_FIXTURE_SEEDS_PATH})"
        )
    if mode_val == "fixture" and doc.lineage_mode == "live":
        raise SeedRegisterError(
            "mode=fixture cannot load a live seed register"
        )
    return doc


def assert_not_fixture_seed_id(entity_id: str, *, mode: str) -> None:
    """Reject fixture-prefixed or fixture-lineage IDs under live mode."""
    mode_val = getattr(mode, "value", mode)
    if mode_val != "live":
        return
    lowered = entity_id.casefold()
    if lowered.startswith("fixture.") or ".fixture." in lowered:
        raise FixtureSeedRejectedError(
            f"mode=live rejects fixture seed id: {entity_id!r}"
        )


def _normalize_seed_payload(raw: dict[str, Any]) -> dict[str, Any]:
    """Accept production shape or test `_meta` / per-row `lineage` shape."""
    meta = raw.get("_meta") if isinstance(raw.get("_meta"), dict) else {}
    lineage = (
        raw.get("lineage_mode")
        or meta.get("lineage_mode")
        or meta.get("lineage")
        or ("fixture" if meta.get("not_for_live_export") else None)
        or "fixture"
    )
    if lineage not in ("fixture", "live"):
        lineage = "fixture"
    revision = (
        raw.get("revision")
        or meta.get("revision")
        or "unspecified"
    )
    rule_version = (
        raw.get("rule_version")
        or meta.get("rule_version")
        or RULE_VERSION
    )
    identities_raw = raw.get("identities") or []
    identities: list[dict[str, Any]] = []
    for item in identities_raw:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        if not row.get("evidence_url"):
            # No fabricated placeholder: a seed without evidence keeps none.
            row["evidence_url"] = row.get("url") or ""
        row.setdefault("aliases", [])
        row.setdefault("domains", [])
        row.setdefault("source_ids", {})
        row.setdefault("metadata", {})
        identities.append(row)
    return {
        "format_version": str(raw.get("format_version") or meta.get("format_version") or "1.0"),
        "revision": str(revision),
        "lineage_mode": lineage,
        "rule_version": str(rule_version),
        "identities": identities,
    }


def _validate_identity_graph(identities: list[SeedIdentity]) -> None:
    by_id = {item.id: item for item in identities}
    if len(by_id) != len(identities):
        raise SeedRegisterError("duplicate seed identity ids")
    for item in identities:
        if item.namespace is Namespace.PRODUCT:
            if not item.owner_id:
                raise SeedRegisterError(
                    f"product seed {item.id!r} requires owner_id"
                )
            owner = by_id.get(item.owner_id)
            if owner is None:
                raise SeedRegisterError(
                    f"product seed {item.id!r} owner_id {item.owner_id!r} missing"
                )
            if owner.namespace is not Namespace.STARTUP:
                raise SeedRegisterError(
                    f"product seed {item.id!r} owner must be a startup"
                )
        elif item.owner_id is not None:
            raise SeedRegisterError(
                f"startup seed {item.id!r} must not set owner_id"
            )


def seed_document_to_dict(doc: SeedRegisterDocument) -> dict[str, Any]:
    return doc.model_dump(mode="json")


__all__ = [
    "DEFAULT_FIXTURE_SEEDS_PATH",
    "RULE_VERSION",
    "SEED_FORMAT_VERSION",
    "assert_not_fixture_seed_id",
    "load_seed_document",
    "load_seeds_for_mode",
    "seed_document_to_dict",
]
