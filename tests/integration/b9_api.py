"""Read-only helpers for B9 release-packet gate artifacts.

Oracle: docs/build-plan.md (B9) + docs/contracts.md + planner ReleaseManifest.

Surfaces (verifier only reads; never publishes / never mutates live):

  artifacts/release/release_manifest.json
  artifacts/release/viewer_checks.json          # optional companion
  var/live/exports/<run_id>/<as_of>/…           # live ≥1000 export (via b8_api)
  architecture.pdf (repo root)
  artifacts/gates/b8_blocker.json               # B8 dependency

Re-running these helpers must never publish, call a provider, or write live data.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable

from tests.integration import b8_api
from tests.support import FIXTURES

REPO_ROOT = b8_api.REPO_ROOT
ARTIFACTS_RELEASE = REPO_ROOT / "artifacts" / "release"
RELEASE_MANIFEST = ARTIFACTS_RELEASE / "release_manifest.json"
VIEWER_CHECKS_PATH = ARTIFACTS_RELEASE / "viewer_checks.json"
B9_FIX = FIXTURES / "b9"
ARCHITECTURE_PDF = REPO_ROOT / "architecture.pdf"

EXPECTED_TAB_NAMES = (
    "Startups",
    "Products",
    "Research Papers",
    "Jobs",
    "News",
    "Entity Mapping Log",
)
SIGNAL_TABS = ("News", "Jobs")
BULK_MINIMA = b8_api.BULK_MINIMA
MAX_PDF_PAGES = 3

READY_STATUSES = frozenset({"ready", "submitted"})
INCOMPLETE_STATUSES = frozenset(
    {"scaffold", "awaiting_b8", "awaiting_publication", "blocked", "incomplete"}
)


class MissingReleaseArtifactError(AssertionError):
    """Required B9 release artifact is absent — gate stays BLOCKED."""


def load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise MissingReleaseArtifactError(
            f"B9 BLOCKED: missing required artifact {path.relative_to(REPO_ROOT)}"
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise AssertionError(f"{path} must be a JSON object")
    return data


def try_load_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else None


def load_release_manifest() -> dict[str, Any]:
    return load_json(RELEASE_MANIFEST)


def try_load_release_manifest() -> dict[str, Any] | None:
    return try_load_json(RELEASE_MANIFEST)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(256 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def pdf_page_count(path: Path) -> int:
    """Count PDF page objects without optional deps (Type/Page, not /Pages)."""
    data = path.read_bytes()
    return len(re.findall(rb"/Type\s*/Page(?!\s*s)", data))


def b8_unmet_shortfalls() -> list[str]:
    """Delegate to B8 recomputation — B9 cannot close while these remain."""
    return list(b8_api.unmet_gate_shortfalls())


def b8_is_blocked() -> bool:
    shortfalls = b8_unmet_shortfalls()
    if shortfalls:
        return True
    blocker = b8_api.try_load_json(b8_api.B8_BLOCKER)
    if blocker is None:
        return False
    if blocker.get("blocked") is True:
        return True
    if str(blocker.get("status") or "").upper() == "BLOCKED":
        return True
    return False


def workbook_id_of(manifest: dict[str, Any]) -> str | None:
    for key in ("workbook_id", "workbookId", "sheet_id", "spreadsheet_id"):
        val = manifest.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    sheet = manifest.get("sheet") or manifest.get("google_sheet") or {}
    if isinstance(sheet, dict):
        for key in ("workbook_id", "id", "spreadsheet_id"):
            val = sheet.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
    return None


def viewer_checks_of(manifest: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    data = manifest if manifest is not None else try_load_release_manifest()
    checks: list[Any] = []
    if isinstance(data, dict):
        raw = data.get("viewer_checks") or data.get("viewerChecks") or []
        if isinstance(raw, list):
            checks.extend(raw)
    companion = try_load_json(VIEWER_CHECKS_PATH)
    if companion is not None:
        extra = companion.get("checks") or companion.get("viewer_checks") or []
        if isinstance(extra, list):
            checks.extend(extra)
        elif companion.get("signed_out_ok") is True:
            checks.append(companion)
    out: list[dict[str, Any]] = []
    for item in checks:
        if isinstance(item, dict):
            out.append(item)
    return out


def clean_viewer_checks(checks: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return checks that claim a successful signed-out / public viewer read."""
    passed: list[dict[str, Any]] = []
    for item in checks:
        if item.get("ok") is False or item.get("passed") is False:
            continue
        signed_out = item.get("signed_out")
        if signed_out is False:
            continue
        mode = str(item.get("mode") or item.get("session") or "signed_out").lower()
        if mode in {"signed-in", "authenticated", "logged_in"}:
            continue
        status = str(item.get("status") or "").lower()
        if status in {"fail", "failed", "error", "blocked", "pending"}:
            continue
        if item.get("ok") is True or item.get("passed") is True or item.get("signed_out_ok") is True:
            passed.append(item)
            continue
        if status in {"ok", "pass", "passed", "success", "clean"}:
            passed.append(item)
    return passed


def model_assisted_row_id(manifest: dict[str, Any] | None = None) -> str | None:
    data = manifest if manifest is not None else try_load_release_manifest()
    if not isinstance(data, dict):
        return None
    for key in (
        "model_assisted_row_id",
        "modelAssistedRowId",
        "released_model_assisted_row_id",
        "llm_proof_row_id",
    ):
        val = data.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    nested = data.get("model_assisted_row") or data.get("llm_proof") or {}
    if isinstance(nested, dict):
        for key in ("durable_identity", "durableIdentity", "row_id", "id"):
            val = nested.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
    return None


def release_status(manifest: dict[str, Any] | None = None) -> str:
    data = manifest if manifest is not None else try_load_release_manifest()
    if not isinstance(data, dict):
        return "missing"
    return str(data.get("status") or "incomplete").strip().lower()


def normalize_id_list(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if text:
            out.append(text)
    return out


def assert_no_duplicate_ids(ids: Iterable[Any], *, label: str = "ids") -> None:
    seen: set[str] = set()
    dups: list[str] = []
    for item in normalize_id_list(ids):
        if item in seen:
            dups.append(item)
        else:
            seen.add(item)
    assert not dups, f"B9 BLOCKED: duplicate {label}: {dups[:20]}"


def assert_bidirectional_id_match(
    expected: Iterable[Any],
    observed: Iterable[Any],
    *,
    label: str = "signal IDs",
) -> None:
    """Expected and observed durable IDs must match in both directions (set equality)."""
    exp = set(normalize_id_list(expected))
    obs = set(normalize_id_list(observed))
    missing = sorted(exp - obs)
    extra = sorted(obs - exp)
    assert not missing, (
        f"B9 BLOCKED: omitted eligible {label}: {missing[:20]}"
    )
    assert not extra, (
        f"B9 BLOCKED: unexpected/stale {label} in public tab: {extra[:20]}"
    )


def mutate_omit_eligible(ids: list[str], *, omit: str | None = None) -> list[str]:
    """Copy of ID list with one eligible ID removed (mutation fixture helper)."""
    copy_ids = list(ids)
    if not copy_ids:
        raise ValueError("cannot omit from empty ID list")
    target = omit if omit is not None else copy_ids[0]
    return [i for i in copy_ids if i != target]


def mutate_inject_stale(ids: list[str], *, stale_id: str = "stale-out-of-window") -> list[str]:
    return list(ids) + [stale_id]


def mutate_duplicate_id(ids: list[str], *, dup: str | None = None) -> list[str]:
    copy_ids = list(ids)
    if not copy_ids:
        raise ValueError("cannot duplicate empty ID list")
    target = dup if dup is not None else copy_ids[0]
    return copy_ids + [target]


def load_synthetic_signal_case() -> dict[str, Any]:
    path = B9_FIX / "eligible_signal_ids.json"
    data = load_json(path)
    meta = data.get("_meta") or {}
    assert meta.get("not_for_live_export") is True, (
        "synthetic B9 fixture must be labeled not_for_live_export"
    )
    return data


def aged_out_job_case() -> dict[str, Any]:
    return load_json(B9_FIX / "aged_out_b8_job.json")


def release_readiness_shortfalls(
    manifest: dict[str, Any] | None = None,
) -> list[str]:
    """Compute exact B9 shortfalls. Empty list only when release predicates hold."""
    shortfalls: list[str] = []

    b8_short = b8_unmet_shortfalls()
    if b8_short:
        shortfalls.append("b8_still_blocked:" + ";".join(b8_short[:6]))

    data = manifest if manifest is not None else try_load_release_manifest()
    if data is None:
        shortfalls.append("missing_release_manifest")
        shortfalls.append("missing_workbook_id")
        shortfalls.append("missing_viewer_checks")
        shortfalls.append("missing_model_assisted_row")
        shortfalls.append("missing_live_export_ge_1000")
        return shortfalls

    status = release_status(data)
    if status in INCOMPLETE_STATUSES or status == "missing":
        shortfalls.append(f"release_status={status}")

    if workbook_id_of(data) is None:
        shortfalls.append("missing_workbook_id")

    clean = clean_viewer_checks(viewer_checks_of(data))
    if len(clean) < 2:
        shortfalls.append(f"viewer_checks_clean={len(clean)}<2")

    if model_assisted_row_id(data) is None:
        # Fall back to live B8 lineage only when it still links into a live export.
        try:
            lineage = b8_api.load_llm_lineage()
            durable = str(
                lineage.get("durable_identity")
                or lineage.get("export_row_id")
                or lineage.get("canonical_entity_id")
                or ""
            ).strip()
            if not durable:
                shortfalls.append("missing_model_assisted_row")
            else:
                export_dir = b8_api.latest_live_export_dir()
                row = b8_api.find_export_row_by_identity(export_dir, durable_id=durable)
                if row is None:
                    shortfalls.append(
                        f"model_assisted_row_not_in_export:{durable}"
                    )
        except (b8_api.MissingLiveArtifactError, AssertionError):
            shortfalls.append("missing_model_assisted_row")

    try:
        export_dir = b8_api.latest_live_export_dir()
        counts = b8_api.recompute_unique_bulk_counts(export_dir)
        for cls in b8_api.BULK_CLASSES:
            n = int(counts.get(cls, 0))
            if n < BULK_MINIMA:
                shortfalls.append(f"bulk_{cls.lower()}_unique={n}<{BULK_MINIMA}")
    except b8_api.MissingLiveArtifactError:
        shortfalls.append("missing_live_export_ge_1000")

    for key in ("cutoff_utc", "export_cutoff_utc", "cutoff"):
        if data.get(key):
            break
    else:
        shortfalls.append("missing_export_cutoff_utc")

    if not data.get("repo_rev") and not data.get("repo_revision") and not data.get("git_sha"):
        shortfalls.append("missing_repo_revision")

    pdf_digest = data.get("pdf_sha256") or data.get("pdf_digest")
    pdf_pages = data.get("pdf_pages") or data.get("pdf_page_count")
    if not pdf_digest:
        shortfalls.append("missing_pdf_digest")
    if pdf_pages is None:
        shortfalls.append("missing_pdf_page_count")
    elif int(pdf_pages) > MAX_PDF_PAGES:
        shortfalls.append(f"pdf_pages={pdf_pages}>{MAX_PDF_PAGES}")

    if status in READY_STATUSES and shortfalls:
        # Claiming ready/submitted while shortfalls remain is itself a defect.
        shortfalls.insert(0, f"status_claims_{status}_with_shortfalls")

    return shortfalls


def deep_copy_ids(ids: list[str]) -> list[str]:
    return copy.deepcopy(list(ids))


__all__ = [
    "ARCHITECTURE_PDF",
    "ARTIFACTS_RELEASE",
    "B9_FIX",
    "BULK_MINIMA",
    "EXPECTED_TAB_NAMES",
    "INCOMPLETE_STATUSES",
    "MAX_PDF_PAGES",
    "MissingReleaseArtifactError",
    "READY_STATUSES",
    "RELEASE_MANIFEST",
    "REPO_ROOT",
    "SIGNAL_TABS",
    "VIEWER_CHECKS_PATH",
    "aged_out_job_case",
    "assert_bidirectional_id_match",
    "assert_no_duplicate_ids",
    "b8_is_blocked",
    "b8_unmet_shortfalls",
    "clean_viewer_checks",
    "deep_copy_ids",
    "load_json",
    "load_release_manifest",
    "load_synthetic_signal_case",
    "model_assisted_row_id",
    "mutate_duplicate_id",
    "mutate_inject_stale",
    "mutate_omit_eligible",
    "normalize_id_list",
    "pdf_page_count",
    "release_readiness_shortfalls",
    "release_status",
    "sha256_file",
    "try_load_json",
    "try_load_release_manifest",
    "viewer_checks_of",
    "workbook_id_of",
]
