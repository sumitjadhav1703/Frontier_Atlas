"""ReleaseManifest schema and local JSON writer.

Publication and form submission are out of scope. Status stays awaiting_b8 while
the B8 live gate is blocked; awaiting_publication once B8 closes but Sheet/viewer
checks are still pending.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from signal_ingest.export.tabs import TAB_NAMES

RELEASE_MANIFEST_PATH: Final = Path("artifacts/release/release_manifest.json")

ReleaseStatus = Literal[
    "scaffold",
    "awaiting_b8",
    "awaiting_publication",
    "ready",
    "submitted",
]

RELEASE_STATUSES: Final[tuple[str, ...]] = (
    "scaffold",
    "awaiting_b8",
    "awaiting_publication",
    "ready",
    "submitted",
)


class _Exact(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class ViewerCheck(_Exact):
    """Signed-out public Sheet viewer check receipt (pending until published)."""

    name: str
    status: Literal["pending", "pass", "fail"] = "pending"
    checked_at_utc: str | None = None
    note: str | None = None


class ReleaseManifest(_Exact):
    """Binds export cutoff, counts, workbook, PDF, and viewer evidence for B9."""

    schema_version: Literal["1.0"] = "1.0"
    status: ReleaseStatus
    cutoff_utc: str | None = None
    cutoff_ist: str | None = None
    run_id: str | None = None
    repo_rev: str | None = None
    workbook_id: str | None = None
    sheet_url: str | None = None
    tab_names: tuple[str, ...] = Field(default=TAB_NAMES)
    tab_counts: dict[str, int | None] = Field(default_factory=dict)
    source_scan_receipts: list[dict[str, Any]] = Field(default_factory=list)
    pdf_path: str = "architecture.pdf"
    pdf_sha256: str | None = None
    pdf_pages: int | None = None
    viewer_checks: list[ViewerCheck] = Field(default_factory=list)
    model_assisted_row_id: str | None = None
    ready_artifacts: list[str] = Field(default_factory=list)
    pending: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    fabricated_1000: Literal[False] = False
    publication: Literal["not_started"] | str = "not_started"
    form_url: str = "https://forms.gle/3JJhukomumTFueRW7"
    form_status: Literal["not_submitted", "submitted"] = "not_submitted"
    written_at_utc: str
    note: str | None = None

    @field_validator("status")
    @classmethod
    def _status_known(cls, value: str) -> str:
        if value not in RELEASE_STATUSES:
            raise ValueError(f"unknown release status: {value!r}")
        return value

    @field_validator("fabricated_1000")
    @classmethod
    def _never_fabricate(cls, value: bool) -> bool:
        if value is True:
            raise ValueError("fabricated_1000 must remain false")
        return False


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def default_pending_viewer_checks() -> list[ViewerCheck]:
    return [
        ViewerCheck(
            name="signed_out_tab_headers",
            status="pending",
            note="Await public view-only Sheet URL; no publish this pass.",
        ),
        ViewerCheck(
            name="signed_out_row_readback",
            status="pending",
            note="Await public view-only Sheet URL; no publish this pass.",
        ),
    ]


def default_tab_counts_pending() -> dict[str, int | None]:
    return {name: None for name in TAB_NAMES}


def build_scaffold_manifest(
    *,
    status: ReleaseStatus = "awaiting_b8",
    pdf_sha256: str | None = None,
    pdf_pages: int | None = None,
    repo_rev: str | None = None,
    ready_artifacts: list[str] | None = None,
    pending: list[str] | None = None,
    blockers: list[str] | None = None,
    note: str | None = None,
) -> ReleaseManifest:
    """Assemble a local scaffold; does not claim live ≥1000 or publication."""
    ready = ready_artifacts or [
        "README.md",
        "architecture.pdf",
        "docs/architecture-source.md",
        "src/",
        "artifacts/gates/B7.json",
        "artifacts/gates/b7_benchmark.json",
        "artifacts/gates/b8_blocker.json",
        "var/live/manifests/monitors_qualification.json",
        "var/live/manifests/bulk_pilots.json",
        "var/live/manifests/llm_canary.json",
        "var/live/manifests/qualification_summary.json",
        "artifacts/release/release_manifest.json",
    ]
    pending_items = pending or [
        "B8 close: free Gemini/Groq keys or reachable Ollama canary",
        "live export ≥1000 unique STARTUP / PRODUCT / RESEARCH_PAPER after dedup",
        "live seed register ≥50 distinct identities (observed 30)",
        "near-deadline news/jobs refresh with UTC/IST cutoff",
        "public view-only Google Sheet (workbook_id + sheet_url)",
        "two signed-out viewer checks",
        "form submit + receipt (applicant-owned)",
    ]
    block = blockers or [
        "B8 BLOCKED: no live LLM canary; no ≥1000 live export; seeds 30/50",
        "see artifacts/gates/b8_blocker.json",
    ]
    return ReleaseManifest(
        status=status,
        cutoff_utc=None,
        cutoff_ist=None,
        run_id=None,
        repo_rev=repo_rev,
        workbook_id=None,
        sheet_url=None,
        tab_counts=default_tab_counts_pending(),
        source_scan_receipts=[],
        pdf_sha256=pdf_sha256,
        pdf_pages=pdf_pages,
        viewer_checks=default_pending_viewer_checks(),
        model_assisted_row_id=None,
        ready_artifacts=ready,
        pending=pending_items,
        blockers=block,
        fabricated_1000=False,
        publication="not_started",
        form_status="not_submitted",
        written_at_utc=_utc_now_iso(),
        note=note
        or (
            "Local B9 release packet scaffold only. Status awaiting_b8 while B8 "
            "is blocked; after B8 closes this becomes awaiting_publication until "
            "Sheet/viewer/form evidence exists. Pilot counts are not ≥1000."
        ),
    )


def write_release_manifest(
    manifest: ReleaseManifest,
    path: Path | str = RELEASE_MANIFEST_PATH,
) -> Path:
    """Validate and write release_manifest.json (local only; no network)."""
    out = Path(path)
    if not out.is_absolute():
        # Resolve relative to repository root when invoked from package code.
        out = Path.cwd() / out
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = manifest.model_dump(mode="json")
    # Defensive: never persist a fabricated success flag.
    if payload.get("fabricated_1000") is True:
        raise ValueError("refusing to write fabricated_1000=true")
    if payload.get("status") in {"ready", "submitted"}:
        if payload.get("workbook_id") is None:
            raise ValueError("ready/submitted requires workbook_id")
        counts = payload.get("tab_counts") or {}
        for tab in ("Startups", "Products", "Research Papers"):
            n = counts.get(tab)
            if not isinstance(n, int) or n < 1000:
                raise ValueError(
                    f"ready/submitted requires {tab} count ≥1000; got {n!r}"
                )
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    out.write_text(text, encoding="utf-8")
    return out
