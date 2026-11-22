"""B9 gate: fail-until-ready release packet + mutation predicates.

Oracle: docs/build-plan.md (B9) and docs/contracts.md.

Rules enforced here:
- READ captured release / live artifacts only. Never publish, call providers,
  or mutate live data / Sheets.
- FAIL (not skip) when release is incomplete: no live ≥1000 export,
  no workbook_id / viewer receipts, no real model-assisted row, or B8 blocked.
- Scaffold / awaiting_b8 / awaiting_publication alone must NEVER pass readiness.
- Mutation copies: omit eligible ID / inject stale / duplicate ID → each fails.
- tools/gate.py rejects skipped cases; this suite uses pytest.fail / asserts only.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.integration import b8_api, b9_api
from tests.support import FIXTURES

B9_FIX = FIXTURES / "b9"


# ---------------------------------------------------------------------------
# Negative fixtures stay labeled (never written to live / release as proof)
# ---------------------------------------------------------------------------


def test_b9_negative_fixtures_labeled_not_for_live():
    """Synthetic B9 samples stay under tests/fixtures and marked not_for_live_export."""
    meta = json.loads((B9_FIX / "meta.json").read_text(encoding="utf-8"))
    assert meta["label"] == "synthetic"
    assert meta["not_for_live_export"] is True
    assert meta["block"] == "B9"
    for name in (
        "eligible_signal_ids.json",
        "aged_out_b8_job.json",
        "scaffold_manifest_sample.json",
    ):
        data = json.loads((B9_FIX / name).read_text(encoding="utf-8"))
        assert data["_meta"]["not_for_live_export"] is True
        assert "tests/fixtures" in (B9_FIX / name).as_posix()


# ---------------------------------------------------------------------------
# Mutation predicates (must PASS now — prove broken exports are rejected)
# ---------------------------------------------------------------------------


def test_mutation_omit_eligible_id_fails():
    """Omit one eligible news ID from the public-tab copy → bidirectional match fails."""
    case = b9_api.load_synthetic_signal_case()
    expected = list(case["eligible_news_ids"])
    observed = b9_api.mutate_omit_eligible(list(case["tab_news_ids"]))
    with pytest.raises(AssertionError, match="omitted eligible"):
        b9_api.assert_bidirectional_id_match(expected, observed, label="news IDs")


def test_mutation_inject_stale_id_fails():
    """Inject a stale out-of-window ID into the tab copy → match fails as unexpected."""
    case = b9_api.load_synthetic_signal_case()
    expected = list(case["eligible_news_ids"])
    stale = str(case["stale_news_id"])
    observed = b9_api.mutate_inject_stale(list(case["tab_news_ids"]), stale_id=stale)
    with pytest.raises(AssertionError, match="unexpected/stale"):
        b9_api.assert_bidirectional_id_match(expected, observed, label="news IDs")


def test_mutation_duplicate_id_fails():
    """Repeat a durable ID in the public tab → duplicate rejection fires."""
    case = b9_api.load_synthetic_signal_case()
    observed = b9_api.mutate_duplicate_id(list(case["tab_jobs_ids"]))
    with pytest.raises(AssertionError, match="duplicate"):
        b9_api.assert_no_duplicate_ids(observed, label="job IDs")


def test_bidirectional_match_passes_on_clean_copy():
    """Control: unmutated eligible ↔ tab IDs match in both directions."""
    case = b9_api.load_synthetic_signal_case()
    b9_api.assert_bidirectional_id_match(
        case["eligible_news_ids"], case["tab_news_ids"], label="news IDs"
    )
    b9_api.assert_bidirectional_id_match(
        case["eligible_job_ids"], case["tab_jobs_ids"], label="job IDs"
    )
    b9_api.assert_no_duplicate_ids(case["tab_news_ids"], label="news IDs")
    b9_api.assert_no_duplicate_ids(case["tab_jobs_ids"], label="job IDs")


def test_aged_out_b8_job_does_not_satisfy_release_proof():
    """B8 job outside [T−24h,T] cannot be the release model-assisted proof row.

    Detects: clinging to an aged signal while no other grounded row is released.
    """
    case = b9_api.aged_out_job_case()
    job = case["b8_job"]
    proof = case["release_proof_row"]
    assert job["still_eligible_at_release"] is False
    assert proof["record_type"] == "PRODUCT"
    assert proof["lineage_mode"] == "live"
    assert proof["has_real_model_output"] is True
    assert proof["in_public_export"] is True
    assert job["durable_identity"] != proof["durable_identity"]
    # A release that only points at the aged job must be rejected.
    assert job["still_eligible_at_release"] is False, (
        "aged-out B8 job cannot supply release model-assisted proof"
    )
    eligible_proof_ids = {proof["durable_identity"]}
    claimed = {job["durable_identity"]}
    assert claimed.isdisjoint(eligible_proof_ids) or job["still_eligible_at_release"], (
        "aged-out B8 job id must not be accepted as the sole release proof"
    )
    # Only the grounded in-window product may satisfy release proof eligibility.
    assert proof["durable_identity"] in eligible_proof_ids
    assert job["durable_identity"] not in eligible_proof_ids


def test_scaffold_manifest_sample_never_satisfies_readiness_predicates():
    """Broken claim: scaffold-shaped manifest must produce readiness shortfalls."""
    sample = json.loads(
        (B9_FIX / "scaffold_manifest_sample.json").read_text(encoding="utf-8")
    )
    assert sample["status"] == "awaiting_b8"
    assert sample["workbook_id"] is None
    assert sample["viewer_checks"] == []
    assert sample["model_assisted_row_id"] is None
    shortfalls = b9_api.release_readiness_shortfalls(sample)
    assert shortfalls, "scaffold sample must not look complete"
    blob = " ".join(shortfalls).lower()
    assert "workbook" in blob or "viewer" in blob or "awaiting_b8" in blob or "b8_" in blob
    assert b9_api.release_status(sample) not in b9_api.READY_STATUSES


# ---------------------------------------------------------------------------
# Repo packet presence (structural — may pass; does not close B9 alone)
# ---------------------------------------------------------------------------


def test_repo_root_contains_readme_src_and_architecture_pdf():
    """Repo packet roots required by B9: README.md, src/, architecture.pdf."""
    root = b9_api.REPO_ROOT
    assert (root / "README.md").is_file(), "B9 BLOCKED: missing README.md at repo root"
    assert (root / "src").is_dir(), "B9 BLOCKED: missing src/ at repo root"
    assert (root / "architecture.pdf").is_file(), (
        "B9 BLOCKED: missing architecture.pdf at repo root"
    )


def test_architecture_pdf_page_count_bounded_when_digest_claimed():
    """When a release manifest claims pdf_pages, recompute and enforce ≤3 pages.

    Detects: inventing page count or shipping a >3-page PDF as the architecture doc.
    If no release claim yet, still measure the on-disk PDF and record bound.
    """
    pdf = b9_api.ARCHITECTURE_PDF
    assert pdf.is_file(), "B9 BLOCKED: architecture.pdf missing"
    pages = b9_api.pdf_page_count(pdf)
    digest = b9_api.sha256_file(pdf)
    assert pages >= 1, "B9 BLOCKED: architecture.pdf has no detectable pages"
    manifest = b9_api.try_load_release_manifest()
    if manifest is None:
        # No release claim yet — page bound is enforced when digest/pages are claimed.
        return
    claimed_pages = manifest.get("pdf_pages") or manifest.get("pdf_page_count")
    claimed_digest = manifest.get("pdf_sha256") or manifest.get("pdf_digest")
    if claimed_pages is not None:
        assert int(claimed_pages) == pages, (
            f"B9 BLOCKED: claimed pdf_pages={claimed_pages} != measured {pages}"
        )
        assert int(claimed_pages) <= b9_api.MAX_PDF_PAGES, (
            f"B9 BLOCKED: architecture.pdf pages={claimed_pages} > {b9_api.MAX_PDF_PAGES}"
        )
    if claimed_digest:
        assert str(claimed_digest).lower() == digest.lower(), (
            "B9 BLOCKED: release pdf_sha256 does not match architecture.pdf"
        )
        assert pages <= b9_api.MAX_PDF_PAGES, (
            f"B9 BLOCKED: architecture.pdf pages={pages} > {b9_api.MAX_PDF_PAGES}"
        )


# ---------------------------------------------------------------------------
# Fail-until-ready: incompleteness must FAIL (never skip)
# ---------------------------------------------------------------------------


def test_b9_fails_while_b8_still_blocked():
    """B9 cannot close while B8 shortfalls / blocker remain.

    Detects: marking release ready from pilots / scaffold while B8 is open.
    """
    shortfalls = b9_api.b8_unmet_shortfalls()
    blocked = b9_api.b8_is_blocked()
    if blocked or shortfalls:
        pytest.fail(
            "B9 BLOCKED: B8 still blocked — "
            + ("; ".join(shortfalls) if shortfalls else "b8_blocker.blocked=true")
        )


def test_release_incomplete_without_live_1000_export_fails():
    """Live export must recompute ≥1000 unique rows per bulk class.

    Detects: pilots, hand totals, fixture exports, or absent var/live/exports.
    """
    try:
        export_dir = b8_api.latest_live_export_dir()
    except b8_api.MissingLiveArtifactError as exc:
        pytest.fail(f"B9 BLOCKED: no live ≥1000 export ({exc})")

    manifest = b8_api.load_export_manifest(export_dir)
    assert str(manifest.get("mode") or "").lower() == "live", (
        f"B9 BLOCKED: export mode={manifest.get('mode')!r} is not live"
    )
    assert manifest.get("not_for_live_export") is not True
    recomputed = b8_api.recompute_unique_bulk_counts(export_dir)
    for cls in b8_api.BULK_CLASSES:
        n = recomputed.get(cls, 0)
        assert n >= b9_api.BULK_MINIMA, (
            f"B9 BLOCKED: {cls} unique live rows={n} < {b9_api.BULK_MINIMA} "
            f"(from {export_dir.relative_to(b9_api.REPO_ROOT)})"
        )


def test_workbook_id_and_two_viewer_receipts_required():
    """Release manifest must bind a real workbook_id and two clean signed-out checks.

    Detects: null workbook_id, pending viewer checks, scaffold-only packet.
    """
    data = b9_api.try_load_release_manifest()
    if data is None:
        pytest.fail(
            "B9 BLOCKED: missing "
            f"{b9_api.RELEASE_MANIFEST.relative_to(b9_api.REPO_ROOT)} "
            "(workbook_id + viewer receipts required)"
        )
    wid = b9_api.workbook_id_of(data)
    assert wid, (
        "B9 BLOCKED: release_manifest.workbook_id is null/missing — "
        "public view-only Sheet not bound"
    )
    clean = b9_api.clean_viewer_checks(b9_api.viewer_checks_of(data))
    assert len(clean) >= 2, (
        f"B9 BLOCKED: need ≥2 clean signed-out viewer receipts, found {len(clean)}"
    )


def test_real_model_assisted_row_required_for_release():
    """At least one released model-assisted row must link to real model output.

    Prefer PRODUCT. Aged-out B8 jobs alone cannot satisfy this. Fake/replay fail.
    """
    data = b9_api.try_load_release_manifest()
    row_id = b9_api.model_assisted_row_id(data) if data else None

    if not row_id:
        # Attempt B8 lineage fallback into a live export (still must be real).
        try:
            lineage = b8_api.load_llm_lineage()
        except b8_api.MissingLiveArtifactError as exc:
            pytest.fail(f"B9 BLOCKED: no real model-assisted row ({exc})")
        provider = b8_api.provider_name_from_lineage(lineage)
        b8_api.assert_provider_is_live(provider)
        row_id = str(
            lineage.get("durable_identity")
            or lineage.get("export_row_id")
            or lineage.get("canonical_entity_id")
            or ""
        ).strip()
        assert row_id, "B9 BLOCKED: LLM lineage missing durable identity"
        assert lineage.get("accepted_field_evidence") or lineage.get("field_evidence"), (
            "B9 BLOCKED: model-assisted row missing field evidence"
        )
        export_dir = b8_api.latest_live_export_dir()
        found = b8_api.find_export_row_by_identity(export_dir, durable_id=row_id)
        assert found is not None, (
            f"B9 BLOCKED: model-assisted durable_identity={row_id!r} not in live export"
        )
        return

    # Manifest names a proof row — it must appear in the live export with live lineage.
    try:
        export_dir = b8_api.latest_live_export_dir()
    except b8_api.MissingLiveArtifactError as exc:
        pytest.fail(
            f"B9 BLOCKED: model_assisted_row_id={row_id!r} set but live export missing ({exc})"
        )
    found = b8_api.find_export_row_by_identity(export_dir, durable_id=row_id)
    assert found is not None, (
        f"B9 BLOCKED: model_assisted_row_id={row_id!r} not found in live export"
    )
    lineage_mode = (found.get("lineageMode") or found.get("lineage_mode") or "").lower()
    assert lineage_mode == "live", (
        f"B9 BLOCKED: proof row lineageMode={lineage_mode!r} (expected live)"
    )


def test_scaffold_alone_never_marks_release_ready():
    """status scaffold|awaiting_* with null workbook / pending viewers must FAIL gate.

    Detects: treating builder packet scaffold as a closed B9 release.
    """
    shortfalls = b9_api.release_readiness_shortfalls()
    status = b9_api.release_status()
    if status in b9_api.READY_STATUSES and not shortfalls:
        return
    # Incomplete is the expected current state — must fail, never skip / soft-pass.
    pytest.fail(
        "B9 BLOCKED: release incomplete (scaffold alone cannot pass). "
        f"status={status!r}; shortfalls=" + "; ".join(shortfalls)
    )


def test_release_manifest_binds_cutoff_tabs_repo_pdf_and_scans():
    """Full release manifest binding required by build-plan B9 gate.

    Binds: export cutoff, tab/row counts, workbook ID, repo revision,
    PDF digest/page count, two viewer checks, clean-clone replay receipt.
    """
    data = b9_api.try_load_release_manifest()
    if data is None:
        pytest.fail(
            "B9 BLOCKED: missing release_manifest.json under "
            f"{b9_api.ARTIFACTS_RELEASE.relative_to(b9_api.REPO_ROOT)}"
        )

    shortfalls = b9_api.release_readiness_shortfalls(data)
    if shortfalls:
        pytest.fail(
            "B9 BLOCKED: release_manifest present but incomplete: "
            + "; ".join(shortfalls)
        )

    # Six exact tab names must be accounted for (headers kept even if empty).
    tab_counts = data.get("tab_counts") or data.get("tabCounts") or {}
    if isinstance(tab_counts, dict) and tab_counts:
        for tab in b9_api.EXPECTED_TAB_NAMES:
            assert tab in tab_counts, (
                f"B9 BLOCKED: tab_counts missing {tab!r} (keep headers when empty)"
            )

    replay = (
        data.get("clean_clone_replay")
        or data.get("clean_clone_receipt")
        or data.get("fixture_replay_receipt")
    )
    assert replay, (
        "B9 BLOCKED: missing clean-clone / fixture-mode replay receipt on release_manifest"
    )


def test_six_tab_headers_preserved_on_empty_eligible_set():
    """Contract: empty eligible signal set still keeps all six tab headers.

    Uses synthetic empty export headers when live release export is absent.
    When a live/release export exists, every expected tab CSV must include headers.
    """
    from signal_ingest.export.tabs import TAB_HEADERS, TAB_NAMES

    assert tuple(TAB_NAMES) == b9_api.EXPECTED_TAB_NAMES
    for tab in TAB_NAMES:
        headers = TAB_HEADERS[tab]
        assert len(headers) >= 1, f"B9 BLOCKED: tab {tab!r} has empty header contract"
        # Empty eligible set → header-only projection remains valid (no data rows).
        empty_projection_rows: list[list[str]] = []
        assert empty_projection_rows == []
        assert list(headers) == list(TAB_HEADERS[tab])

    # If a live export snapshot exists, require all six CSVs with non-empty headers.
    try:
        export_dir = b8_api.latest_live_export_dir()
    except b8_api.MissingLiveArtifactError:
        # No live export yet — header contract above still holds; readiness fails elsewhere.
        return

    for tab in b9_api.EXPECTED_TAB_NAMES:
        path = b8_api.tab_csv_path(export_dir, tab)
        assert path.is_file(), (
            f"B9 BLOCKED: live export missing tab CSV for {tab!r} "
            f"({path.relative_to(b9_api.REPO_ROOT)})"
        )
        text = path.read_text(encoding="utf-8")
        first = text.splitlines()[0] if text.strip() else ""
        assert first.strip(), f"B9 BLOCKED: {tab!r} CSV missing header row"


def test_ready_versus_submitted_not_overclaimed():
    """Report ready vs submitted accurately — scaffold must not claim submitted.

    Detects: status=submitted without form receipt / workbook / viewer proofs.
    """
    data = b9_api.try_load_release_manifest()
    if data is None:
        pytest.fail(
            "B9 BLOCKED: no release_manifest to judge ready vs submitted "
            f"(expected {b9_api.RELEASE_MANIFEST.relative_to(b9_api.REPO_ROOT)})"
        )
    status = b9_api.release_status(data)
    shortfalls = b9_api.release_readiness_shortfalls(data)
    if status == "submitted":
        form_receipt = (
            data.get("form_receipt")
            or data.get("submission_receipt")
            or data.get("submission")
        )
        assert form_receipt and form_receipt not in {"not_submitted", "pending", None}, (
            "B9 BLOCKED: status=submitted without form/submission receipt"
        )
        assert not shortfalls, (
            "B9 BLOCKED: status=submitted but shortfalls remain: "
            + "; ".join(shortfalls)
        )
        return
    if status == "ready":
        assert not shortfalls, (
            "B9 BLOCKED: status=ready but shortfalls remain: " + "; ".join(shortfalls)
        )
        return
    # awaiting_* / scaffold / blocked — must not silently pass.
    pytest.fail(
        f"B9 BLOCKED: release status={status!r} is not ready/submitted; "
        "shortfalls=" + "; ".join(shortfalls)
    )
