"""B8 gate: read-only inspection of current live manifests / DB export.

Oracle: docs/build-plan.md (B8) and docs/contracts.md.

Rules enforced here:
- READ current live artifacts only (var/live/**, artifacts/gates/b8_*.json,
  config/sources status). Never mutate live data. Never call providers.
- FAIL (not skip) when ≥1,000 unique accepted rows per bulk class are missing.
- FAIL when fewer than 10 qualified successful monitors are recorded.
- FAIL when real LLM lineage into the live export is missing; fake/replay names fail.
- FAIL when fixture lineage appears in a live export projection.
- Absent artifacts → clear AssertionError / MissingLiveArtifactError.
  If the gate is incomplete, artifacts/gates/b8_blocker.json must document the
  exact shortfall (unlock / counts). Skips are rejected by tools/gate.py.

Builder artifact names accepted:
  monitors_qualification.json (fallback monitors.json),
  bulk_pilots.json, llm_canary.json, qualification_summary.json,
  b8_blocker.json.
"""

from __future__ import annotations

import csv
import json
from typing import Any

import pytest

from tests.integration import b8_api
from tests.support import FIXTURES

B8_FIX = FIXTURES / "b8"


# ---------------------------------------------------------------------------
# Negative fixtures stay labeled (never written to live)
# ---------------------------------------------------------------------------


def test_b8_negative_fixtures_labeled_not_for_live():
    """Synthetic B8 samples stay under tests/fixtures and marked not_for_live_export."""
    meta = json.loads((B8_FIX / "meta.json").read_text(encoding="utf-8"))
    assert meta["label"] == "synthetic"
    assert meta["not_for_live_export"] is True
    assert meta["block"] == "B8"
    for name in (
        "fake_llm_lineage.json",
        "replay_llm_lineage.json",
        "fixture_lineage_export_row.json",
    ):
        data = json.loads((B8_FIX / name).read_text(encoding="utf-8"))
        assert data["_meta"]["not_for_live_export"] is True
        assert "tests/fixtures" in (B8_FIX / name).as_posix()


# ---------------------------------------------------------------------------
# Artifact presence — never silent skip
# ---------------------------------------------------------------------------


def test_live_manifest_roots_exist_or_blocker_documents_absence():
    """Live qualification manifests must exist; export may still be missing.

    Detects: empty var/live with no blocker (silent incomplete gate).
    When manifests exist but live export / 1k / LLM are missing, still FAIL
    (prefer fail until evidence) while requiring blocker shortfalls.
    """
    manifests_ok = b8_api.MANIFESTS_DIR.is_dir() and any(
        b8_api.MANIFESTS_DIR.glob("*.json")
    )
    monitors_ok = (
        b8_api.MONITORS_QUALIFICATION.is_file()
        or b8_api.MONITORS_MANIFEST_LEGACY.is_file()
    )
    exports_ok = False
    if b8_api.EXPORT_ROOT.is_dir():
        exports_ok = any(b8_api.EXPORT_ROOT.glob("*/*/manifest.json"))

    if manifests_ok and monitors_ok and exports_ok:
        return

    blocker = b8_api.try_load_json(b8_api.B8_BLOCKER)
    assert blocker is not None, (
        "B8 BLOCKED: incomplete live artifacts and no "
        f"{b8_api.B8_BLOCKER.relative_to(b8_api.REPO_ROOT)} documenting the shortfall"
    )
    shortfalls = _blocker_shortfall_list(blocker)
    assert shortfalls, (
        "B8 BLOCKED: b8_blocker.json present but shortfalls/unlock empty while "
        "live evidence is incomplete"
    )
    _assert_blocker_covers_required_themes(shortfalls)

    if not exports_ok or not manifests_ok:
        pytest.fail(
            "B8 BLOCKED: live export and/or full acquisition evidence absent; "
            "blocker documents: " + "; ".join(shortfalls)
        )


# ---------------------------------------------------------------------------
# ≥1000 per bulk class (recomputed)
# ---------------------------------------------------------------------------


def test_three_bulk_classes_meet_1000_unique_after_dedup():
    """Independently recompute unique live durable IDs for STARTUP/PRODUCT/PAPER.

    Detects: hand-entered / pilot totals, missing export, fixture rows counted
    as live, or any class below 1,000 after dedup.
    """
    export_dir = b8_api.latest_live_export_dir()
    manifest = b8_api.load_export_manifest(export_dir)
    assert str(manifest.get("mode") or "").lower() == "live", (
        f"B8 BLOCKED: export mode={manifest.get('mode')!r} is not live"
    )
    assert manifest.get("not_for_live_export") is not True
    assert str(manifest.get("lineage_mode") or "live").lower() == "live"

    recomputed = b8_api.recompute_unique_bulk_counts(export_dir)
    claimed_blob = (
        b8_api.try_load_json(b8_api.ACQUISITION_MANIFEST)
        or b8_api.try_load_json(b8_api.BULK_PILOTS_MANIFEST)
        or b8_api.try_load_json(b8_api.QUALIFICATION_SUMMARY)
    )
    claimed = b8_api.claimed_bulk_counts(claimed_blob) if claimed_blob else {}
    for cls in b8_api.BULK_CLASSES:
        n = recomputed.get(cls, 0)
        assert n >= b8_api.BULK_MINIMA, (
            f"B8 BLOCKED: {cls} unique live rows={n} < {b8_api.BULK_MINIMA} "
            f"(recomputed from {export_dir.relative_to(b8_api.REPO_ROOT)})"
        )
        if cls in claimed and claimed[cls] >= b8_api.BULK_MINIMA:
            assert claimed[cls] == n, (
                f"B8 BLOCKED: hand-entered {cls} count={claimed[cls]} != "
                f"recomputed unique={n}"
            )


def test_hand_entered_acquisition_total_without_export_fails():
    """Pilot/acquisition claims cannot satisfy the 1k gate without export rows.

    Detects: bulk_pilots / acquisition present while live export is missing.
    """
    claimed_blob = (
        b8_api.try_load_json(b8_api.ACQUISITION_MANIFEST)
        or b8_api.try_load_json(b8_api.BULK_PILOTS_MANIFEST)
        or b8_api.try_load_json(b8_api.QUALIFICATION_SUMMARY)
    )
    if claimed_blob is None:
        pytest.fail(
            "B8 BLOCKED: missing bulk_pilots/acquisition/qualification_summary under "
            f"{b8_api.MANIFESTS_DIR.relative_to(b8_api.REPO_ROOT)}"
        )

    assert claimed_blob.get("fabricated_1000") is not True, (
        "B8 BLOCKED: fabricated_1000 must not be claimed"
    )
    claimed = b8_api.claimed_bulk_counts(claimed_blob)

    try:
        export_dir = b8_api.latest_live_export_dir()
    except b8_api.MissingLiveArtifactError as exc:
        pytest.fail(
            "B8 BLOCKED: bulk/acquisition manifest present but live export missing "
            f"({exc}); claimed={claimed or 'pilots-only'} cannot pass without "
            "recomputed ≥1000 rows"
        )

    recomputed = b8_api.recompute_unique_bulk_counts(export_dir)
    for cls in b8_api.BULK_CLASSES:
        if claimed.get(cls, 0) >= b8_api.BULK_MINIMA:
            assert recomputed.get(cls, 0) >= b8_api.BULK_MINIMA, (
                f"B8 BLOCKED: acquisition claims {cls}={claimed[cls]} but export "
                f"recomputes {recomputed.get(cls, 0)}"
            )


# ---------------------------------------------------------------------------
# Ten qualified monitors
# ---------------------------------------------------------------------------


def test_ten_qualified_successful_monitors():
    """All ten configured monitors must be qualified in monitors_qualification.json.

    Zero fresh signals is allowed. Accepts builder shape with nested
    discovery/detail/probe blocks and qualified_count.
    """
    required_ids = b8_api.configured_monitor_ids()
    assert len(required_ids) == b8_api.REQUIRED_MONITOR_COUNT, (
        f"expected {b8_api.REQUIRED_MONITOR_COUNT} configured monitors, "
        f"got {len(required_ids)}: {required_ids}"
    )

    path = b8_api.resolve_monitors_manifest_path()
    manifest = b8_api.load_json(path)
    assert str(manifest.get("mode") or "live").lower() == "live"

    qualified = b8_api.qualified_monitor_entries(manifest)
    qualified_ids = {
        str(e.get("source_id") or e.get("id") or "") for e in qualified
    }
    qualified_ids.discard("")
    qcount = b8_api.monitors_qualified_count(manifest)

    missing = [sid for sid in required_ids if sid not in qualified_ids]
    assert not missing, (
        f"B8 BLOCKED: monitors not successfully qualified ({len(qualified_ids)}/"
        f"{b8_api.REQUIRED_MONITOR_COUNT}) via {path.name}: missing={missing}"
    )
    assert qcount >= b8_api.REQUIRED_MONITOR_COUNT, (
        f"B8 BLOCKED: only qualified_count={qcount} "
        f"(need {b8_api.REQUIRED_MONITOR_COUNT})"
    )
    assert len(qualified_ids) >= b8_api.REQUIRED_MONITOR_COUNT, (
        f"B8 BLOCKED: only {len(qualified_ids)} qualified successful monitors"
    )

    for sid in required_ids:
        inv = b8_api.source_yaml_status(sid)
        assert str(inv.get("status") or "").lower() == "qualified", (
            f"B8 BLOCKED: config/sources/{sid}.yaml status={inv.get('status')!r} "
            "(expected qualified after successful probe)"
        )


# ---------------------------------------------------------------------------
# Real LLM lineage → live export (fake/replay fail)
# ---------------------------------------------------------------------------


def test_real_llm_lineage_into_live_export():
    """Redacted real-provider lineage must link into a live export row.

    Prefer PRODUCT. Blocked llm_canary (live_llm_success=false) fails.
    Fake/replay/detached demos fail.
    """
    lineage = b8_api.load_llm_lineage()
    meta = lineage.get("_meta") or {}
    assert meta.get("not_for_live_export") is not True, (
        "B8 BLOCKED: LLM lineage artifact is labeled not_for_live_export"
    )
    assert meta.get("label") != "synthetic", (
        "B8 BLOCKED: synthetic LLM lineage cannot satisfy live gate"
    )

    provider = b8_api.provider_name_from_lineage(lineage)
    b8_api.assert_provider_is_live(provider)
    assert b8_api.claims_live_llm_success(lineage), (
        "B8 BLOCKED: LLM lineage does not claim an accepted live extraction"
    )

    durable = str(
        lineage.get("durable_identity")
        or lineage.get("export_row_id")
        or lineage.get("canonical_entity_id")
        or ""
    ).strip()
    assert durable, (
        "B8 BLOCKED: LLM lineage missing durable_identity/export_row_id link"
    )
    assert lineage.get("accepted_field_evidence") or lineage.get("field_evidence"), (
        "B8 BLOCKED: LLM lineage missing accepted field evidence"
    )
    assert lineage.get("canonical_entity_id"), (
        "B8 BLOCKED: LLM lineage missing canonical_entity_id"
    )

    export_dir = b8_api.latest_live_export_dir()
    preferred_tab = "Products"
    row = b8_api.find_export_row_by_identity(
        export_dir, durable_id=durable, tab=preferred_tab
    )
    if row is None:
        row = b8_api.find_export_row_by_identity(export_dir, durable_id=durable)
    assert row is not None, (
        f"B8 BLOCKED: LLM lineage durable_identity={durable!r} not found in "
        f"live export {export_dir.relative_to(b8_api.REPO_ROOT)}"
    )
    lineage_mode = (row.get("lineageMode") or row.get("lineage_mode") or "").lower()
    assert lineage_mode == "live", (
        f"B8 BLOCKED: linked export row lineageMode={lineage_mode!r} (expected live)"
    )


def test_fake_and_replay_provider_names_fail_live_qualification():
    """Broken claim: synthetic fake/replay lineages must be rejected by predicates.

    Does not read or write live stores — proves the gate predicate itself.
    """
    fake = json.loads((B8_FIX / "fake_llm_lineage.json").read_text(encoding="utf-8"))
    replay = json.loads((B8_FIX / "replay_llm_lineage.json").read_text(encoding="utf-8"))
    from signal_ingest.llm.providers_config import is_live_provider

    for sample in (fake, replay):
        provider = b8_api.provider_name_from_lineage(sample)
        with pytest.raises(AssertionError):
            b8_api.assert_provider_is_live(provider)
        assert is_live_provider(provider) is False


# ---------------------------------------------------------------------------
# Fixture lineage must not appear in live export
# ---------------------------------------------------------------------------


def test_live_export_contains_no_fixture_lineage():
    """Every projected bulk/signal row in the live export must be lineageMode=live.

    Detects: promoting fixture DB rows or unlabeled fixture CSV into live root.
    """
    export_dir = b8_api.latest_live_export_dir()
    manifest = b8_api.load_export_manifest(export_dir)
    assert str(manifest.get("mode") or "").lower() == "live"
    assert str(manifest.get("lineage_mode") or "live").lower() == "live"
    assert manifest.get("not_for_live_export") is not True

    label_path = export_dir / "LABEL.txt"
    if label_path.is_file():
        label = label_path.read_text(encoding="utf-8")
        assert "FIXTURE" not in label.upper() or "LIVE_EXPORT" in label.upper()
        assert "not_for_live_export" not in label.lower()

    assert not (export_dir / "NOT_FOR_LIVE_EXPORT").exists(), (
        "B8 BLOCKED: live export directory carries NOT_FOR_LIVE_EXPORT marker"
    )

    tabs = list(b8_api.BULK_TAB_BY_CLASS.values()) + ["Jobs", "News", "Entity Mapping Log"]
    fixture_hits: list[str] = []
    for tab in tabs:
        path = b8_api.tab_csv_path(export_dir, tab)
        if not path.is_file():
            continue
        with path.open(newline="", encoding="utf-8") as fh:
            for i, row in enumerate(csv.DictReader(fh), start=2):
                mode = (row.get("lineageMode") or row.get("lineage_mode") or "").strip().lower()
                if mode == "fixture":
                    ident = row.get("durableIdentity") or row.get("rawName") or f"row{i}"
                    fixture_hits.append(f"{tab}:{ident}")
    assert not fixture_hits, (
        "B8 BLOCKED: fixture lineage rows in live export: "
        + ", ".join(fixture_hits[:20])
    )


def test_fixture_lineage_row_sample_is_rejected_by_predicate():
    """Synthetic fixture-mode row must fail the live-admission check."""
    sample = json.loads(
        (B8_FIX / "fixture_lineage_export_row.json").read_text(encoding="utf-8")
    )
    assert sample["lineageMode"] == "fixture"
    from signal_ingest.config.settings import Mode
    from signal_ingest.storage.lineage import FixtureLineageError, assert_live_admission

    with pytest.raises(FixtureLineageError):
        assert_live_admission(lineage_mode="fixture", target_mode=Mode.LIVE)


# ---------------------------------------------------------------------------
# Blocker documents exact shortfall when gate incomplete
# ---------------------------------------------------------------------------


def test_blocker_documents_exact_shortfall_when_gate_incomplete():
    """If any B8 requirement is unmet, b8_blocker.json must list exact shortfalls.

    Required themes when incomplete: missing ≥1000/class, missing live LLM
    lineage, missing live export, incomplete seeds if applicable.
    Still FAIL until evidence exists (prefer fail over silent pass).
    """
    shortfalls = b8_api.unmet_gate_shortfalls()
    if not shortfalls:
        blocker = b8_api.try_load_json(b8_api.B8_BLOCKER)
        if blocker is not None:
            assert blocker.get("blocked") in {False, None, "false"} or not _blocker_shortfall_list(
                blocker
            ) or blocker.get("gate_complete") is True
        return

    blocker = b8_api.try_load_json(b8_api.B8_BLOCKER)
    assert blocker is not None, (
        "B8 BLOCKED: unmet shortfalls "
        + "; ".join(shortfalls)
        + f" but missing {b8_api.B8_BLOCKER.relative_to(b8_api.REPO_ROOT)}"
    )
    assert blocker.get("blocked") is not False, (
        "B8 BLOCKED: b8_blocker.json must set blocked=true while shortfalls remain"
    )
    documented = _blocker_shortfall_list(blocker)
    assert documented, (
        "B8 BLOCKED: b8_blocker.json must list exact shortfalls / unlock steps; "
        f"computed unmet={shortfalls}"
    )
    _assert_blocker_covers_required_themes(documented)
    unlock = blocker.get("unlock") or blocker.get("unlock_steps") or blocker.get("required_env")
    pytest.fail(
        "B8 BLOCKED: shortfalls remain: "
        + "; ".join(shortfalls)
        + f" | blocker documents: {documented}"
        + (f" | unlock={unlock!r}" if unlock else "")
    )


def test_run_mode_and_quota_fields_present_when_manifests_exist():
    """Live qualification manifests must record mode=live and zero-spend policy.

    Detects: fixture-mode manifests under var/live or missing paid_spend_cap=0.
    """
    monitors = (
        b8_api.try_load_json(b8_api.MONITORS_QUALIFICATION)
        or b8_api.try_load_json(b8_api.MONITORS_MANIFEST_LEGACY)
    )
    summary = b8_api.try_load_json(b8_api.QUALIFICATION_SUMMARY)
    bulk = b8_api.try_load_json(b8_api.BULK_PILOTS_MANIFEST)
    canary = b8_api.try_load_json(b8_api.LLM_CANARY_MANIFEST)
    if monitors is None and summary is None and bulk is None:
        pytest.fail(
            "B8 BLOCKED: no live qualification manifests under "
            f"{b8_api.MANIFESTS_DIR.relative_to(b8_api.REPO_ROOT)}"
        )
    for name, data in (
        ("monitors_qualification", monitors),
        ("qualification_summary", summary),
        ("bulk_pilots", bulk),
        ("llm_canary", canary),
    ):
        if data is None:
            continue
        mode = str(data.get("mode") or "").lower()
        assert mode == "live", f"B8 BLOCKED: {name} mode={mode!r} (expected live)"

    paid = b8_api.paid_spend_cap_from_live_artifacts()
    assert paid == 0, (
        "B8 BLOCKED: live artifacts/config missing paid_spend_cap=0 "
        f"(observed {paid!r})"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _blocker_shortfall_list(blocker: dict[str, Any]) -> list[str]:
    raw = (
        blocker.get("shortfalls")
        or blocker.get("shortfall")
        or blocker.get("missing")
        or blocker.get("deficits")
        or []
    )
    if isinstance(raw, str):
        return [raw] if raw.strip() else []
    if isinstance(raw, dict):
        out = [f"{k}={v}" for k, v in raw.items()]
        unlock = blocker.get("unlock") or blocker.get("required_env")
        if unlock:
            out.append(f"unlock={unlock}")
        return out
    if isinstance(raw, list):
        return [str(x) for x in raw if str(x).strip()]
    unlock = blocker.get("unlock") or blocker.get("required_env")
    return [f"unlock={unlock}"] if unlock else []


def _assert_blocker_covers_required_themes(documented: list[str]) -> None:
    """Blocker shortfalls must mention export/1k, LLM lineage, and seeds if short."""
    blob = " ".join(documented).lower()
    assert (
        "1000" in blob
        or "≥1000" in blob
        or "export" in blob
        or "startup" in blob
        or "product" in blob
        or "research" in blob
    ), f"blocker shortfalls missing ≥1000/export theme: {documented}"
    assert "llm" in blob or "canary" in blob or "gemini" in blob or "groq" in blob, (
        f"blocker shortfalls missing live LLM lineage theme: {documented}"
    )
    seeds_n, seeds_target = b8_api.live_seed_progress()
    if seeds_n is not None and seeds_n < seeds_target:
        assert "seed" in blob, (
            f"blocker shortfalls missing incomplete seeds theme "
            f"(live_seeds={seeds_n}<{seeds_target}): {documented}"
        )
