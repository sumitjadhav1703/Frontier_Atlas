"""B0 gate: contracts, fixture/live boundaries, and default network guard.

Oracle: docs/build-plan.md (B0) and docs/contracts.md — not production source.
Cases must FAIL when behavior is missing/wrong and PASS when integrated correctly.
The gate runner rejects skipped cases.
"""

from __future__ import annotations

import json
import socket
from copy import deepcopy
from typing import Any

import pytest
from pydantic import ValidationError

from tests.support import FIXTURES, load_json

REQUIRED_TYPES = {"STARTUP", "PRODUCT", "RESEARCH_PAPER", "JOB", "NEWS"}


def _payload_for_model(envelope: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Split fixture envelope into (recordType, model payload without discriminator)."""
    data = deepcopy(envelope)
    record_type = data.pop("recordType")
    return record_type, data


def parse_typed(envelope: dict[str, Any]) -> Any:
    """Parse via public models API: parse_record(record_type, data)."""
    from signal_ingest.models import parse_record

    record_type, payload = _payload_for_model(envelope)
    return parse_record(record_type, payload)


def expect_validation_error(envelope: dict[str, Any]) -> None:
    with pytest.raises((ValidationError, ValueError, TypeError)):
        parse_typed(envelope)


def dump_model(model: Any) -> dict[str, Any]:
    return model.model_dump(mode="json")


def employee_count_of(dumped: dict[str, Any]) -> Any:
    return dumped["content"]["data"]["employeeCount"]


# ---------------------------------------------------------------------------
# 1. Accept five typed records (fixture mode)
# ---------------------------------------------------------------------------


def test_validates_five_typed_records_from_fixture():
    """One valid instance of each of five types must parse under employer casing.

    Detects: missing models, invented defaults, or type map gaps (no DB required).
    """
    bundle = load_json("b0_five_records.json")
    assert bundle["_meta"]["label"] == "synthetic"
    assert bundle["_meta"]["not_for_live_export"] is True
    records = bundle["records"]
    assert {r["recordType"] for r in records} == REQUIRED_TYPES
    parsed = [parse_typed(r) for r in records]
    assert len(parsed) == 5
    for envelope, model in zip(records, parsed, strict=True):
        dumped = dump_model(model)
        assert dumped["schemaVersion"] == "1.0"
        assert "source" in dumped and "content" in dumped


def test_accepts_five_typed_records_into_fixture_db(tmp_fixture_env, frozen_clock):
    """Validated five-type fixture must persist under mode=fixture with fixture lineage.

    Detects: init-db/migration gaps, mode/DSN connect refusal, missing lineage_mode.
    """
    from signal_ingest.config import Mode, load_settings
    from signal_ingest.storage import connect, create_run, init_db, persist_record

    records = load_json("b0_five_records.json")["records"]
    settings = load_settings(mode=Mode.FIXTURE)
    assert settings.mode is Mode.FIXTURE

    init_db(settings)
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings)
        stored_ids = []
        for envelope in records:
            rtype, payload = _payload_for_model(envelope)
            stored_ids.append(
                persist_record(
                    conn,
                    run_id=run_id,
                    record_type=rtype,
                    payload=payload,
                    settings=settings,
                )
            )
        conn.commit()
        assert len(stored_ids) == 5
        rows = conn.execute(
            "SELECT record_type, lineage_mode FROM records WHERE run_id = %s",
            (run_id,),
        ).fetchall()
        assert {row[0] for row in rows} == REQUIRED_TYPES
        assert all(row[1] == "fixture" for row in rows)

# ---------------------------------------------------------------------------
# 2. Schema rejections named in B0 gate
# ---------------------------------------------------------------------------


def test_rejects_missing_required_field():
    """Missing content.entityName must fail validation."""
    expect_validation_error(
        load_json("b0_negatives.json")["cases"]["missing_required_entityName"]
    )


def test_rejects_wrong_field_case():
    """Employer casing is exact: entity_name must not satisfy entityName."""
    expect_validation_error(
        load_json("b0_negatives.json")["cases"]["wrong_case_entity_name"]
    )


def test_rejects_invalid_pricing_model_enum():
    """pricingModel must be FREE/FREEMIUM/PAID/ENTERPRISE."""
    expect_validation_error(
        load_json("b0_negatives.json")["cases"]["invalid_pricing_model"]
    )


def test_rejects_negative_github_stars():
    """github_stars < 0 must be rejected."""
    expect_validation_error(
        load_json("b0_negatives.json")["cases"]["negative_github_stars"]
    )


def test_rejects_naive_datetime():
    """Timezone-naive collectedAt must be rejected at the model boundary."""
    expect_validation_error(
        load_json("b0_negatives.json")["cases"]["naive_collected_at"]
    )


def test_rejects_is_remote_as_int_one():
    """is_remote must be a Boolean; int 1 must not coerce."""
    expect_validation_error(
        load_json("b0_negatives.json")["cases"]["is_remote_as_int"]
    )


def test_rejects_is_remote_as_int_zero():
    """is_remote int 0 must also be rejected (happy-path miss if only 1 checked)."""
    expect_validation_error(
        load_json("b0_negatives.json")["cases"]["is_remote_as_zero"]
    )


# ---------------------------------------------------------------------------
# 3. Optional null employeeCount round-trip
# ---------------------------------------------------------------------------


def test_round_trip_startup_null_employee_count_model():
    """employeeCount=null must survive validate → model_dump (optional field, not coerced)."""
    envelope = load_json("b0_startup_null_employees.json")["record"]
    dumped = dump_model(parse_typed(envelope))
    assert employee_count_of(dumped) is None


def test_round_trip_startup_null_employee_count_persist(tmp_fixture_env, frozen_clock):
    """employeeCount=null must survive fixture persist and reload from JSONB."""
    from signal_ingest.config import Mode, load_settings
    from signal_ingest.storage import connect, create_run, init_db, persist_record

    envelope = load_json("b0_startup_null_employees.json")["record"]
    settings = load_settings(mode=Mode.FIXTURE)
    init_db(settings)
    rtype, payload = _payload_for_model(envelope)
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings)
        record_id = persist_record(
            conn,
            run_id=run_id,
            record_type=rtype,
            payload=payload,
            settings=settings,
        )
        conn.commit()
        row = conn.execute(
            "SELECT payload FROM records WHERE id = %s",
            (record_id,),
        ).fetchone()
        assert row is not None
        assert employee_count_of(row[0]) is None


# ---------------------------------------------------------------------------
# 4. Fixture lineage / mode-DSN mix rejection
# ---------------------------------------------------------------------------


def test_rejects_fixture_mode_with_live_dsn(tmp_fixture_env):
    """mode=fixture paired with the live DSN must raise before connect/write."""
    from signal_ingest.config import Mode, load_settings
    from signal_ingest.config.settings import ModeDsnMismatchError

    settings = load_settings(mode=Mode.FIXTURE)
    live_dsn = str(tmp_fixture_env["dsn_live"])
    with pytest.raises(ModeDsnMismatchError):
        settings.assert_mode_dsn(live_dsn)


def test_rejects_live_mode_with_fixture_dsn(tmp_fixture_env):
    """mode=live paired with the fixture DSN must raise (cross-pair)."""
    from signal_ingest.config import Mode, load_settings
    from signal_ingest.config.settings import ModeDsnMismatchError

    settings = load_settings(mode=Mode.LIVE)
    fixture_dsn = str(tmp_fixture_env["dsn_fixture"])
    with pytest.raises(ModeDsnMismatchError):
        settings.assert_mode_dsn(fixture_dsn)


def test_rejects_fixture_lineage_promotion_to_live(tmp_fixture_env):
    """Fixture-tagged observation must not be admitted into live."""
    from signal_ingest.storage import FixtureLineageError, assert_live_admission

    with pytest.raises(FixtureLineageError):
        assert_live_admission(lineage_mode="fixture", target_mode="live")


# ---------------------------------------------------------------------------
# 5. Default network guard
# ---------------------------------------------------------------------------


def test_network_guard_blocks_non_loopback(tmp_fixture_env):
    """Deliberate non-loopback DNS/connect must be blocked under the default guard."""
    from signal_ingest.config.network import (
        NetworkGuardError,
        install_network_guard,
        uninstall_network_guard,
    )

    install_network_guard()
    try:
        with pytest.raises(NetworkGuardError):
            socket.getaddrinfo("example.com", 80)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(0.2)
            with pytest.raises(NetworkGuardError):
                sock.connect(("1.1.1.1", 80))
        finally:
            sock.close()
    finally:
        uninstall_network_guard()


def test_network_guard_allows_loopback(tmp_fixture_env):
    """Loopback resolve must remain allowed (over-broad guard would break fixture DB)."""
    from signal_ingest.config.network import install_network_guard, uninstall_network_guard

    install_network_guard()
    try:
        assert socket.getaddrinfo("127.0.0.1", 54329)
        assert socket.getaddrinfo("localhost", 54329)
    finally:
        uninstall_network_guard()


# ---------------------------------------------------------------------------
# Extra regressions the happy path often misses
# ---------------------------------------------------------------------------


def test_accepts_zero_github_stars():
    """Zero observed stars is valid; only negatives are illegal."""
    paper = next(
        r
        for r in load_json("b0_five_records.json")["records"]
        if r["recordType"] == "RESEARCH_PAPER"
    )
    assert paper["content"]["github_stars"] == 0
    dumped = dump_model(parse_typed(paper))
    assert dumped["content"]["github_stars"] == 0


def test_five_record_fixture_file_is_labeled_synthetic():
    """B0 fixtures must be labeled synthetic and kept out of live paths."""
    path = FIXTURES / "b0_five_records.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["_meta"]["label"] == "synthetic"
    assert data["_meta"]["not_for_live_export"] is True
    assert "tests/fixtures" in path.as_posix()
