"""B7 gate: integrated offline replay, six-tab export, dual-worker raw, benchmarks.

Oracle: docs/build-plan.md (B7) and docs/contracts.md tab / evidence contracts.
Offline only — fixture mode, mock Sheets, frozen clock, no live publish/spend.
Cases must FAIL when behavior is missing/wrong and PASS when integrated correctly.
"""

from __future__ import annotations

import csv
import io
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from tests.integration import b7_api
from tests.support import FIXTURES

B7_FIX = FIXTURES / "b7"
ARTIFACTS = Path(__file__).resolve().parents[2] / "artifacts" / "gates"
FROZEN = datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc)
AS_OF = "2026-11-21T12:00:00Z"


# ---------------------------------------------------------------------------
# Env
# ---------------------------------------------------------------------------


@pytest.fixture
def b7_env(tmp_fixture_env, frozen_clock, tmp_path, monkeypatch):
    """Fixture-mode settings with isolated export + raw roots under tmp."""
    from signal_ingest.config import Mode, clear_settings_cache, load_settings
    from signal_ingest.config.network import install_network_guard
    from signal_ingest.storage import connect, init_db

    export_fixture = tmp_path / "export_fixture"
    export_live = tmp_path / "export_live"
    export_fixture.mkdir()
    export_live.mkdir()
    monkeypatch.setenv("SIGNAL_INGEST_EXPORT_ROOT_FIXTURE", str(export_fixture))
    monkeypatch.setenv("SIGNAL_INGEST_EXPORT_ROOT_LIVE", str(export_live))
    clear_settings_cache()
    install_network_guard()
    settings = load_settings(mode=Mode.FIXTURE)
    init_db(settings)
    env = dict(tmp_fixture_env)
    env["export_fixture"] = export_fixture
    env["export_live"] = export_live
    env["settings"] = settings
    env["child_env"] = {
        "SIGNAL_INGEST_MODE": "fixture",
        "SIGNAL_INGEST_DSN_FIXTURE": str(env["dsn_fixture"]),
        "SIGNAL_INGEST_DSN_LIVE": str(env["dsn_live"]),
        "SIGNAL_INGEST_RAW_ROOT_FIXTURE": str(env["raw_fixture"]),
        "SIGNAL_INGEST_RAW_ROOT_LIVE": str(env["raw_live"]),
        "SIGNAL_INGEST_EXPORT_ROOT_FIXTURE": str(export_fixture),
        "SIGNAL_INGEST_EXPORT_ROOT_LIVE": str(export_live),
        "SIGNAL_INGEST_NETWORK_GUARD": "1",
        "SIGNAL_INGEST_PAID_SPEND_CAP": "0",
    }
    # Smoke that DB is reachable for this gate.
    with connect(settings) as conn:
        conn.execute("SELECT 1")
        conn.commit()
    return env


def _settings(b7_env: dict[str, Any]):
    return b7_env["settings"]


# ---------------------------------------------------------------------------
# Exact six tab names
# ---------------------------------------------------------------------------


def test_exact_six_tab_names():
    """Export exposes exactly the six assessment tab titles — no aliases/renames.

    Detects: missing Entity Mapping Log, pluralization drift, extra tabs.
    """
    names = b7_api.tab_names()
    assert names == b7_api.EXPECTED_TAB_NAMES
    assert len(names) == 6
    assert len(set(names)) == 6
    assert "Mapping Log" not in names  # must be full "Entity Mapping Log"
    assert "ResearchPaper" not in names
    assert "Startups" in names


# ---------------------------------------------------------------------------
# Fixtures isolated / labeled
# ---------------------------------------------------------------------------


def test_b7_fixtures_isolated_and_labeled():
    """All B7 fixtures stay synthetic / not_for_live_export under tests/fixtures/b7."""
    meta = b7_api.load_json(B7_FIX / "meta.json")
    assert meta["label"] == "synthetic"
    assert meta["not_for_live_export"] is True
    assert meta["block"] == "B7"

    for name in (
        "corpus.json",
        "changed_observation.json",
        "sheets_rows.json",
        "csv_escape_cases.json",
        "tiny_regression.json",
        "benchmark_expected_keys.json",
    ):
        data = b7_api.load_json(B7_FIX / name)
        m = data.get("_meta") or data
        assert m.get("label") == "synthetic" or m.get("label") == meta["label"]
        assert m.get("not_for_live_export") is True
        path = B7_FIX / name
        assert "tests/fixtures" in path.as_posix()


def test_corpus_has_accept_and_reject_per_vertical():
    """Mixed corpus supplies ≥1 accept and ≥1 reject for each applicable vertical."""
    corpus = b7_api.load_json(B7_FIX / "corpus.json")
    by_v: dict[str, set[str]] = {}
    for item in corpus["items"]:
        by_v.setdefault(item["vertical"], set()).add(item["decision"])
    required = {
        "STARTUP",
        "PRODUCT",
        "RESEARCH_PAPER",
        "JOB",
        "NEWS",
        "ENTITY_MAPPING",
    }
    assert required <= set(by_v)
    for vertical in required:
        assert "accept" in by_v[vertical], f"{vertical} missing accept"
        assert "reject" in by_v[vertical], f"{vertical} missing reject"


def test_fixture_export_directory_labeled_and_segregated(b7_env):
    """Local export lands under fixture export root with visible LABEL markers.

    Detects: writing into live export root or unlabeled snapshot dirs.
    """
    settings = _settings(b7_env)
    from signal_ingest.storage import connect
    from signal_ingest.storage.records import create_run

    with connect(settings) as conn:
        run_id = create_run(
            conn,
            settings=settings,
            metadata={"fixture": True, "not_for_live_export": True},
        )
        conn.commit()

    dest = b7_api.export_snapshot_dir(settings, run_id=str(run_id), as_of=AS_OF)
    export_root = Path(b7_env["export_fixture"]).resolve()
    live_root = Path(b7_env["export_live"]).resolve()
    assert dest.resolve().is_relative_to(export_root)
    assert not dest.resolve().is_relative_to(live_root)
    label = (dest / "LABEL.txt").read_text(encoding="utf-8")
    assert "FIXTURE" in label.upper()
    assert "not_for_live_export" in label.lower() or (dest / "NOT_FOR_LIVE_EXPORT").is_file()
    assert settings.export_root.resolve() == export_root


# ---------------------------------------------------------------------------
# Dual-worker content_key reopen (byte-equal)
# ---------------------------------------------------------------------------


def test_dual_worker_content_key_reopen_byte_equal(b7_env, tmp_path):
    """Worker-2 reopens worker-1 blob via relative content_key under shared raw root.

    Separate process cwd must not break resolution. Fails if absolute/cwd-private paths required.
    """
    body = b"B7 dual-worker shared raw payload - synthetic only\n"
    cwd_a = tmp_path / "worker_a_cwd"
    cwd_b = tmp_path / "worker_b_cwd"
    cwd_a.mkdir()
    cwd_b.mkdir()
    shared = Path(b7_env["raw_fixture"])

    w1, w2 = b7_api.dual_worker_content_key_roundtrip(
        shared_raw=shared,
        cwd_a=cwd_a,
        cwd_b=cwd_b,
        body=body,
        env=b7_env["child_env"],
    )
    assert w1["content_key"]
    assert "/" in w1["content_key"]  # relative ab/cd/<sha>
    assert not Path(w1["content_key"]).is_absolute()
    assert w2["body"] == body
    assert Path(w1["cwd"]).resolve() != Path(w2["cwd"]).resolve()

    # Also exercise pipeline open_observation_by_content_key after DB insert.
    from signal_ingest.crawl.store import RawStore, insert_observation
    from signal_ingest.crawl.types import FetchOutcomeKind, RawObservation
    from signal_ingest.storage import connect
    from signal_ingest.storage.records import create_run

    settings = _settings(b7_env)
    store = RawStore.from_settings(settings)
    key, digest = store.write(body)
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings, metadata={"b7": "dual"})
        task_id = uuid4()
        conn.execute(
            """
            INSERT INTO tasks (
                id, source_id, stage, url, semantic_key, external_id, status, run_id
            ) VALUES (
                %s, 'b7.dual', 'fetch', %s, %s, %s, 'done', %s
            )
            """,
            (
                str(task_id),
                "https://fixtures.local/b7/dual",
                f"b7.dual:fetch:{task_id}",
                str(task_id),
                str(run_id),
            ),
        )
        obs = RawObservation(
            request_url="https://fixtures.local/b7/dual",
            final_url="https://fixtures.local/b7/dual",
            status_code=200,
            outcome=FetchOutcomeKind.OK,
            body=body,
            content_type="text/plain",
            content_key=key,
            content_sha256=digest,
            fetched_at=FROZEN,
            metadata={"synthetic": True, "not_for_live_export": True},
        )
        insert_observation(
            conn, task_id=task_id, source_id="b7.dual", observation=obs, run_id=run_id
        )
        opened = b7_api.open_observation_by_content_key(
            conn, content_key=key, settings=settings
        )
        conn.commit()
    assert opened["body"] == body
    assert opened["body_length"] == len(body)
    assert opened["observation"] is not None
    assert opened["observation"]["content_key"] == key


# ---------------------------------------------------------------------------
# Kill / rerun duplicate-free
# ---------------------------------------------------------------------------


def test_kill_rerun_duplicate_free(b7_env):
    """Kill mid-run (stale lease) → resume/rerun yields one observation per task.

    Detects: double-commit after reclaim or lost recovery.
    """
    from signal_ingest.config import get_clock
    from signal_ingest.crawl.store import RawStore, insert_observation, save_raw_observation_blob
    from signal_ingest.crawl.types import FetchOutcomeKind, RawObservation
    from signal_ingest.storage import connect
    from signal_ingest.storage.records import create_run

    settings = _settings(b7_env)
    store = RawStore.from_settings(settings)
    body = b'{"synthetic":true,"b7":"kill-rerun"}'

    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings, metadata={"scenario": "kill_rerun"})
        task_id = uuid4()
        conn.execute(
            """
            INSERT INTO tasks (
                id, source_id, stage, url, semantic_key, external_id,
                status, run_id, attempt_count, max_attempts,
                lease_owner, lease_expires, fencing_token, next_attempt
            ) VALUES (
                %s, 'b7.kill', 'fetch', %s, %s, %s,
                'leased', %s, 1, 5,
                'worker-a', %s, 1, %s
            )
            """,
            (
                str(task_id),
                "https://fixtures.local/b7/kill",
                f"b7.kill:fetch:{task_id}",
                "kill-1",
                str(run_id),
                get_clock().now() + timedelta(seconds=60),
                get_clock().now(),
            ),
        )
        obs = RawObservation(
            request_url="https://fixtures.local/b7/kill",
            final_url="https://fixtures.local/b7/kill",
            status_code=200,
            outcome=FetchOutcomeKind.OK,
            body=body,
            content_type="application/json",
            fetched_at=FROZEN,
            metadata={"synthetic": True},
        )
        save_raw_observation_blob(store, obs)
        # Crash: blob written, observation NOT committed. Expire lease.
        conn.execute(
            """
            UPDATE tasks SET lease_expires = %s WHERE id = %s
            """,
            (get_clock().now() - timedelta(seconds=5), str(task_id)),
        )
        conn.commit()

    # Resume releases stale lease and would continue work; complete manually under fence.
    resumed = b7_api.resume_run(
        run_id=run_id, settings=settings, worker_id="worker-b", max_tasks=0
    )
    assert resumed["stale_leases_released"] >= 1

    with connect(settings) as conn:
        # Reclaim / mark ready then commit one observation (unique on task_id).
        conn.execute(
            """
            UPDATE tasks
            SET status = 'ready', lease_owner = NULL, lease_expires = NULL,
                fencing_token = fencing_token + 1
            WHERE id = %s
            """,
            (str(task_id),),
        )
        obs2 = RawObservation(
            request_url="https://fixtures.local/b7/kill",
            final_url="https://fixtures.local/b7/kill",
            status_code=200,
            outcome=FetchOutcomeKind.OK,
            body=body,
            content_type="application/json",
            content_key=obs.content_key,
            content_sha256=obs.content_sha256,
            fetched_at=FROZEN,
            metadata={"synthetic": True},
        )
        first = insert_observation(
            conn, task_id=task_id, source_id="b7.kill", observation=obs2, run_id=run_id
        )
        second = insert_observation(
            conn, task_id=task_id, source_id="b7.kill", observation=obs2, run_id=run_id
        )
        conn.execute(
            "UPDATE tasks SET status = 'done' WHERE id = %s",
            (str(task_id),),
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM observations WHERE task_id = %s",
            (str(task_id),),
        ).fetchone()
        conn.commit()

    assert first is not None
    assert second is None  # unique(task_id) → no duplicate committed effect
    assert count is not None and int(count[0]) == 1

    # Second resume must not invent another observation.
    b7_api.resume_run(run_id=run_id, settings=settings, worker_id="worker-c", max_tasks=5)
    with connect(settings) as conn:
        count2 = conn.execute(
            "SELECT COUNT(*) FROM observations WHERE run_id = %s",
            (str(run_id),),
        ).fetchone()
    assert count2 is not None and int(count2[0]) == 1


# ---------------------------------------------------------------------------
# Changed observation updates only that ID
# ---------------------------------------------------------------------------


def test_changed_observation_updates_only_that_id(b7_env):
    """Replaying one changed observation updates only that durable identity.

    Detects: wholesale rewrite of peer rows or ID churn on unchanged keys.
    """
    from signal_ingest.storage import connect
    from signal_ingest.storage.records import create_run

    settings = _settings(b7_env)
    changed = b7_api.load_json(B7_FIX / "changed_observation.json")
    corpus = b7_api.load_json(B7_FIX / "corpus.json")
    accepted = [
        i
        for i in corpus["items"]
        if i["decision"] == "accept" and i["vertical"] in {
            "STARTUP", "PRODUCT", "RESEARCH_PAPER", "JOB", "NEWS"
        }
    ]

    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings, metadata={"scenario": "change"})
        for item in accepted:
            b7_api.insert_accepted_candidate(
                conn,
                run_id=run_id,
                record_type=item["vertical"],
                external_id=item["logical_id"],
                payload=item["body"],
                evidence={
                    "body": item.get("full_text") or "",
                    "span": item.get("evidence_span"),
                },
            )
        conn.commit()
        snap1 = b7_api.build_export_snapshot(
            conn, run_id=run_id, as_of=AS_OF, settings=settings
        )

    def _index(snap: dict) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for tab, rows in snap["tabs"].items():
            if tab == "Entity Mapping Log":
                continue
            for row in rows:
                did = str(row.get("durableIdentity") or "")
                out[did] = row
        return out

    before = _index(snap1)
    assert before, "expected accepted projection rows"

    target_ext = changed["target_logical_id"]
    target_key = f"b7.fixture:STARTUP:{target_ext}"
    with connect(settings) as conn:
        b7_api.update_candidate_payload(
            conn, semantic_key=target_key, payload=changed["updated_body"]
        )
        conn.commit()
        snap2 = b7_api.build_export_snapshot(
            conn, run_id=run_id, as_of=AS_OF, settings=settings
        )
    after = _index(snap2)

    assert set(before) == set(after), "durable identity set must stay stable"
    # Locate target by external id substring in durableIdentity / source.url
    target_rows = [
        (did, row)
        for did, row in after.items()
        if target_ext in did or target_ext in str(row.get("source.url") or "")
    ]
    assert target_rows, "changed startup missing after replay"
    _, tgt = target_rows[0]
    assert int(tgt.get("content.data.employeeCount") or -1) == 55

    for peer in changed["unchanged_peers"]:
        peer_hits = [
            (did, row)
            for did, row in after.items()
            if peer in did or peer in str(row.get("source.url") or "")
        ]
        assert peer_hits, f"peer {peer} missing"
        did, row = peer_hits[0]
        assert row == before[did], f"peer {peer} mutated unexpectedly"


# ---------------------------------------------------------------------------
# Mock Sheets: RAW / stale clear / secrets absent
# ---------------------------------------------------------------------------


def test_mock_sheets_raw_stale_clear_and_secrets_absent(b7_env):
    """Mock transport uses RAW, clears stale trailing rows, and leaks no secrets."""
    from signal_ingest.storage import connect
    from signal_ingest.storage.records import create_run

    settings = _settings(b7_env)
    sheets_fix = b7_api.load_json(B7_FIX / "sheets_rows.json")
    transport = b7_api.mock_sheets_transport()
    sid = "mock-b7-stale"

    # Seed stale trailing content then export replace.
    transport.spreadsheets[sid] = {
        "Startups": [
            ["content.entityName"],
            ["STALE_ROW_SHOULD_CLEAR"],
            ["ANOTHER_STALE"],
        ]
    }

    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings, metadata={"sheets": True})
        startup = sheets_fix["tabs"]["Startups"][0]
        # Rebuild nested payload for candidate insert.
        payload = {
            "schemaVersion": "1.0",
            "source": {
                "name": startup["source.name"],
                "url": startup["source.url"],
            },
            "content": {
                "entityName": startup["content.entityName"],
                "data": {"employeeCount": startup["content.data.employeeCount"]},
            },
            "collectedAt": startup["collectedAt"],
        }
        # Inject formula-like + secret-like values into evidence only if exporter
        # projects them — public cells must still stay RAW/secret-free.
        b7_api.insert_accepted_candidate(
            conn,
            run_id=run_id,
            record_type="STARTUP",
            external_id=startup["logical_id"],
            payload=payload,
            evidence={"note": "fixture"},
        )
        # Formula-like entity name regression via second product-like startup field
        # is covered through export_via_transport on a crafted snapshot.
        snap = b7_api.build_export_snapshot(
            conn, run_id=run_id, as_of=AS_OF, settings=settings
        )
        conn.commit()

    # Inject formula-like cell into snapshot before transport write.
    formula = sheets_fix["formula_like_cells"][0]["value"]
    if snap["tabs"]["Startups"]:
        snap["tabs"]["Startups"][0]["content.entityName"] = formula

    result = b7_api.export_via_transport(snap, transport, spreadsheet_id=sid)
    assert all(w["value_input_option"] == "RAW" for w in transport.writes)
    assert (sid, "Startups") in transport.clears

    read_back = transport.read_range(sid, "Startups", "A:ZZ")
    assert read_back, "expected header + rows after replace"
    # Stale trailing rows gone: only header + current data rows.
    assert len(read_back) == 1 + snap["counts"]["Startups"]
    flat_text = json.dumps(read_back, default=str)
    assert "STALE_ROW_SHOULD_CLEAR" not in flat_text
    # R3-C: RAW input already stores formula-like strings literally; an apostrophe
    # prefix would be stored as a visible character (build-plan B7 "RAW strings").
    headers = read_back[0]
    name_idx = headers.index("content.entityName")
    written_name = read_back[1][name_idx]
    assert isinstance(written_name, str)
    assert written_name == formula
    assert all(
        w["value_input_option"] == "RAW" for w in transport.writes if w["tab"] == "Startups"
    )

    forbidden = sheets_fix["forbidden_secret_substrings"]
    hits = b7_api.secrets_scan(flat_text, forbidden)
    assert hits == [], f"secret-like substrings in Sheets payload: {hits}"
    assert result["tabs"]["Startups"]["read_back_rows"] == snap["counts"]["Startups"]


# ---------------------------------------------------------------------------
# CSV escape
# ---------------------------------------------------------------------------


def test_csv_escape_formulas_unicode_newlines():
    """CSV fallback prefixes formulas and quotes Unicode/newlines for round-trip."""
    cases = b7_api.load_json(B7_FIX / "csv_escape_cases.json")["cases"]
    headers = ["id", "value"]
    rows = [[c["id"], c["raw"]] for c in cases]
    text = b7_api.write_csv_rows(headers, rows)
    assert text.startswith("id,value\n") or text.startswith("id,value\r\n")

    parsed = list(csv.reader(io.StringIO(text)))
    body = {r[0]: r[1] for r in parsed[1:]}

    for case in cases:
        cell = body[case["id"]]
        escaped = b7_api.escape_formula_cell(case["raw"])
        if case.get("must_not_start_unquoted_formula"):
            # Escaped form must not begin with formula trigger for spreadsheet import.
            assert isinstance(escaped, str)
            assert escaped[0] == "'" or escaped[0] not in "=+-@"
            assert cell == escaped or cell.startswith("'")
        if case.get("must_roundtrip"):
            # Reader returns the cell content after CSV parse (apostrophe kept as written).
            expected = escaped if isinstance(escaped, str) else case["raw"]
            assert cell == expected
        if case.get("must_be_quoted"):
            # Original CSV text must quote fields with comma/newline.
            assert '"' in text or "\n" in case["raw"]


# ---------------------------------------------------------------------------
# Saved benchmark verified + tiny regression
# ---------------------------------------------------------------------------


def test_saved_benchmark_artifact_verified():
    """Cumulative gate verifies saved 1k/10k×{1,4} artifact — does not re-run heavy bench.

    If missing, builder must produce artifacts/gates/b7_benchmark.json once.
    """
    expect = b7_api.load_json(B7_FIX / "benchmark_expected_keys.json")
    path = ARTIFACTS / "b7_benchmark.json"
    if not path.is_file():
        # Allow generating once via API (cheap enough on first miss for local close),
        # but prefer pre-written artifact. Writing here keeps gate actionable.
        report = b7_api.run_benchmark()
        b7_api.write_benchmark_artifact(report, path=path)
    data = json.loads(path.read_text(encoding="utf-8"))
    for key in expect["required_top_keys"]:
        assert key in data, f"benchmark missing top key {key}"
    assert expect["label_must_contain"] in str(data.get("label", "")).lower()
    runs = data["runs"]
    sizes = {int(r["n"]) for r in runs}
    workers = {int(r["workers"]) for r in runs}
    assert set(expect["required_run_sizes"]) <= sizes
    assert set(expect["required_worker_counts"]) <= workers
    for r in runs:
        for metric in expect["required_per_run_metrics"]:
            assert metric in r, f"run n={r.get('n')} w={r.get('workers')} missing {metric}"
        assert r.get("not_live_throughput") is True
        assert int(r.get("duplicate_commits", -1)) == 0
        assert "scrape" not in str(r.get("bottleneck", "")).lower()
    assert data.get("projection_notes", {}).get("measurements_pending_live") is True or (
        "pending" in str(data.get("db_settings", {})).lower()
    )


def test_tiny_benchmark_regression():
    """Cheap regression: tiny synthetic set stays duplicate-free and bounded."""
    cfg = b7_api.load_json(B7_FIX / "tiny_regression.json")
    n = int(cfg["candidate_count"])
    workers = int(cfg["workers"])
    report = b7_api.run_benchmark(sizes=(n,), worker_counts=(workers,))
    assert len(report["runs"]) == 1
    run = report["runs"][0]
    assert run["n"] == n
    assert run["committed_rows"] == n
    assert run["duplicate_commits"] == 0
    assert run["wall_time_s"] <= float(cfg["max_wall_seconds"])
    assert run["queue_depth_peak"] <= n
    assert run["not_live_throughput"] is True
    assert "synthetic" in report["label"].lower()


# ---------------------------------------------------------------------------
# Bounded working set
# ---------------------------------------------------------------------------


def test_bounded_working_set_pool_and_bench_waves(b7_env):
    """HttpPool pending/active caps hold; bench waves keep a bounded queue peak.

    Detects: unbounded task materialization / pending queue growth.
    """
    from signal_ingest.crawl.pool import HttpPool, PoolLimits

    limits = PoolLimits(max_active=2, max_pending=4)
    pool = HttpPool(limits)
    assert pool.limits.max_active == 2
    assert pool.limits.max_pending == 4

    # Bench path: wave size bounds peak relative to n.
    report = b7_api.run_benchmark(sizes=(64,), worker_counts=(4,))
    run = report["runs"][0]
    assert run["queue_depth_peak"] <= 64
    # Wave size in runner is max(batch_hint=250, workers) but n=64 → peak ≤ 64.
    assert run["committed_rows"] == 64
    assert run["duplicate_commits"] == 0


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------


def test_full_text_provenance_on_news_and_jobs(b7_env):
    """Accepted News/Jobs projection carries body hash/length + source URL lineage.

    Detects: summary-only export without provenance fields.
    """
    from signal_ingest.storage import connect
    from signal_ingest.storage.records import create_run

    settings = _settings(b7_env)
    corpus = b7_api.load_json(B7_FIX / "corpus.json")
    news = next(i for i in corpus["items"] if i["logical_id"] == "news:aurora-raises-seed")
    job = next(i for i in corpus["items"] if i["logical_id"] == "job:aurora-ml-engineer")

    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings, metadata={"prov": True})
        for item in (news, job):
            b7_api.insert_accepted_candidate(
                conn,
                run_id=run_id,
                record_type=item["vertical"],
                external_id=item["logical_id"],
                payload=item["body"],
                evidence={
                    "body": item["full_text"],
                    "span": item["evidence_span"],
                    "date_extraction_method": "structured_datePublished",
                },
            )
        snap = b7_api.build_export_snapshot(
            conn, run_id=run_id, as_of=AS_OF, settings=settings
        )
        conn.commit()

    news_rows = snap["tabs"]["News"]
    job_rows = snap["tabs"]["Jobs"]
    assert len(news_rows) == 1
    assert len(job_rows) == 1
    n0 = news_rows[0]
    j0 = job_rows[0]
    assert n0.get("source.url")
    assert n0.get("lineageMode") == "fixture"
    assert n0.get("content.bodyHash")
    # Exporter hashes payload content.text when present (else evidence body).
    news_body = str(n0.get("content.text") or news["full_text"])
    assert int(n0.get("content.bodyLength") or 0) == len(news_body)
    assert len(news_body) > 40
    assert news["evidence_span"] in news_body or news["evidence_span"] in news["full_text"]
    assert j0.get("source.url")
    assert j0.get("lineageMode") == "fixture"
    assert j0.get("content.bodyHash")
    job_body = job["full_text"]  # jobs fixture stores description in evidence body
    assert int(j0.get("content.bodyLength") or 0) == len(job_body)

    # Local export manifest must mark fixture / not for live.
    dest = b7_api.export_snapshot_dir(settings, run_id=str(run_id), as_of=AS_OF)
    manifest = b7_api.write_snapshot_local(snap, dest)
    assert manifest["not_for_live_export"] is True
    assert manifest["fixture_labeled"] is True
    assert list(manifest["tab_names"]) == list(b7_api.EXPECTED_TAB_NAMES)

    audit = b7_api.audit_run(run_id=run_id, settings=settings)
    assert audit["fixture_labeled"] is True
    assert audit["lineage_mismatches"] == 0
    assert audit["ok"] is True


# ---------------------------------------------------------------------------
# Mixed-corpus six-tab export (REOPEN)
# ---------------------------------------------------------------------------


def test_mixed_corpus_run_export_six_tabs_accepts_only(b7_env):
    """Load mixed corpus → run_export: six tabs, accepts present, rejects absent.

    Detects: rejected quarantined rows leaking into public tabs, missing mapping tab,
    or unlabeled / live-root export directory.
    """
    from signal_ingest.storage import connect
    from signal_ingest.storage.records import create_run

    settings = _settings(b7_env)
    corpus = b7_api.load_json(B7_FIX / "corpus.json")

    with connect(settings) as conn:
        run_id = create_run(
            conn,
            settings=settings,
            metadata={"scenario": "mixed_corpus", "not_for_live_export": True},
        )
        loaded = b7_api.load_mixed_corpus(conn, run_id=run_id, corpus=corpus)
        conn.commit()

    assert loaded["accepted"], "corpus must yield accepted records"
    assert loaded["rejected"], "corpus must yield rejected records"
    assert loaded["mapping_accepted"], "corpus must yield accepted mapping"
    assert loaded["mapping_rejected"], "corpus must yield rejected mapping"

    result = b7_api.run_export(
        run_id=run_id,
        as_of=corpus.get("as_of") or AS_OF,
        settings=settings,
        write_local=True,
    )

    assert list(result["tab_names"]) == list(b7_api.EXPECTED_TAB_NAMES)
    assert set(result["counts"]) == set(b7_api.EXPECTED_TAB_NAMES)
    for tab in b7_api.EXPECTED_TAB_NAMES:
        assert tab in result["counts"]

    export_dir = Path(result["export_dir"])
    export_root = Path(b7_env["export_fixture"]).resolve()
    live_root = Path(b7_env["export_live"]).resolve()
    assert export_dir.resolve().is_relative_to(export_root)
    assert not export_dir.resolve().is_relative_to(live_root)
    assert (export_dir / "LABEL.txt").is_file()
    assert (export_dir / "NOT_FOR_LIVE_EXPORT").is_file()
    label = (export_dir / "LABEL.txt").read_text(encoding="utf-8")
    assert "FIXTURE" in label.upper()
    assert result["mode"] == "fixture"
    assert result["local"]["not_for_live_export"] is True

    # All six CSV tabs written.
    for tab in b7_api.EXPECTED_TAB_NAMES:
        csv_path = export_dir / f"{tab.replace(' ', '_')}.csv"
        assert csv_path.is_file(), f"missing tab CSV: {csv_path.name}"

    # Flatten export text for presence/absence checks.
    blob = "\n".join(
        (export_dir / f"{tab.replace(' ', '_')}.csv").read_text(encoding="utf-8")
        for tab in b7_api.EXPECTED_TAB_NAMES
    )
    snap_blob = json.dumps(result.get("counts"), default=str)

    for item in loaded["accepted"]:
        tab = b7_api.VERTICAL_TO_TAB[item["vertical"]]
        tab_csv = (export_dir / f"{tab.replace(' ', '_')}.csv").read_text(encoding="utf-8")
        markers = b7_api.accept_markers(item)
        assert any(m in tab_csv for m in markers), (
            f"accepted {item['logical_id']} missing from {tab}"
        )

    for item in loaded["mapping_accepted"]:
        map_csv = (
            export_dir / "Entity_Mapping_Log.csv"
        ).read_text(encoding="utf-8")
        assert item["body"]["rawName"] in map_csv
        assert item["body"]["canonicalID"] in map_csv

    for item in loaded["rejected"]:
        markers = b7_api.reject_markers(item)
        # Rejected record content must not appear in any public tab CSV.
        hits = [m for m in markers if m in blob]
        assert hits == [], f"rejected {item['logical_id']} leaked into export: {hits}"

    # Mapping reject (abstain) must not appear as a successful canonical match.
    map_csv = (export_dir / "Entity_Mapping_Log.csv").read_text(encoding="utf-8")
    for item in loaded["mapping_rejected"]:
        raw = item["body"]["rawName"]
        # If the abstain observation is logged, it must not carry a canonical ID.
        if raw in map_csv:
            assert "seed:" not in map_csv.split(raw, 1)[-1].split("\n", 1)[0] or (
                item["body"].get("canonicalID") in (None, "")
            )
            # Stronger: canonicalID from accept path must not be forged onto reject.
            assert item["body"].get("canonicalID") in (None, "")
        # Rejected mapping must never look like a match with a canonical ID in export.
        assert not (
            raw in map_csv
            and item["body"].get("canonicalName")
            and item["body"]["canonicalName"] in map_csv
        )

    # At least one accepted row landed in each of the five record tabs + mapping.
    assert result["counts"]["Startups"] >= 1
    assert result["counts"]["Products"] >= 1
    assert result["counts"]["Research Papers"] >= 1
    assert result["counts"]["Jobs"] >= 1
    assert result["counts"]["News"] >= 1
    assert result["counts"]["Entity Mapping Log"] >= 1
    _ = snap_blob  # counts already asserted


# ---------------------------------------------------------------------------
# CLI wiring: export smoke + crawl → work → export chain (REOPEN)
# ---------------------------------------------------------------------------


def test_cli_export_entry_point_wired(b7_env):
    """CLI `export` invokes run_export (not B0 stub exit 2)."""
    from typer.testing import CliRunner

    from signal_ingest.storage import connect
    from signal_ingest.storage.records import create_run

    settings = _settings(b7_env)
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings, metadata={"cli": True})
        conn.commit()

    app = b7_api.load_cli_app()
    runner = CliRunner()
    # Ensure env matches b7_env for subprocess-less invoke.
    for k, v in b7_env["child_env"].items():
        os.environ[k] = v
    result = runner.invoke(
        app,
        ["--mode", "fixture", "export", "--run", str(run_id), "--as-of", AS_OF],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["tab_names"] == list(b7_api.EXPECTED_TAB_NAMES)
    assert payload["mode"] == "fixture"


def test_cli_crawl_work_export_stage_chain(b7_env):
    """CLI connects crawl → work → export on a fixture bulk source.

    Detects: stage stubs still exit 2, work processing zero tasks, or export
    missing tabs after a real stage chain.
    """
    from typer.testing import CliRunner

    from signal_ingest.storage import connect

    for k, v in b7_env["child_env"].items():
        os.environ[k] = v

    settings = _settings(b7_env)
    # Task semantic_key ignores partition; clear prior bulk.yc rows so this run
    # owns fresh ready work (duplicate discovery is otherwise a no-op).
    with connect(settings) as conn:
        conn.execute(
            """
            DELETE FROM candidates
            WHERE source_id = 'bulk.yc'
               OR observation_id IN (
                    SELECT id FROM observations
                    WHERE task_id IN (SELECT id FROM tasks WHERE source_id = 'bulk.yc')
               )
            """
        )
        conn.execute(
            """
            DELETE FROM quarantines
            WHERE source_id = 'bulk.yc'
               OR observation_id IN (
                    SELECT id FROM observations
                    WHERE task_id IN (SELECT id FROM tasks WHERE source_id = 'bulk.yc')
               )
            """
        )
        conn.execute(
            """
            DELETE FROM relationships
            WHERE observation_id IN (
                SELECT id FROM observations
                WHERE task_id IN (SELECT id FROM tasks WHERE source_id = 'bulk.yc')
            )
            """
        )
        conn.execute(
            """
            DELETE FROM repo_metrics
            WHERE observation_id IN (
                SELECT id FROM observations
                WHERE task_id IN (SELECT id FROM tasks WHERE source_id = 'bulk.yc')
            )
            """
        )
        conn.execute(
            """
            DELETE FROM observations
            WHERE task_id IN (SELECT id FROM tasks WHERE source_id = 'bulk.yc')
            """
        )
        conn.execute("DELETE FROM tasks WHERE source_id = 'bulk.yc'")
        conn.execute("DELETE FROM source_checkpoints WHERE source_id = 'bulk.yc'")
        conn.commit()

    app = b7_api.load_cli_app()
    runner = CliRunner()

    partition = f"b7-cli-{uuid4().hex[:10]}"
    crawl = runner.invoke(
        app,
        [
            "--mode",
            "fixture",
            "crawl",
            "--source",
            "bulk.yc",
            "--partition",
            partition,
        ],
        catch_exceptions=False,
    )
    assert crawl.exit_code == 0, crawl.output
    crawl_payload = json.loads(crawl.output)
    run_id = crawl_payload["run_id"]
    assert crawl_payload["mode"] == "fixture"
    assert crawl_payload["source_id"] == "bulk.yc"
    discover = crawl_payload.get("discover") or {}
    created = int(discover.get("created") or 0)
    assert created >= 1, f"crawl created no tasks: {discover}"

    work = runner.invoke(
        app,
        [
            "--mode",
            "fixture",
            "work",
            "--stage",
            "any",
            "--run",
            run_id,
            "--worker-id",
            "cli-worker-1",
            "--max-tasks",
            "20",
        ],
        catch_exceptions=False,
    )
    assert work.exit_code == 0, work.output
    work_payload = json.loads(work.output)
    assert work_payload["run_id"] == run_id
    assert int(work_payload["processed"]) >= 1
    assert any(
        o.get("content_key") or o.get("ok") for o in work_payload.get("outcomes") or []
    )

    export = runner.invoke(
        app,
        ["--mode", "fixture", "export", "--run", run_id, "--as-of", AS_OF],
        catch_exceptions=False,
    )
    assert export.exit_code == 0, export.output
    export_payload = json.loads(export.output)
    assert export_payload["tab_names"] == list(b7_api.EXPECTED_TAB_NAMES)
    assert export_payload["mode"] == "fixture"
    assert export_payload["counts"]["Startups"] >= 1
    export_dir = Path(export_payload["export_dir"])
    assert export_dir.resolve().is_relative_to(Path(b7_env["export_fixture"]).resolve())
    assert (export_dir / "LABEL.txt").is_file()
    assert (export_dir / "Startups.csv").is_file()
    assert (export_dir / "Entity_Mapping_Log.csv").is_file()


# ---------------------------------------------------------------------------
# R3-C export
# ---------------------------------------------------------------------------


def _r3c_snapshot(tab: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    from signal_ingest.export.tabs import TAB_HEADERS, TAB_NAMES

    return {
        "run_id": "r3c-run",
        "as_of": AS_OF,
        "mode": "fixture",
        "lineage_mode": "fixture",
        "tab_names": list(TAB_NAMES),
        "headers": {n: list(TAB_HEADERS[n]) for n in TAB_NAMES},
        "tabs": {n: (rows if n == tab else []) for n in TAB_NAMES},
        "counts": {n: (len(rows) if n == tab else 0) for n in TAB_NAMES},
    }


def test_r3c_formula_escaped_exactly_once_per_output(tmp_path):
    """Sheets RAW gets the literal value; CSV gets one apostrophe; tab/CR prefixes covered."""
    from signal_ingest.export.writer import export_via_transport, write_snapshot_local

    values = ["=1+1", "\t=cmd", "\r=cmd", "-2", "plain"]
    rows = [{"content.entityName": v, "durableIdentity": f"id-{i}"} for i, v in enumerate(values)]
    snap = _r3c_snapshot("Startups", rows)
    transport = b7_api.mock_sheets_transport()
    export_via_transport(snap, transport, spreadsheet_id="r3c-once")
    back = transport.read_range("r3c-once", "Startups", "A:ZZ")
    idx = back[0].index("content.entityName")
    assert [r[idx] for r in back[1:]] == values

    write_snapshot_local(snap, tmp_path)
    with open(tmp_path / "Startups.csv", encoding="utf-8", newline="") as fh:
        parsed = list(csv.reader(fh))
    cidx = parsed[0].index("content.entityName")
    assert [r[cidx] for r in parsed[1:]] == ["'=1+1", "'\t=cmd", "'\r=cmd", "'-2", "plain"]


def test_r3c_long_cell_truncated_with_marker_and_byte_budget_batches():
    """>50,000-char cells are truncated visibly with full length; requests stay under byte budget."""
    from signal_ingest.export.writer import SHEETS_CELL_CHAR_LIMIT, export_via_transport

    long_text = "x" * 120_000
    rows = [
        {"content.title": f"t{i}", "content.text": long_text, "durableIdentity": f"n{i}"}
        for i in range(4)
    ]
    snap = _r3c_snapshot("News", rows)
    transport = b7_api.mock_sheets_transport()
    budget = 120_000
    export_via_transport(snap, transport, spreadsheet_id="r3c-long", batch_bytes=budget)
    back = transport.read_range("r3c-long", "News", "A:ZZ")
    assert len(back) == 1 + len(rows)
    tidx = back[0].index("content.text")
    for row in back[1:]:
        cell = row[tidx]
        assert len(cell) <= SHEETS_CELL_CHAR_LIMIT
        assert "TRUNCATED" in cell and "120000" in cell
    news_writes = [w for w in transport.writes if w["tab"] == "News"]
    assert len(news_writes) > 1, "byte budget must split the request"
    starts = [w["start_a1"] for w in news_writes]
    assert starts[0] == "A1" and len(set(starts)) == len(starts)


def test_r3c_adapter_fields_projected_to_sheet_columns():
    """Paper repoId/starsObservedAt (meta) and job listing/employer URLs (content) reach the tab."""
    from signal_ingest.export.project import _candidate_to_flat

    as_of = FROZEN
    paper = {
        "status": "accepted",
        "record_type": "RESEARCH_PAPER",
        "external_id": "2404.00004",
        "payload": {
            "schemaVersion": "1.0",
            "content": {"title": "P", "github_stars": 7},
            "meta": {
                "starsObservedAt": "2026-11-21T11:00:00Z",
                "repo_association": {"repo_id": "222002"},
            },
        },
        "evidence": {},
    }
    flat = _candidate_to_flat(paper, as_of=as_of, lineage_mode="fixture", run_id="r")
    assert flat["content.repoId"] == "222002"
    assert flat["content.starsObservedAt"] == "2026-11-21T11:00:00Z"

    job = {
        "status": "accepted",
        "record_type": "JOB",
        "external_id": "job-1",
        "payload": {
            "schemaVersion": "1.0",
            "content": {
                "date": "2026-11-21T06:00:00+00:00",
                "listing_url": "https://board.example/jobs/1",
                "original_employer_job_url": "https://employer.example/careers/1",
                "description": "Full job body.",
            },
        },
        "evidence": {"body": {"complete": True}},
    }
    jflat = _candidate_to_flat(job, as_of=as_of, lineage_mode="fixture", run_id="r")
    assert jflat["content.listingUrl"] == "https://board.example/jobs/1"
    assert jflat["content.employerJobUrl"] == "https://employer.example/careers/1"
    assert jflat["content.bodyLength"] == len("Full job body.")


def test_r3_export_freshness_judges_whole_publication_interval():
    """A fresh lower bound with an upper bound past as_of (day-only date) is not exported."""
    from datetime import datetime, timezone

    from signal_ingest.export.project import _candidate_to_flat

    as_of = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)

    def job(publication):
        return {
            "status": "accepted",
            "record_type": "JOB",
            "external_id": "job-r3",
            "payload": {"schemaVersion": "1.0", "content": {
                "date": "2026-11-21T00:00:00+00:00",
                "publication": publication,
                "description": "Body.",
            }},
            "evidence": {"body": {"complete": True}},
        }

    day_only = {"lower": "2026-11-21T00:00:00+00:00", "upper": "2026-11-22T00:00:00+00:00",
                "precision": "day"}
    exact = {"lower": "2026-11-21T00:00:00+00:00", "upper": "2026-11-21T00:00:00+00:00",
             "precision": "instant"}
    assert _candidate_to_flat(job(day_only), as_of=as_of, lineage_mode="fixture", run_id="r") is None
    assert _candidate_to_flat(job(exact), as_of=as_of, lineage_mode="fixture", run_id="r") is not None


# ---------------------------------------------------------------------------
# R4 — nearDuplicateOf / suggested_candidates export columns (notes/r4-work-order.md)
# Offline FakeEmbedder only; synthetic fixture rows, not for live export.
# ---------------------------------------------------------------------------

R4_TITLES = {
    "r4-news-a": "Aurora raises 5M seed round led by Acme Ventures",
    "r4-news-b": "Aurora raises 5M seed round, led by Acme Ventures!",  # near-dup of a
    "r4-news-c": "Aurora raises 5M seed round led by Acme Capital",  # 0.89 < 0.95
}


def _r4_news_run(conn, settings):
    from copy import deepcopy

    from signal_ingest.storage.records import create_run

    corpus = b7_api.load_json(B7_FIX / "corpus.json")
    base = next(i for i in corpus["items"] if i["logical_id"] == "news:aurora-raises-seed")
    run_id = create_run(conn, settings=settings, metadata={"scenario": "r4_near_dup"})
    for ext, title in R4_TITLES.items():
        body = deepcopy(base["body"])
        body["content"]["title"] = title
        url = f"https://fixtures.local/b7/news/{ext}"
        body["content"]["article_url"] = url
        body["source"]["url"] = url
        b7_api.insert_accepted_candidate(
            conn, run_id=run_id, record_type="NEWS", external_id=ext, payload=body,
            evidence={"body": base["full_text"]},
        )
    return run_id


def test_r4_near_duplicate_news_flagged_not_dropped(b7_env, tmp_path):
    """Same-run titles with cosine >= 0.95 get nearDuplicateOf; every row is still exported.

    Detects: near-dups dropped, flag on the wrong row, or threshold too loose (0.89 flagged).
    """
    from tests.integration import r4_api

    from signal_ingest.resolution.embed import flag_near_duplicate_news
    from signal_ingest.storage import connect

    settings = _settings(b7_env)
    assert r4_api.cosine(R4_TITLES["r4-news-a"], R4_TITLES["r4-news-b"]) >= 0.95
    assert r4_api.cosine(R4_TITLES["r4-news-a"], R4_TITLES["r4-news-c"]) < 0.95
    with connect(settings) as conn:
        run_id = _r4_news_run(conn, settings)
        assert flag_near_duplicate_news(conn, r4_api.FakeEmbedder(), run_id) == 1
        conn.commit()
        statuses = conn.execute(
            "SELECT external_id, status FROM candidates WHERE run_id = %s ORDER BY external_id",
            (str(run_id),),
        ).fetchall()
        snap = b7_api.build_export_snapshot(conn, run_id=run_id, as_of=AS_OF, settings=settings)
        conn.commit()

    assert statuses == [(e, "accepted") for e in sorted(R4_TITLES)]
    news = {r["durableIdentity"]: r for r in snap["tabs"]["News"]}
    assert set(news) == set(R4_TITLES), "near-duplicate rows must never be dropped"
    assert news["r4-news-b"]["nearDuplicateOf"] == "r4-news-a"
    assert not news["r4-news-a"].get("nearDuplicateOf")
    assert not news["r4-news-c"].get("nearDuplicateOf")
    assert "nearDuplicateOf" in snap["headers"]["News"]

    (tmp_path / "r4").mkdir()
    manifest = b7_api.write_snapshot_local(snap, tmp_path / "r4")
    assert manifest["not_for_live_export"] is True
    with open(tmp_path / "r4" / "News.csv", encoding="utf-8", newline="") as fh:
        parsed = list(csv.reader(fh))
    idx = parsed[0].index("nearDuplicateOf")
    assert sorted(r[idx] for r in parsed[1:]) == ["", "", "r4-news-a"]


def test_r4_no_embedding_export_byte_stable_without_r4_columns(b7_env, tmp_path):
    """Without embeddings the export headers equal TAB_HEADERS and bytes repeat exactly.

    Detects: R4 columns leaking into fixture exports that never computed embeddings.
    """
    from signal_ingest.export.tabs import TAB_HEADERS, TAB_NAMES
    from signal_ingest.storage import connect
    from signal_ingest.storage.records import create_run

    settings = _settings(b7_env)
    corpus = b7_api.load_json(B7_FIX / "corpus.json")
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings, metadata={"scenario": "r4_stable"})
        b7_api.load_mixed_corpus(conn, run_id=run_id, corpus=corpus)
        conn.commit()
        snaps = [
            b7_api.build_export_snapshot(conn, run_id=run_id, as_of=AS_OF, settings=settings)
            for _ in range(2)
        ]
        conn.commit()

    for snap in snaps:
        assert snap["headers"] == {n: list(TAB_HEADERS[n]) for n in TAB_NAMES}
        assert snap["counts"]["Entity Mapping Log"] >= 1
        for row in snap["tabs"]["Entity Mapping Log"]:
            assert not row.get("suggested_candidates")
        for row in snap["tabs"]["News"]:
            assert not row.get("nearDuplicateOf")
    dirs = [tmp_path / "s0", tmp_path / "s1"]
    for snap, d in zip(snaps, dirs, strict=True):
        d.mkdir()
        b7_api.write_snapshot_local(snap, d)
    for tab in TAB_NAMES:
        name = f"{tab.replace(' ', '_')}.csv"
        a, b = ((d / name).read_bytes() for d in dirs)
        assert a == b, tab
        assert b"suggested_candidates" not in a and b"nearDuplicateOf" not in a


def test_r4_mapping_export_carries_suggested_candidates_column(b7_env, tmp_path):
    """Once embeddings exist, Entity_Mapping_Log.csv gains suggested_candidates; decision unchanged."""
    from tests.integration import r4_api

    from signal_ingest.resolution import comparison_key
    from signal_ingest.resolution.mapping import annotate_mapping_suggestions
    from signal_ingest.storage import connect
    from signal_ingest.storage.records import create_run

    settings = _settings(b7_env)
    fake = r4_api.FakeEmbedder()
    with connect(settings) as conn:
        try:
            conn.execute("TRUNCATE source_identities, entities CASCADE")
            for eid, name in (
                ("fixture.r4.zephyr", "Zephyr Quantum Robotics"),
                ("fixture.r4.bluefin", "Bluefin Bakery"),
            ):
                conn.execute(
                    "INSERT INTO entities (id, namespace, canonical_name, lineage_mode, "
                    "comparison_key) VALUES (%s, 'startup', %s, 'fixture', %s)",
                    (eid, name, comparison_key(name)),
                )
            run_id = create_run(conn, settings=settings, metadata={"scenario": "r4_sugg"})
            b7_api.insert_mapping_event(
                conn, run_id=run_id, source_item_id="r4-zephyr",
                body={"entityType": "startup", "rawName": "Zephyr Quantum Robotics Labs",
                      "decision": "abstain", "method": "fixture"},
            )
            r4_api.embed_entities(conn, fake)
            assert annotate_mapping_suggestions(conn, fake, run_id=run_id) == 1
            conn.commit()
            snap = b7_api.build_export_snapshot(
                conn, run_id=run_id, as_of=AS_OF, settings=settings
            )
        finally:
            conn.rollback()
            conn.execute("TRUNCATE resolution_events, source_identities, entities CASCADE")
            conn.commit()

    assert "suggested_candidates" in snap["headers"]["Entity Mapping Log"]
    (tmp_path / "m").mkdir()
    b7_api.write_snapshot_local(snap, tmp_path / "m")
    with open(tmp_path / "m" / "Entity_Mapping_Log.csv", encoding="utf-8", newline="") as fh:
        parsed = list(csv.reader(fh))
    head = parsed[0]
    row = next(r for r in parsed[1:] if r[head.index("rawName")] == "Zephyr Quantum Robotics Labs")
    assert row[head.index("decision")] == "abstain"
    assert row[head.index("canonicalID")] == ""
    assert row[head.index("suggested_candidates")] == "Zephyr Quantum Robotics (0.87)"


# ---------------------------------------------------------------------------
# R5: XLSX handoff, architecture PDF page cap, bench mode guard
# ---------------------------------------------------------------------------


def _r5_xlsx_snapshot(tmp_path: Path) -> Path:
    from signal_ingest.export.writer import write_snapshot_local

    rows = [
        {"content.entityName": '=HYPERLINK("x")', "durableIdentity": "r5-formula"},
        {"content.entityName": "Ctrl\x01Char", "durableIdentity": "r5-ctrl"},
        {"content.entityName": "Plain Co", "durableIdentity": "r5-plain"},
    ]
    snap_dir = tmp_path / "snap"
    snap_dir.mkdir()
    write_snapshot_local(_r3c_snapshot("Startups", rows), snap_dir)
    return snap_dir


def test_r5_xlsx_six_tabs_headers_freeze_filter_and_text_cells(tmp_path):
    import zipfile

    from openpyxl import load_workbook

    from signal_ingest.export.tabs import TAB_NAMES
    from signal_ingest.export.xlsx import write_xlsx

    snap_dir = _r5_xlsx_snapshot(tmp_path)
    out = write_xlsx(snap_dir, tmp_path / "out.xlsx")
    wb = load_workbook(out["path"])
    assert wb.sheetnames == list(TAB_NAMES)
    for tab in TAB_NAMES:
        with open(snap_dir / f"{tab.replace(' ', '_')}.csv", encoding="utf-8", newline="") as fh:
            csv_rows = list(csv.reader(fh))
        ws = wb[tab]
        assert [c.value for c in ws[1]] == csv_rows[0], tab
        assert ws.freeze_panes == "A2", tab
        assert ws.auto_filter.ref == ws.dimensions, (tab, ws.auto_filter.ref, ws.dimensions)
        assert ws.max_row == len(csv_rows), tab

    ws = wb["Startups"]
    col = [c.value for c in ws[1]].index("content.entityName")
    values = [row[col].value for row in ws.iter_rows(min_row=2)]
    assert values == ['=HYPERLINK("x")', "CtrlChar", "Plain Co"]
    assert all(row[col].data_type == "s" for row in ws.iter_rows(min_row=2))

    with zipfile.ZipFile(out["path"]) as zf:
        sheets = [n for n in zf.namelist() if n.startswith("xl/worksheets/sheet")]
        assert len(sheets) == 6
        for name in sheets:
            xml = zf.read(name).decode("utf-8")
            assert "<f>" not in xml and "<f " not in xml, f"formula element in {name}"


def test_r5_xlsx_manifest_row_count_mismatch_raises(tmp_path):
    from signal_ingest.export.xlsx import write_xlsx

    snap_dir = _r5_xlsx_snapshot(tmp_path)
    manifest = json.loads((snap_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest["counts"]["Startups"] = 99
    (snap_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="Startups"):
        write_xlsx(snap_dir, tmp_path / "bad.xlsx")


def test_r5_pdf_over_three_pages_raises_and_writes_nothing(tmp_path):
    from signal_ingest.release.pdf import render_architecture_pdf

    src = tmp_path / "long.md"
    para = "Synthetic filler paragraph for the page cap check. " * 12
    src.write_text("# Too long\n\n" + "\n\n".join(f"## S{i}\n\n{para}" for i in range(80)), encoding="utf-8")
    out = tmp_path / "long.pdf"
    with pytest.raises(ValueError, match="pages"):
        render_architecture_pdf(out, src)
    assert not out.exists()


def test_r5_real_architecture_source_fits_three_pages_deterministic(tmp_path):
    from signal_ingest.release.pdf import (
        ARCHITECTURE_SOURCE_PATH,
        MAX_PAGES,
        count_pdf_pages,
        render_architecture_pdf,
        sha256_file,
    )

    source = Path(__file__).resolve().parents[2] / ARCHITECTURE_SOURCE_PATH
    a = render_architecture_pdf(tmp_path / "a.pdf", source)
    b = render_architecture_pdf(tmp_path / "b.pdf", source)
    assert 1 <= count_pdf_pages(a) <= MAX_PAGES == 3
    assert sha256_file(a) == sha256_file(b)


def test_r5_bench_refuses_live_mode(tmp_fixture_env):
    from signal_ingest.bench.runner import run_benchmark
    from signal_ingest.config import Mode, load_settings

    live = load_settings(mode=Mode.LIVE)
    with pytest.raises(RuntimeError, match="fixture mode"):
        run_benchmark(sizes=(1,), worker_counts=(1,), settings=live, origins=1)
