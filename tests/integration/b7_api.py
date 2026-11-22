"""Import helpers for B7 export / dual-worker / benchmark surfaces.

Thin wrappers over `signal_ingest.export`, `signal_ingest.pipeline`, and
`signal_ingest.bench` so gates fail clearly when the B7 stack is incomplete.
Oracle: planner B7 + docs/build-plan.md / docs/contracts.md.
"""

from __future__ import annotations

import importlib
import json
import multiprocessing as mp
import os
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from tests.support import FIXTURES

B7_FIXTURES = FIXTURES / "b7"
CORPUS_PATH = B7_FIXTURES / "corpus.json"
CHANGED_OBS_PATH = B7_FIXTURES / "changed_observation.json"
SHEETS_ROWS_PATH = B7_FIXTURES / "sheets_rows.json"
CSV_CASES_PATH = B7_FIXTURES / "csv_escape_cases.json"
TINY_REGRESSION_PATH = B7_FIXTURES / "tiny_regression.json"
BENCH_KEYS_PATH = B7_FIXTURES / "benchmark_expected_keys.json"
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BENCH_ARTIFACT = REPO_ROOT / "artifacts" / "gates" / "b7_benchmark.json"

EXPECTED_TAB_NAMES = (
    "Startups",
    "Products",
    "Research Papers",
    "Jobs",
    "News",
    "Entity Mapping Log",
)


class MissingB7APIError(AssertionError):
    """Required B7 API is not importable."""


def _import(path: str) -> Any:
    try:
        return importlib.import_module(path)
    except ImportError as exc:
        raise MissingB7APIError(f"Cannot import {path}: {exc}") from exc


def require_attr(module: Any, *names: str) -> Any:
    tried = []
    for name in names:
        tried.append(f"{getattr(module, '__name__', module)}.{name}")
        if hasattr(module, name):
            return getattr(module, name)
    raise MissingB7APIError("Missing API: " + ", ".join(tried))


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_export_pkg() -> Any:
    return _import("signal_ingest.export")


def load_pipeline_pkg() -> Any:
    return _import("signal_ingest.pipeline")


def load_bench_pkg() -> Any:
    return _import("signal_ingest.bench")


def tab_names() -> tuple[str, ...]:
    pkg = load_export_pkg()
    names = require_attr(pkg, "TAB_NAMES")
    return tuple(names)


def mock_sheets_transport() -> Any:
    pkg = load_export_pkg()
    cls = require_attr(pkg, "MockSheetsTransport")
    return cls()


def escape_formula_cell(value: Any) -> Any:
    mod = _import("signal_ingest.export.csv_escape")
    return require_attr(mod, "escape_formula_cell")(value)


def write_csv_rows(headers: list[str], rows: list[list[Any]]) -> str:
    mod = _import("signal_ingest.export.csv_escape")
    return require_attr(mod, "write_csv_rows")(headers, rows)


def export_snapshot_dir(settings: Any, *, run_id: str, as_of: str) -> Path:
    pkg = load_export_pkg()
    return require_attr(pkg, "export_snapshot_dir")(
        settings, run_id=run_id, as_of=as_of
    )


def build_export_snapshot(conn: Any, *, run_id: Any, as_of: str, settings: Any) -> dict:
    pkg = load_export_pkg()
    return require_attr(pkg, "build_export_snapshot")(
        conn, run_id=run_id, as_of=as_of, settings=settings
    )


def write_snapshot_local(snapshot: dict, dest: Path) -> dict:
    pkg = load_export_pkg()
    return require_attr(pkg, "write_snapshot_local")(snapshot, dest)


def export_via_transport(
    snapshot: dict,
    transport: Any,
    *,
    spreadsheet_id: str,
) -> dict:
    pkg = load_export_pkg()
    return require_attr(pkg, "export_via_transport")(
        snapshot, transport, spreadsheet_id=spreadsheet_id
    )


def run_export(**kwargs: Any) -> dict:
    pkg = load_export_pkg()
    return require_attr(pkg, "run_export")(**kwargs)


def open_observation_by_content_key(conn: Any, *, content_key: str, settings: Any) -> dict:
    pkg = load_pipeline_pkg()
    return require_attr(pkg, "open_observation_by_content_key")(
        conn, content_key=content_key, settings=settings
    )


def crawl_source(**kwargs: Any) -> dict:
    pkg = load_pipeline_pkg()
    return require_attr(pkg, "crawl_source")(**kwargs)


def work_stage(**kwargs: Any) -> dict:
    pkg = load_pipeline_pkg()
    return require_attr(pkg, "work_stage")(**kwargs)


def resume_run(**kwargs: Any) -> dict:
    pkg = load_pipeline_pkg()
    return require_attr(pkg, "resume_run")(**kwargs)


def audit_run(**kwargs: Any) -> dict:
    pkg = load_pipeline_pkg()
    return require_attr(pkg, "audit_run")(**kwargs)


def run_benchmark(**kwargs: Any) -> dict:
    pkg = load_bench_pkg()
    return require_attr(pkg, "run_benchmark")(**kwargs)


def write_benchmark_artifact(report: dict | None = None, *, path: Path | None = None) -> Path:
    pkg = load_bench_pkg()
    fn = require_attr(pkg, "write_benchmark_artifact")
    if path is not None:
        return fn(report, path=path)
    return fn(report)


def load_cli_app() -> Any:
    mod = _import("signal_ingest.cli.main")
    return require_attr(mod, "app")


# ---------------------------------------------------------------------------
# Dual-worker subprocess helpers (separate cwd, shared raw root)
# ---------------------------------------------------------------------------


def _worker1_write(q: Any, shared_raw: str, cwd: str, body: bytes, env: dict[str, str]) -> None:
    os.chdir(cwd)
    for k, v in env.items():
        os.environ[k] = v
    from signal_ingest.config.settings import Mode, clear_settings_cache, load_settings
    from signal_ingest.crawl.store import RawStore

    clear_settings_cache()
    settings = load_settings(mode=Mode.FIXTURE)
    store = RawStore.from_settings(settings)
    assert str(store.root.resolve()) == str(Path(shared_raw).resolve())
    key, digest = store.write(body)
    q.put({"content_key": key, "sha256": digest, "cwd": os.getcwd()})


def _worker2_read(q: Any, shared_raw: str, cwd: str, content_key: str, env: dict[str, str]) -> None:
    os.chdir(cwd)
    for k, v in env.items():
        os.environ[k] = v
    from signal_ingest.config.settings import Mode, clear_settings_cache, load_settings
    from signal_ingest.crawl.store import RawStore

    clear_settings_cache()
    settings = load_settings(mode=Mode.FIXTURE)
    store = RawStore.from_settings(settings)
    assert str(store.root.resolve()) == str(Path(shared_raw).resolve())
    # Must succeed using relative content_key only (no absolute path from worker-1).
    body = store.read_key(content_key)
    q.put({"body": body, "cwd": os.getcwd(), "raw_root": str(store.root)})


def dual_worker_content_key_roundtrip(
    *,
    shared_raw: Path,
    cwd_a: Path,
    cwd_b: Path,
    body: bytes,
    env: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Spawn two processes with distinct cwd; reopen via relative content_key."""
    ctx = mp.get_context("spawn")
    q1: mp.Queue = ctx.Queue()
    q2: mp.Queue = ctx.Queue()
    p1 = ctx.Process(
        target=_worker1_write,
        args=(q1, str(shared_raw), str(cwd_a), body, env),
    )
    p1.start()
    p1.join(timeout=30)
    if p1.exitcode != 0:
        raise RuntimeError(f"worker-1 failed exit={p1.exitcode}")
    w1 = q1.get(timeout=5)
    p2 = ctx.Process(
        target=_worker2_read,
        args=(q2, str(shared_raw), str(cwd_b), w1["content_key"], env),
    )
    p2.start()
    p2.join(timeout=30)
    if p2.exitcode != 0:
        raise RuntimeError(f"worker-2 failed exit={p2.exitcode}")
    w2 = q2.get(timeout=5)
    return w1, w2


RECORD_VERTICALS = frozenset(
    {"STARTUP", "PRODUCT", "RESEARCH_PAPER", "JOB", "NEWS"}
)
VERTICAL_TO_TAB = {
    "STARTUP": "Startups",
    "PRODUCT": "Products",
    "RESEARCH_PAPER": "Research Papers",
    "JOB": "Jobs",
    "NEWS": "News",
    "ENTITY_MAPPING": "Entity Mapping Log",
}


def insert_candidate(
    conn: Any,
    *,
    run_id: UUID,
    record_type: str,
    external_id: str,
    payload: dict[str, Any],
    status: str = "accepted",
    evidence: dict[str, Any] | None = None,
    source_id: str = "b7.fixture",
) -> UUID:
    """Insert/upsert a candidate row (accepted or rejected status)."""
    from datetime import datetime, timezone

    from psycopg.types.json import Jsonb

    if status not in {
        "pending",
        "accepted",
        "quarantined",
        "ineligible",
        "superseded",
    }:
        raise ValueError(f"invalid candidate status: {status}")

    cid = uuid4()
    semantic = f"{source_id}:{record_type}:{external_id}"
    row = conn.execute(
        """
        INSERT INTO candidates (
            id, run_id, source_id, record_type, external_id, semantic_key,
            status, payload, evidence, collected_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (semantic_key) DO UPDATE SET
            run_id = EXCLUDED.run_id,
            payload = EXCLUDED.payload,
            evidence = EXCLUDED.evidence,
            status = EXCLUDED.status,
            updated_at = NOW()
        RETURNING id
        """,
        (
            str(cid),
            str(run_id),
            source_id,
            record_type,
            external_id,
            semantic,
            status,
            Jsonb(payload),
            Jsonb(evidence or {}),
            datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc),
        ),
    ).fetchone()
    return UUID(str(row[0])) if row else cid


def insert_accepted_candidate(
    conn: Any,
    *,
    run_id: UUID,
    record_type: str,
    external_id: str,
    payload: dict[str, Any],
    evidence: dict[str, Any] | None = None,
    source_id: str = "b7.fixture",
) -> UUID:
    """Insert one accepted candidate row for export / change-replay tests."""
    return insert_candidate(
        conn,
        run_id=run_id,
        record_type=record_type,
        external_id=external_id,
        payload=payload,
        status="accepted",
        evidence=evidence,
        source_id=source_id,
    )


def insert_mapping_event(
    conn: Any,
    *,
    run_id: UUID,
    body: dict[str, Any],
    source_item_id: str,
) -> UUID:
    """Persist a corpus mapping row into resolution_events via production API."""
    from signal_ingest.resolution.mapping import record_resolution_event
    from signal_ingest.resolution.types import DecisionKind, Namespace, ResolveDecision

    raw_decision = str(body.get("decision") or "abstain")
    if raw_decision in {"merged", "match", "unchanged"}:
        kind = DecisionKind.MATCH
    elif raw_decision in {"new"}:
        kind = DecisionKind.NEW
    else:
        kind = DecisionKind.ABSTAIN

    from signal_ingest.resolution.mapping import mapping_observation_key

    ns_raw = str(body.get("entityType") or "startup")
    namespace = Namespace(ns_raw if ns_raw in {"startup", "product"} else "startup")
    raw_name = str(body.get("rawName") or "")
    rule_version = str(body.get("ruleVersion") or "fixture-seeds-v1")
    decision = ResolveDecision(
        decision=kind,
        canonical_id=body.get("canonicalID"),
        canonical_name=body.get("canonicalName"),
        method=str(body.get("method") or "fixture"),
        rule_version=rule_version,
        raw_name=raw_name,
        comparison_key="",
        namespace=namespace,
        evidence_url=body.get("evidenceURL"),
        observation_key=mapping_observation_key(
            # Include run_id so reloads bind events to the current export run
            # (observation_key is globally unique; DO NOTHING would keep a stale run_id).
            source_item_id=f"{run_id}:{source_item_id}",
            field="entityName",
            raw_value=raw_name,
            rule_version=rule_version,
        ),
        reason=None,
    )
    return record_resolution_event(conn, decision, run_id=run_id)


def load_mixed_corpus(
    conn: Any,
    *,
    run_id: UUID,
    corpus: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Load B7 mixed corpus: accepts → exportable; rejects → non-accepted status."""
    data = corpus if corpus is not None else load_json(CORPUS_PATH)
    loaded = {"accepted": [], "rejected": [], "mapping_accepted": [], "mapping_rejected": []}
    for item in data["items"]:
        vertical = item["vertical"]
        decision = item["decision"]
        if vertical == "ENTITY_MAPPING":
            insert_mapping_event(
                conn,
                run_id=run_id,
                body=item["body"],
                source_item_id=item["logical_id"],
            )
            bucket = (
                loaded["mapping_accepted"]
                if decision == "accept"
                else loaded["mapping_rejected"]
            )
            bucket.append(item)
            continue
        if vertical not in RECORD_VERTICALS:
            raise ValueError(f"unknown vertical: {vertical}")
        status = "accepted" if decision == "accept" else "quarantined"
        insert_candidate(
            conn,
            run_id=run_id,
            record_type=vertical,
            external_id=item["logical_id"],
            payload=item["body"],
            status=status,
            evidence={
                "body": item.get("full_text") or "",
                "span": item.get("evidence_span"),
                "reject_reason": item.get("reason"),
                "date_extraction_method": "structured_datePublished",
            },
        )
        if decision == "accept":
            loaded["accepted"].append(item)
        else:
            loaded["rejected"].append(item)
    return loaded


def reject_markers(item: dict[str, Any]) -> list[str]:
    """Stable strings that must not appear in public export for a rejected item."""
    markers: list[str] = [item["logical_id"], item.get("source_url") or ""]
    body = item.get("body") or {}
    content = body.get("content") or {}
    for key in (
        "entityName",
        "productName",
        "title",
        "company",
        "rawName",
    ):
        val = content.get(key) if key != "rawName" else body.get(key)
        if val:
            markers.append(str(val))
    return [m for m in markers if m]


def accept_markers(item: dict[str, Any]) -> list[str]:
    """At least one of these should appear for an accepted item."""
    return reject_markers(item)


def update_candidate_payload(
    conn: Any,
    *,
    semantic_key: str,
    payload: dict[str, Any],
) -> None:
    from psycopg.types.json import Jsonb

    conn.execute(
        """
        UPDATE candidates
        SET payload = %s, updated_at = NOW()
        WHERE semantic_key = %s
        """,
        (Jsonb(payload), semantic_key),
    )


def secrets_scan(text: str, forbidden: list[str]) -> list[str]:
    hits = []
    for needle in forbidden:
        if needle and needle in text:
            hits.append(needle)
    return hits
