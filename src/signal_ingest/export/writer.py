"""Batched deterministic export writer with RAW cells and snapshot manifest."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from uuid import UUID

from signal_ingest.config.settings import Mode, Settings, get_settings
from signal_ingest.export.csv_escape import write_csv_rows
from signal_ingest.export.flatten import project_row
from signal_ingest.export.paths import export_snapshot_dir
from signal_ingest.export.project import build_export_snapshot
from signal_ingest.export.tabs import TAB_HEADERS, TAB_NAMES
from signal_ingest.export.transport import MockSheetsTransport, SheetsTransport
from signal_ingest.storage.db import connect

DEFAULT_BATCH_SIZE = 500
# Google Sheets hard limit per cell, and a request byte budget under the ~10 MB cap.
SHEETS_CELL_CHAR_LIMIT = 50_000
DEFAULT_BATCH_BYTES = 8_000_000


def _bound_cell(value: Any) -> Any:
    """Truncate over-limit text with a visible marker carrying the full length.

    Formula-like strings are left as-is: RAW input keeps them literal in Sheets,
    and the CSV writer escapes them exactly once.
    """
    if isinstance(value, str) and len(value) > SHEETS_CELL_CHAR_LIMIT:
        marker = f"\n[TRUNCATED: full length {len(value)} chars; see content.bodyHash]"
        return value[: SHEETS_CELL_CHAR_LIMIT - len(marker)] + marker
    return value


def _tab_matrix(snapshot: dict[str, Any], tab: str) -> list[list[Any]]:
    headers = snapshot["headers"][tab]
    rows = snapshot["tabs"][tab]
    matrix: list[list[Any]] = [list(headers)]
    for flat in rows:
        cells = project_row(flat, tuple(headers))
        matrix.append([_bound_cell(c) for c in cells])
    return matrix


def _row_bytes(row: list[Any]) -> int:
    return len(json.dumps(row, ensure_ascii=False, default=str).encode("utf-8"))


def _batches(
    matrix: list[list[Any]], *, batch_size: int, batch_bytes: int
) -> list[tuple[int, list[list[Any]]]]:
    """Split into (start_row_index, rows) chunks bounded by row count and bytes."""
    out: list[tuple[int, list[list[Any]]]] = []
    start = 0
    chunk: list[list[Any]] = []
    size = 0
    for idx, row in enumerate(matrix):
        nbytes = _row_bytes(row)
        if chunk and (len(chunk) >= batch_size or size + nbytes > batch_bytes):
            out.append((start, chunk))
            start, chunk, size = idx, [], 0
        chunk.append(row)
        size += nbytes
    if chunk:
        out.append((start, chunk))
    return out


def _hash_matrix(matrix: list[list[Any]]) -> str:
    blob = json.dumps(matrix, ensure_ascii=False, sort_keys=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def write_snapshot_local(
    snapshot: dict[str, Any],
    dest: Path,
) -> dict[str, Any]:
    """Write six CSVs + manifest under a segregated export directory."""
    tab_hashes: dict[str, str] = {}
    tab_counts: dict[str, int] = {}
    for tab in TAB_NAMES:
        matrix = _tab_matrix(snapshot, tab)
        headers = matrix[0]
        body = matrix[1:]
        csv_text = write_csv_rows(headers, body)
        path = dest / f"{tab.replace(' ', '_')}.csv"
        path.write_text(csv_text, encoding="utf-8")
        tab_hashes[tab] = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()
        tab_counts[tab] = len(body)
    manifest = {
        "run_id": snapshot["run_id"],
        "as_of": snapshot["as_of"],
        "mode": snapshot["mode"],
        "lineage_mode": snapshot["lineage_mode"],
        "tab_names": list(TAB_NAMES),
        "counts": tab_counts,
        "hashes": tab_hashes,
        "fixture_labeled": snapshot["mode"] == Mode.FIXTURE.value,
        "not_for_live_export": snapshot["mode"] == Mode.FIXTURE.value,
        "directory": str(dest),
    }
    (dest / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def export_via_transport(
    snapshot: dict[str, Any],
    transport: SheetsTransport,
    *,
    spreadsheet_id: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    batch_bytes: int = DEFAULT_BATCH_BYTES,
) -> dict[str, Any]:
    """
    Replace each tab (clear + rewrite) with batched RAW writes.
    Idempotent retry: same snapshot → same matrix; clear removes stale trailing rows.
    Each request stays under batch_size rows and batch_bytes serialized bytes.
    """
    results: dict[str, Any] = {"tabs": {}, "spreadsheet_id": spreadsheet_id}
    for tab in TAB_NAMES:
        matrix = _tab_matrix(snapshot, tab)
        transport.clear_tab(spreadsheet_id, tab)
        # Batch including header in first chunk.
        written = 0
        for start, chunk in _batches(matrix, batch_size=batch_size, batch_bytes=batch_bytes):
            # Fixed A1 ranges: first batch replaces at A1; later batches append rows.
            start_a1 = "A1" if start == 0 else f"A{start + 1}"
            transport.write_range(
                spreadsheet_id,
                tab,
                start_a1,
                chunk,
                value_input_option="RAW",
            )
            written += len(chunk)
        read_back = transport.read_range(spreadsheet_id, tab, "A:ZZ")
        results["tabs"][tab] = {
            "written_rows": max(0, written - 1),  # exclude header
            "read_back_rows": max(0, len(read_back) - 1) if read_back else 0,
            "hash": _hash_matrix(matrix),
            "headers": list((snapshot.get("headers") or {}).get(tab) or TAB_HEADERS[tab]),
        }
    results["counts"] = {
        tab: results["tabs"][tab]["written_rows"] for tab in TAB_NAMES
    }
    return results


def run_export(
    *,
    run_id: UUID | str,
    as_of: str,
    settings: Settings | None = None,
    transport: SheetsTransport | None = None,
    spreadsheet_id: str | None = None,
    write_local: bool = True,
) -> dict[str, Any]:
    """Project + write fixture/live-segregated local export; optional mock Sheets."""
    cfg = settings or get_settings()
    with connect(cfg) as conn:
        snapshot = build_export_snapshot(conn, run_id=run_id, as_of=as_of, settings=cfg)
        conn.commit()

    out: dict[str, Any] = {
        "run_id": snapshot["run_id"],
        "as_of": snapshot["as_of"],
        "mode": snapshot["mode"],
        "counts": snapshot["counts"],
        "tab_names": list(TAB_NAMES),
    }

    if write_local:
        dest = export_snapshot_dir(cfg, run_id=str(snapshot["run_id"]), as_of=as_of)
        out["local"] = write_snapshot_local(snapshot, dest)
        out["export_dir"] = str(dest)

    sheets = transport
    if sheets is None and cfg.mode is Mode.FIXTURE:
        sheets = MockSheetsTransport()
    if sheets is not None:
        sid = spreadsheet_id or f"mock-{snapshot['mode']}-{snapshot['run_id']}"
        out["sheets"] = export_via_transport(snapshot, sheets, spreadsheet_id=sid)
        out["spreadsheet_id"] = sid
        if isinstance(sheets, MockSheetsTransport):
            out["transport"] = "mock"
    return out


__all__ = [
    "DEFAULT_BATCH_BYTES",
    "DEFAULT_BATCH_SIZE",
    "SHEETS_CELL_CHAR_LIMIT",
    "export_via_transport",
    "run_export",
    "write_snapshot_local",
]
