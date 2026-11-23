"""Six-tab .xlsx built from a local export snapshot (user imports it into Google Sheets)."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.utils import get_column_letter

from signal_ingest.export.flatten import looks_like_formula
from signal_ingest.export.tabs import TAB_NAMES
from signal_ingest.export.writer import SHEETS_CELL_CHAR_LIMIT

DEFAULT_XLSX_NAME = "signal_ingest_export.xlsx"


def _read_csv(path: Path) -> list[list[str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.reader(fh))


def _cell_text(value: str) -> str:
    # Undo csv_escape.escape_formula_cell: the string cell type already keeps "=..." inert.
    # shortcut: an original value that was literally "'=..." also loses its apostrophe.
    if value.startswith("'") and looks_like_formula(value[1:]):
        value = value[1:]
    # XML 1.0 forbids C0 control chars; openpyxl raises on them.
    return ILLEGAL_CHARACTERS_RE.sub("", value)[:SHEETS_CELL_CHAR_LIMIT]


def write_xlsx(snapshot_dir: Path, out_path: Path) -> dict[str, Any]:
    """Write one workbook with exactly the six tabs, frozen header row and autofilter.

    The CSV's formula guard apostrophe is removed and every cell is forced to the
    string type, so ``=1+1`` is stored as inert text, never as a formula.
    """
    snapshot_dir = Path(snapshot_dir)
    manifest = json.loads((snapshot_dir / "manifest.json").read_text(encoding="utf-8"))
    if list(manifest.get("tab_names") or []) != list(TAB_NAMES):
        raise ValueError(f"manifest tab_names {manifest.get('tab_names')!r} != {TAB_NAMES!r}")

    wb = Workbook()
    wb.remove(wb.active)
    counts: dict[str, int] = {}
    for tab in TAB_NAMES:
        rows = _read_csv(snapshot_dir / f"{tab.replace(' ', '_')}.csv")
        if not rows:
            raise ValueError(f"{tab}: CSV has no header row")
        expected = manifest.get("counts", {}).get(tab)
        if expected is not None and int(expected) != len(rows) - 1:
            raise ValueError(f"{tab}: CSV rows {len(rows) - 1} != manifest count {expected}")
        ws = wb.create_sheet(title=tab)
        for r, row in enumerate(rows, start=1):
            for c, value in enumerate(row, start=1):
                if value == "":
                    continue
                cell = ws.cell(row=r, column=c)
                cell.value = _cell_text(value)
                cell.data_type = "s"
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:{get_column_letter(len(rows[0]))}{len(rows)}"
        counts[tab] = len(rows) - 1

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out_path)
    return {
        "path": str(out_path),
        "snapshot_dir": str(snapshot_dir),
        "run_id": manifest.get("run_id"),
        "as_of": manifest.get("as_of"),
        "mode": manifest.get("mode"),
        "tab_names": list(TAB_NAMES),
        "counts": counts,
        "sha256": hashlib.sha256(out_path.read_bytes()).hexdigest(),
    }


__all__ = ["DEFAULT_XLSX_NAME", "write_xlsx"]
