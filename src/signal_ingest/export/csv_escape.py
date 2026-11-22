"""CSV import fallback: formula escaping and quoted Unicode/newlines."""

from __future__ import annotations

import csv
import io
from typing import Any, Sequence

from signal_ingest.export.flatten import looks_like_formula


def escape_formula_cell(value: Any) -> Any:
    """Prefix formula-like strings so spreadsheet import treats them as text."""
    if looks_like_formula(value):
        return "'" + value
    return value


def write_csv_rows(
    headers: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> str:
    """RFC4180 CSV with quoting for commas/newlines/Unicode; formula-safe cells."""
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    writer.writerow(list(headers))
    for row in rows:
        writer.writerow([escape_formula_cell(c) for c in row])
    return buf.getvalue()


__all__ = ["escape_formula_cell", "write_csv_rows"]
