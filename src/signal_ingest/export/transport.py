"""Sheets transport boundary: mock for fixture/contract tests; no live Sheets in B7."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SheetsTransport(Protocol):
    """Minimal batch-values interface used by the exporter."""

    def clear_tab(self, spreadsheet_id: str, tab: str) -> None: ...

    def write_range(
        self,
        spreadsheet_id: str,
        tab: str,
        start_a1: str,
        values: list[list[Any]],
        *,
        value_input_option: str = "RAW",
    ) -> dict[str, Any]: ...

    def read_range(
        self,
        spreadsheet_id: str,
        tab: str,
        a1_range: str,
    ) -> list[list[Any]]: ...


@dataclass
class MockSheetsTransport:
    """In-memory Sheets stand-in. Records every write; never opens a network socket."""

    spreadsheets: dict[str, dict[str, list[list[Any]]]] = field(default_factory=dict)
    writes: list[dict[str, Any]] = field(default_factory=list)
    clears: list[tuple[str, str]] = field(default_factory=list)

    def clear_tab(self, spreadsheet_id: str, tab: str) -> None:
        book = self.spreadsheets.setdefault(spreadsheet_id, {})
        book[tab] = []
        self.clears.append((spreadsheet_id, tab))

    def write_range(
        self,
        spreadsheet_id: str,
        tab: str,
        start_a1: str,
        values: list[list[Any]],
        *,
        value_input_option: str = "RAW",
    ) -> dict[str, Any]:
        if value_input_option != "RAW":
            raise ValueError("exporter requires RAW value_input_option")
        book = self.spreadsheets.setdefault(spreadsheet_id, {})
        # Replace from A1 for staging snapshots (clears trailing stale rows when
        # caller clears first, then writes the full tab including header).
        if start_a1.upper() in {"A1", f"'{tab}'!A1", f"{tab}!A1"}:
            book[tab] = [list(row) for row in values]
        else:
            existing = book.setdefault(tab, [])
            # Append-style for non-A1 (deterministic fixed ranges in batches).
            existing.extend(list(row) for row in values)
        record = {
            "spreadsheet_id": spreadsheet_id,
            "tab": tab,
            "start_a1": start_a1,
            "rows": len(values),
            "value_input_option": value_input_option,
        }
        self.writes.append(record)
        return {"updatedRows": len(values), "updatedRange": f"{tab}!{start_a1}"}

    def read_range(
        self,
        spreadsheet_id: str,
        tab: str,
        a1_range: str,
    ) -> list[list[Any]]:
        _ = a1_range
        book = self.spreadsheets.get(spreadsheet_id) or {}
        return [list(row) for row in book.get(tab, [])]


class LiveSheetsTransport:
    """Placeholder — live Google Sheets is deferred to B8/B9. Raises if invoked."""

    def clear_tab(self, spreadsheet_id: str, tab: str) -> None:
        raise RuntimeError("live Sheets transport disabled through B7")

    def write_range(
        self,
        spreadsheet_id: str,
        tab: str,
        start_a1: str,
        values: list[list[Any]],
        *,
        value_input_option: str = "RAW",
    ) -> dict[str, Any]:
        raise RuntimeError("live Sheets transport disabled through B7")

    def read_range(
        self,
        spreadsheet_id: str,
        tab: str,
        a1_range: str,
    ) -> list[list[Any]]:
        raise RuntimeError("live Sheets transport disabled through B7")


__all__ = [
    "LiveSheetsTransport",
    "MockSheetsTransport",
    "SheetsTransport",
]
