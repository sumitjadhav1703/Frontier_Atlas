"""Relational graph demo queries over the relationships table (no graph DB)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

QUERIES_DIR = Path(__file__).resolve().parents[3] / "docs" / "queries"


def papers_by_yc_startups(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Papers whose implementing repo is owned (1..6 OWNS hops) by a YC startup."""
    sql = (QUERIES_DIR / "papers_by_yc_startups.sql").read_text(encoding="utf-8")
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        return list(cur.fetchall())


__all__ = ["QUERIES_DIR", "papers_by_yc_startups"]
