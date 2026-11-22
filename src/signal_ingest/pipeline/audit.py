"""Programmatic run audit: counts, lineage, quarantines, raw provenance."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row

from signal_ingest.config.settings import Settings, get_settings
from signal_ingest.storage.db import connect


def audit_run(
    *,
    run_id: UUID | str,
    settings: Settings | None = None,
) -> dict[str, Any]:
    cfg = settings or get_settings()
    rid = UUID(str(run_id))
    with connect(cfg) as conn:
        run = conn.execute(
            "SELECT id, mode, status, created_at, cutoff_at, metadata FROM runs WHERE id = %s",
            (str(rid),),
        ).fetchone()
        if run is None:
            raise LookupError(f"run not found: {rid}")

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT record_type, status, COUNT(*) AS n
                FROM candidates
                WHERE run_id = %s
                GROUP BY record_type, status
                ORDER BY record_type, status
                """,
                (str(rid),),
            )
            candidate_counts = [dict(r) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT record_type, COUNT(*) AS n
                FROM records
                WHERE run_id = %s
                GROUP BY record_type
                ORDER BY record_type
                """,
                (str(rid),),
            )
            record_counts = [dict(r) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT COUNT(*) AS n FROM quarantines WHERE run_id = %s
                """,
                (str(rid),),
            )
            quarantine_n = int(cur.fetchone()["n"])

            cur.execute(
                """
                SELECT outcome, COUNT(*) AS n
                FROM observations
                WHERE run_id = %s
                GROUP BY outcome
                ORDER BY outcome
                """,
                (str(rid),),
            )
            observation_outcomes = [dict(r) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT COUNT(*) AS n
                FROM observations
                WHERE run_id = %s AND content_key IS NOT NULL
                """,
                (str(rid),),
            )
            with_content_key = int(cur.fetchone()["n"])

            cur.execute(
                """
                SELECT status, COUNT(*) AS n
                FROM tasks
                WHERE run_id = %s
                GROUP BY status
                ORDER BY status
                """,
                (str(rid),),
            )
            task_status = [dict(r) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT COUNT(*) AS n FROM resolution_events
                WHERE run_id = %s
                """,
                (str(rid),),
            )
            mapping_n = int(cur.fetchone()["n"])

        report = {
            "run_id": str(rid),
            "mode": run[1],
            "status": run[2],
            "created_at": run[3].isoformat() if run[3] else None,
            "cutoff_at": run[4].isoformat() if run[4] else None,
            "metadata": run[5] if isinstance(run[5], dict) else {},
            "candidates": candidate_counts,
            "records": record_counts,
            "quarantines": quarantine_n,
            "observations_by_outcome": observation_outcomes,
            "observations_with_content_key": with_content_key,
            "tasks_by_status": task_status,
            "mapping_events": mapping_n,
            "raw_root": str(cfg.assert_mode_raw_root()),
            "fixture_labeled": run[1] == "fixture",
            "ok": True,
        }
        # Soft checks: fixture runs must not claim live lineage on records.
        bad = conn.execute(
            """
            SELECT COUNT(*) FROM records
            WHERE run_id = %s AND lineage_mode <> %s
            """,
            (str(rid), run[1]),
        ).fetchone()
        lineage_mismatches = int(bad[0]) if bad else 0
        report["lineage_mismatches"] = lineage_mismatches
        report["ok"] = lineage_mismatches == 0
        conn.execute(
            "UPDATE runs SET metadata = metadata || %s::jsonb WHERE id = %s",
            (json.dumps({"last_audit_ok": report["ok"]}), str(rid)),
        )
        conn.commit()
    return report


__all__ = ["audit_run"]
