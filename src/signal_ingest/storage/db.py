"""PostgreSQL connection and migration application."""

from __future__ import annotations

from pathlib import Path

import psycopg
from psycopg.pq import ExecStatus

from signal_ingest.config.settings import Settings, get_settings

# migrations/ at repository root (storage → signal_ingest → src → root)
_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_MIGRATIONS = _REPO_ROOT / "migrations"


def connect(settings: Settings | None = None, *, dsn: str | None = None):
    """Open a connection after mode/DSN cross-pair rejection."""
    cfg = settings or get_settings()
    resolved = cfg.assert_mode_dsn(dsn)
    cfg.assert_mode_raw_root()
    return psycopg.connect(resolved)


def connection_pool(settings: Settings | None = None, *, size: int = 2):
    """Fixed-size pool for one worker process; broken connections are replaced.

    `check` pings each connection on checkout, so a dropped DB socket gets a new
    connection instead of failing the task that happens to draw it.
    """
    from psycopg_pool import ConnectionPool

    cfg = settings or get_settings()
    resolved = cfg.assert_mode_dsn(None)
    cfg.assert_mode_raw_root()
    return ConnectionPool(
        resolved,
        min_size=size,
        max_size=size,
        kwargs={"connect_timeout": 10},
        check=ConnectionPool.check_connection,
        open=True,
    )


def _run_sql_script(conn: psycopg.Connection, script: str) -> None:
    """Execute a multi-statement SQL script via libpq simple query protocol."""
    # Clear any open transaction so PQexec can run the full script cleanly.
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        conn.commit()
    result = conn.pgconn.exec_(script.encode("utf-8"))
    if result is None:
        raise RuntimeError(
            conn.pgconn.error_message.decode()
            if conn.pgconn.error_message
            else "migration script failed"
        )
    # Drain results from multi-statement scripts.
    current = result
    while current is not None:
        if current.status == ExecStatus.FATAL_ERROR:
            msg = current.error_message
            text = msg.decode() if isinstance(msg, (bytes, bytearray)) else (msg or "error")
            raise RuntimeError(f"migration failed: {text}")
        if current.status not in (
            ExecStatus.COMMAND_OK,
            ExecStatus.TUPLES_OK,
            ExecStatus.EMPTY_QUERY,
            ExecStatus.NONFATAL_ERROR,
        ):
            msg = current.error_message
            text = msg.decode() if isinstance(msg, (bytes, bytearray)) else (msg or "error")
            raise RuntimeError(f"migration unexpected status {current.status}: {text}")
        current = conn.pgconn.get_result()


def init_db(
    settings: Settings | None = None,
    *,
    dsn: str | None = None,
    migrations_dir: Path | None = None,
) -> list[str]:
    """Apply pending SQL migrations for the active mode DSN. Returns applied versions."""
    cfg = settings or get_settings()
    resolved = cfg.assert_mode_dsn(dsn)
    cfg.assert_mode_raw_root()
    directory = migrations_dir or _DEFAULT_MIGRATIONS
    if not directory.is_dir():
        raise FileNotFoundError(f"migrations directory not found: {directory}")

    sql_files = sorted(directory.glob("*.sql"))
    applied: list[str] = []
    with psycopg.connect(resolved) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        conn.commit()
        for path in sql_files:
            version = path.name
            row = conn.execute(
                "SELECT 1 FROM schema_migrations WHERE version = %s",
                (version,),
            ).fetchone()
            if row:
                continue
            sql = path.read_text(encoding="utf-8")
            _run_sql_script(conn, sql)
            conn.execute(
                "INSERT INTO schema_migrations (version) VALUES (%s)",
                (version,),
            )
            conn.commit()
            applied.append(version)
    return applied
