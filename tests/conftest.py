"""Shared pytest fixtures for offline gates (temp paths, frozen clock helpers)."""

from __future__ import annotations

import os

# Before anything can import litellm: its DEV mode runs load_dotenv() and would
# pull a local .env (real keys) into the test process.
os.environ["LITELLM_MODE"] = "PRODUCTION"
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")

from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.support import FIXTURES

FROZEN_NOW = datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture
def tmp_fixture_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path | str]:
    """Isolate fixture-mode DSN/raw-root under a temporary directory.

    Uses a distinct fixture DSN hostname/path marker so live DSN cannot match.
    Does not touch live storage roots.
    """
    raw_fixture = tmp_path / "raw_fixture"
    raw_live = tmp_path / "raw_live"
    raw_fixture.mkdir()
    raw_live.mkdir()

    # Distinct namespaces: fixture DB name vs live DB name on loopback.
    dsn_fixture = os.environ.get(
        "SIGNAL_INGEST_TEST_DSN_FIXTURE",
        "postgresql://signal:signal@127.0.0.1:54329/signal_fixture",
    )
    dsn_live = os.environ.get(
        "SIGNAL_INGEST_TEST_DSN_LIVE",
        "postgresql://signal:signal@127.0.0.1:54329/signal_live",
    )

    monkeypatch.setenv("SIGNAL_INGEST_MODE", "fixture")
    monkeypatch.setenv("SIGNAL_INGEST_DSN_FIXTURE", dsn_fixture)
    monkeypatch.setenv("SIGNAL_INGEST_DSN_LIVE", dsn_live)
    monkeypatch.setenv("SIGNAL_INGEST_RAW_ROOT_FIXTURE", str(raw_fixture))
    monkeypatch.setenv("SIGNAL_INGEST_RAW_ROOT_LIVE", str(raw_live))
    monkeypatch.setenv("SIGNAL_INGEST_NETWORK_GUARD", "1")
    monkeypatch.setenv("SIGNAL_INGEST_PAID_SPEND_CAP", "0")

    return {
        "raw_fixture": raw_fixture,
        "raw_live": raw_live,
        "dsn_fixture": dsn_fixture,
        "dsn_live": dsn_live,
        "mode": "fixture",
    }


@pytest.fixture
def frozen_clock():
    """Install package FrozenClock when available; yield the frozen instant."""
    from signal_ingest.config import FrozenClock, get_clock, set_clock

    previous = get_clock()
    clock = FrozenClock(FROZEN_NOW)
    set_clock(clock)
    try:
        yield FROZEN_NOW
    finally:
        set_clock(previous)
