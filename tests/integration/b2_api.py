"""Import helpers and fixture constructors for B2 bulk adapters."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from signal_ingest.crawl.types import FetchOutcomeKind, RawObservation
from tests.support import FIXTURES

BULK_FIXTURES = FIXTURES / "bulk"


def load_bulk_json(name: str) -> dict[str, Any]:
    return json.loads((BULK_FIXTURES / name).read_text(encoding="utf-8"))


def assert_synthetic(meta: dict[str, Any]) -> None:
    assert meta.get("label") == "synthetic"
    assert meta.get("not_for_live_export") is True


def raw_json(
    url: str,
    payload: Any,
    *,
    outcome: FetchOutcomeKind = FetchOutcomeKind.OK,
    status_code: int | None = 200,
    etag: str | None = None,
    content_sha256: str | None = None,
    fetched_at: datetime | None = None,
    request_url: str | None = None,
) -> RawObservation:
    body = json.dumps(payload).encode("utf-8")
    when = fetched_at or datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc)
    return RawObservation(
        request_url=request_url or url,
        final_url=url,
        status_code=status_code,
        outcome=outcome,
        body=body,
        content_type="application/json",
        etag=etag,
        fetched_at=when,
        content_sha256=content_sha256,
        metadata={"synthetic": True, "not_for_live_export": True},
    )


def raw_empty(
    url: str,
    *,
    outcome: FetchOutcomeKind,
    status_code: int | None = None,
    reason: str | None = None,
) -> RawObservation:
    return RawObservation(
        request_url=url,
        final_url=url,
        status_code=status_code,
        outcome=outcome,
        body=None,
        reason=reason,
        fetched_at=datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc),
        metadata={"synthetic": True},
    )


def fixture_paths() -> list[Path]:
    return sorted(BULK_FIXTURES.glob("*"))


def http_pool(*, max_active: int = 2) -> Any:
    """Bounded HttpPool for fixture-mode bulk drain (no live net)."""
    from signal_ingest.crawl import HttpPool, PoolLimits

    return HttpPool(
        PoolLimits(
            max_active=max_active,
            max_pending=max(max_active * 4, 8),
            per_origin_concurrency=max_active,
            per_origin_min_interval_s=0.0,
            connect_timeout_s=0.5,
            read_timeout_s=0.5,
            overall_timeout_s=1.0,
            max_compressed_bytes=64 * 1024,
            max_decoded_bytes=128 * 1024,
            max_redirects=5,
        )
    )
