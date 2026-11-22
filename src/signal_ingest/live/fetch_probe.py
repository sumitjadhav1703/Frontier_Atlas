"""Shared live HTTP probe using Mode.LIVE FetchPolicy (no fixture loopback)."""

from __future__ import annotations

import asyncio
from typing import Any, Mapping

from signal_ingest.config.settings import Mode, Settings
from signal_ingest.crawl.fetch import FetchPolicy, fetch_with_retries
from signal_ingest.crawl.pool import HttpPool, PoolLimits
from signal_ingest.crawl.types import FetchOutcomeKind, RawObservation
from signal_ingest.live.manifests import redact_headers


async def probe_url(
    url: str,
    *,
    settings: Settings,
    headers: Mapping[str, str] | None = None,
    pool: HttpPool | None = None,
    max_retries: int = 2,
) -> RawObservation:
    policy = FetchPolicy(mode=Mode.LIVE, max_retries=max_retries, retry_cap_s=4.0)
    return await fetch_with_retries(
        url,
        policy,
        pool=pool,
        settings=settings,
        headers=headers,
        sleep=True,
    )


def probe_url_sync(
    url: str,
    *,
    settings: Settings,
    headers: Mapping[str, str] | None = None,
    max_retries: int = 2,
) -> RawObservation:
    return asyncio.run(probe_url(url, settings=settings, headers=headers, max_retries=max_retries))


def observation_summary(obs: RawObservation) -> dict[str, Any]:
    return {
        "request_url": obs.request_url,
        "final_url": obs.final_url,
        "status_code": obs.status_code,
        "outcome": obs.outcome.value if isinstance(obs.outcome, FetchOutcomeKind) else str(obs.outcome),
        "reason": obs.reason,
        "content_type": obs.content_type,
        "body_bytes": len(obs.body) if obs.body is not None else 0,
        "fetched_at": obs.fetched_at.isoformat() if obs.fetched_at else None,
        "headers": redact_headers(dict(obs.headers or {})),
    }


async def with_pool(coro_factory):
    pool = HttpPool(PoolLimits(user_agent="signal-ingest/0.1 (+live-qualify)"))
    try:
        return await coro_factory(pool)
    finally:
        await pool.aclose()
