"""Retry-After parsing and full-jitter backoff (injectable clock / RNG)."""

from __future__ import annotations

import email.utils
import random
from datetime import datetime, timezone
from typing import Protocol


class RNG(Protocol):
    def random(self) -> float: ...


def ensure_aware_utc(instant: datetime) -> datetime:
    if instant.tzinfo is None:
        return instant.replace(tzinfo=timezone.utc)
    return instant.astimezone(timezone.utc)


def parse_retry_after(
    value: str | None,
    *,
    now: datetime,
) -> float | None:
    """Parse Retry-After as delay-seconds or HTTP-date (RFC 9110 §10.2.3).

    Returns delay in seconds relative to ``now``, or None if absent/unparseable.
    Never negative.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    now_utc = ensure_aware_utc(now)
    if text.isdigit():
        return float(text)
    # Some servers send an integer with trailing unit-ish junk; reject those.
    try:
        as_float = float(text)
        if as_float >= 0 and "." in text:
            return as_float
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if when is None:
        return None
    when = ensure_aware_utc(when)
    return max(0.0, (when - now_utc).total_seconds())


def full_jitter(
    attempt: int,
    *,
    base_s: float,
    cap_s: float,
    rng: RNG | None = None,
) -> float:
    """Full jitter in [0, min(cap, base × 2^attempt)].

    ``attempt`` is zero-based. Inject ``rng`` for deterministic tests.
    """
    if attempt < 0:
        raise ValueError("attempt must be >= 0")
    if base_s < 0 or cap_s < 0:
        raise ValueError("base_s and cap_s must be >= 0")
    high = min(cap_s, base_s * (2**attempt))
    if high <= 0:
        return 0.0
    source = rng if rng is not None else random
    return float(source.random()) * high


def resolve_backoff_delay(
    *,
    attempt: int,
    retry_after_s: float | None,
    base_s: float,
    cap_s: float,
    rng: RNG | None = None,
    local_budget_s: float | None = None,
) -> tuple[float, bool]:
    """Choose delay without shortening a server cooldown.

    Returns (delay_seconds, exceeds_local_budget).
    When Retry-After is present it is used as-is (never capped below server value).
    Otherwise full jitter applies (bounded by cap_s).
    """
    if retry_after_s is not None and retry_after_s >= 0:
        delay = float(retry_after_s)
        exceeds = local_budget_s is not None and delay > local_budget_s
        return delay, exceeds
    delay = full_jitter(attempt, base_s=base_s, cap_s=cap_s, rng=rng)
    exceeds = local_budget_s is not None and delay > local_budget_s
    return delay, exceeds
