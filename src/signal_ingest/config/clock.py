"""Injectable UTC clock; naive datetimes are rejected at model boundaries."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """Return an aware UTC datetime."""


class SystemClock:
    """Wall-clock time in UTC."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class FrozenClock:
    """Deterministic clock for tests and frozen run cutoffs."""

    def __init__(self, instant: datetime) -> None:
        if instant.tzinfo is None:
            raise ValueError("FrozenClock requires an aware UTC datetime")
        self._instant = instant.astimezone(timezone.utc)

    def now(self) -> datetime:
        return self._instant


class ControllableClock:
    """Injectable clock that tests can advance without sleeping (B5 jitter)."""

    def __init__(self, instant: datetime) -> None:
        if instant.tzinfo is None:
            raise ValueError("ControllableClock requires an aware UTC datetime")
        self._instant = instant.astimezone(timezone.utc)

    def now(self) -> datetime:
        return self._instant

    def set(self, instant: datetime) -> None:
        if instant.tzinfo is None:
            raise ValueError("ControllableClock.set requires an aware UTC datetime")
        self._instant = instant.astimezone(timezone.utc)

    def advance(self, seconds: float) -> datetime:
        from datetime import timedelta

        self._instant = self._instant + timedelta(seconds=float(seconds))
        return self._instant


_clock: Clock = SystemClock()


def get_clock() -> Clock:
    return _clock


def set_clock(clock: Clock) -> None:
    global _clock
    _clock = clock
