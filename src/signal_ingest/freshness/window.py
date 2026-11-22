"""24h freshness window: admit iff entire interval ⊆ [T−24h, T]."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from signal_ingest.freshness.interval import PublicationInterval

WINDOW = timedelta(hours=24)


def freshness_bounds(cutoff: datetime) -> tuple[datetime, datetime]:
    """Return closed window [T−24h, T] in UTC."""
    if cutoff.tzinfo is None:
        raise ValueError("cutoff must be timezone-aware UTC")
    t = cutoff.astimezone(timezone.utc)
    return t - WINDOW, t


def in_freshness_window(
    interval: PublicationInterval,
    cutoff: datetime,
) -> bool:
    """
    Admit iff every plausible publication instant is inside [T−24h, T].

    Endpoints are included. A straddling or future interval is rejected.
    """
    if cutoff.tzinfo is None:
        raise ValueError("cutoff must be timezone-aware UTC")
    window_lower, window_upper = freshness_bounds(cutoff)
    return interval.lower >= window_lower and interval.upper <= window_upper
