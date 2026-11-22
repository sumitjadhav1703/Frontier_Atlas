"""Lineage helpers: fixture observations cannot be promoted as live."""

from __future__ import annotations

from signal_ingest.config.settings import Mode


class FixtureLineageError(PermissionError):
    """Raised when fixture-derived data is offered for live admission."""


def lineage_mode_for(mode: Mode | str) -> str:
    if isinstance(mode, Mode):
        return mode.value
    return Mode(mode).value


def assert_live_admission(*, lineage_mode: str, target_mode: Mode | str) -> None:
    """Reject fixture lineage when admitting into a live run/export."""
    target = target_mode.value if isinstance(target_mode, Mode) else Mode(target_mode).value
    if target == Mode.LIVE.value and lineage_mode == Mode.FIXTURE.value:
        raise FixtureLineageError(
            "fixture lineage cannot be promoted into live mode"
        )
