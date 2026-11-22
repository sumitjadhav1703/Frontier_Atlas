"""Fixture/live segregated export directories with visible labels."""

from __future__ import annotations

from pathlib import Path

from signal_ingest.config.settings import Mode, Settings, get_settings

FIXTURE_LABEL = "FIXTURE_EXPORT — synthetic / not_for_live_export"
LIVE_LABEL = "LIVE_EXPORT — live provenance required"


def export_snapshot_dir(
    settings: Settings | None,
    *,
    run_id: str,
    as_of: str,
) -> Path:
    """Mode-scoped root / run_id / as_of slug. Creates directory + LABEL."""
    cfg = settings or get_settings()
    root = cfg.assert_mode_export_root()
    safe_as_of = as_of.replace(":", "").replace("+", "p").replace(" ", "_")
    dest = root / str(run_id) / safe_as_of
    dest.mkdir(parents=True, exist_ok=True)
    label = FIXTURE_LABEL if cfg.mode is Mode.FIXTURE else LIVE_LABEL
    (dest / "LABEL.txt").write_text(
        f"{label}\nmode={cfg.mode.value}\nrun_id={run_id}\nas_of={as_of}\n",
        encoding="utf-8",
    )
    if cfg.mode is Mode.FIXTURE:
        (dest / "NOT_FOR_LIVE_EXPORT").write_text(
            "This directory holds fixture-mode projections only.\n",
            encoding="utf-8",
        )
    return dest


__all__ = [
    "FIXTURE_LABEL",
    "LIVE_LABEL",
    "export_snapshot_dir",
]
