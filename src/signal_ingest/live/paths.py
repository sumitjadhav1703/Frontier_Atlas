"""Mode-scoped live artifact directories under var/live/."""

from __future__ import annotations

from pathlib import Path

from signal_ingest.config.settings import Mode, Settings

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LIVE_ROOT = _REPO_ROOT / "var" / "live"
DEFAULT_ARTIFACTS_GATES = _REPO_ROOT / "artifacts" / "gates"
B8_BLOCKER_PATH = DEFAULT_ARTIFACTS_GATES / "b8_blocker.json"


def assert_live_mode(settings: Settings) -> None:
    if settings.mode is not Mode.LIVE:
        raise RuntimeError(
            f"live qualification requires mode=live (got {settings.mode.value})"
        )


def live_root(settings: Settings | None = None) -> Path:
    """Return the mode-scoped live working root (sibling of raw/exports)."""
    if settings is not None:
        assert_live_mode(settings)
        # raw_root_live is var/live/raw → parent is var/live
        return settings.raw_root_live.resolve().parent
    return DEFAULT_LIVE_ROOT.resolve()


def ensure_live_dirs(settings: Settings) -> dict[str, Path]:
    """Create live subdirs; refuse fixture roots."""
    assert_live_mode(settings)
    root = live_root(settings)
    paths = {
        "root": root,
        "raw": settings.assert_mode_raw_root(),
        "exports": settings.assert_mode_export_root(),
        "manifests": root / "manifests",
        "probes": root / "probes",
        "canary": root / "canary",
        "seeds": root / "seeds",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    DEFAULT_ARTIFACTS_GATES.mkdir(parents=True, exist_ok=True)
    return paths
