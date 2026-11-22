"""Mode-scoped settings: DSN, raw roots, network guard."""

from __future__ import annotations

from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Self
from urllib.parse import urlparse

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Mode(str, Enum):
    FIXTURE = "fixture"
    LIVE = "live"


class ModeDsnMismatchError(ValueError):
    """Raised when mode is paired with the wrong DSN or raw root."""


def _db_identity(dsn: str) -> tuple[str, str, str]:
    """Return (host, port, database/path) for DSN comparison."""
    parsed = urlparse(dsn)
    host = (parsed.hostname or "").lower()
    port = str(parsed.port or "")
    db = (parsed.path or "").lstrip("/").lower()
    return host, port, db


class Settings(BaseSettings):
    """Reads SIGNAL_INGEST_* from environment / .env."""

    model_config = SettingsConfigDict(
        env_prefix="SIGNAL_INGEST_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    mode: Mode = Mode.FIXTURE
    dsn_fixture: str = "postgresql://signal:signal@127.0.0.1:54329/signal_fixture"
    dsn_live: str = "postgresql://signal:signal@127.0.0.1:54329/signal_live"
    raw_root_fixture: Path = Path("var/fixture/raw")
    raw_root_live: Path = Path("var/live/raw")
    export_root_fixture: Path = Path("var/fixture/exports")
    export_root_live: Path = Path("var/live/exports")
    network_guard: bool = True
    paid_spend_cap: int = Field(default=0)

    @model_validator(mode="after")
    def _distinct_namespaces(self) -> Self:
        if _db_identity(self.dsn_fixture) == _db_identity(self.dsn_live):
            raise ModeDsnMismatchError(
                "fixture and live DSNs must target distinct databases/namespaces"
            )
        if self.raw_root_fixture.resolve() == self.raw_root_live.resolve():
            raise ModeDsnMismatchError(
                "fixture and live raw roots must be distinct paths"
            )
        if self.export_root_fixture.resolve() == self.export_root_live.resolve():
            raise ModeDsnMismatchError(
                "fixture and live export roots must be distinct paths"
            )
        return self

    @property
    def dsn(self) -> str:
        return self.dsn_fixture if self.mode is Mode.FIXTURE else self.dsn_live

    @property
    def raw_root(self) -> Path:
        return (
            self.raw_root_fixture
            if self.mode is Mode.FIXTURE
            else self.raw_root_live
        )

    @property
    def export_root(self) -> Path:
        return (
            self.export_root_fixture
            if self.mode is Mode.FIXTURE
            else self.export_root_live
        )

    def assert_mode_dsn(self, dsn: str | None = None) -> str:
        """Reject cross-pairing of mode with the wrong DSN before connect/write."""
        candidate = dsn if dsn is not None else self.dsn
        expected = self.dsn
        if _db_identity(candidate) != _db_identity(expected):
            raise ModeDsnMismatchError(
                f"mode={self.mode.value} refuses DSN that does not match "
                f"SIGNAL_INGEST_DSN_{self.mode.value.upper()}"
            )
        opposite = self.dsn_live if self.mode is Mode.FIXTURE else self.dsn_fixture
        if _db_identity(candidate) == _db_identity(opposite):
            raise ModeDsnMismatchError(
                f"mode={self.mode.value} cannot use the opposite-mode DSN"
            )
        return candidate

    def assert_mode_raw_root(self, root: Path | None = None) -> Path:
        candidate = (root if root is not None else self.raw_root).resolve()
        expected = self.raw_root.resolve()
        if candidate != expected:
            raise ModeDsnMismatchError(
                f"mode={self.mode.value} refuses raw root that does not match "
                f"SIGNAL_INGEST_RAW_ROOT_{self.mode.value.upper()}"
            )
        opposite = (
            self.raw_root_live
            if self.mode is Mode.FIXTURE
            else self.raw_root_fixture
        ).resolve()
        if candidate == opposite:
            raise ModeDsnMismatchError(
                f"mode={self.mode.value} cannot use the opposite-mode raw root"
            )
        return candidate

    def assert_mode_export_root(self, root: Path | None = None) -> Path:
        candidate = (root if root is not None else self.export_root).resolve()
        expected = self.export_root.resolve()
        if candidate != expected:
            raise ModeDsnMismatchError(
                f"mode={self.mode.value} refuses export root that does not match "
                f"SIGNAL_INGEST_EXPORT_ROOT_{self.mode.value.upper()}"
            )
        opposite = (
            self.export_root_live
            if self.mode is Mode.FIXTURE
            else self.export_root_fixture
        ).resolve()
        if candidate == opposite:
            raise ModeDsnMismatchError(
                f"mode={self.mode.value} cannot use the opposite-mode export root"
            )
        return candidate


def load_settings(**overrides: object) -> Settings:
    """Load settings from environment; apply network guard when enabled."""
    data = {k: v for k, v in overrides.items() if v is not None}
    if "mode" in data and isinstance(data["mode"], str):
        data["mode"] = Mode(data["mode"])
    settings = Settings(**data)  # type: ignore[arg-type]
    if settings.network_guard:
        from signal_ingest.config.network import install_network_guard

        install_network_guard()
    return settings


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()


def clear_settings_cache() -> None:
    get_settings.cache_clear()
