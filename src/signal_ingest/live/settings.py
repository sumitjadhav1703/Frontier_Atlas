"""Live-mode settings helpers (network guard off for public HTTPS)."""

from __future__ import annotations

import os
from typing import Any

from signal_ingest.config.network import uninstall_network_guard
from signal_ingest.config.settings import Mode, Settings, load_settings


def load_live_settings(**overrides: Any) -> Settings:
    """
    Load settings forced to mode=live with network_guard disabled.

    Prefer brew Postgres :5432 when SIGNAL_INGEST_DSN_LIVE is unset and the
    compose port is unreachable — callers may still override via env.
    """
    data: dict[str, Any] = {"mode": Mode.LIVE, "network_guard": False}
    data.update({k: v for k, v in overrides.items() if v is not None})
    # Ensure env mode does not stick fixture.
    os.environ.setdefault("SIGNAL_INGEST_MODE", "live")
    settings = load_settings(**data)
    # Defensive: load_settings may have installed a guard when env said so.
    uninstall_network_guard()
    if settings.mode is not Mode.LIVE:
        raise RuntimeError("failed to force mode=live")
    if settings.network_guard:
        # Rebuild without guard.
        settings = load_settings(mode=Mode.LIVE, network_guard=False)
        uninstall_network_guard()
    return settings


def github_token_from_env() -> str | None:
    """Return a GitHub token from common env names without logging the value."""
    for name in (
        "GITHUB_TOKEN",
        "GH_TOKEN",
        "GITHUB_PAT_TOKEN",
        "GITHUB_PERSONAL_ACCESS_TOKEN",
    ):
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    return None


def provider_keys_present() -> dict[str, bool]:
    return {
        "GEMINI_API_KEY": bool(os.environ.get("GEMINI_API_KEY", "").strip()),
        "GROQ_API_KEY": bool(os.environ.get("GROQ_API_KEY", "").strip()),
        "github_token_env": github_token_from_env() is not None,
    }
