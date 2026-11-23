"""Load config/providers.yaml stubs (no secrets)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from signal_ingest.llm.provider import ProviderLimits
from signal_ingest.llm.quota import ProviderQuotaConfig, QuotaLedger

# Fake / replay never qualifies as a live provider for B5/B8 gates.
_LIVE_NAMES = frozenset({"gemini", "groq", "meta", "ollama"})
_FAKE_PREFIXES = ("fake", "replay", "mock", "scripted")

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROVIDERS_PATH = _REPO_ROOT / "config" / "providers.yaml"


def load_providers_config(path: Path | None = None) -> dict[str, Any]:
    target = path or DEFAULT_PROVIDERS_PATH
    loaded = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"{target.name}: expected a mapping at top level")
    return loaded


def provider_limits_from_config(
    name: str = "fake",
    *,
    path: Path | None = None,
) -> ProviderLimits:
    cfg = load_providers_config(path)
    providers = cfg.get("providers") or {}
    if name not in providers:
        raise KeyError(f"unknown provider stub: {name}")
    block = providers[name]
    return ProviderLimits(
        provider=str(block.get("provider") or name),
        model=str(block.get("model") or "PLACEHOLDER"),
        context_tokens=int(block["context_tokens"]),
        reserved_output_tokens=int(block["reserved_output_tokens"]),
        max_input_tokens=int(block["max_input_tokens"]),
        max_request_bytes=int(block["max_request_bytes"]),
        safety_margin_tokens=_get_int(block, "safety_margin_tokens", 64),
        framing_tokens=_get_int(block, "framing_tokens", 0),
        enabled=bool(block.get("enabled", True)),
    )


def quota_config_from_block(name: str, block: dict[str, Any]) -> ProviderQuotaConfig:
    billing = block.get("billing_enabled", False)
    if billing is None or (isinstance(billing, str) and billing.lower() == "null"):
        billing_enabled: bool | None = None
    else:
        billing_enabled = bool(billing)
    return ProviderQuotaConfig(
        provider=str(block.get("provider") or name),
        requests_per_day=_get_int(block, "requests_per_day", 10_000),
        tokens_per_day=_get_int(block, "tokens_per_day", 10_000_000),
        requests_per_run=int(block["requests_per_run"])
        if block.get("requests_per_run") is not None
        else None,
        tokens_per_run=int(block["tokens_per_run"])
        if block.get("tokens_per_run") is not None
        else None,
        billing_enabled=billing_enabled,
        is_local=bool(block.get("is_local", False)),
        is_fake=bool(block.get("is_fake", False)) or name == "fake",
    )


def quota_ledger_from_config(path: Path | None = None) -> QuotaLedger:
    """Build an in-memory QuotaLedger with paid_spend_cap from providers.yaml."""
    cfg = load_providers_config(path)
    defaults = cfg.get("defaults") or {}
    ledger = QuotaLedger(paid_spend_cap=int(defaults.get("paid_spend_cap", 0)))
    for name, block in (cfg.get("providers") or {}).items():
        if isinstance(block, dict):
            ledger.configure(quota_config_from_block(str(name), block))
    return ledger


def _get_int(block: dict[str, Any], key: str, default: int) -> int:
    value = block.get(key)
    return default if value is None else int(value)


def is_live_provider(provider: str) -> bool:
    """Return True only for real free-tier/local tier names — never fake."""
    name = (provider or "").strip().lower()
    if not name or name.startswith(_FAKE_PREFIXES):
        return False
    return name in _LIVE_NAMES


def __getattr__(name: str) -> Any:
    # Lazy re-export: importing config must not import litellm.
    if name == "build_chain":
        from signal_ingest.llm.router import build_chain

        return build_chain
    raise AttributeError(name)
