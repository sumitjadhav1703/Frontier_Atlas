"""Load config/providers.yaml stubs (no secrets)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from signal_ingest.llm.provider import ProviderLimits
from signal_ingest.llm.quota import ProviderQuotaConfig, QuotaLedger

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROVIDERS_PATH = _REPO_ROOT / "config" / "providers.yaml"


def load_providers_config(path: Path | None = None) -> dict[str, Any]:
    target = path or DEFAULT_PROVIDERS_PATH
    text = target.read_text(encoding="utf-8")
    return _parse_nested_yaml(text, source=target.name)


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


def _parse_nested_yaml(text: str, *, source: str) -> dict[str, Any]:
    """Indentation-based map parser for stub YAML (no lists of maps required)."""
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]
    for lineno, raw in enumerate(text.splitlines(), start=1):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        if "\t" in raw.split(":")[0]:
            raise ValueError(f"{source}:{lineno}: tabs not allowed")
        line = raw.strip()
        if ":" not in line:
            raise ValueError(f"{source}:{lineno}: expected key: value")
        key, _, value = line.partition(":")
        key = key.strip()
        value = _strip_inline_comment(value.strip())
        while stack and indent <= stack[-1][0]:
            stack.pop()
        if not stack:
            raise ValueError(f"{source}:{lineno}: invalid indentation")
        parent = stack[-1][1]
        if value == "":
            child: dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _coerce(value)
    return root


def _strip_inline_comment(value: str) -> str:
    """`enabled: false # note` → `false` (a quoted value keeps its `#`)."""
    if value[:1] in {'"', "'"}:
        return value
    hash_at = value.find(" #")
    return value[:hash_at].rstrip() if hash_at >= 0 else value


def _get_int(block: dict[str, Any], key: str, default: int) -> int:
    value = block.get(key)
    return default if value is None else int(value)


def _coerce(value: str) -> Any:
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        return value[1:-1]
    if value.startswith("'") and value.endswith("'") and len(value) >= 2:
        return value[1:-1]
    lower = value.lower()
    if lower == "null":
        return None
    if lower == "true":
        return True
    if lower == "false":
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def build_chain(
    *,
    path: Path | None = None,
    connect: Any = None,
    transport: Any = None,
    order: tuple[str, ...] = ("gemini", "groq", "ollama"),
) -> Any:
    """Live extraction chain from providers.yaml, for extract(candidate, chain).

    Enabled tiers in `order`; keys come from each block's api_key_env. With
    `connect` (zero-arg psycopg connection factory) the daily quota is shared
    across worker processes through PostgreSQL.
    """
    from signal_ingest.llm.adapters.base import provider_from_config
    from signal_ingest.llm.fallback import ChainProvider, FallbackOrchestrator
    from signal_ingest.llm.http_transport import HttpxTransport
    from signal_ingest.llm.quota import PgQuotaLedger

    blocks = load_providers_config(path).get("providers") or {}
    ledger = quota_ledger_from_config(path)
    if connect is not None:
        ledger = PgQuotaLedger(
            paid_spend_cap=ledger.paid_spend_cap, providers=ledger.providers, connect=connect
        )
    providers = [
        provider_from_config(
            name,
            # Local CPU inference is slow; hosted tiers answer well within 30s.
            transport=transport or HttpxTransport(timeout_s=120.0 if name == "ollama" else 30.0),
            config=blocks[name],
            limits=provider_limits_from_config(name, path=path),
        )
        for name in order
        if isinstance(blocks.get(name), dict) and blocks[name].get("enabled")
    ]
    return ChainProvider(FallbackOrchestrator(providers=providers, quota=ledger))
