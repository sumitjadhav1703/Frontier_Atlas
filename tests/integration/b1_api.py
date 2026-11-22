"""Import helpers for signal_ingest.crawl public surfaces.

Prefer package exports; fall back to submodules so gates fail clearly
when the B1 crawl package is incomplete.
"""

from __future__ import annotations

import importlib
from typing import Any


class MissingCrawlAPIError(AssertionError):
    """Required B1 crawl API is not importable."""


def _import_first(*module_paths: str) -> Any:
    errors: list[str] = []
    for path in module_paths:
        try:
            return importlib.import_module(path)
        except ImportError as exc:
            errors.append(f"{path}: {exc}")
    raise MissingCrawlAPIError(
        "Cannot import crawl modules. Tried: " + "; ".join(errors)
    )


def require_attr(module: Any, *names: str) -> Any:
    tried = []
    for name in names:
        tried.append(f"{module.__name__}.{name}")
        if hasattr(module, name):
            return getattr(module, name)
    raise MissingCrawlAPIError("Missing API: " + ", ".join(tried))


def load_crawl() -> Any:
    """Load signal_ingest.crawl package or a shim of submodules."""
    try:
        return importlib.import_module("signal_ingest.crawl")
    except ImportError:
        # Namespace / missing __init__: expose submodule attributes via a simple namespace.
        class _Shim:
            fetch_mod = _import_first("signal_ingest.crawl.fetch")
            pool_mod = _import_first("signal_ingest.crawl.pool")
            queue_mod = _import_first("signal_ingest.crawl.queue")
            discover_mod = _import_first("signal_ingest.crawl.discover")
            store_mod = _import_first("signal_ingest.crawl.store")
            types_mod = _import_first("signal_ingest.crawl.types")
            policy_mod = _import_first("signal_ingest.crawl.policy")
            worker_mod = _import_first("signal_ingest.crawl.worker")

            fetch = fetch_mod.fetch
            fetch_with_retries = fetch_mod.fetch_with_retries
            FetchPolicy = fetch_mod.FetchPolicy
            HttpPool = pool_mod.HttpPool
            PoolLimits = pool_mod.PoolLimits
            claim_tasks = queue_mod.claim_tasks
            enqueue_task = queue_mod.enqueue_task
            complete_task = queue_mod.complete_task
            complete_with_blob = queue_mod.complete_with_blob
            upsert_checkpoint = queue_mod.upsert_checkpoint
            get_checkpoint = queue_mod.get_checkpoint
            get_task = queue_mod.get_task
            discover = discover_mod.discover
            persist_discovery_page = discover_mod.persist_discovery_page
            RawStore = store_mod.RawStore
            save_raw_observation_blob = store_mod.save_raw_observation_blob
            FetchOutcomeKind = types_mod.FetchOutcomeKind
            ClaimedTask = types_mod.ClaimedTask
            DiscoveredItem = types_mod.DiscoveredItem
            RawObservation = types_mod.RawObservation
            UrlPolicy = policy_mod.UrlPolicy
            process_claimed_task = worker_mod.process_claimed_task
            recover_orphaned_blob_effect = worker_mod.recover_orphaned_blob_effect

        return _Shim()


def outcome_kind(result: Any) -> str:
    if result is None:
        return "none"
    outcome = getattr(result, "outcome", result)
    if hasattr(outcome, "value"):
        return str(outcome.value).lower()
    return str(outcome).lower().rsplit(".", 1)[-1]


def kind_is(result: Any, *accepted: str) -> bool:
    kind = outcome_kind(result)
    return kind in {a.lower() for a in accepted}
