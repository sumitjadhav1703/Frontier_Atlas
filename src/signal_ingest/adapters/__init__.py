"""Source adapters (bulk in B2; news/jobs monitors in B3)."""

from signal_ingest.adapters.bulk import (
    SOURCE_ARXIV,
    SOURCE_GITHUB,
    SOURCE_PAPERS,
    SOURCE_PRODUCTS,
    SOURCE_YC,
    discover_and_enqueue as discover_and_enqueue_bulk,
    drain_bulk_once,
    get_adapter as get_bulk_adapter,
    process_bulk_task,
)
from signal_ingest.adapters import monitors
from signal_ingest.adapters.monitors import (
    MONITOR_SOURCES,
    discover_and_enqueue as discover_and_enqueue_monitor,
    drain_monitor_once,
    get_adapter as get_monitor_adapter,
    is_monitor_source,
    process_monitor_task,
)

# Preserve B2 names; monitors use discover_and_enqueue_monitor / process_monitor_task.
discover_and_enqueue = discover_and_enqueue_bulk
get_adapter = get_bulk_adapter

__all__ = [
    "MONITOR_SOURCES",
    "SOURCE_ARXIV",
    "SOURCE_GITHUB",
    "SOURCE_PAPERS",
    "SOURCE_PRODUCTS",
    "SOURCE_YC",
    "discover_and_enqueue",
    "discover_and_enqueue_bulk",
    "discover_and_enqueue_monitor",
    "drain_bulk_once",
    "drain_monitor_once",
    "get_adapter",
    "get_bulk_adapter",
    "get_monitor_adapter",
    "is_monitor_source",
    "monitors",
    "process_bulk_task",
    "process_monitor_task",
]
