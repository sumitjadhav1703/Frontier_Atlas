"""B3 news/jobs monitors: freshness-aware discover + detail on the B1 queue."""

from signal_ingest.adapters.monitors import (
    jobs_aicommunity,
    jobs_aijobs,
    jobs_aiworkportal,
    jobs_inferencejobs,
    jobs_moaijobs,
    news_arstechnica,
    news_guardian,
    news_mit,
    news_sciencedaily,
    news_techcrunch,
)
from signal_ingest.adapters.monitors.config import (
    list_monitor_source_ids,
    load_monitor_config,
    load_monitor_configs,
)
from signal_ingest.adapters.monitors.detail import parse_detail
from signal_ingest.adapters.monitors.discover import discover_list, prove_descending
from signal_ingest.adapters.monitors.ledger import (
    classify_health,
    get_latest_monitor_run,
    insert_monitor_run,
)
from signal_ingest.adapters.monitors.pipeline import (
    ADAPTERS,
    MONITOR_SOURCES,
    STAGE_FETCH,
    STAGE_PARSE,
    discover_and_enqueue,
    dispatch_parse,
    drain_monitor_once,
    finalize_monitor_run,
    get_adapter,
    is_monitor_source,
    process_monitor_task,
    process_parse_stage,
)
from signal_ingest.adapters.monitors.types import (
    BodyExtraction,
    DiscoverScanResult,
    MonitorHealth,
    MonitorParseResult,
    MonitorSourceConfig,
)

__all__ = [
    "ADAPTERS",
    "BodyExtraction",
    "DiscoverScanResult",
    "MONITOR_SOURCES",
    "MonitorHealth",
    "MonitorParseResult",
    "MonitorSourceConfig",
    "STAGE_FETCH",
    "STAGE_PARSE",
    "classify_health",
    "discover_and_enqueue",
    "discover_list",
    "dispatch_parse",
    "drain_monitor_once",
    "finalize_monitor_run",
    "get_adapter",
    "get_latest_monitor_run",
    "insert_monitor_run",
    "is_monitor_source",
    "jobs_aicommunity",
    "jobs_aijobs",
    "jobs_aiworkportal",
    "jobs_inferencejobs",
    "jobs_moaijobs",
    "list_monitor_source_ids",
    "load_monitor_config",
    "load_monitor_configs",
    "news_arstechnica",
    "news_guardian",
    "news_mit",
    "news_sciencedaily",
    "news_techcrunch",
    "parse_detail",
    "process_monitor_task",
    "process_parse_stage",
    "prove_descending",
]
