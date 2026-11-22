"""B1 crawler: discover → queue → fetch → observation (bounded HTTP + leases).

B2 bulk adapters register through worker stage dispatch (parse / enrich_stars).
"""

from signal_ingest.crawl.discover import discover, persist_discovery_page
from signal_ingest.crawl.fetch import FetchPolicy, fetch, fetch_with_retries
from signal_ingest.crawl.parse import parse
from signal_ingest.crawl.policy import (
    PolicyError,
    RobotsCache,
    TRACKING_PARAMS,
    UrlPolicy,
    looks_like_challenge,
)
from signal_ingest.crawl.pool import (
    BackpressureError,
    BodyTooLargeError,
    DecodingError,
    HttpPool,
    PoolLimits,
)
from signal_ingest.crawl.queue import (
    claim_tasks,
    complete_task,
    complete_with_blob,
    enqueue_task,
    get_checkpoint,
    get_task,
    release_lease,
    reap_expired,
    renew_lease,
    requeue_failed,
    schedule_retry,
    semantic_key,
    upsert_checkpoint,
)
from signal_ingest.crawl.store import (
    RawStore,
    content_sha256,
    get_observation_for_task,
    insert_observation,
    load_cached_body,
    save_raw_observation_blob,
)
from signal_ingest.crawl.types import (
    ClaimedTask,
    DiscoveredItem,
    FetchOutcomeKind,
    ParseCandidate,
    RawObservation,
)
from signal_ingest.crawl.worker import (
    process_claimed_task,
    recover_orphaned_blob_effect,
    run_worker_once,
)

__all__ = [
    "BackpressureError",
    "BodyTooLargeError",
    "ClaimedTask",
    "DecodingError",
    "DiscoveredItem",
    "FetchOutcomeKind",
    "FetchPolicy",
    "HttpPool",
    "ParseCandidate",
    "PolicyError",
    "PoolLimits",
    "RawObservation",
    "RawStore",
    "RobotsCache",
    "TRACKING_PARAMS",
    "UrlPolicy",
    "claim_tasks",
    "complete_task",
    "complete_with_blob",
    "content_sha256",
    "discover",
    "enqueue_task",
    "fetch",
    "fetch_with_retries",
    "get_checkpoint",
    "get_observation_for_task",
    "get_task",
    "insert_observation",
    "load_cached_body",
    "looks_like_challenge",
    "parse",
    "persist_discovery_page",
    "process_claimed_task",
    "recover_orphaned_blob_effect",
    "release_lease",
    "reap_expired",
    "renew_lease",
    "requeue_failed",
    "run_worker_once",
    "save_raw_observation_blob",
    "schedule_retry",
    "semantic_key",
    "upsert_checkpoint",
]
