"""Shared crawl types and fetch outcomes."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID


class FetchOutcomeKind(str, Enum):
    OK = "ok"
    RETRY_AT = "retry_at"
    DENIED = "denied"
    NOT_FOUND = "not_found"
    INVALID = "invalid"
    BLOCKED = "blocked"
    NOT_MODIFIED = "not_modified"


TERMINAL_OUTCOMES = frozenset(
    {
        FetchOutcomeKind.DENIED,
        FetchOutcomeKind.NOT_FOUND,
        FetchOutcomeKind.INVALID,
        FetchOutcomeKind.BLOCKED,
    }
)

TASK_STATUS_FOR_OUTCOME = {
    FetchOutcomeKind.OK: "done",
    FetchOutcomeKind.NOT_MODIFIED: "done",
    FetchOutcomeKind.DENIED: "denied",
    FetchOutcomeKind.NOT_FOUND: "not_found",
    FetchOutcomeKind.INVALID: "invalid",
    FetchOutcomeKind.BLOCKED: "blocked",
}


@dataclass(frozen=True)
class DiscoveredItem:
    """One discovered external ID/URL with optional opaque cursor."""

    url: str
    external_id: str | None = None
    cursor: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ClaimedTask:
    """Lease held by a worker after a short claim transaction."""

    id: UUID
    run_id: UUID | None
    source_id: str
    stage: str
    semantic_key: str
    external_id: str | None
    url: str
    attempt_count: int
    max_attempts: int
    fencing_token: int
    lease_owner: str
    lease_expires: datetime
    payload: dict[str, Any] = field(default_factory=dict)
    parent_task_id: UUID | None = None


@dataclass
class RawObservation:
    """Successful or terminal fetch payload ready for persistence."""

    request_url: str
    final_url: str
    status_code: int | None
    outcome: FetchOutcomeKind
    body: bytes | None = None
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    fetched_at: datetime | None = None
    content_key: str | None = None
    content_sha256: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    retry_at: datetime | None = None
    reason: str | None = None
    alternate_url: str | None = None


@dataclass
class ParseCandidate:
    """Minimal B1 parse output; full adapters land in B2+."""

    url: str
    fields: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    links: list[str] = field(default_factory=list)
