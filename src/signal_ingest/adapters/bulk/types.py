"""Shared bulk-adapter result types."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass
class FieldEvidence:
    """Pointer to a captured observation for one field."""

    path: str
    excerpt: str | None = None
    observation_id: str | None = None
    url: str | None = None


@dataclass
class BulkParseResult:
    """Outcome of parsing one raw observation into a candidate or quarantine."""

    ok: bool
    record_type: str | None = None
    external_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    relations: list[dict[str, Any]] = field(default_factory=list)
    quarantine_reason: str | None = None
    quarantine_detail: dict[str, Any] = field(default_factory=dict)
    # Follow-on work (e.g. GitHub enrich for a paper primary repo).
    enrich_stars: dict[str, Any] | None = None
    # True when the candidate already satisfies counting eligibility.
    eligible: bool = False


@dataclass
class RepoMetricSnapshot:
    repo_id: str
    repo_url: str
    stars: int
    observed_at: datetime
    etag: str | None = None
    content_sha256: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    observation_id: UUID | None = None
    renamed_from: str | None = None
    private_or_deleted: bool = False
