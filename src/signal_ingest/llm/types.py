"""Shared LLM extraction types (segments, evidence, candidates, results)."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class _Exact(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class SegmentKind(str, Enum):
    TITLE = "title"
    DATE = "date"
    PRICING = "pricing"
    OWNER = "owner"
    CODE = "code"
    JSON_LD = "json_ld"
    META = "meta"
    BODY = "body"
    OTHER = "other"


class Segment(_Exact):
    """Stable labeled text span retained after boilerplate strip."""

    segment_id: str
    kind: SegmentKind
    text: str
    start_offset: int = Field(ge=0)
    end_offset: int = Field(ge=0)
    priority: int = Field(default=50, ge=0, le=100)
    entity_hints: tuple[str, ...] = ()

    @property
    def length(self) -> int:
        return len(self.text)


class FieldDecision(str, Enum):
    ACCEPTED = "accepted"
    ABSTAIN = "abstain"
    REVIEW = "review"
    REJECTED = "rejected"


class FieldEvidence(_Exact):
    field: str
    value: Any = None
    quote: str | None = None
    segment_ids: tuple[str, ...] = ()
    decision: FieldDecision = FieldDecision.ABSTAIN
    reason: str | None = None
    source: Literal["model", "deterministic", "merge"] = "model"


class ExtractCandidate(_Exact):
    """Input to extract(): unstructured HTML/text plus locked deterministic fields."""

    candidate_id: str
    record_type: str
    html: str | None = None
    text: str | None = None
    target_entity: str | None = None
    target_aliases: tuple[str, ...] = ()
    # Other entities on the same page (competitors); used to reject wrong-entity quotes.
    known_entities: tuple[str, ...] = ()
    fields_requested: tuple[str, ...] = ()
    # Authoritative values finalized by parsers; model cannot overwrite.
    deterministic_fields: dict[str, Any] = Field(default_factory=dict)
    raw_hash: str | None = None
    source_url: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class ExtractStatus(str, Enum):
    OK = "ok"
    ABSTAIN = "abstain"
    REVIEW = "review"
    FAILED = "failed"
    CACHE_HIT = "cache_hit"
    ROUTE_DISABLED = "route_disabled"


class ExtractResult(_Exact):
    status: ExtractStatus
    fields: dict[str, FieldEvidence] = Field(default_factory=dict)
    contradictions: list[dict[str, Any]] = Field(default_factory=list)
    discarded_coverage: list[str] = Field(default_factory=list)
    segments: list[Segment] = Field(default_factory=list)
    cache_key: str | None = None
    attempts: int = 0
    provider: str | None = None
    model: str | None = None
    reason: str | None = None
    usage: dict[str, Any] = Field(default_factory=dict)


# Fields the model may propose for B4 (subset; deterministic ones stay locked).
MODEL_EXTRACTABLE_FIELDS: frozenset[str] = frozenset(
    {
        "entityName",
        "startupName",
        "productName",
        "owner",
        "pricingModel",
        "price",
        "title",
        "authors",
        "company",
        "is_remote",
        "role_family",
        "role_title",
        "article_url",
        "product_url",
        "paper_url",
        "github_url",
        "date",
        "published_date",
        "text_summary",
    }
)

DETERMINISTIC_LOCKED_FIELDS: frozenset[str] = frozenset(
    {
        "github_stars",
        "starsObservedAt",
        "collectedAt",
        "fetched_at",
        "raw_timestamp",
        "repo_id",
        "schemaVersion",
    }
)
