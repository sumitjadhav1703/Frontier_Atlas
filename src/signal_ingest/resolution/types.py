"""Typed inputs and outputs for deterministic entity resolution."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Namespace(str, Enum):
    STARTUP = "startup"
    PRODUCT = "product"


class DecisionKind(str, Enum):
    MATCH = "match"
    NEW = "new"
    ABSTAIN = "abstain"


class IdentityKind(str, Enum):
    ALIAS = "alias"
    SOURCE_ID = "source_id"
    DOMAIN = "domain"
    COMPARISON_KEY = "comparison_key"
    CANONICAL_NAME = "canonical_name"


class SeedIdentity(BaseModel):
    """One labeled canonical identity in the seed register."""

    model_config = ConfigDict(extra="ignore")

    id: str
    namespace: Namespace
    canonical_name: str
    aliases: tuple[str, ...] = ()
    domains: tuple[str, ...] = ()
    source_ids: dict[str, str] = Field(default_factory=dict)
    evidence_url: str = ""
    owner_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("aliases", "domains", mode="before")
    @classmethod
    def _as_tuple(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            return (value,)
        return tuple(str(v) for v in value)

    @field_validator("id", "canonical_name")
    @classmethod
    def _nonempty(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("must be non-empty")
        return text


class SeedRegisterDocument(BaseModel):
    """On-disk seed register (`config/seeds.fixture.json` or test fixtures)."""

    model_config = ConfigDict(extra="ignore")

    format_version: str = "1.0"
    revision: str = "unspecified"
    lineage_mode: Literal["fixture", "live"] = "fixture"
    rule_version: str = "resolution-v1"
    identities: list[SeedIdentity]

    @field_validator("identities")
    @classmethod
    def _min_identities(cls, value: list[SeedIdentity]) -> list[SeedIdentity]:
        if len(value) < 50:
            raise ValueError("seed register requires at least 50 labeled identities")
        return value


class ResolveCandidate(BaseModel):
    """Input to resolve(): a raw name plus optional corroboration."""

    model_config = ConfigDict(extra="ignore")

    raw_name: str
    namespace: Namespace
    mode: Literal["fixture", "live"] = "fixture"
    source_item_id: str | None = None
    field: str = "entityName"
    evidence_url: str | None = None
    source_id: str | None = None
    source_system: str | None = None
    domain: str | None = None
    owner_id: str | None = None
    ownership_evidenced: bool = False
    allow_create: bool = False
    run_id: str | None = None
    # Live-path rejection surfaces (may arrive from adapters/tests).
    seed_id: str | None = None
    canonical_id: str | None = None
    observation_id: str | None = None

    @field_validator("raw_name")
    @classmethod
    def _raw_nonempty(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("raw_name must be non-empty")
        return text

    @field_validator("mode", mode="before")
    @classmethod
    def _coerce_mode(cls, value: Any) -> str:
        if value is None:
            return "fixture"
        if hasattr(value, "value"):
            return str(value.value)
        return str(value)


class ResolveDecision(BaseModel):
    """Stable identity or auditable abstention."""

    model_config = ConfigDict(extra="ignore")

    decision: DecisionKind
    canonical_id: str | None = None
    canonical_name: str | None = None
    method: str
    rule_version: str
    candidate_ids: tuple[str, ...] = ()
    raw_name: str
    comparison_key: str
    namespace: Namespace
    reason: str | None = None
    evidence_url: str | None = None
    observation_key: str | None = None


class MappingLogRecord(BaseModel):
    """Entity Mapping Log row shape (contracts.md)."""

    model_config = ConfigDict(extra="forbid")

    entityType: str
    rawName: str
    canonicalName: str | None
    canonicalID: str | None
    method: str
    decision: str
    evidenceURL: str | None
    runID: str | None
    ruleVersion: str


class FixtureSeedRejectedError(ValueError):
    """Raised when live mode would admit a fixture seed ID or alias."""


class SeedRegisterError(ValueError):
    """Invalid or inconsistent seed register."""
