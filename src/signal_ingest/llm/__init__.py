"""LLM extraction + LiteLLM Router fallback / zero-spend control."""

from signal_ingest.llm.budget import ComputedBudget, compute_budget, content_token_budget
from signal_ingest.llm.cache import ExtractionCache, build_cache_key
from signal_ingest.llm.chunk import Chunk, ChunkPlan, plan_chunks, rebudget_on_overflow
from signal_ingest.llm.errors import (
    AllProvidersFailedError,
    LiveQualificationError,
    MissingCredentialsError,
    PaidSpendBlockedError,
    QuotaExhaustedError,
)
from signal_ingest.llm.extract import extract
from signal_ingest.llm.fake import FakeProvider, default_fake_limits
from signal_ingest.llm.merge import merge_field_evidence
from signal_ingest.llm.predicates import quote_describes_entity, quote_in_segment
from signal_ingest.llm.provider import (
    ContextLengthExceeded,
    PayloadTooLarge,
    Provider,
    ProviderLimits,
    ProviderRequest,
    ProviderResult,
    RequestBudget,
    UnchangedOversizedRetry,
    measure_provider_request,
)
from signal_ingest.llm.providers_config import (
    is_live_provider,
    load_providers_config,
    provider_limits_from_config,
    quota_ledger_from_config,
)
from signal_ingest.llm.quota import PgQuotaLedger, ProviderQuotaConfig, QuotaLedger, QuotaReservation
from signal_ingest.llm.schema import EXTRACTION_JSON_SCHEMA, ModelExtractionPayload, ModelFieldClaim
from signal_ingest.llm.segments import html_to_segments, load_segments_from_plain_text, segments_hash
from signal_ingest.llm.tokens import estimate_tokens
from signal_ingest.llm.transcript import AttemptRecord, TranscriptRecorder, redact_headers, redact_payload
from signal_ingest.llm.types import (
    ExtractCandidate,
    ExtractResult,
    ExtractStatus,
    FieldDecision,
    FieldEvidence,
    Segment,
    SegmentKind,
)
from signal_ingest.llm.validate import parse_model_payload, validate_claim, validate_payload
from signal_ingest.llm.versions import (
    EXTRACTION_POLICY_VERSION,
    PROMPT_VERSION,
    SCHEMA_VERSION,
    SEGMENTER_VERSION,
)

__all__ = [
    "EXTRACTION_JSON_SCHEMA",
    "EXTRACTION_POLICY_VERSION",
    "PROMPT_VERSION",
    "SCHEMA_VERSION",
    "SEGMENTER_VERSION",
    "AllProvidersFailedError",
    "Attempt",
    "AttemptRecord",
    "Chunk",
    "ChunkPlan",
    "ComputedBudget",
    "ContextLengthExceeded",
    "ExtractCandidate",
    "ExtractResult",
    "ExtractStatus",
    "ExtractionCache",
    "FakeProvider",
    "FieldDecision",
    "FieldEvidence",
    "LiteLLMChain",
    "LiveQualificationError",
    "MissingCredentialsError",
    "ModelExtractionPayload",
    "ModelFieldClaim",
    "PaidSpendBlockedError",
    "PayloadTooLarge",
    "PgQuotaLedger",
    "Provider",
    "ProviderLimits",
    "ProviderQuotaConfig",
    "ProviderRequest",
    "ProviderResult",
    "QuotaExhaustedError",
    "QuotaLedger",
    "QuotaReservation",
    "RequestBudget",
    "Segment",
    "SegmentKind",
    "Tier",
    "TranscriptRecorder",
    "UnchangedOversizedRetry",
    "build_cache_key",
    "build_chain",
    "classify",
    "compute_budget",
    "content_token_budget",
    "default_fake_limits",
    "estimate_tokens",
    "extract",
    "html_to_segments",
    "is_live_provider",
    "load_providers_config",
    "load_segments_from_plain_text",
    "measure_provider_request",
    "merge_field_evidence",
    "parse_model_payload",
    "plan_chunks",
    "provider_limits_from_config",
    "quota_ledger_from_config",
    "quote_describes_entity",
    "quote_in_segment",
    "rebudget_on_overflow",
    "redact_headers",
    "redact_payload",
    "segments_hash",
    "validate_claim",
    "validate_payload",
]

_ROUTER_EXPORTS = frozenset({"Attempt", "LiteLLMChain", "Tier", "build_chain", "classify"})


def __getattr__(name: str):
    # Lazy: litellm loads only when the live chain is used, never for fake/fixture runs.
    if name in _ROUTER_EXPORTS:
        from signal_ingest.llm import router

        return getattr(router, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
