"""LLM extraction + B5 provider fallback / zero-spend control."""

from signal_ingest.llm.adapters import (
    GeminiProvider,
    GroqProvider,
    HttpProviderBase,
    OllamaProvider,
    classify_http_error,
    is_live_provider,
    provider_from_config,
)
from signal_ingest.llm.budget import ComputedBudget, compute_budget, content_token_budget
from signal_ingest.llm.cache import ExtractionCache, build_cache_key
from signal_ingest.llm.chunk import Chunk, ChunkPlan, plan_chunks, rebudget_on_overflow
from signal_ingest.llm.circuit import CircuitBreaker, CircuitBreakerRegistry, CircuitState
from signal_ingest.llm.errors import (
    AllProvidersFailedError,
    AuthError,
    CircuitOpenError,
    InvalidOutputError,
    LiveQualificationError,
    MissingCredentialsError,
    PaidSpendBlockedError,
    ProviderDisabledError,
    QuotaExhaustedError,
    QuotaForbiddenError,
    RateLimitedError,
    TransientProviderError,
)
from signal_ingest.llm.extract import extract
from signal_ingest.llm.fake import FakeProvider, default_fake_limits
from signal_ingest.llm.fallback import (
    ChainProvider,
    FallbackOrchestrator,
    FallbackResult,
    default_chain_names,
)
from signal_ingest.llm.http_transport import (
    HttpResponse,
    HttpTransport,
    HttpxTransport,
    ScriptedHttpTransport,
)
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
    build_chain,
    load_providers_config,
    provider_limits_from_config,
    quota_ledger_from_config,
)
from signal_ingest.llm.quota import PgQuotaLedger, ProviderQuotaConfig, QuotaLedger, QuotaReservation
from signal_ingest.llm.retry import full_jitter, parse_retry_after, resolve_backoff_delay
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
    "AttemptRecord",
    "ChainProvider",
    "AuthError",
    "Chunk",
    "ChunkPlan",
    "CircuitBreaker",
    "CircuitBreakerRegistry",
    "CircuitOpenError",
    "CircuitState",
    "ComputedBudget",
    "ContextLengthExceeded",
    "ExtractCandidate",
    "ExtractResult",
    "ExtractStatus",
    "ExtractionCache",
    "FakeProvider",
    "FallbackOrchestrator",
    "FallbackResult",
    "FieldDecision",
    "FieldEvidence",
    "GeminiProvider",
    "GroqProvider",
    "HttpProviderBase",
    "HttpResponse",
    "HttpTransport",
    "HttpxTransport",
    "InvalidOutputError",
    "LiveQualificationError",
    "MissingCredentialsError",
    "ModelExtractionPayload",
    "ModelFieldClaim",
    "OllamaProvider",
    "PaidSpendBlockedError",
    "PayloadTooLarge",
    "PgQuotaLedger",
    "Provider",
    "ProviderDisabledError",
    "ProviderLimits",
    "ProviderQuotaConfig",
    "ProviderRequest",
    "ProviderResult",
    "QuotaExhaustedError",
    "QuotaForbiddenError",
    "QuotaLedger",
    "QuotaReservation",
    "RateLimitedError",
    "RequestBudget",
    "ScriptedHttpTransport",
    "Segment",
    "SegmentKind",
    "TranscriptRecorder",
    "TransientProviderError",
    "UnchangedOversizedRetry",
    "build_cache_key",
    "build_chain",
    "classify_http_error",
    "compute_budget",
    "content_token_budget",
    "default_chain_names",
    "default_fake_limits",
    "estimate_tokens",
    "extract",
    "full_jitter",
    "html_to_segments",
    "is_live_provider",
    "load_providers_config",
    "load_segments_from_plain_text",
    "measure_provider_request",
    "merge_field_evidence",
    "parse_model_payload",
    "parse_retry_after",
    "plan_chunks",
    "provider_from_config",
    "provider_limits_from_config",
    "quota_ledger_from_config",
    "quote_describes_entity",
    "quote_in_segment",
    "rebudget_on_overflow",
    "redact_headers",
    "redact_payload",
    "resolve_backoff_delay",
    "segments_hash",
    "validate_claim",
    "validate_payload",
]
