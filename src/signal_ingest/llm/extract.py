"""Grounded extract(candidate, budget) orchestration (fake-provider path)."""

from __future__ import annotations

import json
from typing import Any

from signal_ingest.llm.budget import ComputedBudget, compute_budget
from signal_ingest.llm.cache import ExtractionCache, build_cache_key, hash_segments, stable_json_hash
from signal_ingest.llm.errors import AllProvidersFailedError
from signal_ingest.llm.chunk import plan_chunks, rebudget_on_overflow
from signal_ingest.llm.merge import merge_field_evidence
from signal_ingest.llm.provider import (
    ContextLengthExceeded,
    PayloadTooLarge,
    Provider,
    ProviderRequest,
    UnchangedOversizedRetry,
    measure_provider_request,
)
from signal_ingest.llm.schema import (
    EXTRACTION_JSON_SCHEMA,
    SYSTEM_INSTRUCTIONS,
    build_user_prompt,
)
from signal_ingest.llm.segments import html_to_segments, load_segments_from_plain_text, raw_content_hash
from signal_ingest.llm.tokens import estimate_tokens
from signal_ingest.llm.types import (
    ExtractCandidate,
    ExtractResult,
    ExtractStatus,
    FieldDecision,
    FieldEvidence,
    Segment,
)
from signal_ingest.llm.validate import parse_model_payload, validate_payload
from signal_ingest.llm.versions import (
    EXTRACTION_POLICY_VERSION,
    PROMPT_VERSION,
    SCHEMA_VERSION,
)


async def extract(
    candidate: ExtractCandidate,
    provider: Provider,
    *,
    budget: ComputedBudget | None = None,
    cache: ExtractionCache | None = None,
    max_depth: int = 4,
) -> ExtractResult:
    """Run grounded LLM extraction with chunking, validation, merge, and cache."""
    limits = provider.limits
    schema_overhead = estimate_tokens(
        json.dumps(EXTRACTION_JSON_SCHEMA, ensure_ascii=False)
    )
    instruction_tokens = estimate_tokens(SYSTEM_INSTRUCTIONS)
    computed = budget or compute_budget(
        limits,
        schema_overhead_tokens=schema_overhead,
        instruction_tokens=instruction_tokens,
    )
    if computed.route_disabled:
        return ExtractResult(
            status=ExtractStatus.ROUTE_DISABLED,
            reason=computed.disable_reason or "route_disabled",
            provider=limits.provider,
            model=limits.model,
        )

    segments = _segments_for(candidate)
    raw_hash = candidate.raw_hash or raw_content_hash(
        candidate.html or candidate.text or candidate.candidate_id
    )
    seg_hash = hash_segments(segments)
    cache_key = build_cache_key(
        raw_hash=raw_hash,
        segments_hash=seg_hash,
        provider=limits.provider,
        model=limits.model,
        prompt_version=PROMPT_VERSION,
        schema_version=SCHEMA_VERSION,
        policy_version=EXTRACTION_POLICY_VERSION,
        scope=stable_json_hash(
            {
                "record_type": candidate.record_type,
                "target_entity": candidate.target_entity,
                "target_aliases": list(candidate.target_aliases),
                "known_entities": list(candidate.known_entities),
                "fields_requested": list(candidate.fields_requested),
                "deterministic_fields": candidate.deterministic_fields,
            }
        ),
    )
    if cache is not None:
        hit = cache.get(cache_key)
        if hit is not None:
            return hit.model_copy(
                update={
                    "status": ExtractStatus.CACHE_HIT,
                    "cache_key": cache_key,
                    "provider": limits.provider,
                    "model": limits.model,
                }
            )

    plan = plan_chunks(segments, computed, max_depth=max_depth)
    if not plan.chunks:
        result = _finalize(
            {},
            [],
            plan.discarded_coverage,
            segments=segments,
            cache_key=cache_key,
            provider=limits.provider,
            model=limits.model,
            attempts=0,
            reason="no_chunks",
            deterministic_fields=candidate.deterministic_fields,
        )
        if cache is not None:
            cache.put(cache_key, result)
        return result

    batches: list[list[FieldEvidence]] = []
    attempts = 0
    last_overflow_bytes: int | None = None
    payload_error: str | None = None
    usage: dict[str, Any] = {"request_bytes": 0, "input_tokens": 0, "output_tokens": 0}
    discarded = list(plan.discarded_coverage)

    while True:
        try:
            for chunk in plan.chunks:
                req = _build_request(
                    candidate=candidate,
                    chunk_text=chunk.text,
                    chunk_segments=chunk.segments,
                    computed=plan.budget,
                    provider_name=limits.provider,
                    model=limits.model,
                )
                measured = measure_provider_request(req)
                # A retry of unchanged oversized bytes fails the gate.
                if (
                    last_overflow_bytes is not None
                    and measured.request_bytes == last_overflow_bytes
                ):
                    raise UnchangedOversizedRetry(
                        "unchanged oversized payload retried without shrinking",
                        serialized_bytes=measured.request_bytes,
                    )
                attempts += 1
                try:
                    result = await provider.complete(req)
                except AllProvidersFailedError as exc:
                    # Transient chain failure: report it, never cache it.
                    return ExtractResult(
                        status=ExtractStatus.FAILED,
                        reason=str(exc),
                        segments=segments,
                        cache_key=cache_key,
                        attempts=attempts,
                        provider=limits.provider,
                        model=limits.model,
                        usage=usage,
                    )
                usage["request_bytes"] = int(usage["request_bytes"]) + result.request_bytes
                usage["input_tokens"] = int(usage["input_tokens"]) + result.input_tokens
                usage["output_tokens"] = int(usage["output_tokens"]) + result.output_tokens

                payload, err = parse_model_payload(
                    result.parsed if result.parsed is not None else result.text
                )
                if err:
                    payload_error = err
                    batches.append(
                        [
                            FieldEvidence(
                                field="_payload",
                                value=None,
                                decision=FieldDecision.REJECTED,
                                reason=err,
                                source="model",
                            )
                        ]
                    )
                    continue
                assert payload is not None
                validated = validate_payload(
                    payload,
                    segments=chunk.segments,
                    target_entity=candidate.target_entity,
                    target_aliases=candidate.target_aliases,
                    known_entities=candidate.known_entities,
                    deterministic_fields=candidate.deterministic_fields,
                    fields_requested=candidate.fields_requested,
                )
                batches.append(validated)
            break
        except (PayloadTooLarge, ContextLengthExceeded) as exc:
            serialized_bytes = getattr(exc, "serialized_bytes", None)
            if (
                last_overflow_bytes is not None
                and serialized_bytes is not None
                and serialized_bytes == last_overflow_bytes
            ):
                raise UnchangedOversizedRetry(
                    "unchanged oversized retry after 413",
                    serialized_bytes=serialized_bytes,
                ) from exc
            last_overflow_bytes = serialized_bytes
            if plan.depth + 1 >= plan.max_depth:
                raise
            new_plan = rebudget_on_overflow(plan, factor=0.5)
            if not new_plan.chunks:
                raise UnchangedOversizedRetry(
                    "413 re-budget produced no smaller chunk",
                    serialized_bytes=serialized_bytes or 0,
                ) from exc
            # If every chunk text is identical to prior plan, treat as unchanged.
            old_texts = [c.text for c in plan.chunks]
            new_texts = [c.text for c in new_plan.chunks]
            if old_texts == new_texts:
                raise UnchangedOversizedRetry(
                    "413 re-budget did not shrink chunk payloads",
                    serialized_bytes=serialized_bytes or 0,
                ) from exc
            plan = new_plan
            discarded.extend(plan.discarded_coverage)
            batches = []
            continue

    fields, contradictions, merge_discarded = merge_field_evidence(
        batches,
        deterministic_fields=candidate.deterministic_fields,
    )
    discarded.extend(merge_discarded)
    # Drop internal payload sentinel from fields map.
    fields.pop("_payload", None)

    out = _finalize(
        fields,
        contradictions,
        discarded,
        segments=segments,
        cache_key=cache_key,
        provider=limits.provider,
        model=limits.model,
        attempts=attempts,
        usage=usage,
        reason=payload_error,
        deterministic_fields=candidate.deterministic_fields,
    )
    if cache is not None and payload_error is None:
        cache.put(cache_key, out)
    return out


def _segments_for(candidate: ExtractCandidate) -> list[Segment]:
    hints = tuple(
        dict.fromkeys(
            [
                x
                for x in (
                    candidate.target_entity,
                    *candidate.target_aliases,
                    *candidate.known_entities,
                )
                if x
            ]
        )
    )
    if candidate.html:
        return html_to_segments(candidate.html, entity_hints=hints)
    if candidate.text:
        return load_segments_from_plain_text(candidate.text, entity_hints=hints)
    return []


def _build_request(
    *,
    candidate: ExtractCandidate,
    chunk_text: str,
    chunk_segments: list[Segment],
    computed: ComputedBudget,
    provider_name: str,
    model: str,
) -> ProviderRequest:
    catalog = [
        {"segment_id": s.segment_id, "kind": s.kind.value, "text": s.text}
        for s in chunk_segments
    ]
    user = build_user_prompt(
        record_type=candidate.record_type,
        target_entity=candidate.target_entity,
        fields_requested=candidate.fields_requested,
        chunk_text=chunk_text,
        segment_catalog=catalog,
    )
    return ProviderRequest(
        messages=[
            {"role": "system", "content": SYSTEM_INSTRUCTIONS},
            {"role": "user", "content": user},
        ],
        response_schema=EXTRACTION_JSON_SCHEMA,
        budget=computed.to_request_budget(),
        provider=provider_name,
        model=model,
        metadata={
            "candidate_id": candidate.candidate_id,
            "prompt_version": PROMPT_VERSION,
            "schema_version": SCHEMA_VERSION,
            "policy_version": EXTRACTION_POLICY_VERSION,
        },
    )


def _finalize(
    fields: dict[str, FieldEvidence],
    contradictions: list[dict[str, Any]],
    discarded: list[str],
    *,
    segments: list[Segment],
    cache_key: str,
    provider: str,
    model: str,
    attempts: int,
    usage: dict[str, Any] | None = None,
    reason: str | None = None,
    deterministic_fields: dict[str, Any] | None = None,
) -> ExtractResult:
    # Ensure deterministic fields present even if model omitted them.
    deterministic_fields = deterministic_fields or {}
    for key, value in deterministic_fields.items():
        fields.setdefault(
            key,
            FieldEvidence(
                field=key,
                value=value,
                decision=FieldDecision.ACCEPTED if value is not None else FieldDecision.ABSTAIN,
                reason="deterministic_locked",
                source="deterministic",
            ),
        )

    if contradictions:
        status = ExtractStatus.REVIEW
    elif not fields or all(
        f.decision in {FieldDecision.ABSTAIN, FieldDecision.REJECTED} for f in fields.values()
        if f.source != "deterministic"
    ):
        # Only deterministic / abstentions.
        model_fields = [f for f in fields.values() if f.source == "model"]
        if model_fields and all(f.decision != FieldDecision.ACCEPTED for f in model_fields):
            status = ExtractStatus.ABSTAIN
        elif any(f.decision == FieldDecision.ACCEPTED for f in fields.values()):
            status = ExtractStatus.OK
        else:
            status = ExtractStatus.ABSTAIN
    else:
        status = ExtractStatus.OK

    return ExtractResult(
        status=status,
        fields=fields,
        contradictions=contradictions,
        discarded_coverage=list(dict.fromkeys(discarded)),
        segments=segments,
        cache_key=cache_key,
        attempts=attempts,
        provider=provider,
        model=model,
        reason=reason,
        usage=usage or {},
    )
