"""LangGraph extraction graph: budget > segment > cache > plan > call_llm (<> rebudget) > merge > finalize."""

from __future__ import annotations

import json
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from signal_ingest.llm.budget import ComputedBudget, compute_budget
from signal_ingest.llm.cache import ExtractionCache, build_cache_key, hash_segments, stable_json_hash
from signal_ingest.llm.chunk import ChunkPlan, plan_chunks, rebudget_on_overflow
from signal_ingest.llm.errors import AllProvidersFailedError
from signal_ingest.llm.merge import merge_field_evidence
from signal_ingest.llm.provider import (
    ContextLengthExceeded,
    PayloadTooLarge,
    Provider,
    ProviderError,
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


class ExtractState(TypedDict, total=False):
    candidate: ExtractCandidate
    provider: Provider
    cache: ExtractionCache | None
    max_depth: int
    budget: ComputedBudget | None
    segments: list[Segment]
    cache_key: str
    plan: ChunkPlan
    batches: list[list[FieldEvidence]]
    attempts: int
    usage: dict[str, Any]
    last_overflow_bytes: int | None
    payload_error: str | None
    discarded: list[str]
    overflow: ProviderError | None  # pending 413 / context-length from call_llm
    fields: dict[str, FieldEvidence]
    contradictions: list[dict[str, Any]]
    result: ExtractResult | None


def _budget(state: ExtractState) -> dict[str, Any]:
    limits = state["provider"].limits
    computed = state.get("budget") or compute_budget(
        limits,
        schema_overhead_tokens=estimate_tokens(json.dumps(EXTRACTION_JSON_SCHEMA, ensure_ascii=False)),
        instruction_tokens=estimate_tokens(SYSTEM_INSTRUCTIONS),
    )
    if computed.route_disabled:
        return {
            "result": ExtractResult(
                status=ExtractStatus.ROUTE_DISABLED,
                reason=computed.disable_reason or "route_disabled",
                provider=limits.provider,
                model=limits.model,
            )
        }
    return {"budget": computed}


def _segment(state: ExtractState) -> dict[str, Any]:
    return {"segments": _segments_for(state["candidate"])}


def _cache_lookup(state: ExtractState) -> dict[str, Any]:
    candidate = state["candidate"]
    limits = state["provider"].limits
    segments = state["segments"]
    raw_hash = candidate.raw_hash or raw_content_hash(
        candidate.html or candidate.text or candidate.candidate_id
    )
    cache_key = build_cache_key(
        raw_hash=raw_hash,
        segments_hash=hash_segments(segments),
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
    cache = state.get("cache")
    hit = cache.get(cache_key) if cache is not None else None
    if hit is not None:
        return {
            "cache_key": cache_key,
            "result": hit.model_copy(
                update={
                    "status": ExtractStatus.CACHE_HIT,
                    "cache_key": cache_key,
                    "provider": limits.provider,
                    "model": limits.model,
                }
            ),
        }
    return {"cache_key": cache_key}


def _plan(state: ExtractState) -> dict[str, Any]:
    assert state["budget"] is not None
    plan = plan_chunks(state["segments"], state["budget"], max_depth=state["max_depth"])
    out: dict[str, Any] = {
        "plan": plan,
        "discarded": list(plan.discarded_coverage),
        "fields": {},
        "contradictions": [],
        "attempts": 0,
        "payload_error": "no_chunks" if not plan.chunks else None,
    }
    if plan.chunks:
        out.update(
            batches=[],
            last_overflow_bytes=None,
            usage={"request_bytes": 0, "input_tokens": 0, "output_tokens": 0},
        )
    return out


async def _call_llm(state: ExtractState) -> dict[str, Any]:
    candidate = state["candidate"]
    provider = state["provider"]
    limits = provider.limits
    plan = state["plan"]
    attempts = state["attempts"]
    usage = dict(state["usage"])
    payload_error = state["payload_error"]
    last_overflow_bytes = state["last_overflow_bytes"]
    batches: list[list[FieldEvidence]] = []
    progress = {"attempts": attempts, "usage": usage, "payload_error": payload_error}
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
            if last_overflow_bytes is not None and measured.request_bytes == last_overflow_bytes:
                raise UnchangedOversizedRetry(
                    "unchanged oversized payload retried without shrinking",
                    serialized_bytes=measured.request_bytes,
                )
            attempts += 1
            progress["attempts"] = attempts
            try:
                result = await provider.complete(req)
            except AllProvidersFailedError as exc:
                # Transient chain failure: report it, never cache it.
                return {
                    **progress,
                    "result": ExtractResult(
                        status=ExtractStatus.FAILED,
                        reason=str(exc),
                        segments=state["segments"],
                        cache_key=state["cache_key"],
                        attempts=attempts,
                        provider=limits.provider,
                        model=limits.model,
                        usage=usage,
                    ),
                }
            usage["request_bytes"] = int(usage["request_bytes"]) + result.request_bytes
            usage["input_tokens"] = int(usage["input_tokens"]) + result.input_tokens
            usage["output_tokens"] = int(usage["output_tokens"]) + result.output_tokens

            payload, err = parse_model_payload(
                result.parsed if result.parsed is not None else result.text
            )
            if err:
                payload_error = err
                progress["payload_error"] = err
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
            batches.append(
                validate_payload(
                    payload,
                    segments=chunk.segments,
                    target_entity=candidate.target_entity,
                    target_aliases=candidate.target_aliases,
                    known_entities=candidate.known_entities,
                    deterministic_fields=candidate.deterministic_fields,
                    fields_requested=candidate.fields_requested,
                )
            )
    except (PayloadTooLarge, ContextLengthExceeded) as exc:
        return {**progress, "overflow": exc}
    return {**progress, "batches": batches, "overflow": None}


def _rebudget(state: ExtractState) -> dict[str, Any]:
    exc = state["overflow"]
    assert exc is not None
    plan = state["plan"]
    last_overflow_bytes = state["last_overflow_bytes"]
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
    if plan.depth + 1 >= plan.max_depth:
        raise exc
    new_plan = rebudget_on_overflow(plan, factor=0.5)
    if not new_plan.chunks:
        raise UnchangedOversizedRetry(
            "413 re-budget produced no smaller chunk",
            serialized_bytes=serialized_bytes or 0,
        ) from exc
    # If every chunk text is identical to prior plan, treat as unchanged.
    if [c.text for c in plan.chunks] == [c.text for c in new_plan.chunks]:
        raise UnchangedOversizedRetry(
            "413 re-budget did not shrink chunk payloads",
            serialized_bytes=serialized_bytes or 0,
        ) from exc
    return {
        "plan": new_plan,
        "discarded": [*state["discarded"], *new_plan.discarded_coverage],
        "last_overflow_bytes": serialized_bytes,
        "overflow": None,
    }


def _merge(state: ExtractState) -> dict[str, Any]:
    fields, contradictions, merge_discarded = merge_field_evidence(
        state["batches"],
        deterministic_fields=state["candidate"].deterministic_fields,
    )
    # Drop internal payload sentinel from fields map.
    fields.pop("_payload", None)
    return {
        "fields": fields,
        "contradictions": contradictions,
        "discarded": [*state["discarded"], *merge_discarded],
    }


def _finalize_node(state: ExtractState) -> dict[str, Any]:
    limits = state["provider"].limits
    out = _finalize(
        state["fields"],
        state["contradictions"],
        state["discarded"],
        segments=state["segments"],
        cache_key=state["cache_key"],
        provider=limits.provider,
        model=limits.model,
        attempts=state["attempts"],
        usage=state.get("usage"),
        reason=state["payload_error"],
        deterministic_fields=state["candidate"].deterministic_fields,
    )
    cache = state.get("cache")
    # "no_chunks" is a planned outcome, not a payload error: it is cached.
    if cache is not None and state["payload_error"] in (None, "no_chunks"):
        cache.put(state["cache_key"], out)
    return {"result": out}


def _done_or(next_node: str):
    def route(state: ExtractState) -> str:
        return END if state.get("result") is not None else next_node

    return route


def _after_plan(state: ExtractState) -> str:
    return "call_llm" if state["plan"].chunks else "finalize"


def _after_call(state: ExtractState) -> str:
    if state.get("result") is not None:
        return END
    return "rebudget" if state.get("overflow") is not None else "merge"


def _build_graph() -> Any:
    g = StateGraph(ExtractState)
    g.add_node("budget", _budget)
    g.add_node("segment", _segment)
    g.add_node("cache_lookup", _cache_lookup)
    g.add_node("plan", _plan)
    g.add_node("call_llm", _call_llm)
    g.add_node("rebudget", _rebudget)
    g.add_node("merge", _merge)
    g.add_node("finalize", _finalize_node)
    g.add_edge(START, "budget")
    g.add_conditional_edges("budget", _done_or("segment"), ["segment", END])
    g.add_edge("segment", "cache_lookup")
    g.add_conditional_edges("cache_lookup", _done_or("plan"), ["plan", END])
    g.add_conditional_edges("plan", _after_plan, ["call_llm", "finalize"])
    g.add_conditional_edges("call_llm", _after_call, ["rebudget", "merge", END])
    g.add_edge("rebudget", "call_llm")
    g.add_edge("merge", "finalize")
    g.add_edge("finalize", END)
    return g.compile()


EXTRACTION_GRAPH = _build_graph()


async def run_extraction_graph(
    candidate: ExtractCandidate,
    provider: Provider,
    *,
    budget: ComputedBudget | None = None,
    cache: ExtractionCache | None = None,
    max_depth: int = 4,
) -> ExtractResult:
    final = await EXTRACTION_GRAPH.ainvoke(
        {
            "candidate": candidate,
            "provider": provider,
            "budget": budget,
            "cache": cache,
            "max_depth": max_depth,
            "result": None,
        },
        # Each rebudget loop costs two steps; the linear path is eight.
        config={"recursion_limit": 16 + 2 * max(max_depth, 1)},
    )
    result = final["result"]
    assert result is not None
    return result


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
