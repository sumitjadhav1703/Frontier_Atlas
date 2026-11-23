"""Validate model JSON: Pydantic, quote-in-segment, entity predicates, locked fields."""

from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from signal_ingest.llm.predicates import (
    apply_field_predicate,
    quote_describes_entity,
    quote_in_segment,
    value_supported_by_quote,
)
from signal_ingest.llm.schema import ModelExtractionPayload, ModelFieldClaim
from signal_ingest.llm.types import (
    DETERMINISTIC_LOCKED_FIELDS,
    FieldDecision,
    FieldEvidence,
    Segment,
)


def parse_model_payload(text: str | dict[str, Any] | None) -> tuple[ModelExtractionPayload | None, str | None]:
    if text is None:
        return None, "empty_response"
    if isinstance(text, dict):
        try:
            return ModelExtractionPayload.model_validate(text), None
        except ValidationError as exc:
            return None, f"schema_invalid:{exc.error_count()}"
    raw = text.strip()
    if not raw:
        return None, "empty_response"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None, "malformed_json"
    try:
        return ModelExtractionPayload.model_validate(data), None
    except ValidationError as exc:
        return None, f"schema_invalid:{exc.error_count()}"


def validate_claim(
    claim: ModelFieldClaim,
    *,
    segments_by_id: dict[str, Segment],
    target_entity: str | None,
    target_aliases: tuple[str, ...] = (),
    known_entities: tuple[str, ...] = (),
    deterministic_fields: dict[str, Any] | None = None,
    fields_requested: tuple[str, ...] = (),
) -> FieldEvidence:
    deterministic_fields = deterministic_fields or {}

    # Deterministic fields are never overwriteable by the model.
    if claim.field in DETERMINISTIC_LOCKED_FIELDS or claim.field in deterministic_fields:
        locked = deterministic_fields.get(claim.field)
        return FieldEvidence(
            field=claim.field,
            value=locked,
            quote=None,
            segment_ids=(),
            decision=FieldDecision.ACCEPTED if locked is not None else FieldDecision.ABSTAIN,
            reason="deterministic_locked",
            source="deterministic",
        )

    if fields_requested and claim.field not in fields_requested:
        return FieldEvidence(
            field=claim.field,
            value=None,
            quote=claim.quote,
            segment_ids=tuple(claim.segment_ids),
            decision=FieldDecision.REJECTED,
            reason="field_not_requested",
            source="model",
        )

    if claim.value is None:
        return FieldEvidence(
            field=claim.field,
            value=None,
            quote=claim.quote,
            segment_ids=tuple(claim.segment_ids),
            decision=FieldDecision.ABSTAIN,
            reason="model_null",
            source="model",
        )

    if not claim.quote or not claim.segment_ids:
        return FieldEvidence(
            field=claim.field,
            value=None,
            quote=claim.quote,
            segment_ids=tuple(claim.segment_ids),
            decision=FieldDecision.ABSTAIN,
            reason="missing_evidence",
            source="model",
        )

    # Quote must locate in every cited retained segment (at least one required).
    matched: list[Segment] = []
    for sid in claim.segment_ids:
        seg = segments_by_id.get(sid)
        if seg is None:
            # Some models drop the ":kind" suffix ("seg:0005" for "seg:0005:pricing").
            # Map only when exactly one retained segment has that prefix; the quote check
            # below still has to pass, so grounding is not loosened.
            prefixed = [s for k, s in segments_by_id.items() if k.startswith(f"{sid}:")]
            seg = prefixed[0] if len(prefixed) == 1 else None
        if seg is None:
            return FieldEvidence(
                field=claim.field,
                value=None,
                quote=claim.quote,
                segment_ids=tuple(claim.segment_ids),
                decision=FieldDecision.REJECTED,
                reason=f"unknown_segment:{sid}",
                source="model",
            )
        if not quote_in_segment(claim.quote, seg):
            return FieldEvidence(
                field=claim.field,
                value=None,
                quote=claim.quote,
                segment_ids=tuple(claim.segment_ids),
                decision=FieldDecision.REJECTED,
                reason="quote_not_in_segment",
                source="model",
            )
        matched.append(seg)

    # Entity-aware: reject correct quote for the wrong entity.
    ok_entity, entity_reason = quote_describes_entity(
        quote=claim.quote,
        segment=matched[0],
        target_entity=target_entity,
        target_aliases=target_aliases,
        known_entities=known_entities,
    )
    if not ok_entity:
        return FieldEvidence(
            field=claim.field,
            value=None,
            quote=claim.quote,
            segment_ids=tuple(claim.segment_ids),
            decision=FieldDecision.REJECTED,
            reason=entity_reason or "entity_mismatch",
            source="model",
        )

    ok_pred, pred_reason = apply_field_predicate(claim.field, claim.value, quote=claim.quote)
    if not ok_pred:
        return FieldEvidence(
            field=claim.field,
            value=None,
            quote=claim.quote,
            segment_ids=tuple(claim.segment_ids),
            decision=FieldDecision.REJECTED,
            reason=pred_reason or "predicate_failed",
            source="model",
        )

    if not value_supported_by_quote(claim.field, claim.value, claim.quote):
        return FieldEvidence(
            field=claim.field,
            value=None,
            quote=claim.quote,
            segment_ids=tuple(claim.segment_ids),
            decision=FieldDecision.REJECTED,
            reason="value_not_in_quote",
            source="model",
        )

    return FieldEvidence(
        field=claim.field,
        value=claim.value,
        quote=claim.quote,
        segment_ids=tuple(claim.segment_ids),
        decision=FieldDecision.ACCEPTED,
        reason=None,
        source="model",
    )


def validate_payload(
    payload: ModelExtractionPayload,
    *,
    segments: list[Segment],
    target_entity: str | None,
    target_aliases: tuple[str, ...] = (),
    known_entities: tuple[str, ...] = (),
    deterministic_fields: dict[str, Any] | None = None,
    fields_requested: tuple[str, ...] = (),
) -> list[FieldEvidence]:
    by_id = {s.segment_id: s for s in segments}
    return [
        validate_claim(
            claim,
            segments_by_id=by_id,
            target_entity=target_entity,
            target_aliases=target_aliases,
            known_entities=known_entities,
            deterministic_fields=deterministic_fields,
            fields_requested=fields_requested,
        )
        for claim in payload.fields
    ]
