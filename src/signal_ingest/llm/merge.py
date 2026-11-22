"""Deterministic merge of per-chunk field evidence; preserve contradictions."""

from __future__ import annotations

from typing import Any

from signal_ingest.llm.types import FieldDecision, FieldEvidence


def _norm(value: Any) -> str:
    if isinstance(value, list):
        return "|".join(_norm(v) for v in value)
    if value is None:
        return ""
    return str(value).strip().lower()


def merge_field_evidence(
    batches: list[list[FieldEvidence]],
    *,
    deterministic_fields: dict[str, Any] | None = None,
) -> tuple[dict[str, FieldEvidence], list[dict[str, Any]], list[str]]:
    """Merge claims by field name.

    - Deterministic values always win and cannot be overwritten.
    - Identical accepted values coalesce.
    - Conflicting accepted values → REVIEW with contradiction record.
    - Rejected/abstain do not override an accepted value.
    """
    deterministic_fields = deterministic_fields or {}
    merged: dict[str, FieldEvidence] = {}
    contradictions: list[dict[str, Any]] = []
    discarded: list[str] = []

    # Seed locked deterministic fields first.
    for key, value in deterministic_fields.items():
        merged[key] = FieldEvidence(
            field=key,
            value=value,
            quote=None,
            segment_ids=(),
            decision=FieldDecision.ACCEPTED if value is not None else FieldDecision.ABSTAIN,
            reason="deterministic_locked",
            source="deterministic",
        )

    for batch in batches:
        for ev in batch:
            if ev.field in deterministic_fields:
                # Model proposals for locked fields are discarded.
                if ev.source == "model" and ev.value is not None:
                    discarded.append(f"{ev.field}:model_overwrite_blocked")
                continue

            existing = merged.get(ev.field)
            if existing is None:
                merged[ev.field] = ev
                continue

            if existing.source == "deterministic":
                discarded.append(f"{ev.field}:deterministic_precedence")
                continue

            if existing.decision == FieldDecision.REVIEW:
                # A conflict stays a conflict; a later agreeing chunk cannot settle it.
                continue

            if existing.decision == FieldDecision.ACCEPTED and ev.decision == FieldDecision.ACCEPTED:
                if _norm(existing.value) == _norm(ev.value):
                    # Prefer more segment coverage.
                    if len(ev.segment_ids) > len(existing.segment_ids):
                        merged[ev.field] = ev.model_copy(update={"source": "merge"})
                    continue
                contradictions.append(
                    {
                        "field": ev.field,
                        "values": [existing.value, ev.value],
                        "quotes": [existing.quote, ev.quote],
                        "segment_ids": [
                            list(existing.segment_ids),
                            list(ev.segment_ids),
                        ],
                    }
                )
                merged[ev.field] = FieldEvidence(
                    field=ev.field,
                    value=None,
                    quote=None,
                    segment_ids=tuple(dict.fromkeys([*existing.segment_ids, *ev.segment_ids])),
                    decision=FieldDecision.REVIEW,
                    reason="conflicting_values",
                    source="merge",
                )
                continue

            # Upgrade abstain/reject to accepted when a later chunk grounds it.
            if existing.decision != FieldDecision.ACCEPTED and ev.decision == FieldDecision.ACCEPTED:
                merged[ev.field] = ev
                continue

            if existing.decision == FieldDecision.ABSTAIN and ev.decision == FieldDecision.REJECTED:
                merged[ev.field] = ev
                continue

    return merged, contradictions, discarded
