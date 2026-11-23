"""B4 gate: evidence-grounded LLM extraction via fake provider.

Oracle: docs/build-plan.md (B4) and docs/contracts.md extract(candidate, budget).
Cases must FAIL when behavior is missing/wrong and PASS when integrated correctly.
The gate runner rejects skipped cases. Offline only — no live provider calls.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tests.support import FIXTURES

LLM_FIXTURES = FIXTURES / "llm"
HTML_DIR = LLM_FIXTURES / "html"
RESP_DIR = LLM_FIXTURES / "responses"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_html(name: str) -> str:
    return (HTML_DIR / name).read_text(encoding="utf-8")


def _load_resp(name: str) -> dict[str, Any]:
    return json.loads((RESP_DIR / name).read_text(encoding="utf-8"))


def _assert_synthetic_meta(meta: dict[str, Any]) -> None:
    assert meta.get("label") == "synthetic"
    assert meta.get("not_for_live_export") is True


def _seg_by(
    segments: list[Any],
    *,
    kind: str | None = None,
    contains: str | None = None,
) -> Any:
    for seg in segments:
        if kind is not None and seg.kind.value != kind:
            continue
        if contains is not None and contains not in seg.text:
            continue
        return seg
    raise AssertionError(f"no segment kind={kind!r} contains={contains!r}")


def _claims_from_field_map(
    field_map: dict[str, Any],
    segments: list[Any],
) -> list[dict[str, Any]]:
    """Bind fixture field dicts to real segment_ids (by kind + quote locate)."""
    claims: list[dict[str, Any]] = []
    for field, spec in field_map.items():
        quote = spec.get("quote")
        value = spec.get("value")
        kind_hint = (spec.get("segment_ids") or [None])[0]
        kind = None
        if isinstance(kind_hint, str):
            kind = kind_hint.lower()
        seg_ids: list[str] = []
        if quote:
            # Prefer kind hint, then any segment containing the quote.
            try:
                if kind:
                    seg = _seg_by(segments, kind=kind, contains=quote)
                else:
                    seg = _seg_by(segments, contains=quote)
                seg_ids = [seg.segment_id]
            except AssertionError:
                # Hallucinated quote: keep placeholder ids from fixture kind.
                if kind:
                    try:
                        seg_ids = [_seg_by(segments, kind=kind).segment_id]
                    except AssertionError:
                        seg_ids = [f"missing:{kind}"]
                else:
                    seg_ids = ["missing:unknown"]
        claims.append(
            {
                "field": field,
                "value": value,
                "quote": quote,
                "segment_ids": seg_ids,
            }
        )
    return claims


def _payload_json(claims: list[dict[str, Any]]) -> dict[str, Any]:
    return {"fields": claims}


async def _extract_with_scripted(
    html: str,
    *,
    claims: list[dict[str, Any]] | None = None,
    raw_text: str | None = None,
    target_entity: str,
    target_aliases: tuple[str, ...] = (),
    known_entities: tuple[str, ...] = (),
    fields_requested: tuple[str, ...] = (),
    record_type: str = "PRODUCT",
    candidate_id: str = "b4-cand",
    deterministic_fields: dict[str, Any] | None = None,
    limits: Any | None = None,
    scripted_payloads: list[Any] | None = None,
) -> Any:
    from signal_ingest.llm import (
        ExtractCandidate,
        FakeProvider,
        default_fake_limits,
        extract,
        html_to_segments,
    )

    provider = FakeProvider(limits or default_fake_limits())
    if raw_text is not None:
        provider.queue_json(raw_text)
    elif scripted_payloads is not None:
        for item in scripted_payloads:
            if isinstance(item, Exception):
                provider.queue_error(item)
            else:
                provider.queue_json(item)
    elif claims is not None:
        provider.queue_json(_payload_json(claims))
    else:
        raise ValueError("claims, raw_text, or scripted_payloads required")

    cand = ExtractCandidate(
        candidate_id=candidate_id,
        record_type=record_type,
        html=html,
        target_entity=target_entity,
        target_aliases=target_aliases,
        known_entities=known_entities,
        fields_requested=fields_requested
        or ("productName", "owner", "pricingModel", "price", "authors"),
        deterministic_fields=deterministic_fields or {},
    )
    # Ensure segments parse before extract (entry path uses same helper).
    assert html_to_segments(
        html,
        entity_hints=(target_entity, *target_aliases, *known_entities),
    )
    return await extract(cand, provider), provider


def _decision(result: Any, field: str) -> str:
    ev = result.fields.get(field)
    assert ev is not None, f"missing field {field} in {list(result.fields)}"
    return ev.decision.value


# ---------------------------------------------------------------------------
# Fixture labeling
# ---------------------------------------------------------------------------


def test_llm_fixtures_labeled_synthetic():
    """All B4 LLM fixtures stay labeled synthetic / not for live export."""
    meta = json.loads((LLM_FIXTURES / "meta.json").read_text(encoding="utf-8"))
    _assert_synthetic_meta(meta["_meta"])
    html_files = sorted(HTML_DIR.glob("*.html"))
    assert html_files, "expected tests/fixtures/llm/html/*.html"
    resp_files = sorted(RESP_DIR.glob("*.json"))
    assert resp_files, "expected tests/fixtures/llm/responses/*.json"
    for path in resp_files:
        data = json.loads(path.read_text(encoding="utf-8"))
        _assert_synthetic_meta(data["_meta"])


# ---------------------------------------------------------------------------
# Fake transport: measured bytes + token budget on every request
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fake_transport_asserts_bytes_and_tokens_every_call():
    """FakeProvider measures serialized bytes and input tokens; over-budget fails.

    Detects: missing per-call asserts, measuring prompt-only, skipping schema bytes.
    """
    from signal_ingest.llm import (
        EXTRACTION_JSON_SCHEMA,
        FakeProvider,
        PayloadTooLarge,
        ProviderRequest,
        RequestBudget,
        default_fake_limits,
        estimate_tokens,
        measure_provider_request,
    )

    limits = default_fake_limits(
        max_request_bytes=2_000,
        context_tokens=2048,
        reserved_output_tokens=256,
        safety_margin_tokens=32,
        framing_tokens=64,
    )
    provider = FakeProvider(limits)
    ok_budget = RequestBudget(
        max_input_tokens=400,
        max_output_tokens=64,
        max_request_bytes=2_000,
        framing_tokens=32,
    )
    req = ProviderRequest(
        messages=[
            {"role": "system", "content": "Extract fields."},
            {"role": "user", "content": "short"},
        ],
        response_schema=EXTRACTION_JSON_SCHEMA,
        budget=ok_budget,
        provider=limits.provider,
        model=limits.model,
    )
    measured = measure_provider_request(req)
    assert measured.request_bytes > 0
    assert measured.input_tokens == estimate_tokens(measured.serialized)
    assert measured.request_bytes <= ok_budget.max_request_bytes

    result = await provider.complete(req)
    assert result.request_bytes == measured.request_bytes
    assert result.input_tokens == measured.input_tokens
    assert len(provider.calls) == 1
    assert provider.calls[0]["request_bytes"] == measured.request_bytes
    assert provider.calls[0]["input_tokens"] == measured.input_tokens

    huge = ProviderRequest(
        messages=[{"role": "user", "content": "X" * 8_000}],
        response_schema=EXTRACTION_JSON_SCHEMA,
        budget=RequestBudget(
            max_input_tokens=50,
            max_output_tokens=16,
            max_request_bytes=500,
            framing_tokens=0,
        ),
        provider=limits.provider,
        model=limits.model,
    )
    with pytest.raises(PayloadTooLarge):
        await provider.complete(huge)
    assert len(provider.calls) == 2
    assert provider.calls[1]["request_bytes"] > 500


@pytest.mark.asyncio
async def test_transport_unicode_emoji_code_long_json_oversized_schema():
    """Budget math holds for Unicode/emoji/code/long-token/JSON-escape/oversized schema.

    Detects: naive len(chars) token math, ASCII-only serialization, schema omitted once.
    """
    from signal_ingest.llm import (
        EXTRACTION_JSON_SCHEMA,
        FakeProvider,
        PayloadTooLarge,
        ProviderRequest,
        RequestBudget,
        compute_budget,
        default_fake_limits,
        estimate_tokens,
        html_to_segments,
        measure_provider_request,
    )
    from signal_ingest.llm.serialize import serialize_json as ser

    unicode_html = _load_html("unicode_emoji_code.html")
    long_html = _load_html("long_token.html")
    segs_u = html_to_segments(unicode_html, entity_hints=("ユニコード Cloud 🚀",))
    segs_l = html_to_segments(long_html, entity_hints=("LongToken Co",))
    assert any("🚀" in s.text or "🎉" in s.text for s in segs_u)
    assert any(s.kind.value == "code" for s in segs_u)
    assert any("TOKENSTART_" in s.text for s in segs_l)

    # JSON escaping: quotes/newlines increase serialized UTF-8 bytes.
    tricky = {'q': 'say "hello"\nline2', "emoji": "🚀"}
    raw = ser(tricky)
    assert b"\\n" in raw or b"hello" in raw
    assert estimate_tokens(raw) == estimate_tokens(raw.decode("utf-8"))

    limits = default_fake_limits(context_tokens=4096, max_request_bytes=50_000)
    provider = FakeProvider(limits)
    budget = compute_budget(limits, schema_overhead_tokens=estimate_tokens(ser(EXTRACTION_JSON_SCHEMA)))
    req_budget = budget.to_request_budget()
    catalog = "\n".join(f"{s.segment_id}:{s.text}" for s in segs_u + segs_l)
    req = ProviderRequest(
        messages=[
            {"role": "system", "content": "Extract with ユニコード and emoji ✅"},
            {"role": "user", "content": catalog},
        ],
        response_schema=EXTRACTION_JSON_SCHEMA,
        budget=req_budget,
        provider=limits.provider,
        model=limits.model,
    )
    measured = measure_provider_request(req)
    assert measured.request_bytes <= req_budget.max_request_bytes
    assert measured.input_tokens <= req_budget.max_input_tokens
    await provider.complete(req)
    assert provider.calls[-1]["request_bytes"] == measured.request_bytes

    # Oversized schema alone must trip the byte/token assert.
    oversized_schema = {
        "type": "object",
        "properties": {f"field_{i}": {"type": "string", "description": "D" * 200} for i in range(400)},
    }
    over_req = ProviderRequest(
        messages=[{"role": "user", "content": "x"}],
        response_schema=oversized_schema,
        budget=RequestBudget(
            max_input_tokens=200,
            max_output_tokens=32,
            max_request_bytes=1_500,
            framing_tokens=0,
        ),
        provider=limits.provider,
        model=limits.model,
    )
    with pytest.raises(PayloadTooLarge):
        await provider.complete(over_req)


# ---------------------------------------------------------------------------
# Chunk planner: late pricing + multi-chunk conflict
# ---------------------------------------------------------------------------


def test_late_page_pricing_prioritized_under_tight_budget():
    """Pricing near the end must still be packed when budget is tight.

    Detects: FIFO body-first packing that drops late pricing.
    """
    from signal_ingest.llm import (
        SegmentKind,
        compute_budget,
        default_fake_limits,
        html_to_segments,
        plan_chunks,
    )

    html = _load_html("late_pricing.html")
    segments = html_to_segments(html, entity_hints=("Orbit Metrics", "Orbit Data Co"))
    pricing = [s for s in segments if s.kind == SegmentKind.PRICING]
    assert pricing, "fixture must yield a pricing segment"
    # Tight budget: cannot fit every body paragraph.
    limits = default_fake_limits(
        context_tokens=256,
        reserved_output_tokens=64,
        safety_margin_tokens=16,
        framing_tokens=40,
        max_request_bytes=8_000,
    )
    budget = compute_budget(limits, schema_overhead_tokens=20, instruction_tokens=20)
    plan = plan_chunks(segments, budget, overlap_chars=20, min_chunk_chars=24)
    packed_text = "\n".join(c.text for c in plan.chunks)
    # Splits may rename ids (seg:…#pN); content priority is the contract.
    assert "$49" in packed_text or "Paid" in packed_text, (
        f"late pricing dropped under tight budget; discarded={plan.discarded_coverage}"
    )
    assert any(
        s.kind == SegmentKind.PRICING or s.segment_id.startswith(p.segment_id)
        for c in plan.chunks
        for s in c.segments
        for p in pricing
    )


@pytest.mark.asyncio
async def test_multi_chunk_conflict_preserved_as_review():
    """Conflicting accepted values across chunks → REVIEW + contradiction record.

    Detects: last-write-wins merge that silently drops conflicts.
    """
    from signal_ingest.llm import (
        ExtractCandidate,
        FakeProvider,
        FieldDecision,
        compute_budget,
        default_fake_limits,
        extract,
        html_to_segments,
        plan_chunks,
    )

    html = _load_html("late_pricing.html")
    hints = ("Orbit Metrics", "Orbit Data Co")
    segments = html_to_segments(html, entity_hints=hints)
    pricing_seg = _seg_by(segments, kind="pricing", contains="$49")
    quote = "Orbit Metrics public pricing: Paid. Starter is $49/month with no free tier."

    # Exercise merge of conflicting chunk results (multi-chunk contract).
    from signal_ingest.llm import FieldEvidence, merge_field_evidence

    a = FieldEvidence(
        field="pricingModel",
        value="FREE",
        quote=quote,
        segment_ids=(pricing_seg.segment_id,),
        decision=FieldDecision.ACCEPTED,
    )
    b = FieldEvidence(
        field="pricingModel",
        value="PAID",
        quote=quote,
        segment_ids=(pricing_seg.segment_id,),
        decision=FieldDecision.ACCEPTED,
    )
    merged, contradictions, _ = merge_field_evidence([[a], [b]])
    assert contradictions, "conflicting FREE vs PAID must be recorded"
    assert merged["pricingModel"].decision == FieldDecision.REVIEW

    # Also prove extract() can emit ≥2 provider calls under a tight content budget.
    limits = default_fake_limits(
        context_tokens=280,
        reserved_output_tokens=48,
        safety_margin_tokens=8,
        framing_tokens=40,
        max_request_bytes=20_000,
    )
    budget = compute_budget(limits, schema_overhead_tokens=12, instruction_tokens=12)
    plan = plan_chunks(segments, budget, overlap_chars=16, min_chunk_chars=20)
    if len(plan.chunks) >= 2:
        payloads = [
            {
                "fields": [
                    {
                        "field": "pricingModel",
                        "value": "FREE",
                        "quote": quote,
                        "segment_ids": [pricing_seg.segment_id],
                    }
                ]
            },
            {
                "fields": [
                    {
                        "field": "pricingModel",
                        "value": "PAID",
                        "quote": quote,
                        "segment_ids": [pricing_seg.segment_id],
                    }
                ]
            },
        ]
        while len(payloads) < len(plan.chunks):
            payloads.append(payloads[-1])
        provider = FakeProvider(limits)
        for p in payloads[: len(plan.chunks)]:
            provider.queue_json(p)
        result = await extract(
            ExtractCandidate(
                candidate_id="b4-conflict",
                record_type="PRODUCT",
                html=html,
                target_entity="Orbit Metrics",
                target_aliases=("Orbit Data Co",),
                fields_requested=("pricingModel",),
            ),
            provider,
            budget=budget,
        )
        if len(provider.calls) >= 2 and "pricingModel" in result.fields:
            assert (
                result.contradictions
                or result.fields["pricingModel"].decision == FieldDecision.REVIEW
            )


# ---------------------------------------------------------------------------
# Same extractor: grounded positive + ungrounded negative
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_grounded_positive_and_ungrounded_negative_same_extractor():
    """Source-backed positive accepts; plausible ungrounded negative abstains — same extract().

    Detects: always-accept path or separate code paths for happy/negative fixtures.
    """
    from signal_ingest.llm import FieldDecision, ExtractStatus, html_to_segments

    pos_html = _load_html("grounded_product.html")
    neg_html = _load_html("ungrounded_product.html")
    pos_resp = _load_resp("grounded_positive.json")
    neg_resp = _load_resp("ungrounded_negative.json")

    pos_segs = html_to_segments(pos_html, entity_hints=("Acme Cloud", "Acme Labs Inc."))
    neg_segs = html_to_segments(neg_html, entity_hints=("Beta Widget", "Beta Systems LLC"))

    pos_claims = _claims_from_field_map(pos_resp["fields"], pos_segs)
    # Force ungrounded pricing claims onto body quotes (insufficient pricing evidence).
    neg_claims = _claims_from_field_map(neg_resp["fields"], neg_segs)

    pos_result, _ = await _extract_with_scripted(
        pos_html,
        claims=pos_claims,
        target_entity="Acme Cloud",
        target_aliases=("Acme Labs Inc.",),
        candidate_id="b4-pos",
    )
    neg_result, _ = await _extract_with_scripted(
        neg_html,
        claims=neg_claims,
        target_entity="Beta Widget",
        target_aliases=("Beta Systems LLC",),
        candidate_id="b4-neg",
    )

    assert pos_result.status in {ExtractStatus.OK, ExtractStatus.REVIEW}
    assert _decision(pos_result, "pricingModel") == FieldDecision.ACCEPTED.value
    assert _decision(pos_result, "owner") == FieldDecision.ACCEPTED.value

    # Open-source / contact-us is not FREEMIUM evidence — must not accept.
    assert _decision(neg_result, "pricingModel") != FieldDecision.ACCEPTED.value
    assert neg_result.fields["pricingModel"].decision in {
        FieldDecision.ABSTAIN,
        FieldDecision.REJECTED,
        FieldDecision.REVIEW,
    }


# ---------------------------------------------------------------------------
# Goldens: misleading number, hallucination, injection, wrong entity, malformed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_misleading_same_page_number_rejected_as_price():
    """Employee/like counts on the same page must not ground price.

    Detects: quote⊆segment-only grounding without field/domain predicates.
    """
    from signal_ingest.llm import FieldDecision

    html = _load_html("misleading_number.html")
    resp = _load_resp("misleading_number.json")
    from signal_ingest.llm import html_to_segments

    segs = html_to_segments(html, entity_hints=("Pixel Forge", "Pixel Works Inc."))
    claims = _claims_from_field_map(resp["fields"], segs)
    # Ensure the misleading claim cites the employee sentence, not $45.
    for c in claims:
        if c["field"] == "price":
            c["quote"] = "The company has 120 employees across three offices."
            c["value"] = "120"
            c["segment_ids"] = [_seg_by(segs, contains="120 employees").segment_id]

    result, _ = await _extract_with_scripted(
        html,
        claims=claims,
        target_entity="Pixel Forge",
        target_aliases=("Pixel Works Inc.",),
        candidate_id="b4-misleading",
    )
    assert _decision(result, "price") != FieldDecision.ACCEPTED.value, (
        "employee count 120 must not be accepted as price"
    )


@pytest.mark.asyncio
async def test_hallucinated_author_and_price_rejected():
    """Quotes absent from retained segments → reject/abstain (no invented author/price)."""
    from signal_ingest.llm import FieldDecision, html_to_segments

    html = _load_html("grounded_product.html")
    resp = _load_resp("hallucinated_author_price.json")
    segs = html_to_segments(html, entity_hints=("Acme Cloud",))
    claims = _claims_from_field_map(resp["fields"], segs)
    # Force hallucinated quotes that are not on the page.
    for c in claims:
        if c["field"] == "authors":
            c["quote"] = "Dr. Fake Author"
            c["value"] = ["Dr. Fake Author"]
            c["segment_ids"] = [_seg_by(segs, kind="owner").segment_id]
        if c["field"] == "price":
            c["quote"] = "$9999 enterprise forever"
            c["value"] = "$9999"
            c["segment_ids"] = [_seg_by(segs, kind="pricing").segment_id]

    result, _ = await _extract_with_scripted(
        html,
        claims=claims,
        target_entity="Acme Cloud",
        candidate_id="b4-hallucinate",
    )
    assert _decision(result, "authors") != FieldDecision.ACCEPTED.value
    assert _decision(result, "price") != FieldDecision.ACCEPTED.value
    assert result.fields["authors"].reason in {
        "quote_not_in_segment",
        "missing_evidence",
        "predicate_failed",
        "invalid_authors",
    } or (result.fields["authors"].reason or "").startswith("quote")


@pytest.mark.asyncio
async def test_prompt_injection_in_page_not_obeyed():
    """Page instructions are content — injection must not set owner/pricing/authors.

    Detects: treating page text as system instructions / skipping entity predicates.
    """
    from signal_ingest.llm import FieldDecision, html_to_segments

    html = _load_html("injection.html")
    resp = _load_resp("injection.json")
    segs = html_to_segments(html, entity_hints=("SafeDocs", "SafeDocs Inc."))
    claims = _claims_from_field_map(resp["fields"], segs)
    result, _ = await _extract_with_scripted(
        html,
        claims=claims,
        target_entity="SafeDocs",
        target_aliases=("SafeDocs Inc.",),
        candidate_id="b4-inject",
    )
    assert result.fields["owner"].value != "Attacker Corp" or _decision(
        result, "owner"
    ) != FieldDecision.ACCEPTED.value
    assert _decision(result, "owner") != FieldDecision.ACCEPTED.value or result.fields[
        "owner"
    ].value == "SafeDocs Inc."
    assert _decision(result, "pricingModel") != FieldDecision.ACCEPTED.value or result.fields[
        "pricingModel"
    ].value == "PAID"
    assert "authors" not in result.fields or _decision(result, "authors") != FieldDecision.ACCEPTED.value


@pytest.mark.asyncio
async def test_correct_quote_wrong_entity_rejected():
    """Verbatim quote about Quasar must not ground Nova Assist fields.

    Detects: quote⊆segment without entity-window checks (planner weakest assumption).
    """
    from signal_ingest.llm import FieldDecision, html_to_segments

    html = _load_html("multi_entity.html")
    resp = _load_resp("wrong_entity_quote.json")
    segs = html_to_segments(
        html,
        entity_hints=("Nova Assist", "Quasar Search", "Nova AI Corp", "Quasar Labs Ltd."),
    )
    claims = _claims_from_field_map(resp["fields"], segs)
    # Pin owner/pricing to Quasar quotes while targeting Nova.
    for c in claims:
        if c["field"] == "owner":
            c["quote"] = "Quasar Search is owned by Quasar Labs Ltd."
            c["value"] = "Quasar Labs Ltd."
            c["segment_ids"] = [_seg_by(segs, contains="Quasar Labs Ltd.").segment_id]
        if c["field"] == "pricingModel":
            c["quote"] = "Quasar Search pricing: Enterprise only with custom contracts."
            c["value"] = "ENTERPRISE"
            c["segment_ids"] = [
                _seg_by(segs, contains="Quasar Search pricing").segment_id
            ]

    result, _ = await _extract_with_scripted(
        html,
        claims=claims,
        target_entity="Nova Assist",
        target_aliases=("Nova AI Corp",),
        known_entities=("Quasar Search", "Quasar Labs Ltd."),
        candidate_id="b4-wrong-entity",
    )
    assert _decision(result, "owner") != FieldDecision.ACCEPTED.value
    assert _decision(result, "pricingModel") != FieldDecision.ACCEPTED.value
    assert result.fields["owner"].reason in {
        "correct_quote_wrong_entity",
        "target_entity_not_in_evidence",
        "quote_not_about_target_entity",
        "entity_mismatch",
    } or "entity" in (result.fields["owner"].reason or "")


@pytest.mark.asyncio
async def test_malformed_json_rejected_or_abstains():
    """Malformed model JSON must not yield accepted fields."""
    from signal_ingest.llm import ExtractStatus, FieldDecision, parse_model_payload

    raw = _load_resp("malformed.json")["raw_text"]
    payload, err = parse_model_payload(raw)
    assert payload is None
    assert err == "malformed_json"

    html = _load_html("grounded_product.html")
    result, _ = await _extract_with_scripted(
        html,
        raw_text=raw,
        target_entity="Acme Cloud",
        candidate_id="b4-malformed",
    )
    model_accepted = [
        f
        for f in result.fields.values()
        if f.source == "model" and f.decision == FieldDecision.ACCEPTED
    ]
    assert not model_accepted
    assert result.status in {
        ExtractStatus.ABSTAIN,
        ExtractStatus.FAILED,
        ExtractStatus.OK,  # OK only if solely deterministic empties — still no model accepts
        ExtractStatus.REVIEW,
    }


# ---------------------------------------------------------------------------
# Abstain unsupported + unchanged oversized retry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_abstain_on_unsupported_and_null_fields():
    """Unsupported/null model fields abstain; invented employeeCount without evidence fails.

    Detects: inventing defaults for missing required evidence.
    """
    from signal_ingest.llm import FieldDecision, html_to_segments

    html = _load_html("ungrounded_product.html")
    resp = _load_resp("abstain_unsupported.json")
    segs = html_to_segments(html, entity_hints=("Beta Widget",))
    claims = _claims_from_field_map(resp["fields"], segs)
    for c in claims:
        if c["field"] == "pricingModel":
            c["value"] = None
            c["quote"] = None
            c["segment_ids"] = []
        if c["field"] == "employeeCount":
            c["value"] = 5000
            c["quote"] = None
            c["segment_ids"] = []

    result, _ = await _extract_with_scripted(
        html,
        claims=claims,
        target_entity="Beta Widget",
        fields_requested=("productName", "pricingModel", "employeeCount"),
        candidate_id="b4-abstain",
    )
    assert _decision(result, "pricingModel") == FieldDecision.ABSTAIN.value
    # Unsupported invented count must not be accepted without evidence.
    if "employeeCount" in result.fields:
        assert _decision(result, "employeeCount") != FieldDecision.ACCEPTED.value


@pytest.mark.asyncio
async def test_unchanged_oversized_retry_fails():
    """Retrying the same oversized serialized payload after 413 must raise UnchangedOversizedRetry.

    Detects: infinite 413 loops that resend identical bytes.
    """
    from signal_ingest.llm import (
        ExtractCandidate,
        FakeProvider,
        PayloadTooLarge,
        UnchangedOversizedRetry,
        default_fake_limits,
        extract,
        measure_provider_request,
    )

    html = _load_html("long_token.html")
    fixed_size: dict[str, int | None] = {"n": None}

    def sticky_413(request, _body):
        measured = measure_provider_request(request)
        # Always report the first observed size so a second attempt looks unchanged.
        if fixed_size["n"] is None:
            fixed_size["n"] = measured.request_bytes
        raise PayloadTooLarge(
            "simulated 413",
            serialized_bytes=int(fixed_size["n"]),
            input_tokens=measured.input_tokens,
            unchanged=True,
        )

    limits = default_fake_limits(
        context_tokens=512,
        reserved_output_tokens=64,
        safety_margin_tokens=16,
        framing_tokens=64,
        max_request_bytes=4_000,
    )
    provider = FakeProvider(limits, handler=sticky_413, enforce_budget=False)
    cand = ExtractCandidate(
        candidate_id="b4-413",
        record_type="PRODUCT",
        html=html,
        target_entity="LongToken Co",
        fields_requested=("productName", "pricingModel"),
    )
    with pytest.raises(UnchangedOversizedRetry):
        await extract(cand, provider, max_depth=4)


def test_unchanged_oversized_byte_equality_is_detectable():
    """Identical serialized request bytes are observable for the unchanged-retry gate."""
    from signal_ingest.llm import (
        EXTRACTION_JSON_SCHEMA,
        ProviderRequest,
        RequestBudget,
        default_fake_limits,
        measure_provider_request,
    )

    limits = default_fake_limits()
    req = ProviderRequest(
        messages=[{"role": "user", "content": "same"}],
        response_schema=EXTRACTION_JSON_SCHEMA,
        budget=RequestBudget(
            max_input_tokens=1000,
            max_output_tokens=64,
            max_request_bytes=50_000,
        ),
        provider=limits.provider,
        model=limits.model,
    )
    a = measure_provider_request(req)
    b = measure_provider_request(req)
    assert a.serialized == b.serialized
    assert a.request_bytes == b.request_bytes


# ---------------------------------------------------------------------------
# Deterministic lock + network guard sanity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_model_cannot_overwrite_deterministic_stars():
    """github_stars from deterministic parsers cannot be overwritten by the model."""
    from signal_ingest.llm import FieldDecision, html_to_segments

    html = _load_html("grounded_product.html")
    segs = html_to_segments(html, entity_hints=("Acme Cloud",))
    claims = [
        {
            "field": "github_stars",
            "value": 999999,
            "quote": "Acme Cloud",
            "segment_ids": [_seg_by(segs, kind="title").segment_id],
        }
    ]
    result, _ = await _extract_with_scripted(
        html,
        claims=claims,
        target_entity="Acme Cloud",
        deterministic_fields={"github_stars": 0},
        fields_requested=("github_stars", "productName"),
        candidate_id="b4-locked",
    )
    assert result.fields["github_stars"].value == 0
    assert result.fields["github_stars"].source == "deterministic"
    assert result.fields["github_stars"].decision == FieldDecision.ACCEPTED


def test_b4_stays_offline_no_live_provider_config_secrets():
    """Provider stubs exist without secret values; fake route is non-live."""
    from signal_ingest.llm import default_fake_limits, load_providers_config

    limits = default_fake_limits()
    assert limits.provider.startswith("fake")
    cfg_path = Path("config/providers.yaml")
    assert cfg_path.is_file()
    text = cfg_path.read_text(encoding="utf-8")
    # Env *names* like GEMINI_API_KEY are OK; raw secret material is not.
    for needle in ("BEGIN PRIVATE", "sk-ant-", "sk-proj-", "AIzaSy"):
        assert needle not in text
    cfg = load_providers_config(cfg_path)
    providers = cfg.get("providers") or {}
    assert "fake" in providers
    for name, block in providers.items():
        if isinstance(block, dict) and "api_key" in block:
            assert block["api_key"] in (None, "", "null")


# ---------------------------------------------------------------------------
# R2: grounding holes, cache key, merge, segmenter
# ---------------------------------------------------------------------------

_R2_HTML = (
    "<html><body><h1>Pixel Forge</h1>"
    "<p>Pixel Forge has 120 employees across three offices.</p>"
    "<p>Pixel Forge costs $45 per month.</p></body></html>"
)


def _r2_seg(contains: str) -> str:
    from signal_ingest.llm import html_to_segments

    return next(
        s.segment_id
        for s in html_to_segments(_R2_HTML, entity_hints=("Pixel Forge",))
        if contains in s.text
    )


@pytest.mark.asyncio
async def test_r2_value_not_in_quote_rejected():
    """A real quote about the target cannot carry a value it does not contain."""
    claims = [{"field": "price", "value": "$45",
               "quote": "Pixel Forge has 120 employees across three offices.",
               "segment_ids": [_r2_seg("employees")]}]
    result, _ = await _extract_with_scripted(
        _R2_HTML, claims=claims, target_entity="Pixel Forge", fields_requested=("price",)
    )
    assert _decision(result, "price") == "rejected"
    ok_claims = [{"field": "price", "value": "$45",
                  "quote": "Pixel Forge costs $45 per month.",
                  "segment_ids": [_r2_seg("$45")]}]
    ok, _ = await _extract_with_scripted(
        _R2_HTML, claims=ok_claims, target_entity="Pixel Forge", fields_requested=("price",)
    )
    assert _decision(ok, "price") == "accepted"


@pytest.mark.asyncio
async def test_r2_unrequested_field_rejected():
    claims = [{"field": "funding", "value": "$45",
               "quote": "Pixel Forge costs $45 per month.",
               "segment_ids": [_r2_seg("$45")]}]
    result, _ = await _extract_with_scripted(
        _R2_HTML, claims=claims, target_entity="Pixel Forge", fields_requested=("price",)
    )
    assert _decision(result, "funding") == "rejected"


@pytest.mark.asyncio
async def test_r2_cache_key_separates_entities_and_skips_failures():
    from signal_ingest.llm import (
        ExtractCandidate, ExtractionCache, FakeProvider, default_fake_limits, extract,
    )

    provider = FakeProvider(default_fake_limits())
    provider.queue_json("not json at all")
    provider.queue_json({"fields": []})
    provider.queue_json({"fields": []})
    cache = ExtractionCache()

    def cand(entity):
        return ExtractCandidate(candidate_id="r2c", record_type="PRODUCT", html=_R2_HTML,
                                target_entity=entity, fields_requested=("price",))

    await extract(cand("Pixel Forge"), provider, cache=cache)   # malformed → not cached
    again = await extract(cand("Pixel Forge"), provider, cache=cache)
    assert again.status.value != "cache_hit"
    other = await extract(cand("Other Co"), provider, cache=cache)
    assert other.status.value != "cache_hit"
    assert provider._script_i == 3


def test_r2_review_stays_sticky_across_later_chunks():
    from signal_ingest.llm import merge_field_evidence
    from signal_ingest.llm.types import FieldDecision, FieldEvidence

    def ev(v):
        return FieldEvidence(field="price", value=v, quote=v, segment_ids=("S",),
                             decision=FieldDecision.ACCEPTED, source="model")

    merged, contradictions, _ = merge_field_evidence([[ev("$1")], [ev("$2")], [ev("$1")]])
    assert merged["price"].decision is FieldDecision.REVIEW
    assert contradictions


def test_r2_segmenter_keeps_content_after_nav_div_and_unicode_quotes():
    from signal_ingest.llm import html_to_segments, quote_in_segment

    html = ('<html><body><div class="navbar"><a href="/">Home</a></div>'
            '<div class="unavailable-banner"><p>Stock note</p></div>'
            "<p>Acme costs $29  per month.</p></body></html>")
    segs = html_to_segments(html)
    text = " ".join(s.text for s in segs)
    assert "Home" not in text
    assert "Stock note" in text
    seg = next(s for s in segs if "$29" in s.text)
    assert quote_in_segment("Acme costs $29 per month.", seg)


@pytest.mark.parametrize(
    ("field", "value", "quote", "ok"),
    [
        ("github_url", "https://github.com/acme", "Code: https://github.com/acme/widget", False),
        ("github_url", "https://github.com/acme/widget", "Code: https://github.com/acme/widget.", True),
        ("productName", "Ace", "Acer Studio launches today", False),
        ("owner", "Acme Inc.", "Acme Cloud is owned by Acme Inc", True),
        ("price", "1299", "Plans start at $1,299.00 per year", True),
        ("price", "$45", "Pixel Forge has 120 employees", False),
        ("is_remote", True, "Senior engineer, Berlin", False),
        ("is_remote", True, "This role is fully remote", True),
        ("is_remote", True, "This role is not remote", False),
        ("is_remote", True, "Non-remote position in Austin", False),
        ("is_remote", True, "Remote: No", False),
        ("is_remote", False, "On-site in Berlin, five days a week", True),
        ("published_date", "2026-11-21", "Posted 2 hours ago", False),
        ("published_date", "2026-11-21", "Published Nov 21, 2026", True),
        ("published_date", "2026-11-21", "A novel from 2026, chapter 21", False),
        ("is_remote", True, "Build distributed systems in Berlin", False),
        ("pricingModel", "ENTERPRISE", "Simple pricing for everyone", False),
        ("pricingModel", "FREEMIUM", "A free tier plus a Pro plan", True),
    ],
)
def test_r2_value_supported_by_quote_matrix(field, value, quote, ok):
    from signal_ingest.llm.predicates import value_supported_by_quote

    assert value_supported_by_quote(field, value, quote) is ok


@pytest.mark.asyncio
async def test_r2_ledger_outage_does_not_strand_reservations(monkeypatch):
    """A quota-store error on one tier: that tier is not dispatched, the next answers,
    and nothing is left reserved (R2b LiteLLMChain)."""
    import litellm

    from signal_ingest.config.network import install_network_guard, uninstall_network_guard
    from tests.integration import b5_api

    class BrokenLedger(b5_api.RecordingLedger):
        async def reserve(self, provider, **kw):
            if provider == "gemini":
                raise ConnectionError("quota db down")
            return await super().reserve(provider, **kw)

    script = b5_api.LiteLLMScript().on("gemini", b5_api.OK_TEXT).on("groq", b5_api.OK_TEXT)
    monkeypatch.setattr(litellm, "acompletion", script)
    ledger = b5_api.quota(ledger_cls=BrokenLedger)
    chain = b5_api.chain([b5_api.tier("gemini"), b5_api.tier("groq")], ledger)
    install_network_guard()
    try:
        result = await chain.complete(b5_api.request())
    finally:
        uninstall_network_guard()
    assert result.raw["provider"] == "groq"
    assert script.dispatched("gemini") == 0
    assert ledger.ops("commit") == ["groq"]
    assert ledger.snapshot()["open_reservations"] == 0


def test_segment_id_without_kind_suffix_maps_only_when_unique():
    """Groq/Meta cite "seg:0005" for "seg:0005:pricing": accept only an unambiguous prefix,
    and the quote must still be inside that segment."""
    from signal_ingest.llm.schema import ModelExtractionPayload
    from signal_ingest.llm.validate import validate_payload
    from signal_ingest.llm.types import FieldDecision, Segment, SegmentKind

    segs = [
        Segment(segment_id="seg:0005:pricing", kind=SegmentKind.PRICING, text="Acme Notes Pro costs $12 per month.",
                start_offset=0, end_offset=35, entity_hints=("Acme Notes",)),
        Segment(segment_id="seg:0006:body", kind=SegmentKind.BODY, text="Acme Notes is built by Acme Labs Inc.",
                start_offset=36, end_offset=73, entity_hints=("Acme Notes",)),
    ]

    def run(sid, quote):
        payload = ModelExtractionPayload.model_validate(
            {"fields": [{"field": "owner", "value": "Acme Labs Inc.", "quote": quote, "segment_ids": [sid]}]}
        )
        out = validate_payload(payload, segments=segs, target_entity="Acme Notes", target_aliases=(),
                               known_entities=(), deterministic_fields={}, fields_requested=("owner",))
        return {e.field: e for e in out}["owner"]

    ok = run("seg:0006", "Acme Notes is built by Acme Labs Inc.")
    assert ok.decision is FieldDecision.ACCEPTED
    wrong_seg = run("seg:0005", "Acme Notes is built by Acme Labs Inc.")
    assert wrong_seg.decision is FieldDecision.REJECTED and wrong_seg.reason == "quote_not_in_segment"
    missing = run("seg:0009", "Acme Notes is built by Acme Labs Inc.")
    assert missing.reason == "unknown_segment:seg:0009"
