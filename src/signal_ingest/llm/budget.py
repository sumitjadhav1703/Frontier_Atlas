"""Byte/token budget arithmetic for one provider route."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from signal_ingest.llm.provider import ProviderLimits, RequestBudget
from signal_ingest.llm.tokens import estimate_tokens


class _Exact(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class ComputedBudget(_Exact):
    """Budget split: content (chunks) vs full serialized request cap.

    allowable_input_tokens = context − reserved_out − framing − margin (content room).
    full_input_token_cap = allowable + framing (asserted on measured request).
    """

    context_tokens: int
    reserved_output_tokens: int
    framing_tokens: int
    safety_margin_tokens: int
    provider_input_cap: int
    transport_byte_cap: int
    allowable_input_tokens: int
    full_input_token_cap: int
    route_disabled: bool = False
    disable_reason: str | None = None

    def to_request_budget(self) -> RequestBudget:
        return RequestBudget(
            max_input_tokens=self.full_input_token_cap,
            max_output_tokens=self.reserved_output_tokens,
            max_request_bytes=self.transport_byte_cap,
            framing_tokens=self.framing_tokens,
        )


def compute_budget(
    limits: ProviderLimits,
    *,
    schema_overhead_tokens: int = 0,
    instruction_tokens: int = 0,
) -> ComputedBudget:
    """Compute allowable content tokens; disable route when fixed overhead cannot fit."""
    framing = limits.framing_tokens + schema_overhead_tokens + instruction_tokens
    headroom = (
        limits.context_tokens
        - limits.reserved_output_tokens
        - limits.safety_margin_tokens
    )
    # Full request may not exceed provider input cap or context headroom.
    full_cap = min(max(0, headroom), limits.max_input_tokens)
    # Content room after counting framing once.
    content = full_cap - framing
    disabled = False
    reason: str | None = None
    if framing >= full_cap:
        disabled = True
        reason = "fixed_overhead_exceeds_context"
        content = 0
        full_cap = 0
    elif content <= 0:
        disabled = True
        reason = "no_allowable_input"
        content = 0
    return ComputedBudget(
        context_tokens=limits.context_tokens,
        reserved_output_tokens=limits.reserved_output_tokens,
        framing_tokens=framing,
        safety_margin_tokens=limits.safety_margin_tokens,
        provider_input_cap=limits.max_input_tokens,
        transport_byte_cap=limits.max_request_bytes,
        allowable_input_tokens=max(0, content),
        full_input_token_cap=max(0, full_cap),
        route_disabled=disabled,
        disable_reason=reason,
    )


def content_token_budget(computed: ComputedBudget) -> int:
    """Tokens available for variable chunk/segment content (framing already reserved)."""
    return max(0, computed.allowable_input_tokens)


def fits_request(
    *,
    serialized: bytes,
    computed: ComputedBudget,
) -> tuple[bool, str | None]:
    nbytes = len(serialized)
    ntok = estimate_tokens(serialized)
    if nbytes > computed.transport_byte_cap:
        return False, "byte_cap"
    if ntok > computed.full_input_token_cap:
        return False, "token_cap"
    return True, None


def shrink_content_budget(computed: ComputedBudget, *, factor: float = 0.5) -> ComputedBudget:
    """Re-budget after 413/context-400: reduce allowable content aggressively."""
    if factor <= 0 or factor >= 1:
        raise ValueError("factor must be in (0, 1)")
    new_allow = max(0, int(computed.allowable_input_tokens * factor))
    new_full = new_allow + computed.framing_tokens
    return computed.model_copy(
        update={
            "allowable_input_tokens": new_allow,
            "full_input_token_cap": new_full,
        }
    )
