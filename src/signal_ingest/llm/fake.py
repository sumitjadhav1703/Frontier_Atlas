"""FakeProvider: same async Provider interface; asserts byte+token budget every call."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

from signal_ingest.llm.provider import (
    ContextLengthExceeded,
    PayloadTooLarge,
    ProviderLimits,
    ProviderRequest,
    ProviderResult,
    measure_provider_request,
)
from signal_ingest.llm.tokens import estimate_tokens

ResponseHandler = Callable[[ProviderRequest, dict[str, Any]], ProviderResult | Awaitable[ProviderResult]]


class FakeProvider:
    """In-process fake transport. No HTTP. Asserts measured caps on every complete()."""

    def __init__(
        self,
        limits: ProviderLimits,
        *,
        handler: ResponseHandler | None = None,
        scripted: list[ProviderResult | Exception] | None = None,
        enforce_budget: bool = True,
    ) -> None:
        if limits.provider != "fake" and not limits.provider.startswith("fake"):
            # Allow test aliases like fake-overflow; still non-live.
            pass
        self._limits = limits
        self._handler = handler
        self._scripted = list(scripted or [])
        self._script_i = 0
        self.enforce_budget = enforce_budget
        self.calls: list[dict[str, Any]] = []

    @property
    def limits(self) -> ProviderLimits:
        return self._limits

    async def complete(self, request: ProviderRequest) -> ProviderResult:
        measured = measure_provider_request(request)
        record = {
            "request_bytes": measured.request_bytes,
            "input_tokens": measured.input_tokens,
            "max_request_bytes": request.budget.max_request_bytes,
            "max_input_tokens": request.budget.max_input_tokens,
            "provider": request.provider,
            "model": request.model,
        }
        self.calls.append(record)

        if self.enforce_budget:
            self._assert_budget(request, measured.request_bytes, measured.input_tokens)

        if self._script_i < len(self._scripted):
            item = self._scripted[self._script_i]
            self._script_i += 1
            if isinstance(item, Exception):
                raise item
            return item.model_copy(
                update={
                    "input_tokens": measured.input_tokens,
                    "request_bytes": measured.request_bytes,
                }
            )

        if self._handler is not None:
            maybe = self._handler(request, measured.body)
            if hasattr(maybe, "__await__"):
                result = await maybe  # type: ignore[misc]
            else:
                result = maybe  # type: ignore[assignment]
            return result.model_copy(
                update={
                    "input_tokens": measured.input_tokens,
                    "request_bytes": measured.request_bytes,
                }
            )

        return ProviderResult(
            text="{}",
            parsed={},
            input_tokens=measured.input_tokens,
            output_tokens=1,
            request_bytes=measured.request_bytes,
            finish_reason="stop",
            raw={"fake": True},
        )

    def _assert_budget(
        self,
        request: ProviderRequest,
        request_bytes: int,
        input_tokens: int,
    ) -> None:
        # Every outgoing request: measured serialized bytes and token budget.
        if request_bytes > request.budget.max_request_bytes:
            raise PayloadTooLarge(
                f"serialized request {request_bytes}b exceeds budget "
                f"{request.budget.max_request_bytes}b",
                serialized_bytes=request_bytes,
                input_tokens=input_tokens,
            )
        if input_tokens > request.budget.max_input_tokens:
            raise PayloadTooLarge(
                f"input tokens {input_tokens} exceed budget "
                f"{request.budget.max_input_tokens}",
                serialized_bytes=request_bytes,
                input_tokens=input_tokens,
            )
        if request_bytes > self._limits.max_request_bytes:
            raise PayloadTooLarge(
                f"serialized request {request_bytes}b exceeds provider cap "
                f"{self._limits.max_request_bytes}b",
                serialized_bytes=request_bytes,
                input_tokens=input_tokens,
            )
        if input_tokens > self._limits.max_input_tokens:
            raise ContextLengthExceeded(
                f"input tokens {input_tokens} exceed provider cap "
                f"{self._limits.max_input_tokens}",
                serialized_bytes=request_bytes,
                input_tokens=input_tokens,
            )

    def queue_json(self, payload: dict[str, Any] | list[Any] | str) -> None:
        """Append a successful scripted JSON response."""
        if isinstance(payload, str):
            text = payload
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError:
                parsed = None
        else:
            text = json.dumps(payload, ensure_ascii=False)
            parsed = payload if isinstance(payload, dict) else {"items": payload}
        self._scripted.append(
            ProviderResult(
                text=text,
                parsed=parsed if isinstance(parsed, dict) else None,
                input_tokens=0,
                output_tokens=estimate_tokens(text),
                request_bytes=0,
            )
        )

    def queue_error(self, exc: Exception) -> None:
        self._scripted.append(exc)


def default_fake_limits(
    *,
    context_tokens: int = 8192,
    reserved_output_tokens: int = 1024,
    max_request_bytes: int = 200_000,
    safety_margin_tokens: int = 64,
    framing_tokens: int = 256,
    model: str = "fake-extract-v1",
) -> ProviderLimits:
    max_input = max(1, context_tokens - reserved_output_tokens - safety_margin_tokens)
    return ProviderLimits(
        provider="fake",
        model=model,
        context_tokens=context_tokens,
        reserved_output_tokens=reserved_output_tokens,
        max_input_tokens=max_input,
        max_request_bytes=max_request_bytes,
        safety_margin_tokens=safety_margin_tokens,
        framing_tokens=framing_tokens,
        enabled=True,
    )
