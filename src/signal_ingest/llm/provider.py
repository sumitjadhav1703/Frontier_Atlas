"""Async Provider Protocol shared by FakeProvider (B4) and live adapters (B5)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field


class _Exact(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class ProviderLimits(_Exact):
    """Capability envelope for one provider/model route."""

    provider: str
    model: str
    context_tokens: int = Field(gt=0)
    reserved_output_tokens: int = Field(gt=0)
    max_input_tokens: int = Field(gt=0)
    max_request_bytes: int = Field(gt=0)
    safety_margin_tokens: int = Field(default=64, ge=0)
    # Fixed framing counted once per request (instructions + schema + tool overhead).
    framing_tokens: int = Field(default=0, ge=0)
    enabled: bool = True


class RequestBudget(_Exact):
    """Per-call caps asserted by FakeProvider on every outgoing request."""

    max_input_tokens: int = Field(gt=0)
    max_output_tokens: int = Field(gt=0)
    max_request_bytes: int = Field(gt=0)
    framing_tokens: int = Field(default=0, ge=0)


class ProviderRequest(_Exact):
    messages: list[dict[str, Any]]
    response_schema: dict[str, Any] | None = None
    budget: RequestBudget
    provider: str
    model: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProviderResult(_Exact):
    text: str
    parsed: dict[str, Any] | None = None
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    request_bytes: int = Field(ge=0)
    finish_reason: str = "stop"
    raw: dict[str, Any] = Field(default_factory=dict)


class ProviderError(Exception):
    """Base provider failure."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class PayloadTooLarge(ProviderError):
    """HTTP 413 / context-length overflow."""

    def __init__(
        self,
        message: str,
        *,
        serialized_bytes: int,
        input_tokens: int,
        unchanged: bool = False,
    ) -> None:
        super().__init__(message, status_code=413)
        self.serialized_bytes = serialized_bytes
        self.input_tokens = input_tokens
        self.unchanged = unchanged


class ContextLengthExceeded(ProviderError):
    """HTTP 400 context-length style failure."""

    def __init__(
        self,
        message: str,
        *,
        serialized_bytes: int,
        input_tokens: int,
    ) -> None:
        super().__init__(message, status_code=400)
        self.serialized_bytes = serialized_bytes
        self.input_tokens = input_tokens


class UnchangedOversizedRetry(ProviderError):
    """Gate failure: retried the same oversized serialized payload without shrinking."""

    def __init__(self, message: str, *, serialized_bytes: int) -> None:
        super().__init__(message, status_code=413)
        self.serialized_bytes = serialized_bytes


@runtime_checkable
class Provider(Protocol):
    """Async completion interface — FakeProvider and future live adapters."""

    @property
    def limits(self) -> ProviderLimits: ...

    async def complete(self, request: ProviderRequest) -> ProviderResult: ...


@dataclass
class MeasuredRequest:
    body: dict[str, Any]
    serialized: bytes
    request_bytes: int
    input_tokens: int


def measure_provider_request(request: ProviderRequest) -> MeasuredRequest:
    from signal_ingest.llm.serialize import serialize_json
    from signal_ingest.llm.tokens import estimate_tokens

    body = {
        "provider": request.provider,
        "model": request.model,
        "messages": request.messages,
        "response_schema": request.response_schema,
        "budget": request.budget.model_dump(mode="json"),
        "metadata": request.metadata,
    }
    serialized = serialize_json(body)
    return MeasuredRequest(
        body=body,
        serialized=serialized,
        request_bytes=len(serialized),
        input_tokens=estimate_tokens(serialized),
    )
