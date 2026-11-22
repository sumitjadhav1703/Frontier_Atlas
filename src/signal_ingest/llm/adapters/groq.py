"""Groq OpenAI-compatible chat adapter — HTTP mockable; no live calls in B5."""

from __future__ import annotations

import json
from typing import Any

from signal_ingest.config.clock import Clock
from signal_ingest.llm.adapters.base import check_model_output, loads_model_json, HttpProviderBase
from signal_ingest.llm.errors import InvalidOutputError
from signal_ingest.llm.http_transport import HttpResponse, HttpTransport
from signal_ingest.llm.provider import ProviderLimits, ProviderRequest, ProviderResult
from signal_ingest.llm.tokens import estimate_tokens

DEFAULT_GROQ_BASE = "https://api.groq.com/openai/v1"


class GroqProvider(HttpProviderBase):
    """Groq /chat/completions over injectable HttpTransport."""

    provider_name = "groq"

    def __init__(
        self,
        limits: ProviderLimits,
        *,
        transport: HttpTransport,
        api_key: str | None = None,
        clock: Clock | None = None,
        base_url: str = DEFAULT_GROQ_BASE,
        enforce_budget: bool = True,
    ) -> None:
        super().__init__(
            limits,
            transport=transport,
            api_key=api_key,
            clock=clock,
            enforce_budget=enforce_budget,
            require_api_key=True,
        )
        self.base_url = base_url.rstrip("/")

    def build_http_request(
        self, request: ProviderRequest
    ) -> tuple[str, str, dict[str, str], dict[str, Any]]:
        model = request.model or self._limits.model
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
        }
        body: dict[str, Any] = {
            "model": model,
            "messages": list(request.messages),
            "temperature": 0,
        }
        if request.response_schema:
            body["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "extraction",
                    "schema": request.response_schema,
                },
            }
        else:
            body["response_format"] = {"type": "json_object"}
        return "POST", url, headers, body

    def parse_http_response(
        self,
        response: HttpResponse,
        *,
        request: ProviderRequest,
        measured_bytes: int,
        measured_input: int,
    ) -> ProviderResult:
        payload = json.loads(response.text or "{}")
        choices = payload.get("choices") or []
        if not choices:
            raise InvalidOutputError(
                "groq response missing choices",
                provider="groq",
                raw_text=response.text,
            )
        message = (choices[0] or {}).get("message") or {}
        text = str(message.get("content") or "").strip()
        if not text:
            raise InvalidOutputError(
                "groq empty content",
                provider="groq",
                raw_text=response.text,
            )
        parsed = loads_model_json(text)
        finish_reason = str((choices[0] or {}).get("finish_reason") or "stop")
        check_model_output(
            "groq", parsed=parsed, finish_reason=finish_reason,
            request=request, raw_text=response.text,
        )
        usage = payload.get("usage") or {}
        out_tokens = int(usage.get("completion_tokens") or estimate_tokens(text))
        in_tokens = int(usage.get("prompt_tokens") or measured_input)
        return ProviderResult(
            text=text,
            parsed=parsed,
            input_tokens=in_tokens,
            output_tokens=out_tokens,
            request_bytes=measured_bytes,
            finish_reason=finish_reason,
            raw={"provider": "groq", "usage": usage},
        )
