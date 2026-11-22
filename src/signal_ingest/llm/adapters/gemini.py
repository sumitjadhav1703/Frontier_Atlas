"""Gemini provider adapter — HTTP mockable; no live calls in B5."""

from __future__ import annotations

import json
from typing import Any

from signal_ingest.config.clock import Clock
from signal_ingest.llm.adapters.base import check_model_output, loads_model_json, HttpProviderBase
from signal_ingest.llm.errors import InvalidOutputError
from signal_ingest.llm.http_transport import HttpResponse, HttpTransport
from signal_ingest.llm.provider import ProviderLimits, ProviderRequest, ProviderResult
from signal_ingest.llm.tokens import estimate_tokens

DEFAULT_GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta"


class GeminiProvider(HttpProviderBase):
    """Gemini generateContent over injectable HttpTransport."""

    provider_name = "gemini"

    def __init__(
        self,
        limits: ProviderLimits,
        *,
        transport: HttpTransport,
        api_key: str | None = None,
        clock: Clock | None = None,
        base_url: str = DEFAULT_GEMINI_BASE,
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
        # Key goes in query string; ScriptedHttpTransport redacts it in call logs.
        url = f"{self.base_url}/models/{model}:generateContent"
        headers = {"Content-Type": "application/json"}
        contents = []
        system_parts: list[str] = []
        for message in request.messages:
            role = str(message.get("role") or "user")
            text = str(message.get("content") or "")
            if role == "system":
                system_parts.append(text)
                continue
            gemini_role = "model" if role == "assistant" else "user"
            contents.append({"role": gemini_role, "parts": [{"text": text}]})
        body: dict[str, Any] = {"contents": contents}
        if system_parts:
            body["systemInstruction"] = {"parts": [{"text": "\n".join(system_parts)}]}
        if request.response_schema:
            body["generationConfig"] = {
                "responseMimeType": "application/json",
                "responseSchema": request.response_schema,
            }
        # Attach key as header alternative for mock flexibility; adapters also
        # support ?key= via transport params when using live HttpxTransport later.
        if self._api_key:
            headers["x-goog-api-key"] = self._api_key
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
        candidates = payload.get("candidates") or []
        if not candidates:
            raise InvalidOutputError(
                "gemini response missing candidates",
                provider="gemini",
                raw_text=response.text,
            )
        parts = ((candidates[0] or {}).get("content") or {}).get("parts") or []
        texts = [str(p.get("text") or "") for p in parts if isinstance(p, dict)]
        text = "".join(texts).strip()
        if not text:
            raise InvalidOutputError(
                "gemini empty text",
                provider="gemini",
                raw_text=response.text,
            )
        parsed: dict[str, Any] | None
        parsed = loads_model_json(text)
        finish_reason = str((candidates[0] or {}).get("finishReason") or "stop")
        check_model_output(
            "gemini", parsed=parsed, finish_reason=finish_reason,
            request=request, raw_text=response.text,
        )
        usage = payload.get("usageMetadata") or {}
        out_tokens = int(usage.get("candidatesTokenCount") or estimate_tokens(text))
        in_tokens = int(usage.get("promptTokenCount") or measured_input)
        return ProviderResult(
            text=text,
            parsed=parsed,
            input_tokens=in_tokens,
            output_tokens=out_tokens,
            request_bytes=measured_bytes,
            finish_reason=finish_reason,
            raw={"provider": "gemini", "usage": usage},
        )
