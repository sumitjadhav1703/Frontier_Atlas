"""Local Ollama adapter — HTTP mockable; loopback only in production defaults."""

from __future__ import annotations

import json
from typing import Any

from signal_ingest.config.clock import Clock
from signal_ingest.llm.adapters.base import check_model_output, loads_model_json, HttpProviderBase
from signal_ingest.llm.errors import InvalidOutputError
from signal_ingest.llm.http_transport import HttpResponse, HttpTransport
from signal_ingest.llm.provider import ProviderLimits, ProviderRequest, ProviderResult
from signal_ingest.llm.tokens import estimate_tokens

DEFAULT_OLLAMA_BASE = "http://127.0.0.1:11434"


class OllamaProvider(HttpProviderBase):
    """Ollama /api/chat over injectable HttpTransport (local third tier)."""

    provider_name = "ollama"

    def __init__(
        self,
        limits: ProviderLimits,
        *,
        transport: HttpTransport,
        api_key: str | None = None,
        clock: Clock | None = None,
        base_url: str = DEFAULT_OLLAMA_BASE,
        enforce_budget: bool = True,
    ) -> None:
        super().__init__(
            limits,
            transport=transport,
            api_key=api_key,
            clock=clock,
            enforce_budget=enforce_budget,
            require_api_key=False,
        )
        self.base_url = base_url.rstrip("/")

    def build_http_request(
        self, request: ProviderRequest
    ) -> tuple[str, str, dict[str, str], dict[str, Any]]:
        model = request.model or self._limits.model
        url = f"{self.base_url}/api/chat"
        headers = {"Content-Type": "application/json"}
        body: dict[str, Any] = {
            "model": model,
            "messages": list(request.messages),
            "stream": False,
            # Without num_ctx Ollama uses its small default and silently truncates.
            "options": {"temperature": 0, "num_ctx": self._limits.context_tokens},
        }
        if request.response_schema:
            body["format"] = request.response_schema
        else:
            body["format"] = "json"
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
        message = payload.get("message") or {}
        text = str(message.get("content") or "").strip()
        if not text:
            raise InvalidOutputError(
                "ollama empty content",
                provider="ollama",
                raw_text=response.text,
            )
        parsed = loads_model_json(text)
        finish_reason = str(payload.get("done_reason") or ("stop" if payload.get("done", True) else "length"))
        check_model_output(
            "ollama", parsed=parsed, finish_reason=finish_reason,
            request=request, raw_text=response.text,
        )
        out_tokens = int(payload.get("eval_count") or estimate_tokens(text))
        in_tokens = int(payload.get("prompt_eval_count") or measured_input)
        return ProviderResult(
            text=text,
            parsed=parsed,
            input_tokens=in_tokens,
            output_tokens=out_tokens,
            request_bytes=measured_bytes,
            finish_reason=finish_reason,
            raw={"provider": "ollama", "done": payload.get("done")},
        )
