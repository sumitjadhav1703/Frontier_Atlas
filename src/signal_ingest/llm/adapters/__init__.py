"""HTTP-backed provider adapters (Gemini → Groq → Ollama)."""

from signal_ingest.llm.adapters.gemini import GeminiProvider
from signal_ingest.llm.adapters.groq import GroqProvider
from signal_ingest.llm.adapters.ollama import OllamaProvider
from signal_ingest.llm.adapters.base import (
    HttpProviderBase,
    classify_http_error,
    is_live_provider,
    provider_from_config,
)

__all__ = [
    "GeminiProvider",
    "GroqProvider",
    "HttpProviderBase",
    "OllamaProvider",
    "classify_http_error",
    "is_live_provider",
    "provider_from_config",
]
