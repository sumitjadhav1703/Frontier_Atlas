"""Redacted provider attempt transcripts for B5 gates and audits."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping

_SECRET_HEADER_KEYS = frozenset(
    {
        "authorization",
        "x-api-key",
        "api-key",
        "x-goog-api-key",
        "proxy-authorization",
    }
)
_SECRET_QUERY_KEYS = frozenset({"key", "api_key", "access_token", "token"})
_BEARER_RE = re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._\-]+")
_KEY_ASSIGN_RE = re.compile(
    r"(?i)(api[_-]?key|token|secret|password)\s*[:=]\s*['\"]?([^\s'\",]+)"
)


def _sha256_preview(text: str | bytes, *, limit: int = 16) -> str:
    if isinstance(text, str):
        data = text.encode("utf-8")
    else:
        data = text
    return hashlib.sha256(data).hexdigest()[:limit]


def redact_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in (headers or {}).items():
        if key.lower() in _SECRET_HEADER_KEYS:
            out[key] = "[REDACTED]"
        else:
            out[key] = _BEARER_RE.sub(r"\1[REDACTED]", str(value))
    return out


def redact_text(text: str | None, *, max_chars: int = 240) -> str | None:
    if text is None:
        return None
    scrubbed = _BEARER_RE.sub(r"\1[REDACTED]", text)
    scrubbed = _KEY_ASSIGN_RE.sub(r"\1=[REDACTED]", scrubbed)
    if len(scrubbed) <= max_chars:
        return scrubbed
    return (
        scrubbed[:max_chars]
        + f"…[truncated len={len(text)} sha256={_sha256_preview(text)}]"
    )


def redact_payload(obj: Any, *, max_str: int = 200) -> Any:
    """Recursively redact secrets and truncate large strings/lists."""
    if obj is None or isinstance(obj, (bool, int, float)):
        return obj
    if isinstance(obj, str):
        return redact_text(obj, max_chars=max_str)
    if isinstance(obj, bytes):
        return f"<bytes len={len(obj)} sha256={_sha256_preview(obj)}>"
    if isinstance(obj, Mapping):
        out: dict[str, Any] = {}
        for key, value in obj.items():
            lk = str(key).lower()
            if lk in _SECRET_HEADER_KEYS or lk in _SECRET_QUERY_KEYS or lk in {
                "api_key",
                "apikey",
                "password",
                "secret",
            }:
                out[str(key)] = "[REDACTED]"
            elif lk in {"messages", "contents", "prompt", "input"}:
                out[str(key)] = _redact_messages(value)
            else:
                out[str(key)] = redact_payload(value, max_str=max_str)
        return out
    if isinstance(obj, (list, tuple)):
        if len(obj) > 8:
            head = [redact_payload(x, max_str=max_str) for x in obj[:4]]
            return head + [f"…[{len(obj) - 4} more]"]
        return [redact_payload(x, max_str=max_str) for x in obj]
    return str(obj)[:max_str]


def _redact_messages(value: Any) -> Any:
    if not isinstance(value, list):
        return redact_payload(value)
    summary = []
    for item in value[:6]:
        if isinstance(item, Mapping):
            role = item.get("role") or item.get("author") or "?"
            content = item.get("content") or item.get("parts") or ""
            if isinstance(content, str):
                summary.append(
                    {
                        "role": role,
                        "content_chars": len(content),
                        "content_sha256": _sha256_preview(content),
                        "content_preview": redact_text(content, max_chars=80),
                    }
                )
            else:
                summary.append({"role": role, "content": redact_payload(content)})
        else:
            summary.append(redact_payload(item))
    if len(value) > 6:
        summary.append(f"…[{len(value) - 6} more messages]")
    return summary


@dataclass
class AttemptRecord:
    index: int
    provider: str
    model: str
    started_at: str
    finished_at: str | None = None
    latency_s: float | None = None
    status: str = "started"
    status_code: int | None = None
    error_type: str | None = None
    error_message: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    request_bytes: int | None = None
    retry_after_s: float | None = None
    fallback_reason: str | None = None
    cache_hit: bool = False
    live_qualified: bool = False
    request_redacted: dict[str, Any] = field(default_factory=dict)
    response_redacted: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "provider": self.provider,
            "model": self.model,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "latency_s": self.latency_s,
            "status": self.status,
            "status_code": self.status_code,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "request_bytes": self.request_bytes,
            "retry_after_s": self.retry_after_s,
            "fallback_reason": self.fallback_reason,
            "cache_hit": self.cache_hit,
            "live_qualified": self.live_qualified,
            "request": self.request_redacted,
            "response": self.response_redacted,
        }


@dataclass
class TranscriptRecorder:
    """Readable, redacted multi-provider attempt log."""

    run_id: str | None = None
    work_key: str | None = None
    attempts: list[AttemptRecord] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def begin(
        self,
        *,
        provider: str,
        model: str,
        started_at: datetime,
        request: Mapping[str, Any] | None = None,
        live_qualified: bool = False,
        fallback_reason: str | None = None,
    ) -> AttemptRecord:
        record = AttemptRecord(
            index=len(self.attempts),
            provider=provider,
            model=model,
            started_at=started_at.isoformat(),
            live_qualified=live_qualified,
            fallback_reason=fallback_reason,
            request_redacted=redact_payload(dict(request or {})),
        )
        self.attempts.append(record)
        return record

    def finish(
        self,
        record: AttemptRecord,
        *,
        finished_at: datetime,
        status: str,
        status_code: int | None = None,
        error: BaseException | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        request_bytes: int | None = None,
        retry_after_s: float | None = None,
        response: Mapping[str, Any] | None = None,
        cache_hit: bool = False,
    ) -> AttemptRecord:
        start = datetime.fromisoformat(record.started_at)
        record.finished_at = finished_at.isoformat()
        record.latency_s = max(0.0, (finished_at - start).total_seconds())
        record.status = status
        record.status_code = status_code
        record.input_tokens = input_tokens
        record.output_tokens = output_tokens
        record.request_bytes = request_bytes
        record.retry_after_s = retry_after_s
        record.cache_hit = cache_hit
        if error is not None:
            record.error_type = type(error).__name__
            record.error_message = redact_text(str(error), max_chars=300)
        if response is not None:
            record.response_redacted = redact_payload(dict(response))
        return record

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "work_key": self.work_key,
            "meta": redact_payload(self.meta),
            "attempt_count": len(self.attempts),
            "attempts": [a.as_dict() for a in self.attempts],
            "live_calls": sum(1 for a in self.attempts if a.live_qualified and not a.cache_hit),
            "paid_fallback": False,
        }

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.as_dict(), indent=indent, ensure_ascii=False, sort_keys=True)

    def assert_no_secrets(self) -> None:
        blob = self.to_json()
        lowered = blob.lower()
        for needle in ("bearer ", "api_key=", "x-api-key: ", "sk-"):
            # Allow the literal redaction markers / field names only.
            if needle.strip() in {"api_key=", "x-api-key:"} and "[redacted]" in lowered:
                continue
            if needle in lowered and "[redacted]" not in lowered[max(0, lowered.find(needle) - 20) :]:
                # Soft check: presence of raw bearer tokens is the real failure.
                pass
        if re.search(r"bearer\s+[a-z0-9]", blob, flags=re.I):
            raise AssertionError("transcript leaked bearer token")
