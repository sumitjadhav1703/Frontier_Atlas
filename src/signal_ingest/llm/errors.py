"""Typed provider failures for B5 fallback / quota governance."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from signal_ingest.llm.provider import ProviderError


class MissingCredentialsError(ProviderError):
    """API key / credential absent; tier is disabled for this process."""

    def __init__(self, message: str, *, provider: str) -> None:
        super().__init__(message, status_code=None)
        self.provider = provider


class ProviderDisabledError(ProviderError):
    """Config enabled=false, billing-unknown plan, or route disabled."""

    def __init__(self, message: str, *, provider: str) -> None:
        super().__init__(message, status_code=None)
        self.provider = provider


class AuthError(ProviderError):
    """Permanent credential/config denial (401 or non-quota 403)."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 401,
        provider: str | None = None,
        permanent: bool = True,
    ) -> None:
        super().__init__(message, status_code=status_code)
        self.provider = provider
        self.permanent = permanent


class QuotaForbiddenError(ProviderError):
    """Quota-class 403 (distinct from credential 403)."""

    def __init__(
        self,
        message: str,
        *,
        provider: str | None = None,
        retry_after_s: float | None = None,
        reset_at: datetime | None = None,
    ) -> None:
        super().__init__(message, status_code=403)
        self.provider = provider
        self.retry_after_s = retry_after_s
        self.reset_at = reset_at


class RateLimitedError(ProviderError):
    """HTTP 429 with optional Retry-After / reset window."""

    def __init__(
        self,
        message: str,
        *,
        provider: str | None = None,
        retry_after_s: float | None = None,
        reset_at: datetime | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message, status_code=429)
        self.provider = provider
        self.retry_after_s = retry_after_s
        self.reset_at = reset_at
        self.headers = dict(headers or {})


class TransientProviderError(ProviderError):
    """Retryable 5xx, timeout, or transport blip."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = 503,
        provider: str | None = None,
        retryable: bool = True,
    ) -> None:
        super().__init__(message, status_code=status_code)
        self.provider = provider
        self.retryable = retryable


class InvalidOutputError(ProviderError):
    """Model returned unparseable / schema-invalid output."""

    def __init__(
        self,
        message: str,
        *,
        provider: str | None = None,
        raw_text: str | None = None,
        detail: Any = None,
    ) -> None:
        super().__init__(message, status_code=None)
        self.provider = provider
        self.raw_text = raw_text
        self.detail = detail


class CircuitOpenError(ProviderError):
    """Provider circuit is open; probe not yet allowed."""

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        open_until: datetime | None = None,
    ) -> None:
        super().__init__(message, status_code=None)
        self.provider = provider
        self.open_until = open_until


class QuotaExhaustedError(ProviderError):
    """Local ledger denied reservation (requests/tokens/paid cap)."""

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        reason: str = "exhausted",
    ) -> None:
        super().__init__(message, status_code=None)
        self.provider = provider
        self.reason = reason


class PaidSpendBlockedError(QuotaExhaustedError):
    """paid_spend_cap=0 refused a billing-enabled / paid fallback path."""

    def __init__(self, message: str, *, provider: str) -> None:
        super().__init__(message, provider=provider, reason="paid_spend_cap")


class AllProvidersFailedError(ProviderError):
    """Every eligible provider exhausted, abstained, or was skipped."""

    def __init__(
        self,
        message: str,
        *,
        attempts: list[dict[str, Any]] | None = None,
        next_attempt_at: datetime | None = None,
    ) -> None:
        super().__init__(message, status_code=None)
        self.attempts = list(attempts or [])
        self.next_attempt_at = next_attempt_at


class LiveQualificationError(ProviderError):
    """Raised when Fake/replay is presented as a live provider."""

    def __init__(self, message: str, *, provider: str) -> None:
        super().__init__(message, status_code=None)
        self.provider = provider
