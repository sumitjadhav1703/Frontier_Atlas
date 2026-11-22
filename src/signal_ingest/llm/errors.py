"""Typed provider failures for quota governance and the fallback chain."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from signal_ingest.llm.provider import ProviderError


class MissingCredentialsError(ProviderError):
    """API key / credential absent; tier is disabled for this process."""

    def __init__(self, message: str, *, provider: str) -> None:
        super().__init__(message, status_code=None)
        self.provider = provider


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
