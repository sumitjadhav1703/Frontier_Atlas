"""Publication time intervals with provenance (never invent defaults)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class PublicationInterval:
    """
    Conservative bounds for a publication instant.

    Admit only when the entire closed interval lies inside the freshness window.
    """

    lower: datetime
    upper: datetime
    precision: str
    method: str
    raw: str | None = None
    tz: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.lower.tzinfo is None or self.upper.tzinfo is None:
            raise ValueError("PublicationInterval bounds must be timezone-aware")
        lower = self.lower.astimezone(timezone.utc)
        upper = self.upper.astimezone(timezone.utc)
        if lower > upper:
            raise ValueError("PublicationInterval lower must be <= upper")
        object.__setattr__(self, "lower", lower)
        object.__setattr__(self, "upper", upper)

    def to_dict(self) -> dict[str, Any]:
        return {
            "lower": self.lower.isoformat(),
            "upper": self.upper.isoformat(),
            "precision": self.precision,
            "method": self.method,
            "raw": self.raw,
            "tz": self.tz,
            "evidence": dict(self.evidence),
        }
