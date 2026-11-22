"""Per-provider circuit breaker with one half-open probe."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from signal_ingest.config.clock import Clock, SystemClock
from signal_ingest.llm.errors import CircuitOpenError


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Durable-in-process circuit: closed → open → single half-open probe.

    Inject ``clock`` so tests advance time without sleeping.
    """

    provider: str
    failure_threshold: int = 3
    cooldown_s: float = 30.0
    clock: Clock = field(default_factory=SystemClock)
    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    open_until: datetime | None = None
    half_open_inflight: bool = False
    total_failures: int = 0
    total_successes: int = 0

    def allow(self) -> bool:
        """Return True if a call may proceed; raises CircuitOpenError if blocked."""
        now = self.clock.now()
        if self.state is CircuitState.CLOSED:
            return True
        if self.state is CircuitState.OPEN:
            if self.open_until is not None and now >= self.open_until:
                self.state = CircuitState.HALF_OPEN
                self.half_open_inflight = False
            else:
                raise CircuitOpenError(
                    f"circuit open for {self.provider}",
                    provider=self.provider,
                    open_until=self.open_until,
                )
        if self.state is CircuitState.HALF_OPEN:
            if self.half_open_inflight:
                raise CircuitOpenError(
                    f"circuit half-open probe already in flight for {self.provider}",
                    provider=self.provider,
                    open_until=self.open_until,
                )
            self.half_open_inflight = True
            return True
        return True

    def record_success(self) -> None:
        self.total_successes += 1
        self.consecutive_failures = 0
        self.half_open_inflight = False
        self.state = CircuitState.CLOSED
        self.open_until = None

    def record_failure(self, *, open_for_s: float | None = None) -> None:
        self.total_failures += 1
        self.consecutive_failures += 1
        self.half_open_inflight = False
        cooldown = self.cooldown_s if open_for_s is None else max(self.cooldown_s, open_for_s)
        if self.state is CircuitState.HALF_OPEN:
            self._trip(cooldown)
            return
        if self.consecutive_failures >= self.failure_threshold:
            self._trip(cooldown)

    def release_probe(self) -> None:
        """End a half-open probe that said nothing about provider health
        (bad output, oversize, cancel, missing key): the next call may probe again."""
        self.half_open_inflight = False

    def force_open(self, *, open_for_s: float | None = None) -> None:
        cooldown = self.cooldown_s if open_for_s is None else open_for_s
        self._trip(cooldown)

    def reset(self) -> None:
        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0
        self.open_until = None
        self.half_open_inflight = False

    def _trip(self, cooldown_s: float) -> None:
        self.state = CircuitState.OPEN
        self.open_until = self.clock.now() + timedelta(seconds=max(0.0, cooldown_s))
        self.half_open_inflight = False


@dataclass
class CircuitBreakerRegistry:
    """Named breakers shared across orchestrator workers in-process."""

    clock: Clock = field(default_factory=SystemClock)
    failure_threshold: int = 3
    cooldown_s: float = 30.0
    _breakers: dict[str, CircuitBreaker] = field(default_factory=dict)

    def get(self, provider: str) -> CircuitBreaker:
        if provider not in self._breakers:
            self._breakers[provider] = CircuitBreaker(
                provider=provider,
                failure_threshold=self.failure_threshold,
                cooldown_s=self.cooldown_s,
                clock=self.clock,
            )
        return self._breakers[provider]

    def all_open(self) -> bool:
        if not self._breakers:
            return False
        return all(b.state is CircuitState.OPEN for b in self._breakers.values())
