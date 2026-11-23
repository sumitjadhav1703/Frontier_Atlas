"""Quota ledger: reserve-before-dispatch with paid_spend_cap=0 default."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from signal_ingest.config.clock import Clock, SystemClock
from signal_ingest.llm.errors import PaidSpendBlockedError, QuotaExhaustedError


def _aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class QuotaReservation:
    reservation_id: str
    provider: str
    requests: int
    input_tokens: int
    output_tokens: int
    estimated_paid_micros: int
    created_at: datetime
    day_window: datetime | None = None  # which day bucket holds this reservation


@dataclass
class ProviderQuotaConfig:
    """Per-provider caps. billing_enabled unknown → disabled when paid_spend_cap=0."""

    provider: str
    requests_per_day: int = 10_000
    tokens_per_day: int = 10_000_000
    requests_per_run: int | None = None
    tokens_per_run: int | None = None
    billing_enabled: bool | None = False  # None = unknown → treat as paid risk
    is_local: bool = False  # Ollama / local: zero paid cost
    is_fake: bool = False


@dataclass
class _Bucket:
    requests_limit: int
    tokens_limit: int
    requests_reserved: int = 0
    requests_used: int = 0
    tokens_reserved: int = 0
    tokens_used: int = 0
    window_start: datetime | None = None


@dataclass
class QuotaLedger:
    """In-memory shared ledger. Reserve before every dispatch (incl. retries/probes).

    ``paid_spend_cap`` defaults to 0: any positive estimated paid spend is denied.
    Concurrent callers serialize on an asyncio.Lock so one remaining slot cannot
    be double-reserved.
    """

    paid_spend_cap: int = 0  # whole currency units; 0 = hard zero-spend
    paid_spend_used: int = 0
    clock: Clock = field(default_factory=SystemClock)
    providers: dict[str, ProviderQuotaConfig] = field(default_factory=dict)
    day_buckets: dict[str, _Bucket] = field(default_factory=dict)
    run_buckets: dict[str, _Bucket] = field(default_factory=dict)
    open_reservations: dict[str, QuotaReservation] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def configure(self, cfg: ProviderQuotaConfig) -> None:
        self.providers[cfg.provider] = cfg

    def _day_key(self, now: datetime) -> datetime:
        now = _aware(now)
        return datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

    def _ensure_day(self, provider: str, now: datetime) -> _Bucket:
        cfg = self.providers.get(provider) or ProviderQuotaConfig(provider=provider)
        key = self._day_key(now)
        bucket = self.day_buckets.get(provider)
        if bucket is None or bucket.window_start != key:
            bucket = _Bucket(
                requests_limit=cfg.requests_per_day,
                tokens_limit=cfg.tokens_per_day,
                window_start=key,
            )
            self.day_buckets[provider] = bucket
        return bucket

    def _ensure_run(self, provider: str) -> _Bucket | None:
        cfg = self.providers.get(provider) or ProviderQuotaConfig(provider=provider)
        if cfg.requests_per_run is None and cfg.tokens_per_run is None:
            return None
        bucket = self.run_buckets.get(provider)
        if bucket is None:
            bucket = _Bucket(
                requests_limit=cfg.requests_per_run
                if cfg.requests_per_run is not None
                else 10**9,
                tokens_limit=cfg.tokens_per_run
                if cfg.tokens_per_run is not None
                else 10**12,
            )
            self.run_buckets[provider] = bucket
        return bucket

    def _available_requests(self, bucket: _Bucket) -> int:
        return bucket.requests_limit - bucket.requests_reserved - bucket.requests_used

    def _available_tokens(self, bucket: _Bucket) -> int:
        return bucket.tokens_limit - bucket.tokens_reserved - bucket.tokens_used

    async def reserve(
        self,
        provider: str,
        *,
        requests: int = 1,
        input_tokens: int = 0,
        output_tokens: int = 0,
        estimated_paid_micros: int = 0,
    ) -> QuotaReservation:
        """Atomically reserve capacity. Raises QuotaExhaustedError / PaidSpendBlockedError."""
        if requests < 0 or input_tokens < 0 or output_tokens < 0:
            raise ValueError("reservation amounts must be >= 0")
        async with self._lock:
            return self._reserve_locked(
                provider,
                requests=requests,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                estimated_paid_micros=estimated_paid_micros,
            )

    def reserve_sync(
        self,
        provider: str,
        *,
        requests: int = 1,
        input_tokens: int = 0,
        output_tokens: int = 0,
        estimated_paid_micros: int = 0,
    ) -> QuotaReservation:
        """Synchronous reserve for non-async callers / tests."""
        return self._reserve_locked(
            provider,
            requests=requests,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            estimated_paid_micros=estimated_paid_micros,
        )

    def _reserve_locked(
        self,
        provider: str,
        *,
        requests: int,
        input_tokens: int,
        output_tokens: int,
        estimated_paid_micros: int,
    ) -> QuotaReservation:
        cfg = self.providers.get(provider) or ProviderQuotaConfig(provider=provider)
        now = self.clock.now()

        if cfg.is_fake:
            # Fake never consumes live quota, but still tracks for observability.
            pass

        # Zero-spend policy: unknown billing plans disabled; any paid micros denied
        # when paid_spend_cap == 0. Known-free and local/fake routes may proceed.
        paid_units = max(0, estimated_paid_micros)
        if not cfg.is_local and not cfg.is_fake:
            if cfg.billing_enabled is None:
                raise PaidSpendBlockedError(
                    f"billing-unknown plan disabled for {provider}",
                    provider=provider,
                )
            if cfg.billing_enabled and self.paid_spend_cap <= 0:
                # A billed account can charge even when our estimate says 0.
                raise PaidSpendBlockedError(
                    f"billing-enabled plan blocked for {provider} (paid_spend_cap=0)",
                    provider=provider,
                )
            if cfg.billing_enabled and paid_units <= 0:
                raise PaidSpendBlockedError(
                    f"billed route {provider} needs a cost estimate", provider=provider
                )
            if paid_units > 0:
                cap_micros = max(0, self.paid_spend_cap) * 1_000_000
                if self.paid_spend_used + paid_units > cap_micros:
                    raise PaidSpendBlockedError(
                        f"paid spend blocked for {provider} "
                        f"(paid_spend_cap={self.paid_spend_cap})",
                        provider=provider,
                    )

        tokens = input_tokens + output_tokens
        run = self._ensure_run(provider)
        if run is not None:
            if self._available_requests(run) < requests:
                raise QuotaExhaustedError(
                    f"run request quota exhausted for {provider}",
                    provider=provider,
                    reason="run_requests",
                )
            if tokens and self._available_tokens(run) < tokens:
                raise QuotaExhaustedError(
                    f"run token quota exhausted for {provider}",
                    provider=provider,
                    reason="run_tokens",
                )

        day_window = self._take_day(provider, now, requests=requests, tokens=tokens)
        if run is not None:
            run.requests_reserved += requests
            run.tokens_reserved += tokens

        reservation = QuotaReservation(
            reservation_id=str(uuid.uuid4()),
            provider=provider,
            requests=requests,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            estimated_paid_micros=paid_units,
            created_at=_aware(now),
            day_window=day_window,
        )
        self.open_reservations[reservation.reservation_id] = reservation
        return reservation

    # -- day bucket: in memory here, PostgreSQL in PgQuotaLedger -------------

    def _take_day(self, provider: str, now: datetime, *, requests: int, tokens: int) -> datetime:
        """Check and reserve today's capacity; return the window it was taken from."""
        day = self._ensure_day(provider, now)
        if self._available_requests(day) < requests:
            raise QuotaExhaustedError(
                f"daily request quota exhausted for {provider}",
                provider=provider,
                reason="daily_requests",
            )
        if tokens and self._available_tokens(day) < tokens:
            raise QuotaExhaustedError(
                f"daily token quota exhausted for {provider}",
                provider=provider,
                reason="daily_tokens",
            )
        day.requests_reserved += requests
        day.tokens_reserved += tokens
        assert day.window_start is not None
        return day.window_start

    def _settle_day(
        self,
        reservation: QuotaReservation,
        *,
        used_requests: int,
        used_tokens: int,
    ) -> None:
        """Drop the reservation from its day window and record actual use."""
        day = self.day_buckets.get(reservation.provider)
        if day is None or day.window_start != reservation.day_window:
            return  # window rolled over; the old day's counters are gone
        tokens = reservation.input_tokens + reservation.output_tokens
        day.requests_reserved = max(0, day.requests_reserved - reservation.requests)
        day.tokens_reserved = max(0, day.tokens_reserved - tokens)
        day.requests_used += used_requests
        day.tokens_used += used_tokens

    async def commit(
        self,
        reservation: QuotaReservation,
        *,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        paid_micros: int | None = None,
    ) -> None:
        async with self._lock:
            self._commit_locked(
                reservation,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                paid_micros=paid_micros,
            )

    def commit_sync(
        self,
        reservation: QuotaReservation,
        *,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        paid_micros: int | None = None,
    ) -> None:
        self._commit_locked(
            reservation,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            paid_micros=paid_micros,
        )

    def _commit_locked(
        self,
        reservation: QuotaReservation,
        *,
        input_tokens: int | None,
        output_tokens: int | None,
        paid_micros: int | None,
    ) -> None:
        if reservation.reservation_id not in self.open_reservations:
            return
        provider = reservation.provider
        run = self._ensure_run(provider)

        reserved_tokens = reservation.input_tokens + reservation.output_tokens
        used_in = reservation.input_tokens if input_tokens is None else input_tokens
        used_out = reservation.output_tokens if output_tokens is None else output_tokens
        used_tokens = used_in + used_out

        self._settle_day(reservation, used_requests=reservation.requests, used_tokens=used_tokens)
        del self.open_reservations[reservation.reservation_id]

        if run is not None:
            run.requests_reserved = max(0, run.requests_reserved - reservation.requests)
            run.tokens_reserved = max(0, run.tokens_reserved - reserved_tokens)
            run.requests_used += reservation.requests
            run.tokens_used += used_tokens

        paid = reservation.estimated_paid_micros if paid_micros is None else paid_micros
        if paid > 0:
            # Still record; paid_spend_cap=0 should have blocked reserve.
            self.paid_spend_used += paid

    async def release(self, reservation: QuotaReservation) -> None:
        async with self._lock:
            self._release_locked(reservation)

    def release_sync(self, reservation: QuotaReservation) -> None:
        self._release_locked(reservation)

    def _release_locked(self, reservation: QuotaReservation) -> None:
        if reservation.reservation_id not in self.open_reservations:
            return
        provider = reservation.provider
        run = self._ensure_run(provider)
        tokens = reservation.input_tokens + reservation.output_tokens
        self._settle_day(reservation, used_requests=0, used_tokens=0)
        del self.open_reservations[reservation.reservation_id]
        if run is not None:
            run.requests_reserved = max(0, run.requests_reserved - reservation.requests)
            run.tokens_reserved = max(0, run.tokens_reserved - tokens)

    def remaining_requests(self, provider: str) -> int:
        day = self._ensure_day(provider, self.clock.now())
        return self._available_requests(day)

    def snapshot(self) -> dict[str, Any]:
        return {
            "paid_spend_cap": self.paid_spend_cap,
            "paid_spend_used": self.paid_spend_used,
            "day": {
                p: {
                    "requests_reserved": b.requests_reserved,
                    "requests_used": b.requests_used,
                    "requests_limit": b.requests_limit,
                    "tokens_reserved": b.tokens_reserved,
                    "tokens_used": b.tokens_used,
                    "tokens_limit": b.tokens_limit,
                    "window_start": b.window_start.isoformat() if b.window_start else None,
                }
                for p, b in self.day_buckets.items()
            },
            "open_reservations": len(self.open_reservations),
        }


@dataclass
class PgQuotaLedger(QuotaLedger):
    """QuotaLedger whose daily windows live in PostgreSQL (provider_budgets).

    Every worker process shares one counter per provider/day, so adding
    processes cannot multiply a free-tier quota. Each reservation is one atomic
    UPDATE ... WHERE remaining >= n. Run windows stay per process.
    Windows are UTC days; providers that reset at another midnight get a
    conservative overlap, never an overrun.
    """

    connect: Any = None  # zero-arg callable returning a psycopg connection

    def _take_day(self, provider: str, now: datetime, *, requests: int, tokens: int) -> datetime:
        cfg = self.providers.get(provider) or ProviderQuotaConfig(provider=provider)
        window = self._day_key(now)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO provider_budgets
                    (provider, window_kind, window_start, requests_limit, tokens_limit)
                VALUES (%s, 'day', %s, %s, %s)
                ON CONFLICT (provider, window_kind, window_start) DO NOTHING
                """,
                (provider, window, cfg.requests_per_day, cfg.tokens_per_day),
            )
            row = conn.execute(
                """
                UPDATE provider_budgets
                SET requests_reserved = requests_reserved + %s,
                    tokens_reserved = tokens_reserved + %s,
                    updated_at = NOW()
                WHERE provider = %s AND window_kind = 'day' AND window_start = %s
                  AND requests_limit - requests_reserved - requests_used >= %s
                  AND (%s = 0 OR tokens_limit - tokens_reserved - tokens_used >= %s)
                RETURNING 1
                """,
                (requests, tokens, provider, window, requests, tokens, tokens),
            ).fetchone()
            if row is None:
                left = conn.execute(
                    """
                    SELECT requests_limit - requests_reserved - requests_used
                    FROM provider_budgets
                    WHERE provider = %s AND window_kind = 'day' AND window_start = %s
                    """,
                    (provider, window),
                ).fetchone()
                conn.rollback()
                short_requests = left is None or left[0] < requests
                raise QuotaExhaustedError(
                    f"daily {'request' if short_requests else 'token'} quota exhausted "
                    f"for {provider}",
                    provider=provider,
                    reason="daily_requests" if short_requests else "daily_tokens",
                )
            conn.commit()
        return window

    def _settle_day(
        self,
        reservation: QuotaReservation,
        *,
        used_requests: int,
        used_tokens: int,
    ) -> None:
        tokens = reservation.input_tokens + reservation.output_tokens
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE provider_budgets
                SET requests_reserved = GREATEST(0, requests_reserved - %s),
                    tokens_reserved = GREATEST(0, tokens_reserved - %s),
                    requests_used = requests_used + %s,
                    tokens_used = tokens_used + %s,
                    updated_at = NOW()
                WHERE provider = %s AND window_kind = 'day' AND window_start = %s
                """,
                (
                    reservation.requests,
                    tokens,
                    used_requests,
                    used_tokens,
                    reservation.provider,
                    reservation.day_window,
                ),
            )
            conn.commit()

    def remaining_requests(self, provider: str) -> int:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT requests_limit - requests_reserved - requests_used
                FROM provider_budgets
                WHERE provider = %s AND window_kind = 'day' AND window_start = %s
                """,
                (provider, self._day_key(self.clock.now())),
            ).fetchone()
        if row is None:
            cfg = self.providers.get(provider) or ProviderQuotaConfig(provider=provider)
            return cfg.requests_per_day
        return int(row[0])
