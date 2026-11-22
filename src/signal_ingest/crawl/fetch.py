"""Fetch a task URL under shared pool + policy; typed outcomes."""

from __future__ import annotations

import asyncio
import email.utils
import random
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from urllib.parse import urlsplit

import httpx

from signal_ingest.config.clock import get_clock
from signal_ingest.config.settings import Mode, Settings, get_settings
from signal_ingest.crawl.policy import (
    PolicyError,
    RobotsCache,
    UrlPolicy,
    extract_alternate_feed_hint,
    looks_like_challenge,
    resolved_addresses_unsafe,
)
from signal_ingest.crawl.pool import (
    BackpressureError,
    BodyTooLargeError,
    DecodingError,
    HttpPool,
    PoolLimits,
)
from signal_ingest.crawl.types import ClaimedTask, FetchOutcomeKind, RawObservation

ALLOWED_MIME_PREFIXES = (
    "text/",
    "application/json",
    "application/xml",
    "application/xhtml",
    "application/atom+xml",
    "application/rss+xml",
    "application/javascript",
    "application/ld+json",
    "application/octet-stream",
)


class FetchPolicy:
    """Combined URL/robots/MIME/retry policy for fetch()."""

    def __init__(
        self,
        *,
        mode: Mode = Mode.FIXTURE,
        url_policy: UrlPolicy | None = None,
        robots: RobotsCache | None = None,
        max_retries: int = 4,
        retry_base_s: float = 0.25,
        retry_cap_s: float = 8.0,
        respect_robots: bool = True,
        allowed_mime_prefixes: tuple[str, ...] = ALLOWED_MIME_PREFIXES,
        check_resolved_private: bool = True,
    ) -> None:
        self.mode = mode
        self.url_policy = url_policy or UrlPolicy(mode=mode)
        self.robots = robots or RobotsCache(user_agent=self.url_policy.user_agent)
        self.max_retries = max_retries
        self.retry_base_s = retry_base_s
        self.retry_cap_s = retry_cap_s
        self.respect_robots = respect_robots
        self.allowed_mime_prefixes = allowed_mime_prefixes
        self.check_resolved_private = check_resolved_private


def _aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


MAX_RETRY_AFTER_S = 3600.0
DB_RETRY_BASE_S = 5.0


def _equal_jitter(high: float) -> float:
    """Delay in [high/2, high]: grows with attempt yet still spreads retries."""
    return high / 2 + random.random() * high / 2


def _retry_delay(attempt: int, policy: FetchPolicy, retry_after: float | None) -> float:
    # Server Retry-After is honored (up to 1h); fetch_with_retries decides whether
    # to sleep in-process or hand the wait to the DB queue.
    if retry_after is not None and retry_after >= 0:
        return min(retry_after, MAX_RETRY_AFTER_S)
    return _equal_jitter(min(policy.retry_cap_s, policy.retry_base_s * (2**attempt)))


def db_retry_delay(attempt: int, *, retry_after: float | None = None) -> float:
    """Queue-level backoff for a task on its Nth claim (5s, 10s, 20s … capped 1h)."""
    if retry_after is not None and retry_after >= 0:
        return min(retry_after, MAX_RETRY_AFTER_S)
    return _equal_jitter(min(MAX_RETRY_AFTER_S, DB_RETRY_BASE_S * 2 ** max(attempt - 1, 0)))


def _parse_retry_after(value: str | None, now: datetime) -> float | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return max(0.0, (when - now).total_seconds())
    except (TypeError, ValueError, IndexError):
        return None


_ORIGIN_BOUND = frozenset({"authorization", "cookie", "proxy-authorization", "if-none-match"})


def _origin(url: str) -> tuple[str, str]:
    parts = urlsplit(url)
    return (parts.scheme.lower(), parts.netloc.lower())


def _mime_allowed(content_type: str | None, prefixes: tuple[str, ...]) -> bool:
    if not content_type:
        return True  # unknown → allow; parsers may reject later
    base = content_type.split(";", 1)[0].strip().lower()
    return any(base.startswith(p) for p in prefixes)


async def _ensure_robots(
    pool: HttpPool,
    policy: FetchPolicy,
    url: str,
) -> bool:
    """Load robots.txt once per origin. False when it is unreachable right now.

    RFC 9309: 4xx means no rules (allow); 5xx/network failure means do not crawl
    yet. The failure is not cached, so a later attempt fetches robots again.
    An unreadable file (oversize/bad encoding) keeps the pre-R1 allow-all answer:
    retrying cannot fix it, and blocking would dead-letter every task on the origin.
    """
    if not policy.respect_robots or not policy.robots.needs_fetch(url):
        return True
    origin = policy.robots.origin_of(url)
    robots_url = policy.robots.robots_url(url)
    try:
        # Robots fetch still goes through pool; fixture loopback OK.
        if policy.mode is Mode.LIVE:
            policy.url_policy.validate_destination(robots_url)
        resp = await pool.request("GET", robots_url)
    except (PolicyError, BodyTooLargeError, DecodingError):
        policy.robots.remember(origin, None, status_code=None)
        return True
    except (httpx.HTTPError, BackpressureError):
        return False
    if resp.status_code >= 500:
        return False
    policy.robots.remember(origin, resp.content, status_code=resp.status_code)
    return True


async def fetch(
    task: ClaimedTask | str,
    policy: FetchPolicy | None = None,
    *,
    pool: HttpPool | None = None,
    settings: Settings | None = None,
    headers: Mapping[str, str] | None = None,
    cached_etag: str | None = None,
    cached_body: bytes | None = None,
    cached_meta: Mapping[str, Any] | None = None,
    owns_pool: bool | None = None,
    attempt: int = 0,
) -> RawObservation:
    """
    Fetch URL for a task (or bare URL string). Returns typed RawObservation.

    Does not hold a DB lock. Caller claims first, then fetch, then complete.
    Transient failures return retry_at without holding pool slots during delay
    when used via fetch_with_retries.
    """
    cfg = settings or get_settings()
    fetch_policy = policy or FetchPolicy(mode=cfg.mode)
    url = task.url if isinstance(task, ClaimedTask) else task
    close_pool = False
    if pool is None:
        pool = HttpPool(PoolLimits(user_agent=fetch_policy.url_policy.user_agent))
        close_pool = True if owns_pool is None else owns_pool
    try:
        return await _fetch_once(
            url,
            fetch_policy,
            pool=pool,
            headers=headers,
            cached_etag=cached_etag,
            cached_body=cached_body,
            cached_meta=cached_meta,
            attempt=attempt,
        )
    finally:
        if close_pool:
            await pool.aclose()


async def fetch_with_retries(
    task: ClaimedTask | str,
    policy: FetchPolicy | None = None,
    *,
    pool: HttpPool | None = None,
    settings: Settings | None = None,
    headers: Mapping[str, str] | None = None,
    cached_etag: str | None = None,
    cached_body: bytes | None = None,
    cached_meta: Mapping[str, Any] | None = None,
    sleep: bool = True,
) -> RawObservation:
    """
    Bounded transient retries. Releases HTTP slots between attempts (delay outside pool).
    """
    cfg = settings or get_settings()
    fetch_policy = policy or FetchPolicy(mode=cfg.mode)
    close_pool = pool is None
    if pool is None:
        pool = HttpPool(PoolLimits(user_agent=fetch_policy.url_policy.user_agent))
    attempt = 0
    try:
        while True:
            outcome = await fetch(
                task,
                fetch_policy,
                pool=pool,
                settings=cfg,
                headers=headers,
                cached_etag=cached_etag,
                cached_body=cached_body,
                cached_meta=cached_meta,
                owns_pool=False,
                attempt=attempt,
            )
            if outcome.outcome is not FetchOutcomeKind.RETRY_AT:
                return outcome
            now = get_clock().now()
            wait = 0.0
            if outcome.retry_at is not None:
                wait = max(0.0, (outcome.retry_at - now).total_seconds())
            if isinstance(task, ClaimedTask) and (
                attempt >= fetch_policy.max_retries or wait > fetch_policy.retry_cap_s
            ):
                # Hand the wait to the queue: the slot is freed and the task comes
                # back later (schedule_retry dead-letters it once attempts run out).
                retry_at = max(
                    outcome.retry_at or now,
                    now + timedelta(seconds=db_retry_delay(task.attempt_count)),
                )
                return replace(
                    outcome,
                    retry_at=retry_at,
                    metadata={**outcome.metadata, "exhausted_retries": True},
                )
            if attempt >= fetch_policy.max_retries:
                return RawObservation(
                    request_url=outcome.request_url,
                    final_url=outcome.final_url,
                    status_code=outcome.status_code,
                    outcome=FetchOutcomeKind.INVALID,
                    reason=outcome.reason or "retry budget exhausted",
                    fetched_at=get_clock().now(),
                    metadata={"exhausted_retries": True},
                )
            delay = min(wait, fetch_policy.retry_cap_s)
            # Slot already released by pool.request; sleep without holding workers.
            if sleep and delay > 0:
                await asyncio.sleep(delay)
            attempt += 1
    finally:
        if close_pool:
            await pool.aclose()


async def _fetch_once(
    url: str,
    policy: FetchPolicy,
    *,
    pool: HttpPool,
    headers: Mapping[str, str] | None,
    cached_etag: str | None,
    cached_body: bytes | None,
    cached_meta: Mapping[str, Any] | None,
    attempt: int = 0,
) -> RawObservation:
    now = _aware(get_clock().now())
    request_url = url
    try:
        request_url = policy.url_policy.validate_destination(url)
    except PolicyError as exc:
        return RawObservation(
            request_url=url,
            final_url=url,
            status_code=None,
            outcome=FetchOutcomeKind.DENIED,
            reason=str(exc),
            fetched_at=now,
        )

    host = urlsplit(request_url).hostname or ""
    if (
        policy.check_resolved_private
        and policy.mode is Mode.LIVE
        and resolved_addresses_unsafe(host)
    ):
        return RawObservation(
            request_url=request_url,
            final_url=request_url,
            status_code=None,
            outcome=FetchOutcomeKind.DENIED,
            reason=f"resolved address unsafe for host {host}",
            fetched_at=now,
        )

    if not await _ensure_robots(pool, policy, request_url):
        return RawObservation(
            request_url=request_url,
            final_url=request_url,
            status_code=None,
            outcome=FetchOutcomeKind.RETRY_AT,
            reason="robots.txt unavailable (5xx or network error)",
            retry_at=now + timedelta(seconds=_retry_delay(attempt, policy, None)),
            fetched_at=now,
        )
    if policy.respect_robots and not policy.robots.is_allowed(request_url):
        return RawObservation(
            request_url=request_url,
            final_url=request_url,
            status_code=None,
            outcome=FetchOutcomeKind.DENIED,
            reason="robots.txt disallows URL",
            fetched_at=now,
            metadata={"robots": "denied"},
        )

    req_headers = dict(headers or {})
    if cached_etag:
        req_headers.setdefault("If-None-Match", cached_etag)

    current = request_url
    previous: str | None = None
    seen: set[str] = set()

    for _hop in range(pool.limits.max_redirects + 1):
        if current in seen:
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=None,
                outcome=FetchOutcomeKind.INVALID,
                reason="redirect loop detected",
                fetched_at=now,
            )
        seen.add(current)
        try:
            current = policy.url_policy.validate_redirect_target(
                current, previous=previous
            )
        except PolicyError as exc:
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=None,
                outcome=FetchOutcomeKind.DENIED,
                reason=f"redirect target disallowed: {exc}",
                fetched_at=now,
            )

        try:
            resp = await pool.request("GET", current, headers=req_headers)
        except BodyTooLargeError as exc:
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=None,
                outcome=FetchOutcomeKind.INVALID,
                reason=str(exc),
                fetched_at=now,
            )
        except DecodingError as exc:
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=None,
                outcome=FetchOutcomeKind.INVALID,
                reason=f"decoding failed: {exc}",
                fetched_at=now,
            )
        except httpx.TimeoutException as exc:
            retry_at = now + timedelta(
                seconds=_retry_delay(attempt, policy, None)
            )
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=None,
                outcome=FetchOutcomeKind.RETRY_AT,
                reason=f"timeout: {exc}",
                retry_at=retry_at,
                fetched_at=now,
            )
        except BackpressureError as exc:
            retry_at = now + timedelta(seconds=policy.retry_base_s)
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=None,
                outcome=FetchOutcomeKind.RETRY_AT,
                reason=str(exc),
                retry_at=retry_at,
                fetched_at=now,
            )
        except httpx.HTTPError as exc:
            retry_at = now + timedelta(seconds=_retry_delay(attempt, policy, None))
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=None,
                outcome=FetchOutcomeKind.RETRY_AT,
                reason=f"transport error: {exc}",
                retry_at=retry_at,
                fetched_at=now,
            )

        status = resp.status_code
        hdrs = {k.lower(): v for k, v in resp.headers.items()}
        ctype = hdrs.get("content-type")

        # Redirects
        if status in {301, 302, 303, 307, 308}:
            loc = resp.headers.get("location")
            if not loc:
                return RawObservation(
                    request_url=request_url,
                    final_url=current,
                    status_code=status,
                    outcome=FetchOutcomeKind.INVALID,
                    reason="redirect without Location",
                    headers=hdrs,
                    fetched_at=now,
                )
            previous = current
            # Relative Location
            from urllib.parse import urljoin

            current = urljoin(current, loc)
            if _origin(current) != _origin(previous):
                # Never carry credentials or cache validators to another origin.
                req_headers = {
                    k: v for k, v in req_headers.items() if k.lower() not in _ORIGIN_BOUND
                }
            continue

        if status == 304:
            if cached_body is not None:
                meta = dict(cached_meta or {})
                return RawObservation(
                    request_url=request_url,
                    final_url=current,
                    status_code=304,
                    outcome=FetchOutcomeKind.NOT_MODIFIED,
                    body=cached_body,
                    content_type=meta.get("content_type") or ctype,
                    etag=cached_etag or hdrs.get("etag"),
                    last_modified=hdrs.get("last-modified") or meta.get("last_modified"),
                    headers=hdrs,
                    fetched_at=now,
                    content_key=meta.get("content_key"),
                    content_sha256=meta.get("content_sha256"),
                    metadata={"cache": "hit"},
                )
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=304,
                outcome=FetchOutcomeKind.INVALID,
                reason="304 without cached body",
                headers=hdrs,
                fetched_at=now,
            )

        if status == 404 or status == 410:
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=status,
                outcome=FetchOutcomeKind.NOT_FOUND,
                reason=f"HTTP {status}",
                headers=hdrs,
                fetched_at=now,
            )

        if status in {401, 403}:
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=status,
                outcome=FetchOutcomeKind.DENIED,
                reason=f"HTTP {status}",
                headers=hdrs,
                fetched_at=now,
            )

        if status == 429 or status in {500, 502, 503, 504}:
            ra = _parse_retry_after(resp.headers.get("retry-after"), now)
            delay = _retry_delay(attempt, policy, ra)
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=status,
                outcome=FetchOutcomeKind.RETRY_AT,
                reason=f"HTTP {status}",
                headers=hdrs,
                retry_at=now + timedelta(seconds=delay),
                fetched_at=now,
            )

        if status >= 400:
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=status,
                outcome=FetchOutcomeKind.INVALID,
                reason=f"HTTP {status}",
                headers=hdrs,
                fetched_at=now,
            )

        body = resp.content
        if looks_like_challenge(body, ctype, status):
            alternate = extract_alternate_feed_hint(body, current)
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=status,
                outcome=FetchOutcomeKind.BLOCKED,
                body=body,
                content_type=ctype,
                headers=hdrs,
                fetched_at=now,
                reason="challenge interstitial (HTTP 200)",
                alternate_url=alternate,
                metadata={"challenge": True, "alternate_url": alternate},
            )

        if not _mime_allowed(ctype, policy.allowed_mime_prefixes):
            return RawObservation(
                request_url=request_url,
                final_url=current,
                status_code=status,
                outcome=FetchOutcomeKind.INVALID,
                reason=f"unexpected MIME type: {ctype}",
                content_type=ctype,
                headers=hdrs,
                fetched_at=now,
            )

        return RawObservation(
            request_url=request_url,
            final_url=current,
            status_code=status,
            outcome=FetchOutcomeKind.OK,
            body=body,
            content_type=ctype,
            etag=hdrs.get("etag"),
            last_modified=hdrs.get("last-modified"),
            headers=hdrs,
            fetched_at=now,
        )

    return RawObservation(
        request_url=request_url,
        final_url=current,
        status_code=None,
        outcome=FetchOutcomeKind.INVALID,
        reason="too many redirects",
        fetched_at=now,
    )
