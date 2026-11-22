"""B1 gate: async crawl transport, policy, backpressure and lease recovery.

Oracle: docs/build-plan.md (B1) and docs/contracts.md — not production source.
Cases must FAIL when behavior is missing/wrong and PASS when integrated correctly.
The gate runner rejects skipped cases.
"""

from __future__ import annotations

import asyncio
import socket
from datetime import timedelta
from typing import Any
from urllib.parse import urlsplit

import pytest

from tests.integration.b1_api import kind_is, load_crawl, outcome_kind
from tests.integration.b1_http import start_b1_http_server
from tests.support import FIXTURES

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def b1_http():
    server = start_b1_http_server()
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
def crawl_env(tmp_fixture_env, frozen_clock):
    """Fixture-mode settings + B1 schema (tasks/observations/checkpoints)."""
    from signal_ingest.config import Mode, load_settings
    from signal_ingest.config.network import install_network_guard
    from signal_ingest.storage import connect, init_db

    install_network_guard()
    settings = load_settings(mode=Mode.FIXTURE)
    init_db(settings)
    with connect(settings) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('tasks', 'observations', 'source_checkpoints')
            """
        ).fetchone()
        assert row is not None and int(row[0]) >= 3, (
            "B1 schema missing: expected tasks, observations, source_checkpoints "
            "(migrations/002_b1_crawl.sql)"
        )
    return settings


@pytest.fixture
def crawl():
    return load_crawl()


@pytest.fixture
def offsite_counter(monkeypatch):
    """Count non-loopback DNS/connect attempts while network guard is active."""
    from signal_ingest.config.network import _is_loopback_host

    state = {"attempts": 0}
    orig_getaddrinfo = socket.getaddrinfo
    orig_connect = socket.socket.connect

    def counting_getaddrinfo(host, port, *args, **kwargs):
        host_s = host.decode() if isinstance(host, bytes) else host
        if host_s is not None and not _is_loopback_host(str(host_s)):
            state["attempts"] += 1
        return orig_getaddrinfo(host, port, *args, **kwargs)

    def counting_connect(self, address):
        host = str(address[0]) if isinstance(address, tuple) and address else None
        if host is not None and not _is_loopback_host(host):
            state["attempts"] += 1
        return orig_connect(self, address)

    monkeypatch.setattr(socket, "getaddrinfo", counting_getaddrinfo)
    monkeypatch.setattr(socket.socket, "connect", counting_connect)
    return state


def _pool(crawl: Any, *, max_active: int = 2, **limit_kwargs: Any) -> Any:
    limits = crawl.PoolLimits(
        max_active=max_active,
        max_pending=max(max_active * 4, 8),
        per_origin_concurrency=max_active,
        per_origin_min_interval_s=0.0,
        connect_timeout_s=limit_kwargs.pop("connect_timeout_s", 0.5),
        read_timeout_s=limit_kwargs.pop("read_timeout_s", 0.5),
        overall_timeout_s=limit_kwargs.pop("overall_timeout_s", 1.0),
        max_compressed_bytes=limit_kwargs.pop("max_compressed_bytes", 64 * 1024),
        max_decoded_bytes=limit_kwargs.pop("max_decoded_bytes", 128 * 1024),
        max_redirects=limit_kwargs.pop("max_redirects", 5),
        **limit_kwargs,
    )
    return crawl.HttpPool(limits)


def _policy(crawl: Any, **kwargs: Any) -> Any:
    from signal_ingest.config import Mode

    return crawl.FetchPolicy(mode=Mode.FIXTURE, **kwargs)


# ---------------------------------------------------------------------------
# Local HTTP scenario matrix
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redirect_loop_rejected(b1_http, crawl_env, crawl):
    """Redirect A↔B terminates as invalid — no infinite follow."""
    pool = _pool(crawl, max_redirects=6)
    policy = _policy(crawl)
    try:
        result = await crawl.fetch(
            b1_http.url("/redirect-loop-a"), policy, pool=pool
        )
        assert kind_is(result, "invalid"), outcome_kind(result)
        assert "loop" in (result.reason or "").lower()
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_304_with_cached_body(b1_http, crawl_env, crawl):
    """304 with cached body yields not_modified and reuses bytes."""
    pool = _pool(crawl)
    policy = _policy(crawl)
    try:
        first = await crawl.fetch(b1_http.url("/etag-resource"), policy, pool=pool)
        assert kind_is(first, "ok")
        assert first.body is not None
        etag = first.etag or '"b1-etag-v1"'
        second = await crawl.fetch(
            b1_http.url("/etag-resource"),
            policy,
            pool=pool,
            cached_etag=etag,
            cached_body=first.body,
            cached_meta={
                "content_type": first.content_type,
                "content_sha256": first.content_sha256,
            },
        )
        assert kind_is(second, "not_modified")
        assert second.body == first.body
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_304_without_cached_body(b1_http, crawl_env, crawl):
    """Bare 304 without a local body must not invent content → invalid."""
    pool = _pool(crawl)
    policy = _policy(crawl)
    try:
        primed = await crawl.fetch(b1_http.url("/etag-resource"), policy, pool=pool)
        assert kind_is(primed, "ok")
        result = await crawl.fetch(
            b1_http.url("/etag-resource"),
            policy,
            pool=pool,
            cached_etag=primed.etag or '"b1-etag-v1"',
            cached_body=None,
        )
        assert kind_is(result, "invalid"), outcome_kind(result)
        assert result.body in (None, b"")
        assert "304" in (result.reason or "")
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_timeout(b1_http, crawl_env, crawl):
    """Hung handler surfaces retry_at (timeout) and cancels cleanly."""
    pool = _pool(
        crawl,
        connect_timeout_s=0.15,
        read_timeout_s=0.15,
        overall_timeout_s=0.3,
    )
    policy = _policy(crawl, max_retries=0)
    try:
        result = await crawl.fetch(b1_http.url("/timeout"), policy, pool=pool)
        assert kind_is(result, "retry_at"), outcome_kind(result)
        assert "timeout" in (result.reason or "").lower()
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_429_then_success(b1_http, crawl_env, crawl):
    """429 then 200 succeeds within bounded retries (slots free while delayed)."""
    pool = _pool(crawl, overall_timeout_s=5.0, read_timeout_s=2.0)
    policy = _policy(crawl, max_retries=3, retry_base_s=0.01, retry_cap_s=0.05)
    try:
        result = await crawl.fetch_with_retries(
            b1_http.url("/rate-limit-then-ok"),
            policy,
            pool=pool,
            sleep=True,
        )
        assert kind_is(result, "ok"), outcome_kind(result)
        assert b1_http.state.hits.get("/rate-limit-then-ok", 0) >= 2
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_repeated_503_bounded(b1_http, crawl_env, crawl):
    """Endless 503 stops after retry budget; follow-up fetch still runs (slot free)."""
    pool = _pool(crawl, max_active=1, overall_timeout_s=5.0, read_timeout_s=2.0)
    policy = _policy(crawl, max_retries=2, retry_base_s=0.01, retry_cap_s=0.05)
    try:
        result = await crawl.fetch_with_retries(
            b1_http.url("/always-503"), policy, pool=pool, sleep=True
        )
        assert kind_is(result, "invalid"), outcome_kind(result)
        assert result.metadata.get("exhausted_retries") is True or "exhaust" in (
            result.reason or ""
        ).lower()
        hits = b1_http.state.hits.get("/always-503", 0)
        assert 1 <= hits <= 5, f"retries unbounded: {hits}"
        ok = await crawl.fetch(b1_http.url("/ok"), policy, pool=pool)
        assert kind_is(ok, "ok")
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_permanent_404(b1_http, crawl_env, crawl):
    """Permanent 404 → not_found (terminal)."""
    pool = _pool(crawl)
    policy = _policy(crawl)
    try:
        result = await crawl.fetch(b1_http.url("/missing"), policy, pool=pool)
        assert kind_is(result, "not_found"), outcome_kind(result)
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_robots_denial(b1_http, crawl_env, crawl):
    """robots.txt Disallow denies before GET on the protected path."""
    pool = _pool(crawl)
    policy = _policy(crawl, respect_robots=True)
    try:
        result = await crawl.fetch(
            b1_http.url("/secret/denied"), policy, pool=pool
        )
        assert kind_is(result, "denied"), outcome_kind(result)
        assert b1_http.state.hits.get("/secret/denied", 0) == 0
        assert b1_http.state.hits.get("/robots.txt", 0) >= 1
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_redirect_to_disallowed_destination(b1_http, crawl_env, crawl):
    """Redirect to link-local/private or non-loopback public is rejected."""
    pool = _pool(crawl)
    policy = _policy(crawl)
    try:
        private = await crawl.fetch(
            b1_http.url("/redirect-to-private"), policy, pool=pool
        )
        assert kind_is(private, "denied"), outcome_kind(private)

        public = await crawl.fetch(
            b1_http.url("/redirect-to-public"), policy, pool=pool
        )
        # Fixture transport may only allow loopback; public HTTPS redirect is disallowed
        # either by policy or by never completing an offsite fetch successfully.
        assert kind_is(public, "denied", "invalid", "retry_at"), outcome_kind(public)
        if kind_is(public, "retry_at"):
            # Soft path: policy allowed HTTPS host but transport/guard failed — still
            # must not have observed a successful public body.
            assert public.body is None
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_truncated_gzip(b1_http, crawl_env, crawl):
    """Truncated Content-Encoding: gzip → invalid (must not raise to caller)."""
    pool = _pool(crawl)
    policy = _policy(crawl)
    try:
        try:
            result = await crawl.fetch(
                b1_http.url("/truncated-gzip"), policy, pool=pool
            )
        except Exception as exc:  # noqa: BLE001 — uncaught decode is a gate failure
            pytest.fail(
                f"truncated gzip must return outcome=invalid, not raise {type(exc).__name__}: {exc}"
            )
        assert kind_is(result, "invalid"), outcome_kind(result)
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_unexpected_mime(b1_http, crawl_env, crawl):
    """application/pdf under HTML/JSON policy → invalid."""
    pool = _pool(crawl)
    policy = _policy(
        crawl,
        allowed_mime_prefixes=("text/", "application/json", "application/atom+xml"),
    )
    try:
        result = await crawl.fetch(
            b1_http.url("/unexpected-mime"), policy, pool=pool
        )
        assert kind_is(result, "invalid"), outcome_kind(result)
        assert "mime" in (result.reason or "").lower()
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_duplicate_link_one_task(b1_http, crawl_env, crawl):
    """Normalized duplicate links collapse to one semantic task."""
    from signal_ingest.storage import connect, create_run

    import uuid

    settings = crawl_env
    source_id = f"b1-dup-source-{uuid.uuid4().hex[:12]}"
    ext = f"dup-{uuid.uuid4().hex[:8]}"
    base = b1_http.url("/dup/item")
    items = [
        crawl.DiscoveredItem(url=base, external_id=ext),
        crawl.DiscoveredItem(url=base + "?utm_source=x&utm_medium=y", external_id=ext),
        crawl.DiscoveredItem(url=base + "#section", external_id=ext),
        crawl.DiscoveredItem(url=base + "?utm_source=z", external_id=ext),
    ]
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings)
        conn.commit()
        result = crawl.persist_discovery_page(
            conn,
            source_id=source_id,
            items=items,
            run_id=run_id,
            url_policy=crawl.UrlPolicy(mode=settings.mode),
        )
        conn.commit()
        assert result["created"] == 1
        assert result["skipped"] == 3
        count = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE source_id = %s",
            (source_id,),
        ).fetchone()
        assert count is not None and int(count[0]) == 1


@pytest.mark.asyncio
async def test_oversized_response(b1_http, crawl_env, crawl):
    """Body over max compressed/decoded bytes → invalid."""
    pool = _pool(
        crawl,
        max_compressed_bytes=4_096,
        max_decoded_bytes=4_096,
        overall_timeout_s=2.0,
        read_timeout_s=2.0,
    )
    policy = _policy(crawl)
    try:
        result = await crawl.fetch(b1_http.url("/oversized"), policy, pool=pool)
        assert kind_is(result, "invalid"), outcome_kind(result)
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_challenge_as_200(b1_http, crawl_env, crawl):
    """200 HTML interstitial → blocked, with alternate feed hint when present."""
    pool = _pool(crawl)
    policy = _policy(crawl)
    try:
        result = await crawl.fetch(b1_http.url("/challenge"), policy, pool=pool)
        assert kind_is(result, "blocked"), outcome_kind(result)
        assert result.metadata.get("challenge") is True or "challenge" in (
            result.reason or ""
        ).lower()
        # Fixture challenge page links to alternate-feed.xml
        assert result.alternate_url is not None
        assert "alternate-feed" in result.alternate_url
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_alternate_feed_preserves_lineage(b1_http, crawl_env, crawl):
    """Alternate-feed route keeps fixture source_id lineage on observation."""
    from signal_ingest.storage import connect, create_run

    import uuid

    settings = crawl_env
    source_id = f"b1-alternate-source-{uuid.uuid4().hex[:12]}"
    pool = _pool(crawl)
    policy = _policy(crawl)
    try:
        # Primary challenge surfaces alternate; fetch alternate and persist.
        blocked = await crawl.fetch(
            b1_http.url("/primary-blocked"), policy, pool=pool
        )
        assert kind_is(blocked, "blocked")
        alt = blocked.alternate_url or b1_http.url("/alternate-feed.xml")
        if alt.startswith("/"):
            alt = b1_http.url(alt)

        items = []
        async for item in crawl.discover(
            source_id,
            checkpoint=None,
            page_items=[
                crawl.DiscoveredItem(
                    url=alt,
                    external_id="alt-1",
                    cursor="cursor-alt-v1",
                    metadata={
                        "lineage_mode": "fixture",
                        "via": "alternate-feed",
                        "primary": b1_http.url("/primary-blocked"),
                    },
                )
            ],
        ):
            items.append(item)
        assert len(items) == 1

        with connect(settings) as conn:
            run_id = create_run(conn, settings=settings)
            conn.commit()
            persisted = crawl.persist_discovery_page(
                conn,
                source_id=source_id,
                items=items,
                run_id=run_id,
            )
            conn.commit()
            assert persisted["created"] == 1
            assert persisted["cursor"] == "cursor-alt-v1"
            cp = crawl.get_checkpoint(conn, source_id=source_id)
            assert cp is not None
            assert cp["cursor_opaque"] == "cursor-alt-v1"

            claimed = crawl.claim_tasks(
                conn, worker_id="alt-worker", source_id=source_id, limit=1
            )
            assert len(claimed) == 1
            obs = await crawl.fetch(claimed[0], policy, pool=pool)
            assert kind_is(obs, "ok")
            store = crawl.RawStore.from_settings(settings)
            ok = crawl.complete_with_blob(
                conn,
                claimed[0],
                obs,
                store,
                cursor_opaque="cursor-alt-v1",
            )
            assert ok is True
            row = conn.execute(
                """
                SELECT source_id, outcome, metadata
                FROM observations WHERE source_id = %s
                """,
                (source_id,),
            ).fetchone()
            assert row is not None
            assert row[0] == source_id
            assert row[1] == "ok"

        feed = (FIXTURES / "b1_alternate_feed.xml").read_text(encoding="utf-8")
        assert "synthetic" in feed.lower()
        assert "not_for_live_export" in feed
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_measured_max_in_flight_under_cap(b1_http, crawl_env, crawl):
    """Observed peak in-flight ≤ configured max_active under slow consumers."""
    cap = 2
    pool = _pool(
        crawl,
        max_active=cap,
        overall_timeout_s=5.0,
        read_timeout_s=3.0,
        connect_timeout_s=1.0,
    )
    policy = _policy(crawl, respect_robots=False, max_retries=0)

    async def one() -> Any:
        return await crawl.fetch(b1_http.url("/slow"), policy, pool=pool)

    try:
        results = await asyncio.gather(*[one() for _ in range(6)])
        peak = pool.peak_inflight
        assert peak <= cap, f"peak_inflight {peak} exceeds cap {cap}"
        assert b1_http.state.peak_in_flight <= cap + 1
        assert any(kind_is(r, "ok") for r in results)
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_outside_localhost_attempts_zero(
    b1_http, crawl_env, crawl, offsite_counter
):
    """Network/provider calls outside localhost remain 0 under fixture crawl."""
    pool = _pool(crawl)
    policy = _policy(crawl)
    try:
        ok = await crawl.fetch(b1_http.url("/ok"), policy, pool=pool)
        assert kind_is(ok, "ok")
        denied = await crawl.fetch(
            b1_http.url("/redirect-to-public"), policy, pool=pool
        )
        assert kind_is(denied, "denied", "invalid", "retry_at")
        assert offsite_counter["attempts"] == 0, (
            f"outside-localhost attempts must be 0, got {offsite_counter['attempts']}"
        )
    finally:
        await pool.aclose()


# ---------------------------------------------------------------------------
# Two PostgreSQL workers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_workers_contend_one_lease(b1_http, crawl_env, crawl):
    """Two workers claim one ready task → exactly one lease."""
    import uuid

    from signal_ingest.storage import connect, create_run

    settings = crawl_env
    source_id = f"b1-lease-race-{uuid.uuid4().hex[:12]}"
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings)
        conn.commit()
        tid = crawl.enqueue_task(
            conn,
            source_id=source_id,
            url=b1_http.url("/ok"),
            run_id=run_id,
            external_id=f"one-task-{uuid.uuid4().hex[:8]}",
        )
        assert tid is not None
        conn.commit()

    with connect(settings) as conn_a, connect(settings) as conn_b:
        a = crawl.claim_tasks(conn_a, worker_id="worker-a", source_id=source_id)
        b = crawl.claim_tasks(conn_b, worker_id="worker-b", source_id=source_id)

    winners = [x for x in (a, b) if x]
    assert len(winners) == 1
    assert len(winners[0]) == 1


@pytest.mark.asyncio
async def test_crash_after_fetch_before_commit_recovery(b1_http, crawl_env, crawl):
    """Kill after blob write / before commit → one effect, cursor, fence, successors."""
    import uuid

    from signal_ingest.storage import connect, create_run

    settings = crawl_env
    source_id = f"b1-crash-recovery-{uuid.uuid4().hex[:12]}"
    cursor_token = "cursor-opaque-v1"
    pool = _pool(crawl)
    policy = _policy(crawl)
    store = crawl.RawStore.from_settings(settings)

    try:
        with connect(settings) as conn:
            run_id = create_run(conn, settings=settings)
            conn.commit()
            task_id = crawl.enqueue_task(
                conn,
                source_id=source_id,
                url=b1_http.url("/ok"),
                run_id=run_id,
                external_id=f"crash-task-{uuid.uuid4().hex[:8]}",
            )
            assert task_id is not None
            crawl.upsert_checkpoint(
                conn, source_id=source_id, cursor_opaque=cursor_token
            )
            conn.commit()

        # Worker A: claim → fetch → write blob → crash (no complete).
        with connect(settings) as conn_a:
            claimed_a = crawl.claim_tasks(
                conn_a, worker_id="worker-a", source_id=source_id
            )
            assert len(claimed_a) == 1
            lease_a = claimed_a[0]
            obs_a = await crawl.fetch(lease_a, policy, pool=pool)
            assert kind_is(obs_a, "ok")
            crawl.save_raw_observation_blob(store, obs_a)
            token_a = lease_a.fencing_token
            owner_a = lease_a.lease_owner
            # Process kill: drop connection without complete_task.

        # Expire lease relative to the frozen clock (claim compares against get_clock()).
        from signal_ingest.config import get_clock

        with connect(settings) as conn:
            conn.execute(
                """
                UPDATE tasks
                SET lease_expires = %s
                WHERE id = %s
                """,
                (get_clock().now() - timedelta(seconds=5), str(lease_a.id)),
            )
            conn.commit()

        # Worker B restart: reclaim bumps fencing token, then A is stale.
        with connect(settings) as conn_b:
            claimed_b = crawl.claim_tasks(
                conn_b, worker_id="worker-b", source_id=source_id
            )
            assert len(claimed_b) == 1
            lease_b = claimed_b[0]
            assert lease_b.fencing_token != token_a
            assert lease_b.lease_owner == "worker-b"
            assert owner_a == "worker-a"

            # Stale A complete must be rejected after B's reclaim.
            stale_ok = crawl.complete_task(
                conn_b,
                lease_a,
                obs_a,
                successor_urls=[b1_http.url("/dup/successor")],
                cursor_opaque=cursor_token,
            )
            assert stale_ok is False

            obs_b = await crawl.fetch(lease_b, policy, pool=pool)
            assert kind_is(obs_b, "ok")
            # Prefer reuse of content hash from crash-written blob.
            if obs_a.content_sha256 and store.exists(obs_a.content_sha256):
                obs_b.content_sha256 = obs_a.content_sha256
                obs_b.content_key = obs_a.content_key
                obs_b.body = obs_a.body
            committed = crawl.complete_with_blob(
                conn_b,
                lease_b,
                obs_b,
                store,
                successor_urls=[b1_http.url("/dup/successor")],
                cursor_opaque=cursor_token,
            )
            assert committed is True

        with connect(settings) as conn:
            obs_count = conn.execute(
                "SELECT COUNT(*) FROM observations WHERE source_id = %s",
                (source_id,),
            ).fetchone()
            assert obs_count is not None and int(obs_count[0]) == 1

            assert crawl.recover_orphaned_blob_effect(conn, lease_a.id) is True

            cp = crawl.get_checkpoint(conn, source_id=source_id)
            assert cp is not None
            assert cp["cursor_opaque"] == cursor_token

            task_row = crawl.get_task(conn, lease_a.id)
            assert task_row is not None
            assert int(task_row["attempt_count"]) <= int(task_row["max_attempts"])
            assert int(task_row["attempt_count"]) <= 5
            assert task_row["status"] == "done"

            # Successor present (no lost successors).
            succ = conn.execute(
                """
                SELECT COUNT(*) FROM tasks
                WHERE source_id = %s AND url LIKE %s
                """,
                (source_id, "%/dup/successor%"),
            ).fetchone()
            assert succ is not None and int(succ[0]) >= 1

            # Stale fencing still rejected after success.
            stale_again = crawl.complete_task(conn, lease_a, obs_a)
            assert stale_again is False
    finally:
        await pool.aclose()


def test_b1_fixture_files_labeled_synthetic():
    """B1 fixtures stay labeled synthetic / not for live export."""
    robots = (FIXTURES / "b1_robots.txt").read_text(encoding="utf-8")
    assert "synthetic" in robots.lower()
    feed = (FIXTURES / "b1_alternate_feed.xml").read_text(encoding="utf-8")
    assert "synthetic" in feed.lower()
    assert "not_for_live_export" in feed
    challenge = (FIXTURES / "b1_challenge.html").read_text(encoding="utf-8")
    assert "synthetic" in challenge.lower()


def test_url_normalize_strips_tracking_for_duplicate_identity(crawl_env, crawl):
    """Plausible happy-path miss: tracking params must not create a second identity."""
    from signal_ingest.config import Mode

    policy = crawl.UrlPolicy(mode=Mode.FIXTURE)
    a = policy.normalize("http://127.0.0.1:9/x?utm_source=a&id=1")
    b = policy.normalize("http://127.0.0.1:9/x?id=1&utm_campaign=z")
    assert urlsplit(a).query == urlsplit(b).query
    assert "utm_" not in urlsplit(a).query
    assert "id=1" in urlsplit(a).query


# ---------------------------------------------------------------------------
# R1 self-healing: dead-letter reaper, lease heartbeat, DB backoff, slots
# ---------------------------------------------------------------------------


def _r1_task(settings, url: str, *, max_attempts: int = 5, n: int = 1):
    import uuid

    from signal_ingest.crawl.queue import enqueue_task
    from signal_ingest.storage import connect, create_run

    source_id = f"r1-{uuid.uuid4().hex[:12]}"
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings)
        ids = [
            enqueue_task(
                conn,
                source_id=source_id,
                url=url,
                run_id=run_id,
                external_id=f"t{i}",
                max_attempts=max_attempts,
            )
            for i in range(n)
        ]
        conn.commit()
    return run_id, source_id, ids


def _r1_status(settings, task_id) -> dict:
    from signal_ingest.crawl.queue import get_task
    from signal_ingest.storage import connect

    with connect(settings) as conn:
        return get_task(conn, task_id)


def test_r1_reaper_dead_letters_task_that_died_on_last_attempt(crawl_env):
    """Worker dies holding its final attempt → reaper marks failed, not stuck leased."""
    from signal_ingest.config import ControllableClock, get_clock, set_clock
    from signal_ingest.crawl.queue import claim_tasks, reap_expired
    from signal_ingest.storage import connect

    settings = crawl_env
    clock = ControllableClock(get_clock().now())
    previous = get_clock()
    set_clock(clock)
    try:
        run_id, source_id, (last, spare) = _r1_task(
            settings, "http://127.0.0.1:9/x", max_attempts=1, n=2
        )
        with connect(settings) as conn:
            claimed = claim_tasks(conn, worker_id="dies", source_id=source_id, limit=2)
            assert len(claimed) == 2
            # spare gets an extra attempt budget so it must return to ready.
            conn.execute("UPDATE tasks SET max_attempts = 3 WHERE id = %s", (str(spare),))
            conn.commit()
        clock.advance(120)
        with connect(settings) as conn:
            counts = reap_expired(conn, run_id=run_id)
        assert counts == {"failed": 1, "released": 1}
        dead = _r1_status(settings, last)
        assert dead["status"] == "failed"
        assert "exhausted" in (dead["last_error"] or "")
        assert _r1_status(settings, spare)["status"] == "ready"
    finally:
        set_clock(previous)


def test_r1_requeue_failed_resets_dead_letters(crawl_env):
    from signal_ingest.crawl.queue import requeue_failed
    from signal_ingest.storage import connect

    settings = crawl_env
    run_id, _, (tid,) = _r1_task(settings, "http://127.0.0.1:9/x")
    with connect(settings) as conn:
        conn.execute(
            "UPDATE tasks SET status='failed', attempt_count=5 WHERE id=%s", (str(tid),)
        )
        conn.commit()
        assert requeue_failed(conn, run_id=run_id) == 1
    row = _r1_status(settings, tid)
    assert row["status"] == "ready" and row["attempt_count"] == 0


@pytest.mark.asyncio
async def test_r1_exhausted_transient_goes_to_db_retry_with_growing_backoff(
    b1_http, crawl_env, crawl
):
    """Claimed task on endless 503: retry_at (not terminal invalid); delay grows per attempt."""
    from dataclasses import replace

    from signal_ingest.crawl.queue import claim_tasks
    from signal_ingest.crawl.types import FetchOutcomeKind
    from signal_ingest.storage import connect

    settings = crawl_env
    _, source_id, _ = _r1_task(settings, b1_http.url("/always-503"))
    with connect(settings) as conn:
        (task,) = claim_tasks(conn, worker_id="w", source_id=source_id)
    pool = _pool(crawl)
    policy = _policy(crawl, max_retries=0, retry_base_s=0.01, retry_cap_s=0.05)
    delays = []
    try:
        for attempt in (1, 4):
            obs = await crawl.fetch_with_retries(
                replace(task, attempt_count=attempt), policy, pool=pool
            )
            assert obs.outcome is FetchOutcomeKind.RETRY_AT, obs.outcome
            delays.append((obs.retry_at - obs.fetched_at).total_seconds())
    finally:
        await pool.aclose()
    assert delays[0] <= 5.0 < 20.0 <= delays[1], delays


def test_r1_db_retry_delay_bounds():
    from signal_ingest.crawl.fetch import db_retry_delay

    for attempt in range(1, 20):
        d = db_retry_delay(attempt)
        high = min(3600.0, 5.0 * 2 ** (attempt - 1))
        assert high / 2 <= d <= high
    assert db_retry_delay(3, retry_after=7200) == 3600.0
    assert db_retry_delay(3, retry_after=90) == 90.0


@pytest.mark.asyncio
async def test_r1_gzip_bomb_rejected_without_full_inflate(crawl):
    import gzip

    import httpx

    bomb = gzip.compress(b"\0" * 20_000_000)
    assert len(bomb) < 64 * 1024

    def handler(request):
        return httpx.Response(
            200, stream=httpx.ByteStream(bomb), headers={"content-encoding": "gzip"}
        )

    pool = crawl.HttpPool(
        crawl.PoolLimits(max_compressed_bytes=64 * 1024, max_decoded_bytes=128 * 1024),
        transport=httpx.MockTransport(handler),
    )
    import tracemalloc

    tracemalloc.start()
    try:
        with pytest.raises(crawl.BodyTooLargeError):
            await pool.request("GET", "http://127.0.0.1/bomb")
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        await pool.aclose()
    assert peak < 5_000_000, f"inflated {peak} bytes before rejecting"


@pytest.mark.asyncio
async def test_r1_pacing_one_origin_does_not_block_another(crawl):
    import time

    import httpx

    pool = crawl.HttpPool(
        crawl.PoolLimits(per_origin_min_interval_s=0.6),
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, stream=httpx.ByteStream(b"ok"))
        ),
    )
    try:
        await pool.request("GET", "http://127.0.0.1:1/a")
        slow = asyncio.create_task(pool.request("GET", "http://127.0.0.1:1/b"))
        await asyncio.sleep(0.05)
        t0 = time.monotonic()
        await pool.request("GET", "http://127.0.0.2:2/c")
        assert time.monotonic() - t0 < 0.3
        await slow
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_r1_robots_5xx_retries_later_instead_of_allow_all(crawl):
    import httpx

    from signal_ingest.crawl.types import FetchOutcomeKind

    hits = {"page": 0}

    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(503, stream=httpx.ByteStream(b""))
        hits["page"] += 1
        return httpx.Response(
            200, stream=httpx.ByteStream(b"ok"), headers={"content-type": "text/plain"}
        )

    pool = crawl.HttpPool(transport=httpx.MockTransport(handler))
    policy = _policy(crawl)
    try:
        obs = await crawl.fetch("http://127.0.0.1:1/page", policy, pool=pool)
    finally:
        await pool.aclose()
    assert obs.outcome is FetchOutcomeKind.RETRY_AT
    assert "robots" in (obs.reason or "")
    assert hits["page"] == 0


def _r1_work(settings, run_id, **kw):
    from signal_ingest.pipeline.work import work_stage

    return work_stage(run_id=run_id, stage="fetch", settings=settings, **kw)


def test_r1_concurrent_slots_beat_single_slot(crawl_env):
    """4 slots overlap slow fetches; wall time well under the serial run."""
    import time

    from signal_ingest.config import SystemClock, get_clock, set_clock

    previous = get_clock()
    set_clock(SystemClock())
    server = start_b1_http_server(slow_delay_s=0.4)
    try:
        timings = {}
        for slots in (1, 4):
            run_id, _, _ = _r1_task(crawl_env, server.url("/slow"), n=6)
            t0 = time.monotonic()
            out = _r1_work(crawl_env, run_id, concurrency=slots, max_tasks=6)
            timings[slots] = time.monotonic() - t0
            assert out["processed"] == 6
            assert {o["outcome"] for o in out["outcomes"]} == {"ok"}
        assert timings[4] < timings[1] * 0.7, timings
    finally:
        server.stop()
        set_clock(previous)


def test_r1_heartbeat_keeps_long_task_from_being_stolen(crawl_env):
    """Task runs longer than its lease; heartbeat renews so a rival worker never claims it."""
    import threading
    import time

    from signal_ingest.config import SystemClock, get_clock, set_clock
    from signal_ingest.crawl.queue import claim_tasks
    from signal_ingest.storage import connect

    previous = get_clock()
    set_clock(SystemClock())
    server = start_b1_http_server(slow_delay_s=3.0)
    stolen: list = []
    try:
        run_id, source_id, (tid,) = _r1_task(crawl_env, server.url("/slow"))
        done = threading.Event()

        def rival():
            with connect(crawl_env) as conn:
                # Contend only once the real worker holds the lease.
                while not done.is_set():
                    owner = conn.execute(
                        "SELECT lease_owner FROM tasks WHERE id = %s", (str(tid),)
                    ).fetchone()[0]
                    conn.commit()
                    if owner == "worker-1":
                        break
                    time.sleep(0.05)
                while not done.is_set():
                    stolen.extend(claim_tasks(conn, worker_id="rival", source_id=source_id))
                    time.sleep(0.2)

        t = threading.Thread(target=rival)
        t.start()
        try:
            out = _r1_work(crawl_env, run_id, lease_seconds=1, max_tasks=1)
        finally:
            done.set()
            t.join()
        assert stolen == []
        assert out["outcomes"][0]["ok"] is True
        assert _r1_status(crawl_env, tid)["status"] == "done"
    finally:
        server.stop()
        set_clock(previous)


def test_r1_worker_survives_task_exception(crawl_env, monkeypatch):
    """One task raising does not kill the loop; it is rescheduled with last_error."""
    import signal_ingest.pipeline.work as work_mod

    run_id, _, ids = _r1_task(crawl_env, "http://127.0.0.1:9/x", n=3)
    calls = {"n": 0}

    async def flaky(conn, task, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return {"ok": True, "outcome": "ok"}

    monkeypatch.setattr(work_mod, "process_claimed_task", flaky)
    out = _r1_work(crawl_env, run_id, max_tasks=3)
    assert out["processed"] == 3
    rows = [_r1_status(crawl_env, i) for i in ids]
    errored = [r for r in rows if r["last_error"] and "boom" in r["last_error"]]
    assert len(errored) == 1 and errored[0]["status"] == "ready"
    assert errored[0]["next_attempt"] > errored[0]["updated_at"]


@pytest.mark.asyncio
@pytest.mark.parametrize("coding", ["br", "gzip, gzip", "zstd"])
async def test_r1_uncapped_encodings_rejected(crawl, coding):
    """Encodings we cannot inflate under a cap are rejected, never decoded in full."""
    import gzip

    import httpx

    body = gzip.compress(gzip.compress(b"\0" * 5_000_000))

    def handler(request):
        return httpx.Response(
            200, stream=httpx.ByteStream(body), headers={"content-encoding": coding}
        )

    pool = crawl.HttpPool(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(crawl.DecodingError):
            await pool.request("GET", "http://127.0.0.1/enc")
    finally:
        await pool.aclose()


def test_r1_drain_with_source_filter_terminates(crawl_env):
    """--drain --source X must exit even when another source in the run has ready work."""
    import uuid

    from signal_ingest.crawl.queue import enqueue_task
    from signal_ingest.storage import connect

    run_id, source_id, _ = _r1_task(crawl_env, "http://127.0.0.1:9/x", n=1)
    with connect(crawl_env) as conn:
        enqueue_task(
            conn,
            source_id=f"other-{uuid.uuid4().hex[:8]}",
            url="http://127.0.0.1:9/y",
            run_id=run_id,
            external_id="other",
        )
        conn.execute(
            "UPDATE tasks SET status='done' WHERE run_id=%s AND source_id=%s",
            (str(run_id), source_id),
        )
        conn.commit()
    import signal_ingest.pipeline.work as work_mod

    # Old behavior hung forever here; a timeout turns that into a failure.
    out = asyncio.run(
        asyncio.wait_for(
            work_mod._work_loop(
                run_id=run_id, stage="fetch", worker_id="w", settings=crawl_env,
                max_tasks=5, source_id=source_id, drain=True, idle_poll_s=0.05,
            ),
            timeout=10,
        )
    )
    assert out["processed"] == 0


def test_r1_slot_survives_db_error_on_claim(crawl_env, monkeypatch):
    """A dropped DB connection during claim backs off and continues, not crash."""
    import psycopg

    import signal_ingest.pipeline.work as work_mod

    run_id, _, ids = _r1_task(crawl_env, "http://127.0.0.1:9/x", n=2)
    real_claim = work_mod.claim_tasks
    calls = {"n": 0}

    def flaky_claim(conn, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise psycopg.OperationalError("server closed the connection unexpectedly")
        return real_claim(conn, **kw)

    async def ok(conn, task, **kw):
        return {"ok": True, "outcome": "ok"}

    monkeypatch.setattr(work_mod, "claim_tasks", flaky_claim)
    monkeypatch.setattr(work_mod, "process_claimed_task", ok)
    result = asyncio.run(
        work_mod._work_loop(
            run_id=run_id, stage="fetch", worker_id="w", settings=crawl_env,
            max_tasks=2, source_id=None, idle_poll_s=0.05,
        )
    )
    assert result["processed"] == 2


@pytest.mark.asyncio
async def test_r3_auth_header_stripped_on_cross_origin_redirect(crawl):
    """A token sent to origin A must not follow a redirect to origin B."""
    import httpx

    seen: dict[str, str | None] = {}

    def handler(request):
        if request.url.host == "127.0.0.1":
            return httpx.Response(
                302, headers={"location": "http://127.0.0.2:2/landing"},
                stream=httpx.ByteStream(b""),
            )
        seen["auth"] = request.headers.get("authorization")
        return httpx.Response(200, stream=httpx.ByteStream(b"ok"),
                              headers={"content-type": "text/plain"})

    pool = crawl.HttpPool(transport=httpx.MockTransport(handler))
    policy = _policy(crawl, respect_robots=False)
    try:
        obs = await crawl.fetch("http://127.0.0.1:1/start", policy, pool=pool,
                                headers={"Authorization": "Bearer test-secret"})
    finally:
        await pool.aclose()
    assert obs.outcome.value == "ok", obs.reason
    assert seen["auth"] is None
