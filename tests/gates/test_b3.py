"""B3 gate: news/jobs monitors and original publication freshness.

Oracle: docs/build-plan.md (B3) and docs/contracts.md Time and freshness.
Cases must FAIL when behavior is missing/wrong and PASS when integrated correctly.
The gate runner rejects skipped cases.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from tests.conftest import FROZEN_NOW
from tests.support import FIXTURES

MONITOR_FIXTURES = FIXTURES / "monitors"
T = FROZEN_NOW  # 2026-11-21T12:00:00Z
WINDOW_LOWER = T - timedelta(hours=24)

EXPECTED_MONITOR_IDS = [
    "news_mit",
    "news_sciencedaily",
    "news_techcrunch",
    "news_arstechnica",
    "news_guardian",
    "jobs_aijobs",
    "jobs_moaijobs",
    "jobs_inferencejobs",
    "jobs_aiworkportal",
    "jobs_aicommunity",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_scenarios() -> dict[str, Any]:
    return json.loads((MONITOR_FIXTURES / "scenarios.json").read_text(encoding="utf-8"))


def _load_source_fixture(source_id: str) -> dict[str, Any]:
    path = MONITOR_FIXTURES / "sources" / f"{source_id}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _raw(
    url: str,
    payload: Any,
    *,
    content_type: str = "application/json",
    fetched_at: datetime | None = None,
) -> Any:
    from signal_ingest.crawl.types import FetchOutcomeKind, RawObservation

    if isinstance(payload, (dict, list)):
        body = json.dumps(payload).encode("utf-8")
    elif isinstance(payload, bytes):
        body = payload
    else:
        body = str(payload).encode("utf-8")
    return RawObservation(
        request_url=url,
        final_url=url,
        status_code=200,
        outcome=FetchOutcomeKind.OK,
        body=body,
        content_type=content_type,
        fetched_at=fetched_at or T,
        metadata={"synthetic": True, "not_for_live_export": True},
    )


def _interval_at(instant: datetime, *, method: str = "test"):
    from signal_ingest.freshness import PublicationInterval

    return PublicationInterval(
        lower=instant,
        upper=instant,
        precision="instant",
        method=method,
        raw=instant.isoformat(),
        tz="UTC",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def monitor_env(tmp_fixture_env, frozen_clock):
    """Fixture-mode settings + B3 monitor_runs schema."""
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
            WHERE table_schema = 'public' AND table_name = 'monitor_runs'
            """
        ).fetchone()
        assert row is not None and int(row[0]) == 1, (
            "B3 schema missing: expected monitor_runs (migrations/004_b3_monitors.sql)"
        )
        conn.execute(
            "TRUNCATE monitor_runs, quarantines, candidates RESTART IDENTITY CASCADE"
        )
        conn.execute(
            """
            DELETE FROM observations
            WHERE source_id LIKE 'news_%%' OR source_id LIKE 'jobs_%%'
            """
        )
        conn.execute(
            """
            DELETE FROM tasks
            WHERE source_id LIKE 'news_%%' OR source_id LIKE 'jobs_%%'
            """
        )
        conn.execute(
            """
            DELETE FROM source_checkpoints
            WHERE source_id LIKE 'news_%%' OR source_id LIKE 'jobs_%%'
            """
        )
        conn.commit()
    return settings


# ---------------------------------------------------------------------------
# Fixture labeling
# ---------------------------------------------------------------------------


def test_monitor_fixtures_labeled_synthetic():
    """All B3 monitor fixtures stay labeled synthetic / not for live export."""
    scenarios = _load_scenarios()
    assert scenarios["_meta"]["label"] == "synthetic"
    assert scenarios["_meta"]["not_for_live_export"] is True
    for path in sorted(MONITOR_FIXTURES.rglob("*")):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        if path.suffix == ".json":
            data = json.loads(text)
            meta = data.get("_meta") or {}
            if meta:
                assert meta.get("label") == "synthetic"
                assert meta.get("not_for_live_export") is True
            else:
                assert data.get("_synthetic") is True
        else:
            assert "synthetic" in text.lower()


# ---------------------------------------------------------------------------
# Frozen-clock freshness matrix
# ---------------------------------------------------------------------------


def test_endpoint_T_and_T_minus_24h_admit(frozen_clock):
    """Closed window includes exact T and exact T−24h."""
    from signal_ingest.freshness import in_freshness_window

    assert in_freshness_window(_interval_at(T), T) is True
    assert in_freshness_window(_interval_at(WINDOW_LOWER), T) is True


def test_one_second_older_than_window_rejects(frozen_clock):
    """T−24h−1s must not admit (detects exclusive lower-bound bug)."""
    from signal_ingest.freshness import in_freshness_window

    too_old = WINDOW_LOWER - timedelta(seconds=1)
    assert in_freshness_window(_interval_at(too_old), T) is False


def test_future_after_T_rejects(frozen_clock):
    """Publication after cutoff T is rejected."""
    from signal_ingest.freshness import in_freshness_window

    future = T + timedelta(seconds=1)
    assert in_freshness_window(_interval_at(future), T) is False


def test_timezone_offset_normalized_to_utc_window(frozen_clock):
    """Offset timestamps admit only after UTC normalization into [T−24h,T]."""
    from signal_ingest.freshness import in_freshness_window, parse_publication_value

    # 2026-11-21 04:00-08:00 == 12:00 UTC == T → admit
    at_t = parse_publication_value(
        "2026-11-21T04:00:00-08:00",
        field_name="datePublished",
    )
    assert at_t is not None
    assert in_freshness_window(at_t, T) is True

    # 2026-11-20 03:59:59-08:00 == 11:59:59 UTC on 20th → 1s before WINDOW_LOWER
    too_old = parse_publication_value(
        "2026-11-20T03:59:59-08:00",
        field_name="datePublished",
    )
    assert too_old is not None
    assert in_freshness_window(too_old, T) is False


def test_dst_ambiguity_requires_full_plausible_range(frozen_clock):
    """Ambiguous fall-back local time: admit only if ALL folds lie in the window."""
    from signal_ingest.freshness import PublicationInterval, in_freshness_window

    tz = ZoneInfo("America/New_York")
    naive = datetime(2026, 11, 1, 1, 30, 0)
    fold0 = naive.replace(tzinfo=tz, fold=0).astimezone(timezone.utc)
    fold1 = naive.replace(tzinfo=tz, fold=1).astimezone(timezone.utc)
    assert fold0 != fold1

    # Place window_lower between the two folds so only the later fold is inside.
    earlier, later = sorted([fold0, fold1])
    cutoff = earlier + timedelta(hours=24) + timedelta(minutes=30)
    window_lower = cutoff - timedelta(hours=24)
    assert earlier < window_lower < later

    wide = PublicationInterval(
        lower=earlier,
        upper=later,
        precision="ambiguous_dst",
        method="dst_fold_span",
        raw="2026-11-01T01:30:00",
        tz="America/New_York",
    )
    assert in_freshness_window(wide, cutoff) is False

    # Trap: a single-fold instant on the later fold looks fresh while the sibling
    # fold is outside — contract requires widen-or-quarantine, not silent pick.
    inside_only = _interval_at(later, method="single_fold_bug")
    assert in_freshness_window(inside_only, cutoff) is True

    from signal_ingest.freshness import parse_publication_value

    parsed = parse_publication_value(
        "2026-11-01T01:30:00",
        field_name="datePublished",
        source_timezone="America/New_York",
    )
    if parsed is not None and parsed.lower == parsed.upper:
        # Instant without widening may only survive if it does not falsely admit.
        assert in_freshness_window(parsed, cutoff) is False


def test_date_only_full_local_day_rejects_when_any_instant_outside(frozen_clock):
    """Date-only expands to the full source-local day; partial overlap rejects."""
    from signal_ingest.freshness import date_only_interval, in_freshness_window

    # Full UTC day 2026-11-21 extends past T=12:00Z.
    day = date_only_interval(date(2026, 11, 21), source_timezone="UTC")
    assert in_freshness_window(day, T) is False

    # Full America/New_York day 2026-11-20 starts before WINDOW_LOWER.
    day_et = date_only_interval(date(2026, 11, 20), source_timezone="America/New_York")
    assert in_freshness_window(day_et, T) is False


def test_date_only_admits_when_entire_day_inside_window(frozen_clock):
    """A day wholly inside a tailored cutoff still admits (endpoint-inclusive)."""
    from signal_ingest.freshness import date_only_interval, in_freshness_window

    day = date_only_interval(date(2026, 11, 21), source_timezone="UTC")
    # Cutoff must be >= day.upper (microsecond end-of-day).
    cutoff = day.upper
    assert in_freshness_window(day, cutoff) is True
    assert day.lower >= cutoff - timedelta(hours=24)


def test_relative_23h_ago_straddling_cutoff_rejects(frozen_clock):
    """Relative hour-floor that partially exits [T−24h,T] must reject."""
    from signal_ingest.freshness import (
        in_freshness_window,
        relative_hour_floor_interval,
    )

    # F = T−1h → "23 hours ago" ⇒ [T−25h, T−24h] straddles the lower edge.
    observed = T - timedelta(hours=1)
    interval = relative_hour_floor_interval(23, observed, raw="23 hours ago")
    assert interval.lower == observed - timedelta(hours=24)
    assert interval.upper == observed - timedelta(hours=23)
    assert interval.lower < WINDOW_LOWER
    assert interval.upper == WINDOW_LOWER
    assert in_freshness_window(interval, T) is False


def test_relative_23h_ago_fully_inside_admits(frozen_clock):
    """Same relative form admits when the whole floored band is inside."""
    from signal_ingest.freshness import (
        in_freshness_window,
        relative_hour_floor_interval,
    )

    # F = T → [T−24h, T−23h] ⊆ [T−24h, T]
    interval = relative_hour_floor_interval(23, T, raw="23 hours ago")
    assert in_freshness_window(interval, T) is True


def test_yesterday_relative_rejects_when_local_day_exits_window(frozen_clock):
    """'yesterday' expands to full local day; rejects when any instant is old."""
    from signal_ingest.freshness import in_freshness_window, yesterday_interval

    interval = yesterday_interval(T, source_timezone="America/New_York", raw="yesterday")
    assert in_freshness_window(interval, T) is False


def test_missing_publication_tags_quarantine(frozen_clock):
    """No publication fields → missing evidence (not observed-only promotion)."""
    from signal_ingest.freshness import select_publication_interval

    interval, reason = select_publication_interval({"title": "x"})
    assert interval is None
    assert reason == "missing_publication_evidence"


def test_first_seen_only_not_publication_proof(frozen_clock):
    """first_seen / new badge cannot prove publication freshness."""
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail
    from signal_ingest.freshness import is_non_proof_field, select_publication_interval

    assert is_non_proof_field("first_seen") is True
    interval, reason = select_publication_interval(
        {"first_seen": "2026-11-21T11:00:00+00:00"}
    )
    assert interval is None
    assert reason == "first_seen_or_non_proof_only"

    scenarios = _load_scenarios()
    cfg = load_monitor_config("news_mit")
    raw = _raw(
        "https://example.test/news/first-seen-1",
        scenarios["first_seen_only"],
    )
    result = parse_detail(raw, cfg, cutoff=T)
    assert result.ok is False
    assert result.eligible is False
    assert result.quarantine_reason in {
        "first_seen_or_non_proof_only",
        "missing_publication_evidence",
    }


def test_republished_old_with_fresh_modified_rejects(frozen_clock):
    """Old datePublished + fresh modified/updated must not reopen the window."""
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail
    from signal_ingest.freshness import in_freshness_window, select_publication_interval

    scenarios = _load_scenarios()
    row = scenarios["republished_structured"]
    interval, reason = select_publication_interval(
        {
            "datePublished": row["datePublished"],
            "dateModified": row["dateModified"],
            "updated": row["updated"],
        },
        observed_at=T,
    )
    assert reason is None
    assert interval is not None
    assert in_freshness_window(interval, T) is False

    cfg = load_monitor_config("news_techcrunch")
    result = parse_detail(
        _raw(row["url"], row),
        cfg,
        cutoff=T,
    )
    assert result.ok is True  # body complete + parseable
    assert result.eligible is False  # not proven-fresh
    assert result.publication is not None
    assert in_freshness_window(result.publication, T) is False


def test_conflicting_primary_publication_quarantines(frozen_clock):
    """Disagreeing primary proofs → quarantine (no silent precedence)."""
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail
    from signal_ingest.freshness import select_publication_interval

    scenarios = _load_scenarios()
    row = scenarios["conflict_structured"]
    interval, reason = select_publication_interval(
        {
            "datePublished": row["datePublished"],
            "datePosted": row["datePosted"],
        },
        observed_at=T,
    )
    assert interval is None
    assert reason == "conflicting_publication_evidence"

    cfg = load_monitor_config("news_mit")
    result = parse_detail(_raw(row["url"], row), cfg, cutoff=T)
    assert result.ok is False
    assert result.quarantine_reason == "conflicting_publication_evidence"


# ---------------------------------------------------------------------------
# Body completeness
# ---------------------------------------------------------------------------


def test_teaser_incomplete_body(frozen_clock):
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail
    from signal_ingest.adapters.monitors.body import extract_body

    html = (MONITOR_FIXTURES / "html" / "teaser_only.html").read_text(encoding="utf-8")
    body = extract_body(html)
    assert body.complete is False

    scenarios = _load_scenarios()
    cfg = load_monitor_config("news_mit")
    result = parse_detail(
        _raw(scenarios["teaser_structured"]["url"], scenarios["teaser_structured"]),
        cfg,
        cutoff=T,
    )
    assert result.ok is False
    assert result.body_complete is False
    assert result.quarantine_reason == "incomplete_body"


def test_truncated_rss_incomplete_or_requires_detail(frozen_clock):
    """Truncated RSS description must not satisfy full-body requirement."""
    from signal_ingest.adapters.monitors import discover_list, load_monitor_config
    from signal_ingest.adapters.monitors.body import extract_body

    feed = (MONITOR_FIXTURES / "feeds" / "truncated.rss").read_text(encoding="utf-8")
    cfg = load_monitor_config("news_mit")
    scan = discover_list(cfg, feed, cutoff=T, observed_at=T, feed_ordered=True)
    assert scan.scanned_count >= 1
    # Listing summaries are not full bodies — detail parse of truncated marker fails.
    truncated_html = (
        "<article><div class='truncated' data-truncated='true'>"
        "<p>FIRST_PARA_RSS Opening teaser only… [truncated]</p>"
        "</div></article>"
    )
    body = extract_body(truncated_html)
    assert body.complete is False


def test_missing_last_paragraph_incomplete(frozen_clock):
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail
    from signal_ingest.adapters.monitors.body import extract_body

    html = (MONITOR_FIXTURES / "html" / "missing_last_para.html").read_text(
        encoding="utf-8"
    )
    body = extract_body(
        html,
        expected_start="FIRST_PARA_SYNTHETIC_START",
        expected_end="LAST_PARA_SYNTHETIC_END",
    )
    assert body.complete is False
    assert "missing_end" in body.evidence.get("incomplete_reasons", [])

    scenarios = _load_scenarios()
    cfg = load_monitor_config("news_mit")
    result = parse_detail(
        _raw(
            scenarios["missing_last_structured"]["url"],
            scenarios["missing_last_structured"],
        ),
        cfg,
        cutoff=T,
        expected_end="LAST_PARA_SYNTHETIC_END",
    )
    assert result.ok is False
    assert result.quarantine_reason == "incomplete_body"


def test_full_html_excludes_sidebar_keeps_start_and_end(frozen_clock):
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail
    from signal_ingest.adapters.monitors.body import extract_body

    html = (MONITOR_FIXTURES / "html" / "full_with_sidebar.html").read_text(
        encoding="utf-8"
    )
    body = extract_body(
        html,
        expected_start="FIRST_PARA_SYNTHETIC_START",
        expected_end="LAST_PARA_SYNTHETIC_END",
    )
    assert body.complete is True
    assert "sidebar" not in body.text.lower()
    assert "unrelated nav" not in body.text.lower()
    assert "FIRST_PARA_SYNTHETIC_START" in body.text
    assert "LAST_PARA_SYNTHETIC_END" in body.text
    assert "FIRST_PARA_SYNTHETIC_START" in body.start_excerpt or body.start_excerpt
    assert "LAST_PARA_SYNTHETIC_END" in body.end_excerpt

    cfg = load_monitor_config("news_mit")
    result = parse_detail(
        _raw(
            "https://news.mit.edu/2026/synth-full",
            html.encode("utf-8"),
            content_type="text/html",
        ),
        cfg,
        cutoff=T,
        expected_start="FIRST_PARA_SYNTHETIC_START",
        expected_end="LAST_PARA_SYNTHETIC_END",
    )
    assert result.ok is True
    assert result.eligible is True
    assert result.body_complete is True
    assert "sidebar" not in result.payload["content"]["text"].lower()
    assert "FIRST_PARA_SYNTHETIC_START" in result.payload["content"]["text"]
    assert "LAST_PARA_SYNTHETIC_END" in result.payload["content"]["text"]


def test_html_missing_tags_quarantines(frozen_clock):
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail

    html = (MONITOR_FIXTURES / "html" / "missing_tags.html").read_bytes()
    cfg = load_monitor_config("news_mit")
    result = parse_detail(
        _raw("https://example.test/missing-tags", html, content_type="text/html"),
        cfg,
        cutoff=T,
    )
    assert result.ok is False
    assert result.quarantine_reason == "missing_publication_evidence"


def test_html_republished_old_not_eligible(frozen_clock):
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail
    from signal_ingest.freshness import in_freshness_window

    html = (MONITOR_FIXTURES / "html" / "republished_old.html").read_bytes()
    cfg = load_monitor_config("news_techcrunch")
    result = parse_detail(
        _raw("https://example.test/republished", html, content_type="text/html"),
        cfg,
        cutoff=T,
        expected_start="FIRST_PARA_OLD",
        expected_end="LAST_PARA_OLD",
    )
    assert result.publication is not None
    assert in_freshness_window(result.publication, T) is False
    assert result.eligible is False


# ---------------------------------------------------------------------------
# Jobs: two boards, identity, rerun, empty scan
# ---------------------------------------------------------------------------


def test_same_listing_two_boards_preserves_observations_and_employer_url(frozen_clock):
    """Same requisition on two boards → two observations, shared employer URL/id."""
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail

    scenarios = _load_scenarios()
    a = scenarios["job_board_a"]
    b = scenarios["job_board_b"]
    cfg_a = load_monitor_config("jobs_aijobs")
    cfg_b = load_monitor_config("jobs_moaijobs")
    ra = parse_detail(
        _raw(a["url"], a),
        cfg_a,
        cutoff=T,
        board_url=a["board_url"],
        employer_url=a["employer_url"],
    )
    rb = parse_detail(
        _raw(b["url"], b),
        cfg_b,
        cutoff=T,
        board_url=b["board_url"],
        employer_url=b["employer_url"],
    )
    assert ra.ok and rb.ok
    assert ra.eligible and rb.eligible
    assert ra.board_url != rb.board_url
    assert ra.payload["employer_url"] == rb.payload["employer_url"]
    assert ra.payload["content"]["original_employer_job_url"] == a["employer_url"]
    assert rb.payload["content"]["original_employer_job_url"] == b["employer_url"]
    # Logical identity prefers requisition / employer URL, not board listing URL.
    assert ra.external_id == rb.external_id or (
        ra.payload["content"]["original_employer_job_url"]
        == rb.payload["content"]["original_employer_job_url"]
    )


def test_unordered_feed_does_not_stop_before_fresh_item(frozen_clock):
    """Unordered lists must keep scanning after an old row (no early stop)."""
    from signal_ingest.adapters.monitors import discover_list, load_monitor_config

    scenarios = _load_scenarios()
    cfg = load_monitor_config("news_mit")
    scan = discover_list(
        cfg,
        scenarios["unordered_list"],
        cutoff=T,
        observed_at=T,
        feed_ordered=False,
    )
    assert scan.early_stopped is False
    ids = {item.external_id for item in scan.items}
    assert "old-1" in ids
    assert "fresh-1" in ids


def test_ordered_feed_proves_descending_before_cutoff(frozen_clock):
    from signal_ingest.adapters.monitors import (
        discover_list,
        load_monitor_config,
        prove_descending,
    )

    scenarios = _load_scenarios()
    cfg = load_monitor_config("news_mit")
    scan = discover_list(
        cfg,
        scenarios["ordered_list"],
        cutoff=T,
        observed_at=T,
        feed_ordered=True,
    )
    assert scan.ordered is True
    assert scan.descending_proven is True
    # Newest in-window items are retained; stale row triggers early stop after proof.
    ids = [item.external_id for item in scan.items]
    assert "ord-1" in ids
    assert "ord-3" not in ids
    assert scan.early_stopped is True
    # prove_descending helper itself rejects ascending sequences.
    ascending = [
        _interval_at(T - timedelta(hours=3)),
        _interval_at(T - timedelta(hours=1)),
    ]
    assert prove_descending(ascending) is False
    descending = [
        _interval_at(T - timedelta(hours=1)),
        _interval_at(T - timedelta(hours=3)),
    ]
    assert prove_descending(descending) is True


def test_rerun_at_t_plus_2h_refilters_window(frozen_clock):
    """Rerun with cutoff T+2h drops aged-out rows and can admit newer ones."""
    from signal_ingest.adapters.monitors import discover_list, load_monitor_config
    from signal_ingest.freshness import in_freshness_window, parse_publication_value

    scenarios = _load_scenarios()
    cfg = load_monitor_config("news_mit")
    t2 = T + timedelta(hours=2)
    scan = discover_list(
        cfg,
        scenarios["rerun_list_t_plus_2h"],
        cutoff=t2,
        observed_at=t2,
        feed_ordered=True,
    )
    by_id = {item.external_id: item for item in scan.items}
    # Freshness filter at export/parse time: aged-out must not be eligible at t2.
    aged = parse_publication_value(
        "2026-11-20T12:30:00+00:00", field_name="datePublished"
    )
    kept = parse_publication_value(
        "2026-11-21T11:00:00+00:00", field_name="datePublished"
    )
    newer = parse_publication_value(
        "2026-11-21T13:30:00+00:00", field_name="datePublished"
    )
    assert aged is not None and in_freshness_window(aged, t2) is False
    assert kept is not None and in_freshness_window(kept, t2) is True
    assert newer is not None and in_freshness_window(newer, t2) is True
    assert "rerun-new" in by_id or scan.scanned_count >= 1


def test_legitimate_empty_scan_is_healthy_empty_not_failed(monitor_env, frozen_clock):
    from signal_ingest.adapters.monitors import (
        MonitorHealth,
        classify_health,
        discover_and_enqueue,
        finalize_monitor_run,
        get_latest_monitor_run,
    )
    from signal_ingest.storage import connect, create_run

    scenarios = _load_scenarios()
    settings = monitor_env
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings)
        meta = discover_and_enqueue(
            conn,
            source_id="news_mit",
            payload=scenarios["empty_scan"],
            run_id=run_id,
            cutoff=T,
            observed_at=T,
            feed_ordered=True,
        )
        assert meta["discovered"] == 0
        health = classify_health(
            accepted_count=0,
            partial=False,
            failed=False,
            rate_limited=False,
        )
        assert health == MonitorHealth.HEALTHY_EMPTY
        # Ledger metadata must be JSON-safe (ISO cutoff), not raw datetime objects.
        finalize_monitor_run(
            conn,
            source_id="news_mit",
            cutoff_at=T,
            discover_meta={
                "discovered": meta["discovered"],
                "scan": meta["scan"],
                "pagination_complete": True,
                "cutoff": T.isoformat(),
            },
            run_id=run_id,
            accepted_count=0,
        )
        conn.commit()
        latest = get_latest_monitor_run(conn, source_id="news_mit")
        assert latest is not None
        assert latest["health"] == MonitorHealth.HEALTHY_EMPTY.value
        assert latest["accepted_count"] == 0


def test_health_states_distinguishable(frozen_clock):
    from signal_ingest.adapters.monitors import MonitorHealth, classify_health

    assert classify_health(accepted_count=2) == MonitorHealth.HEALTHY
    assert classify_health(accepted_count=0) == MonitorHealth.HEALTHY_EMPTY
    assert (
        classify_health(accepted_count=1, partial=True, limitation="capped_feed")
        == MonitorHealth.PARTIAL
    )
    assert classify_health(rate_limited=True) == MonitorHealth.RATE_LIMITED
    assert classify_health(failed=True) == MonitorHealth.FAILED
    # Empty must not collapse into failed.
    assert classify_health(accepted_count=0, failed=False) != MonitorHealth.FAILED


# ---------------------------------------------------------------------------
# Per-source parser contracts (10 monitors)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("source_id", EXPECTED_MONITOR_IDS)
def test_per_source_parser_contract(source_id: str, frozen_clock):
    """Each configured monitor parses its synthetic detail fixture via adapter."""
    from signal_ingest.adapters.monitors import (
        MONITOR_SOURCES,
        get_adapter,
        list_monitor_source_ids,
        load_monitor_config,
        parse_detail,
    )

    ids = list_monitor_source_ids()
    assert set(EXPECTED_MONITOR_IDS).issubset(set(ids))
    assert source_id in MONITOR_SOURCES

    cfg = load_monitor_config(source_id)
    assert cfg.source_id == source_id
    assert cfg.original_time_field
    assert cfg.original_timezone

    fixture = _load_source_fixture(source_id)
    assert fixture["_meta"]["source_id"] == source_id
    assert fixture["_meta"]["not_for_live_export"] is True

    adapter = get_adapter(source_id)
    assert adapter is not None
    raw = _raw(fixture["url"], fixture)
    if hasattr(adapter, "parse_detail"):
        result = adapter.parse_detail(raw, cutoff=T)
    else:
        result = parse_detail(raw, cfg, cutoff=T)

    assert result.ok is True, result.quarantine_reason
    assert result.body_complete is True
    assert result.eligible is True
    assert result.publication is not None
    assert result.record_type in {"NEWS", "JOB"}
    if result.record_type == "NEWS":
        text = result.payload["content"]["text"]
        assert text
        assert "FIRST_PARA" in text or result.evidence["body"]["start_excerpt"]
    else:
        assert result.payload["content"]["original_employer_job_url"]
        assert result.board_url or result.payload.get("board_url")


def test_monitor_adapters_cover_exactly_ten_configured_sources(frozen_clock):
    from signal_ingest.adapters.monitors import ADAPTERS, MONITOR_SOURCES

    assert len(MONITOR_SOURCES) == 10
    assert set(ADAPTERS) == set(EXPECTED_MONITOR_IDS)


# ---------------------------------------------------------------------------
# Plausible defect the happy path would miss
# ---------------------------------------------------------------------------


def test_modified_only_cannot_prove_freshness(frozen_clock):
    """Happy-path fresh article would miss: modified-only fields look 'new'."""
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail
    from signal_ingest.freshness import select_publication_interval

    interval, reason = select_publication_interval(
        {"dateModified": "2026-11-21T11:59:00+00:00", "updated": "2026-11-21T11:59:00+00:00"},
        observed_at=T,
    )
    assert interval is None
    assert reason in {"first_seen_or_non_proof_only", "missing_publication_evidence"}

    cfg = load_monitor_config("news_techcrunch")
    result = parse_detail(
        _raw(
            "https://example.test/modified-only",
            {
                "_synthetic": True,
                "title": "Looks new via modified",
                "dateModified": "2026-11-21T11:59:00+00:00",
                "paragraphs": ["FIRST_PARA x", "LAST_PARA y"],
            },
        ),
        cfg,
        cutoff=T,
    )
    assert result.ok is False
    assert result.eligible is not True


def test_straddling_interval_rejects_even_if_midpoint_inside(frozen_clock):
    """Containment must use entire interval — midpoint-inside is insufficient."""
    from signal_ingest.freshness import PublicationInterval, in_freshness_window

    straddling = PublicationInterval(
        lower=WINDOW_LOWER - timedelta(seconds=1),
        upper=WINDOW_LOWER + timedelta(hours=1),
        precision="range",
        method="synthetic_straddle",
    )
    midpoint = WINDOW_LOWER + timedelta(minutes=30)
    assert WINDOW_LOWER <= midpoint <= T
    assert in_freshness_window(straddling, T) is False


# R3-A
# ---------------------------------------------------------------------------
# R3-A regressions: date parsing, sidebar/time traps, JSON-LD @graph, body
# extraction, watermark resume and the new-since-last-run heuristic.
# ---------------------------------------------------------------------------

R3_FIXTURES = MONITOR_FIXTURES / "r3"


@pytest.fixture
def ist_process_tz():
    """Run under Asia/Kolkata so local-time conversions show a 5.5h shift."""
    import os
    import time

    previous = os.environ.get("TZ")
    os.environ["TZ"] = "Asia/Kolkata"
    time.tzset()
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()


def test_r3_feed_published_parsed_is_utc_not_local(ist_process_tz, frozen_clock):
    """feedparser struct_time is UTC; time.mktime read it as IST (5.5h shift)."""
    from signal_ingest.adapters.monitors import discover_list, load_monitor_config

    feed = (R3_FIXTURES / "gmt_feed.rss").read_text(encoding="utf-8")
    scan = discover_list(
        load_monitor_config("news_mit"), feed, cutoff=T, observed_at=T, feed_ordered=True
    )
    assert len(scan.items) == 1
    expected = datetime(2026, 11, 21, 10, 0, tzinfo=timezone.utc)
    # Stored as an ISO-8601 UTC string (task payloads are JSON), same instant.
    assert scan.items[0].metadata["list_fields"]["datePublished"] == expected.isoformat()
    assert scan.items[0].metadata["publication"]["lower"] == expected.isoformat()


def test_r3_rfc822_gmt_not_reassigned_source_timezone(frozen_clock):
    """A literal GMT/offset RFC 822 stamp keeps its zone (TechCrunch is LA-local)."""
    from signal_ingest.freshness import parse_publication_value

    gmt = parse_publication_value(
        "Sat, 21 Nov 2026 10:00:00 GMT",
        field_name="pubDate",
        source_timezone="America/Los_Angeles",
    )
    assert gmt is not None
    assert gmt.lower == gmt.upper == datetime(2026, 11, 21, 10, 0, tzinfo=timezone.utc)
    offset = parse_publication_value(
        "Sat, 21 Nov 2026 10:00:00 +0530", field_name="pubDate", source_timezone="UTC"
    )
    assert offset is not None
    assert offset.lower == datetime(2026, 11, 21, 4, 30, tzinfo=timezone.utc)
    # No zone token: the documented source timezone applies.
    naive = parse_publication_value(
        "Sat, 21 Nov 2026 02:00:00", field_name="pubDate", source_timezone="America/Los_Angeles"
    )
    assert naive is not None
    assert naive.lower == datetime(2026, 11, 21, 10, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("text", "lower", "upper"),
    [
        ("an hour ago", T - timedelta(hours=2), T - timedelta(hours=1)),
        ("1h", T - timedelta(hours=2), T - timedelta(hours=1)),
        ("Posted 3 hours ago", T - timedelta(hours=4), T - timedelta(hours=3)),
        ("5 mins ago", T - timedelta(minutes=6), T - timedelta(minutes=5)),
        ("2 days ago", T - timedelta(days=3), T - timedelta(days=2)),
        ("just now", T - timedelta(hours=1), T),
    ],
)
def test_r3_relative_forms_build_one_unit_intervals(text, lower, upper, frozen_clock):
    from signal_ingest.freshness import parse_publication_value

    interval = parse_publication_value(text, field_name="published", observed_at=T)
    assert interval is not None, text
    assert (interval.lower, interval.upper) == (lower, upper)
    # Relative forms without an anchor are never resolved.
    assert parse_publication_value(text, field_name="published") is None


def test_r3_updated_label_and_coarse_relative_are_not_proof(frozen_clock):
    from signal_ingest.freshness import parse_publication_value

    assert parse_publication_value("Updated 3 hours ago", field_name="published", observed_at=T) is None
    assert parse_publication_value("3 months ago", field_name="published", observed_at=T) is None


def test_r3_epoch_seconds_and_milliseconds(frozen_clock):
    from signal_ingest.freshness import parse_publication_value

    instant = datetime(2026, 11, 21, 8, 0, tzinfo=timezone.utc)
    secs = int(instant.timestamp())
    for value in (secs, str(secs), secs * 1000, str(secs * 1000)):
        interval = parse_publication_value(value, field_name="datePosted")
        assert interval is not None, value
        assert interval.lower == interval.upper == instant


def test_r3_sidebar_time_and_meta_order_cannot_prove_freshness(frozen_clock):
    """Sidebar <time> must not prove freshness; content-first meta is still read."""
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail

    html = (R3_FIXTURES / "sidebar_time_trap.html").read_bytes()
    result = parse_detail(
        _raw("https://techcrunch.com/2026/10/02/r3-old", html, content_type="text/html"),
        load_monitor_config("news_techcrunch"),
        cutoff=T,
        expected_start="FIRST_PARA_R3_START",
        expected_end="LAST_PARA_R3_END",
    )
    assert result.ok is True, result.quarantine_reason
    assert result.eligible is False
    assert result.publication is not None
    assert result.publication.lower == datetime(2026, 10, 2, 16, 0, tzinfo=timezone.utc)
    text = result.payload["content"]["text"]
    assert "SIDEBAR_TRENDING" not in text
    assert "LAST_PARA_R3_END" in text
    assert result.payload["content"]["date_precision"] == "instant"
    assert result.evidence["date_extraction_method"]


def test_r3_body_parser_recovers_after_class_sidebar(frozen_clock):
    """Fallback parser: class-skipped sidebar closes; 'unavailable' is not 'nav'."""
    from signal_ingest.adapters.monitors.body import _parse, extract_body

    html = (R3_FIXTURES / "sidebar_time_trap.html").read_text(encoding="utf-8")
    paragraphs = _parse(html).body_paragraphs()
    joined = "\n".join(paragraphs)
    assert "FIRST_PARA_R3_START" in joined
    assert "UNAVAILABLE_PARA" in joined
    assert "LAST_PARA_R3_END" in joined
    assert "SIDEBAR_TRENDING" not in joined
    assert "PROMO_TEASER" not in joined
    # Mid-body ellipsis and an out-of-article teaser class do not mark truncation.
    body = extract_body(html, expected_start="FIRST_PARA_R3_START", expected_end="LAST_PARA_R3_END")
    assert body.complete is True, body.evidence["incomplete_reasons"]
    assert body.evidence["incomplete_reasons"] == []


def test_r3_itemprop_body_region_closes(frozen_clock):
    from signal_ingest.adapters.monitors.body import _parse

    html = (
        "<html><body><p>OUTSIDE_BEFORE synthetic</p>"
        "<div itemprop='articleBody'><div><p>INSIDE_A synthetic</p></div>"
        "<p>INSIDE_B synthetic</p></div>"
        "<div class='comments'><p>COMMENT_AFTER synthetic</p></div></body></html>"
    )
    joined = "\n".join(_parse(html).body_paragraphs())
    assert "INSIDE_A" in joined and "INSIDE_B" in joined
    assert "COMMENT_AFTER" not in joined
    assert "OUTSIDE_BEFORE" not in joined


def test_r3_trailing_ellipsis_still_truncated(frozen_clock):
    from signal_ingest.adapters.monitors.body import extract_body

    html = "<article><p>FIRST synthetic opening.</p><p>Then the story continues…</p></article>"
    body = extract_body(html)
    assert body.complete is False
    assert "truncated" in body.evidence["incomplete_reasons"]


def test_r3_json_ld_graph_job_employer_and_telecommute(frozen_clock):
    """Yoast @graph JobPosting: datePosted read, employer URL off-board, TELECOMMUTE remote."""
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail

    html = (R3_FIXTURES / "yoast_graph_job.html").read_bytes()
    result = parse_detail(
        _raw("https://aijobs.ai/jobs/staff-ml-engineer-r3", html, content_type="text/html"),
        load_monitor_config("jobs_aijobs"),
        cutoff=T,
    )
    assert result.ok is True, result.quarantine_reason
    assert result.eligible is True
    assert result.payload["content"]["company"] == "Vector Forge"
    assert result.payload["content"]["is_remote"] is True
    assert result.employer_url == "https://vectorforge.example/careers/staff-ml"
    assert "aijobs.ai" not in result.payload["content"]["original_employer_job_url"]


def test_r3_plain_remote_location_is_remote(frozen_clock):
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail

    html = (
        "<html><head><title>synthetic</title><script type='application/ld+json'>"
        '[{"@type":"JobPosting","title":"Applied Scientist","datePosted":"2026-11-21T06:00:00Z",'
        '"hiringOrganization":{"name":"Synth Labs"},'
        '"jobLocation":{"@type":"Place","address":{"addressLocality":"Remote"}}}]'
        "</script></head><body><main><p>FIRST synthetic role.</p><p>LAST synthetic role.</p>"
        "</main></body></html>"
    ).encode()
    result = parse_detail(
        _raw("https://aijobs.ai/jobs/r3-remote", html, content_type="text/html"),
        load_monitor_config("jobs_aijobs"),
        cutoff=T,
    )
    assert result.ok is True, result.quarantine_reason
    assert result.payload["content"]["is_remote"] is True


def test_r3_listing_relative_date_anchored_to_listing_fetch(frozen_clock):
    """'2 hours ago' on a listing fetched at T−10h means [T−13h, T−12h], not detail F."""
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail

    html = (R3_FIXTURES / "dateless_listing_item.html").read_bytes()
    cfg = load_monitor_config("news_mit")
    raw = _raw("https://news.mit.edu/r3/dateless", html, content_type="text/html", fetched_at=T)
    listed_at = T - timedelta(hours=10)
    result = parse_detail(
        raw, cfg, cutoff=T, list_fields={"published": "2 hours ago"}, list_observed_at=listed_at
    )
    assert result.ok is True, result.quarantine_reason
    assert result.publication is not None
    assert result.publication.lower == T - timedelta(hours=13)
    assert result.publication.upper == T - timedelta(hours=12)
    # Without the listing anchor the relative value is not re-anchored to the detail fetch.
    unanchored = parse_detail(raw, cfg, cutoff=T, list_fields={"published": "2 hours ago"})
    assert unanchored.ok is False
    assert unanchored.quarantine_reason == "missing_publication_evidence"


def test_r3_cutoff_none_never_eligible(frozen_clock):
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail

    fixture = _load_source_fixture("news_mit")
    result = parse_detail(_raw(fixture["url"], fixture), load_monitor_config("news_mit"))
    assert result.ok is True
    assert result.eligible is False


def test_r3_feed_capped_uses_configured_page_size(frozen_clock):
    from dataclasses import replace

    from signal_ingest.adapters.monitors import discover_list, load_monitor_config

    items = "".join(
        f"<item><title>t{i}</title><link>https://example.test/r3/cap-{i}</link>"
        f"<guid>cap-{i}</guid><pubDate>Sat, 21 Nov 2026 0{i}:00:00 GMT</pubDate></item>"
        for i in range(3, 0, -1)
    )
    feed = f"<rss version='2.0'><channel><title>synthetic</title>{items}</channel></rss>"
    cfg = load_monitor_config("news_mit")
    sized = replace(cfg, raw={**cfg.raw, "feed_page_size": 3})
    assert discover_list(sized, feed, cutoff=T, observed_at=T).capped_feed is True
    assert discover_list(cfg, feed, cutoff=T, observed_at=T).capped_feed is False


def _r3_rows(*ids: str) -> dict[str, Any]:
    return {
        "_synthetic": True,
        "ordered": True,
        "items": [{"url": f"https://example.test/r3/{i}", "external_id": i, "title": i} for i in ids],
    }


def test_r3_watermark_resume_keeps_items_prepended_to_feed(frozen_clock):
    """Index checkpoints skipped new top-of-feed items; watermark stops at the old newest."""
    from signal_ingest.adapters.monitors import discover_list, load_monitor_config

    cfg = load_monitor_config("news_mit")
    first = discover_list(cfg, _r3_rows("a", "b", "c"), cutoff=T, observed_at=T)
    assert [i.external_id for i in first.items] == ["a", "b", "c"]
    assert first.cursor and first.cursor.startswith("wm:")
    for item in first.items:
        assert "new_since_last_run" not in item.metadata  # no previous run to compare

    second = discover_list(
        cfg, _r3_rows("n1", "n2", "a", "b", "c"), checkpoint=first.cursor, cutoff=T, observed_at=T
    )
    assert [i.external_id for i in second.items] == ["n1", "n2"]
    assert second.metadata["watermark_reached"] is True
    assert '"id":"n1"' in second.cursor
    flag = second.items[0].metadata["new_since_last_run"]
    assert flag["precision"] == "heuristic" and flag["basis"] == "new_since_last_run"

    # Watermark gone (feed overflowed): scan everything and flag a possible gap.
    third = discover_list(cfg, _r3_rows("x1", "x2"), checkpoint=second.cursor, cutoff=T, observed_at=T)
    assert [i.external_id for i in third.items] == ["x1", "x2"]
    assert third.limitation == "watermark_not_found_gap_possible"
    assert all("new_since_last_run" not in i.metadata for i in third.items)


def test_r3_dateless_new_since_last_run_is_observed_only(frozen_clock):
    """Heuristic rows are labeled, never date-proven fresh, never given a date."""
    from signal_ingest.adapters.monitors import load_monitor_config, parse_detail

    html = (R3_FIXTURES / "dateless_listing_item.html").read_bytes()
    cfg = load_monitor_config("news_mit")
    raw = _raw("https://news.mit.edu/r3/dateless", html, content_type="text/html")
    plain = parse_detail(raw, cfg, cutoff=T)
    assert plain.ok is False and plain.quarantine_reason == "missing_publication_evidence"

    flagged = parse_detail(
        raw,
        cfg,
        cutoff=T,
        new_since_last_run={"precision": "heuristic", "basis": "new_since_last_run"},
    )
    assert flagged.ok is True
    assert flagged.eligible is False
    assert flagged.publication is None
    content = flagged.payload["content"]
    assert content["date"] is None
    assert content["date_precision"] == "heuristic"
    assert flagged.payload["publication"]["basis"] == "new_since_last_run"
    assert flagged.payload["publication"]["observed_only"] is True


def test_r3_new_since_flag_dropped_for_previously_seen_ids(monitor_env, frozen_clock):
    """A row above the watermark that an earlier run already enqueued is not 'new'."""
    from signal_ingest.adapters.monitors import discover_and_enqueue, discover_list, load_monitor_config
    from signal_ingest.adapters.monitors.pipeline import _drop_seen_new_since_flags
    from signal_ingest.storage import connect, create_run

    settings = monitor_env
    cfg = load_monitor_config("news_mit")
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings)
        first = discover_and_enqueue(
            conn, source_id="news_mit", payload=_r3_rows("a", "b"), run_id=run_id, cutoff=T, observed_at=T
        )
        cursor = first["checkpoint"]["cursor_opaque"] if isinstance(first.get("checkpoint"), dict) else None
        scan = discover_list(
            cfg, _r3_rows("b", "fresh", "a"), checkpoint=cursor or first["cursor"], cutoff=T, observed_at=T
        )
        assert [i.external_id for i in scan.items] == ["b", "fresh"]
        _drop_seen_new_since_flags(conn, source_id="news_mit", items=scan.items)
        by_id = {i.external_id: i for i in scan.items}
        assert "new_since_last_run" not in by_id["b"].metadata
        assert by_id["fresh"].metadata["new_since_last_run"]["basis"] == "new_since_last_run"


def test_r3_zoneless_timestamp_without_source_tz_is_not_exact():
    """A naive wall-clock time with no configured zone spans UTC-12..UTC+14: not fresh-proof."""
    from datetime import datetime, timezone

    from signal_ingest.freshness.parse import parse_publication_value
    from signal_ingest.freshness.window import in_freshness_window

    cutoff = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    # 01:00 wall clock, zone unknown: real instant could be Nov 20 11:00 UTC (UTC+14),
    # before the window opens at Nov 20 12:00, so it cannot prove freshness.
    vague = parse_publication_value("2026-11-21T01:00:00", observed_at=cutoff)
    assert vague is not None and vague.precision == "unknown_zone"
    assert vague.upper <= cutoff  # never published after it was observed
    assert not in_freshness_window(vague, cutoff)
    known = parse_publication_value("2026-11-21T01:00:00", observed_at=cutoff,
                                    source_timezone="UTC")
    assert in_freshness_window(known, cutoff)
    explicit = parse_publication_value("2026-11-21T10:00:00Z", observed_at=cutoff)
    assert explicit.lower == explicit.upper and in_freshness_window(explicit, cutoff)


def test_r3_conflicting_non_primary_publication_keys_quarantine():
    """published vs pubDate disagreeing → conflict, not 'first key wins'."""
    from datetime import datetime, timezone

    from signal_ingest.freshness.parse import select_publication_interval

    observed = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    interval, reason = select_publication_interval(
        {"published": "2026-11-21T10:00:00Z", "pubDate": "Mon, 10 Nov 2026 01:00:00 GMT"},
        observed_at=observed,
    )
    assert interval is None and reason == "conflicting_publication_evidence"


@pytest.mark.parametrize(
    "value",
    ["2026-11-21", "today", "yesterday", "Nov 21, 2026",
     __import__("datetime").date(2026, 11, 21),
     __import__("datetime").datetime(2026, 11, 21, 1, 0)],
)
def test_r3_zoneless_day_values_are_widened(value):
    """Day-level values with no configured zone are widened, never exact UTC days."""
    from datetime import datetime, timezone

    from signal_ingest.freshness.parse import parse_publication_value
    from signal_ingest.freshness.window import in_freshness_window

    observed = datetime(2026, 11, 21, 23, 0, tzinfo=timezone.utc)
    vague = parse_publication_value(value, observed_at=observed)
    assert vague is not None and vague.precision == "unknown_zone", vague
    assert vague.upper <= observed
    assert not in_freshness_window(vague, observed)


# ---------------------------------------------------------------------------
# B9: live monitor crawl paths (offline; loopback; network guard on)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_b9_work_routes_monitor_task_fetch_then_parse_to_news(monitor_env, frozen_clock):
    """crawl.worker dispatches a monitor task: real loopback GET, then parse -> NEWS candidate."""
    from signal_ingest.crawl.queue import claim_tasks, enqueue_task
    from signal_ingest.crawl.worker import process_claimed_task
    from signal_ingest.storage import connect, create_run
    from tests.integration.b2_api import http_pool
    from tests.integration.b2_listing_http import start_listing_server

    html = (MONITOR_FIXTURES / "html" / "full_with_sidebar.html").read_bytes()
    server = start_listing_server({"/2026/synth-full": ("text/html; charset=utf-8", html)})
    article = server.url("/2026/synth-full")
    pool = http_pool()
    stages: list[str] = []
    try:
        with connect(monitor_env) as conn:
            run_id = create_run(conn, settings=monitor_env)
            # No `row`/`body` in the payload, so the fixture shortcut cannot synthesize a body.
            enqueue_task(
                # Unique per run: semantic keys are global and the fixture DB persists across runs.
                conn, source_id="news_mit", url=article, stage="fetch", external_id=f"b9-full-{run_id}",
                run_id=run_id,
                payload={"cutoff": T.isoformat(), "title": "Synthetic full article",
                         "expected_start": "FIRST_PARA_SYNTHETIC_START",
                         "expected_end": "LAST_PARA_SYNTHETIC_END"},
            )
            conn.commit()
            for _ in ("fetch", "parse"):
                for task in claim_tasks(conn, worker_id="b9-work", source_id="news_mit", run_id=run_id, limit=5):
                    stages.append(task.stage)
                    out = await process_claimed_task(conn, task, pool=pool, settings=monitor_env)
                    assert out["ok"] is True, out
            rows = conn.execute(
                "SELECT record_type, status, payload->'content'->>'text' FROM candidates "
                "WHERE source_id = 'news_mit' AND run_id = %s", (run_id,),
            ).fetchall()
    finally:
        await pool.aclose()
        server.stop()
    assert stages == ["fetch", "parse"]
    assert server.item_hits() == {"/2026/synth-full": 1}, server.hits
    assert len(rows) == 1 and rows[0][0] == "NEWS" and rows[0][1] == "accepted", rows
    assert "LAST_PARA_SYNTHETIC_END" in rows[0][2]


def test_b9_rss_discovery_payload_is_json_serializable(monitor_env, frozen_clock):
    """An RSS item with pubDate must enqueue: task payload cannot carry raw datetimes."""
    from signal_ingest.adapters.monitors import discover_and_enqueue
    from signal_ingest.storage import connect, create_run

    url = "http://127.0.0.1:9/2026/synth-rss"
    rss = (
        '<?xml version="1.0"?><rss version="2.0"><channel><title>Synthetic</title>'
        f"<item><title>Synthetic RSS item</title><link>{url}</link><guid>{url}</guid>"
        "<pubDate>Sat, 21 Nov 2026 11:00:00 GMT</pubDate></item></channel></rss>"
    ).encode()
    with connect(monitor_env) as conn:
        run_id = create_run(conn, settings=monitor_env)
        meta = discover_and_enqueue(
            conn, source_id="news_mit", payload=rss, run_id=run_id,
            cutoff=T, observed_at=T, base_url="http://127.0.0.1:9/",
        )
    assert meta["discovered"] == 1 and meta["created"] == 1, meta


def test_b9_detail_url_pattern_drops_nav_and_auth_links(frozen_clock):
    from signal_ingest.adapters.monitors import load_monitor_config
    from signal_ingest.adapters.monitors.discover import discover_list

    cfg = load_monitor_config("jobs_aijobs")
    assert (cfg.raw or {}).get("detail_url_pattern")
    base = "https://aijobs.ai/"
    page = (
        "<html><body><nav><a href='/'>Home</a><a href='/login'>Log in</a>"
        "<a href='/signup'>Sign up</a><a href='/jobs/category/ml'>ML jobs</a>"
        "<a href='/about'>About</a></nav>"
        "<ul><li><a href='/job/ml-engineer-synth-co'>ML Engineer</a></li>"
        "<li><a href='https://aijobs.ai/job/data-scientist-synth-labs/'>Data Scientist</a></li>"
        "<li><a href='/job/ml-engineer-synth-co?ref=nav'>dup with query</a></li></ul>"
        "</body></html>"
    )
    scan = discover_list(cfg, page, cutoff=T, observed_at=T, base_url=base)
    paths = sorted({__import__("urllib.parse").parse.urlparse(i.url).path for i in scan.items})
    assert paths == ["/job/data-scientist-synth-labs/", "/job/ml-engineer-synth-co"], paths


@pytest.mark.asyncio
async def test_b9_live_discovery_tries_feed_url_before_documented_page(monkeypatch, frozen_clock):
    """Inventory feed_url first; documented page only when the feed fails.

    Live FetchPolicy rejects loopback, so probe_url is replaced by a recorder (no network).
    """
    from signal_ingest.adapters.monitors import load_monitor_config
    from signal_ingest.config import Mode, load_settings
    from signal_ingest.crawl.types import FetchOutcomeKind
    from signal_ingest.live import monitors as live_monitors
    from tests.integration.b2_api import http_pool, raw_empty

    cfg = load_monitor_config("jobs_moaijobs")
    feed = (cfg.raw or {}).get("feed_url")
    assert feed and feed != cfg.documented_access_url
    calls: list[str] = []
    feed_ok = {"value": True}

    async def fake_probe(url, *, settings, pool=None, headers=None, max_retries=2):
        calls.append(url)
        if url == feed and feed_ok["value"]:
            return _raw(url, b"<rss></rss>", content_type="application/rss+xml")
        if url == feed:
            return raw_empty(url, outcome=FetchOutcomeKind.NOT_FOUND, status_code=404)
        return _raw(url, b"<html></html>", content_type="text/html")

    monkeypatch.setattr(live_monitors, "probe_url", fake_probe)
    settings = load_settings(mode=Mode.FIXTURE)
    pool = http_pool()
    try:
        obs, used, tried = await live_monitors._fetch_discovery(settings, "jobs_moaijobs", cfg, pool)
        assert calls == [feed] and used == feed
        calls.clear()
        feed_ok["value"] = False
        obs, used, tried = await live_monitors._fetch_discovery(settings, "jobs_moaijobs", cfg, pool)
        assert calls[:2] == [feed, str(cfg.documented_access_url)], calls
        assert used == str(cfg.documented_access_url)
        assert [t["url"] for t in tried][:2] == calls[:2]
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_b9_pool_accept_encoding_excludes_br_and_zstd(tmp_fixture_env):
    from tests.integration.b2_api import http_pool
    from tests.integration.b2_listing_http import start_listing_server

    server = start_listing_server({"/x": ("text/plain", b"ok")})
    pool = http_pool()
    try:
        resp = await pool.request("GET", server.url("/x"))
        assert resp.status_code == 200
    finally:
        await pool.aclose()
        server.stop()
    accept = server.request_headers["/x"].get("accept-encoding", "")
    codings = {c.split(";")[0].strip().lower() for c in accept.split(",") if c.strip()}
    assert codings and not codings & {"br", "zstd"}, accept
    assert codings <= {"gzip", "deflate", "identity"}, accept


def _ashby_raw(row):
    import json as _json

    from signal_ingest.crawl.types import FetchOutcomeKind, RawObservation

    url = "https://api.ashbyhq.com/posting-api/job-board/acme?includeCompensation=false"
    return RawObservation(request_url=url, final_url=url, status_code=200, outcome=FetchOutcomeKind.OK,
                          body=_json.dumps({"row": row}).encode(), content_type="application/json")


def test_b9_ashby_board_rows_and_job_rules():
    """Ashby feed: unlisted dropped, org:id identity, workplaceType beats isRemote, null/null quarantined."""
    from signal_ingest.adapters.monitors import jobs_ashby_ai as A

    listing = {"url": "https://api.ashbyhq.com/posting-api/job-board/acme", "sha256": "x"}
    base = {"title": "Software Engineer, Inference", "department": "Engineering",
            "publishedAt": "2026-11-21T10:00:00.000+00:00", "descriptionPlain": "Build inference systems.",
            "jobUrl": "https://jobs.ashbyhq.com/acme/1"}
    board = {"jobs": [{**base, "id": "1", "isListed": True, "workplaceType": "Hybrid", "isRemote": True},
                      {**base, "id": "2", "isListed": False, "workplaceType": "Remote"},
                      {**base, "id": "3", "isListed": True, "workplaceType": "Remote", "isRemote": False},
                      {**base, "id": "4", "isListed": True, "workplaceType": None, "isRemote": None}]}
    rows = A.rows_from_board(board, org="acme", company="Acme AI", listing=listing)
    assert [r["id"] for r in rows] == ["1", "3", "4"]  # unlisted posting dropped

    hybrid = A.parse_detail(_ashby_raw(rows[0]))
    assert hybrid.external_id == "acme:1"
    assert hybrid.payload["content"]["is_remote"] is False  # Hybrid is not fully remote even with isRemote=true
    assert hybrid.payload["content"]["role_family"] == "ml_engineering"  # "Inference" in the title
    assert hybrid.publication.precision == "instant" and hybrid.publication.lower.isoformat() == "2026-11-21T10:00:00+00:00"

    assert A.parse_detail(_ashby_raw(rows[1])).payload["content"]["is_remote"] is True
    unknown = A.parse_detail(_ashby_raw(rows[2]))
    assert unknown.quarantine_reason == "remote_unspecified"
    # No workplaceType: isRemote=False is trusted; isRemote=True may be a hybrid role, so unspecified.
    assert A.remote_flag({"workplaceType": None, "isRemote": False}) == (False, "isRemote")
    assert A.remote_flag({"isRemote": True}) == (None, "none")
    loose = A.parse_detail(_ashby_raw({**base, "id": "5", "isListed": True, "workplaceType": None, "isRemote": True}))
    assert loose.quarantine_reason == "remote_unspecified"


def test_b9_extra_job_feed_routes_through_monitor_pipeline():
    """jobs_ashby_ai is not one of the ten monitors but must still use the monitor pipeline."""
    from signal_ingest.adapters.monitors.pipeline import MONITOR_SOURCES, get_adapter

    assert "jobs_ashby_ai" not in MONITOR_SOURCES and len(MONITOR_SOURCES) == 10
    assert get_adapter("jobs_ashby_ai") is not None
    import inspect

    from signal_ingest.crawl import worker

    src = inspect.getsource(worker.process_claimed_task)
    assert "get_adapter(task.source_id) is not None" in src


def test_b9_ashby_llm_remote_only_fills_missing_flag():
    """A grounded LLM is_remote is used only when the posting has no source remote flag."""
    from signal_ingest.adapters.monitors import jobs_ashby_ai as A

    listing = {"url": "https://api.ashbyhq.com/posting-api/job-board/acme"}
    base = {"title": "Research Engineer", "publishedAt": "2026-11-21T10:00:00+00:00", "org": "acme",
            "company": "Acme AI", "descriptionPlain": "This role is fully remote across the US.",
            "_listing": listing}
    llm = {"value": True, "quote": "This role is fully remote", "segment_ids": ["seg:0003:body"],
           "provider": "gemini", "model": "gemini/gemini-2.5-flash"}
    filled = A.parse_detail(_ashby_raw({**base, "id": "9", "workplaceType": None, "isRemote": None}), llm_remote=llm)
    assert filled.ok and filled.payload["content"]["is_remote"] is True
    assert filled.evidence["is_remote"]["basis"] == "llm" and filled.evidence["is_remote"]["quote"] == llm["quote"]
    source_wins = A.parse_detail(_ashby_raw({**base, "id": "10", "workplaceType": "OnSite"}),
                                 llm_remote=llm)
    assert source_wins.payload["content"]["is_remote"] is False  # source flag beats the model
    assert A.parse_detail(_ashby_raw({**base, "id": "11"})).quarantine_reason == "remote_unspecified"


def test_b9_job_remote_llm_skips_accepted_anchors_at_listing_and_caps_failures(monitor_env, monkeypatch):
    """run_job_remote_llm: postings with an accepted JOB are not re-sent, freshness is judged at
    list_observed_at (not wall clock), and extract() errors stop after 5 in a row.

    Detects: re-spending quota on accepted postings, wall-clock windows, one error aborting the run.
    """
    import asyncio
    from uuid import uuid4

    from signal_ingest.live import orchestrate
    from signal_ingest.storage import connect
    from signal_ingest.storage.records import create_run
    from tests.integration import b7_api

    settings = monitor_env
    calls: list[str] = []

    async def failing_extract(candidate, chain):
        calls.append(candidate.candidate_id)
        raise RuntimeError("quota")

    monkeypatch.setattr(orchestrate, "assert_live_mode", lambda _s: None)
    monkeypatch.setattr("signal_ingest.llm.extract", failing_extract)
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings, metadata={"scenario": "b9_job_remote"})
        for n in range(1, 9):
            # acme:8 was listed 30h after publication: stale at observation time.
            observed = "2025-01-02T16:00:00+00:00" if n == 8 else "2025-01-01T20:00:00+00:00"
            row = {"id": str(n), "org": "acme", "publishedAt": "2025-01-01T10:00:00+00:00",
                   "descriptionPlain": "Hybrid or remote, details on request.", "_listing": {"url": "u"}}
            conn.execute(
                "INSERT INTO tasks (id, source_id, stage, url, semantic_key, external_id, run_id, payload, last_error) "
                "VALUES (%s, 'jobs_ashby_ai', 'parse', 'https://jobs.ashbyhq.com/acme', %s, %s, %s, %s, 'remote_unspecified')",
                (str(uuid4()), f"b9-remote:{run_id}:{n}", f"acme:{n}", str(run_id),
                 json.dumps({"row": row, "list_observed_at": observed})),
            )
        b7_api.insert_accepted_candidate(conn, run_id=run_id, record_type="JOB", external_id="acme:1",
                                         payload={"content": {"company": "Acme AI"}}, source_id="jobs_ashby_ai")
        conn.commit()

    out = asyncio.run(orchestrate.run_job_remote_llm(settings, run_id=str(run_id), provider=object()))
    assert out["fresh_unspecified"] == 6  # acme:1 already accepted, acme:8 stale at listing time
    assert calls == ["acme:2", "acme:3", "acme:4", "acme:5", "acme:6"]  # stopped after 5 failures
    assert [o["status"] for o in out["outcomes"]] == ["error"] * 5
    assert out["outcomes"][0]["error"] == "RuntimeError"


def test_b9_dry_probe_includes_feed_sources(monitor_env, monkeypatch):
    """dry_probe_monitors probes the extra job feeds too (load_monitor_configs(include_feeds=True))."""
    import asyncio

    import signal_ingest.adapters.monitors.config as monitor_config
    from signal_ingest.live import monitors as live_monitors

    seen: list[dict] = []
    monkeypatch.setattr(monitor_config, "load_monitor_configs", lambda *a, **kw: seen.append(kw) or [])
    assert asyncio.run(live_monitors.dry_probe_monitors(monitor_env)) == []
    assert seen == [{"include_feeds": True}]
