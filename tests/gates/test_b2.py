"""B2 gate: bulk directories and paper→repo→stars joins.

Oracle: docs/build-plan.md (B2) and docs/contracts.md — not production source.
Cases must FAIL when behavior is missing/wrong and PASS when integrated correctly.
The gate runner rejects skipped cases.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from tests.integration.b2_api import (
    BULK_FIXTURES,
    assert_synthetic,
    fixture_paths,
    http_pool,
    load_bulk_json,
    raw_empty,
    raw_json,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def bulk_env(tmp_fixture_env, frozen_clock):
    """Fixture-mode settings + B2 schema (candidates/relationships/repo_metrics/quarantines)."""
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
              AND table_name IN (
                'candidates', 'relationships', 'repo_metrics', 'quarantines'
              )
            """
        ).fetchone()
        assert row is not None and int(row[0]) >= 4, (
            "B2 schema missing: expected candidates, relationships, "
            "repo_metrics, quarantines (migrations/003_b2_bulk.sql)"
        )
        # Isolate gate cases from leftover fixture rows (shared brew DSN).
        conn.execute(
            "TRUNCATE quarantines, relationships, repo_metrics, candidates "
            "RESTART IDENTITY CASCADE"
        )
        conn.execute(
            """
            DELETE FROM observations
            WHERE source_id LIKE 'bulk.%%' OR source_id LIKE 'b2-%%'
            """
        )
        conn.execute(
            """
            DELETE FROM tasks
            WHERE source_id LIKE 'bulk.%%' OR source_id LIKE 'b2-%%'
            """
        )
        conn.execute(
            """
            DELETE FROM source_checkpoints
            WHERE source_id LIKE 'bulk.%%' OR source_id LIKE 'b2-%%'
            """
        )
        conn.commit()
    return settings


def _run(settings):
    from signal_ingest.storage import connect, create_run

    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings)
        conn.commit()
        return run_id


# ---------------------------------------------------------------------------
# Fixture labeling
# ---------------------------------------------------------------------------


def test_bulk_fixtures_labeled_synthetic():
    """All B2 bulk fixtures stay labeled synthetic / not for live export."""
    paths = fixture_paths()
    assert paths, "expected tests/fixtures/bulk/*"
    for path in paths:
        if path.suffix == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            assert_synthetic(data["_meta"])
        else:
            text = path.read_text(encoding="utf-8")
            assert "synthetic" in text.lower()
            assert "not_for_live_export" in text


# ---------------------------------------------------------------------------
# YC: repeated pages/aliases + missing employee count
# ---------------------------------------------------------------------------


def test_yc_repeated_pages_and_aliases_one_identity(bulk_env):
    """Repeated pages and aliases collapse to one STARTUP identity via queue."""
    from signal_ingest.adapters.bulk import SOURCE_YC, discover_and_enqueue, list_candidates
    from signal_ingest.adapters.bulk import yc as yc_adapter
    from signal_ingest.crawl.queue import claim_tasks
    from signal_ingest.storage import connect

    fixture = load_bulk_json("yc_companies.json")
    settings = bulk_env
    run_id = _run(settings)
    source_id = SOURCE_YC

    with connect(settings) as conn:
        first = discover_and_enqueue(
            conn,
            source_id=source_id,
            payload=fixture["page_a"],
            partition="ai-0",
            run_id=run_id,
            revision="rev-a",
        )
        assert first["created"] >= 2
        second = discover_and_enqueue(
            conn,
            source_id=source_id,
            payload=fixture["page_b_repeat_and_alias"],
            partition="ai-0",
            run_id=run_id,
            revision="rev-b",
        )
        # Same logical yc-aurora + new non-AI row; duplicates skipped by semantic_key.
        assert second["skipped"] >= 1

        claimed = claim_tasks(conn, worker_id="b2-yc", source_id=source_id, limit=20)
        assert claimed
        for task in claimed:
            raw = raw_json(task.url, {"row": task.payload.get("row")})
            result = yc_adapter.parse_observation(raw, row=task.payload.get("row"))
            from signal_ingest.adapters.bulk.persist import insert_quarantine, upsert_candidate

            if not result.ok:
                insert_quarantine(
                    conn,
                    source_id=source_id,
                    reason=result.quarantine_reason or "failed",
                    record_type=result.record_type,
                    external_id=result.external_id,
                    detail=result.quarantine_detail,
                    run_id=run_id,
                )
                continue
            upsert_candidate(
                conn,
                source_id=source_id,
                record_type=result.record_type,
                external_id=result.external_id,
                payload=result.payload,
                evidence=result.evidence,
                status="accepted" if result.eligible else "pending",
                run_id=run_id,
            )
        conn.commit()

        starts = list_candidates(conn, source_id=source_id, record_type="STARTUP")
        aurora = [c for c in starts if c["external_id"] == "yc-aurora"]
        assert len(aurora) == 1, f"alias/repeat must yield one row, got {len(aurora)}"


def test_yc_missing_employee_count_null_still_ai_eligible(bulk_env, frozen_clock):
    """Missing/range employee count → null; explicit AI tag still eligible."""
    from signal_ingest.adapters.bulk import yc as yc_adapter
    from signal_ingest.adapters.bulk.mapping import parse_employee_count

    fixture = load_bulk_json("yc_companies.json")
    range_row = next(r for r in fixture["page_a"] if r["id"] == "yc-range")
    assert parse_employee_count(range_row["team_size"]) is None

    raw = raw_json(
        "https://www.ycombinator.com/companies/range-ai",
        {"row": range_row},
    )
    result = yc_adapter.parse_observation(raw, row=range_row)
    assert result.ok is True
    assert result.eligible is True
    assert result.payload["content"]["data"]["employeeCount"] is None
    assert result.payload["meta"]["team_size_raw"] == "11-50"

    null_alias = next(
        r for r in fixture["page_b_repeat_and_alias"] if r.get("uuid") == "yc-aurora"
    )
    result2 = yc_adapter.parse_observation(
        raw_json("https://www.ycombinator.com/companies/aurora-labs", {"row": null_alias}),
        row=null_alias,
    )
    assert result2.ok is True
    assert result2.payload["content"]["data"]["employeeCount"] is None


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------


def test_two_products_one_owner_count_separately(bulk_env, frozen_clock):
    """Two products by one owner count as two PRODUCT identities."""
    from signal_ingest.adapters.bulk import products as products_adapter
    from signal_ingest.adapters.bulk.persist import upsert_candidate
    from signal_ingest.storage import connect

    fixture = load_bulk_json("products.json")
    settings = bulk_env
    run_id = _run(settings)
    rows = [r for r in fixture["products"] if r.get("product_id") in {"prod-orbit", "prod-lens"}]
    items, _rev, _pk = products_adapter.discover_from_payload(rows, partition="p0")
    assert len(items) == 2

    with connect(settings) as conn:
        for item in items:
            result = products_adapter.parse_observation(
                raw_json(item.url, {"row": item.metadata["row"]}),
                row=item.metadata["row"],
            )
            assert result.ok, result.quarantine_reason
            upsert_candidate(
                conn,
                source_id=products_adapter.SOURCE_ID,
                record_type="PRODUCT",
                external_id=result.external_id,
                payload=result.payload,
                evidence=result.evidence,
                status="accepted",
                run_id=run_id,
            )
        conn.commit()
        from signal_ingest.adapters.bulk import list_candidates

        accepted = list_candidates(
            conn, source_id=products_adapter.SOURCE_ID, record_type="PRODUCT", status="accepted"
        )
        wanted = [c for c in accepted if c["external_id"] in {"prod-orbit", "prod-lens"}]
        assert len(wanted) == 2
        owners = {c["payload"]["content"]["startupName"] for c in wanted}
        assert owners == {"Aurora Labs"}


def test_three_plans_collapse_to_one_product(bulk_env, frozen_clock):
    """Three plan/locale rows for one product → one logical product."""
    from signal_ingest.adapters.bulk import products as products_adapter
    from signal_ingest.adapters.bulk.mapping import collapse_product_plans

    fixture = load_bulk_json("products.json")
    nova_rows = [r for r in fixture["products"] if r.get("product_id") == "prod-nova"]
    assert len(nova_rows) == 3
    collapsed = collapse_product_plans(nova_rows)
    assert len(collapsed) == 1
    items, _, _ = products_adapter.discover_from_payload(nova_rows, partition="plans")
    assert len(items) == 1
    assert items[0].external_id == "prod-nova"

    result = products_adapter.parse_observation(
        raw_json(items[0].url, {"row": items[0].metadata["row"]}),
        row=items[0].metadata["row"],
    )
    assert result.ok, result.quarantine_reason
    assert result.payload["content"]["productName"] == "Nova"


@pytest.mark.asyncio
async def test_missing_ownership_quarantine(bulk_env, frozen_clock):
    """Missing ownership → parse quarantine row persisted in `quarantines`."""
    from signal_ingest.adapters.bulk import (
        SOURCE_PRODUCTS,
        STAGE_FETCH,
        STAGE_PARSE,
        discover_and_enqueue,
        drain_bulk_once,
    )
    from signal_ingest.adapters.bulk import products as products_adapter
    from signal_ingest.storage import connect

    fixture = load_bulk_json("products.json")
    orphan = next(r for r in fixture["products"] if r["product_id"] == "prod-orphan")
    # Unit predicate still holds.
    unit = products_adapter.parse_observation(
        raw_json(orphan["url"], {"row": orphan}),
        row=orphan,
    )
    assert unit.ok is False
    assert unit.quarantine_reason == "missing_ownership_evidence"

    settings = bulk_env
    run_id = _run(settings)
    pool = http_pool()
    try:
        with connect(settings) as conn:
            enq = discover_and_enqueue(
                conn,
                source_id=SOURCE_PRODUCTS,
                payload=[orphan],
                partition="orphan-q",
                run_id=run_id,
                revision="orphan-rev",
            )
            assert enq["created"] == 1

            fetch_r = await drain_bulk_once(
                conn,
                worker_id="b2-own-fetch",
                pool=pool,
                source_id=SOURCE_PRODUCTS,
                stage=STAGE_FETCH,
                settings=settings,
            )
            assert fetch_r is not None and fetch_r["ok"] is True

            parse_r = await drain_bulk_once(
                conn,
                worker_id="b2-own-parse",
                pool=pool,
                source_id=SOURCE_PRODUCTS,
                stage=STAGE_PARSE,
                settings=settings,
            )
            assert parse_r is not None and parse_r["ok"] is True
            assert parse_r.get("quarantined") is True
            assert parse_r.get("reason") == "missing_ownership_evidence"

            row = conn.execute(
                """
                SELECT reason, record_type, external_id, source_id
                FROM quarantines
                WHERE reason = %s AND external_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                ("missing_ownership_evidence", "prod-orphan"),
            ).fetchone()
            assert row is not None, "expected durable quarantines row for missing ownership"
            assert row[1] == "PRODUCT"
            assert row[3] == SOURCE_PRODUCTS
    finally:
        await pool.aclose()


def test_free_trial_vs_freemium(bulk_env, frozen_clock):
    """Temporary free trial + paid → PAID; permanent free + paid → FREEMIUM."""
    from signal_ingest.adapters.bulk import products as products_adapter
    from signal_ingest.adapters.bulk.mapping import classify_pricing
    from signal_ingest.models.records import PricingModel

    fixture = load_bulk_json("products.json")
    trial = next(r for r in fixture["products"] if r["product_id"] == "prod-trial")
    freemium = next(r for r in fixture["products"] if r["product_id"] == "prod-freemium")

    assert (
        classify_pricing(
            pricing=trial.get("pricing"),
            plans=trial.get("plans"),
            trial_only=True,
            has_paid_tier=True,
        )
        is PricingModel.PAID
    )
    assert (
        classify_pricing(
            plans=freemium.get("plans"),
            has_free_tier=True,
            has_paid_tier=True,
        )
        is PricingModel.FREEMIUM
    )

    trial_result = products_adapter.parse_observation(
        raw_json(trial["url"], {"row": trial}), row=trial
    )
    assert trial_result.ok, trial_result.quarantine_reason
    assert trial_result.payload["content"]["pricingModel"] == "PAID"

    free_result = products_adapter.parse_observation(
        raw_json(freemium["url"], {"row": freemium}), row=freemium
    )
    assert free_result.ok, free_result.quarantine_reason
    assert free_result.payload["content"]["pricingModel"] == "FREEMIUM"


# ---------------------------------------------------------------------------
# Papers / arXiv / primary repo
# ---------------------------------------------------------------------------


def test_paper_versions_one_versionless_id_and_v1_date(bulk_env, frozen_clock):
    """Versions of the same paper → one versionless ID; v1 publication date wins."""
    from signal_ingest.adapters.bulk import arxiv as arxiv_adapter
    from signal_ingest.adapters.bulk import papers as papers_adapter
    from signal_ingest.adapters.bulk.mapping import versionless_arxiv_id
    from signal_ingest.adapters.bulk.persist import upsert_candidate
    from signal_ingest.storage import connect

    fixture = load_bulk_json("papers.json")
    versions = [
        p for p in fixture["papers"] if versionless_arxiv_id(p.get("arxiv_id")) == "2401.00001"
    ]
    assert len(versions) == 2
    assert versionless_arxiv_id("2401.00001v3") == "2401.00001"

    arxiv_meta = arxiv_adapter.parse_metadata_bytes(
        (BULK_FIXTURES / "arxiv_2401_00001.json").read_bytes(),
        content_type="application/json",
    )
    assert arxiv_meta is not None
    assert arxiv_meta["arxiv_id"] == "2401.00001"
    assert arxiv_meta["v1_published_date"].startswith("2024-01-15")

    items, _, _ = papers_adapter.discover_from_payload(
        {"papers": versions, "links": fixture["links"]},
        partition="v",
    )
    # discover yields one item per paper row; identity key is versionless.
    ids = {it.external_id for it in items}
    assert ids == {"2401.00001"}

    settings = bulk_env
    run_id = _run(settings)
    with connect(settings) as conn:
        for item in items:
            row = arxiv_adapter.merge_arxiv_into_paper_row(item.metadata["row"], arxiv_meta)
            result = papers_adapter.parse_observation(
                raw_json(item.url, {"row": row}),
                row=row,
                repo_edges=item.metadata.get("repo_edges"),
                arxiv_meta=arxiv_meta,
            )
            assert result.ok, result.quarantine_reason
            assert result.external_id == "2401.00001"
            assert result.payload["content"]["published_date"].startswith("2024-01-15")
            upsert_candidate(
                conn,
                source_id=papers_adapter.SOURCE_ID,
                record_type="RESEARCH_PAPER",
                external_id=result.external_id,
                payload=result.payload,
                evidence=result.evidence,
                status="pending",
                run_id=run_id,
            )
        conn.commit()
        from signal_ingest.adapters.bulk import list_candidates

        rows = list_candidates(
            conn, source_id=papers_adapter.SOURCE_ID, record_type="RESEARCH_PAPER"
        )
        assert len(rows) == 1
        assert rows[0]["payload"]["content"]["published_date"].startswith("2024-01-15")


def test_unrelated_dependency_and_ambiguous_multi_repo_quarantine(frozen_clock):
    """Dependency-only and ambiguous multi-repo ties quarantine (no primary)."""
    from signal_ingest.adapters.bulk import papers as papers_adapter

    fixture = load_bulk_json("papers.json")
    dep = next(p for p in fixture["papers"] if p.get("paper_id") == "paper-dep-only")
    multi = next(p for p in fixture["papers"] if p.get("paper_id") == "paper-multi-repo")

    dep_result = papers_adapter.parse_observation(
        raw_json(dep["paper_url"], {"row": dep}),
        row=dep,
        repo_edges=dep["repos"],
    )
    assert dep_result.ok is False
    assert dep_result.quarantine_reason in {
        "dependency_or_invalid_repo",
        "no_code_repo",
        "ambiguous_multi_repo",
    }

    multi_result = papers_adapter.parse_observation(
        raw_json(multi["paper_url"], {"row": multi}),
        row=multi,
        repo_edges=multi["repos"],
    )
    assert multi_result.ok is False
    assert multi_result.quarantine_reason == "ambiguous_multi_repo"


def test_two_papers_sharing_one_repo_count_separately(bulk_env, frozen_clock):
    """Two papers sharing one repo remain two paper identities."""
    from signal_ingest.adapters.bulk import papers as papers_adapter
    from signal_ingest.adapters.bulk.persist import upsert_candidate
    from signal_ingest.storage import connect

    fixture = load_bulk_json("papers.json")
    shared = [
        p
        for p in fixture["papers"]
        if p.get("paper_id") in {"paper-shared-a", "paper-shared-b"}
    ]
    items, _, _ = papers_adapter.discover_from_payload(shared, partition="share")
    assert {i.external_id for i in items} == {"2404.00004", "2404.00005"}

    settings = bulk_env
    run_id = _run(settings)
    with connect(settings) as conn:
        for item in items:
            result = papers_adapter.parse_observation(
                raw_json(item.url, {"row": item.metadata["row"]}),
                row=item.metadata["row"],
                repo_edges=item.metadata.get("repo_edges"),
            )
            assert result.ok, result.quarantine_reason
            assert result.payload["content"]["github_url"] == (
                "https://github.com/example/shared-suite"
            )
            upsert_candidate(
                conn,
                source_id=papers_adapter.SOURCE_ID,
                record_type="RESEARCH_PAPER",
                external_id=result.external_id,
                payload=result.payload,
                evidence=result.evidence,
                status="pending",
                run_id=run_id,
            )
        conn.commit()
        from signal_ingest.adapters.bulk import list_candidates

        rows = list_candidates(
            conn, source_id=papers_adapter.SOURCE_ID, record_type="RESEARCH_PAPER"
        )
        shared_rows = [r for r in rows if r["external_id"] in {"2404.00004", "2404.00005"}]
        assert len(shared_rows) == 2


def test_title_authors_publication_evidence_required(frozen_clock):
    """Missing title/authors/original publication evidence → quarantine."""
    from signal_ingest.adapters.bulk import papers as papers_adapter

    fixture = load_bulk_json("papers.json")
    missing = next(p for p in fixture["papers"] if p.get("paper_id") == "paper-missing-authors")
    result = papers_adapter.parse_observation(
        raw_json(missing["paper_url"], {"row": missing}),
        row=missing,
        repo_edges=missing["repos"],
    )
    assert result.ok is False
    assert result.quarantine_reason == "missing_title_authors_or_publication"


def test_string_field_drift_quarantine(frozen_clock):
    """Plausible defect: renamed/drifted field keys do not invent required values."""
    from signal_ingest.adapters.bulk import papers as papers_adapter
    from signal_ingest.adapters.bulk import github as github_adapter

    fixture = load_bulk_json("papers.json")
    drift = next(p for p in fixture["papers"] if p.get("paper_id") == "paper-field-drift")
    paper_result = papers_adapter.parse_observation(
        raw_json("https://fixtures.local/papers/drift", {"row": drift}),
        row=drift,
    )
    assert paper_result.ok is False
    assert paper_result.quarantine_reason in {
        "missing_title_authors_or_publication",
        "parse_drift",
        "no_code_repo",
    }

    gh = load_bulk_json("github_repos.json")
    gh_result = github_adapter.parse_observation(
        raw_json("https://api.github.com/repos/example/drift-repo", gh["drift_missing_stars"])
    )
    assert gh_result.ok is False
    assert gh_result.quarantine_reason == "missing_stargazers_count"


def test_no_code_and_dead_links_quarantine(frozen_clock):
    """No-code papers and non-GitHub/dead links enter quarantine."""
    from signal_ingest.adapters.bulk import papers as papers_adapter

    fixture = load_bulk_json("papers.json")
    no_code = next(p for p in fixture["papers"] if p.get("paper_id") == "paper-no-code")
    dead = next(p for p in fixture["papers"] if p.get("paper_id") == "paper-dead-link")

    r1 = papers_adapter.parse_observation(
        raw_json(no_code["paper_url"], {"row": no_code}),
        row=no_code,
        repo_edges=[],
    )
    assert r1.ok is False
    assert r1.quarantine_reason == "no_code_repo"

    r2 = papers_adapter.parse_observation(
        raw_json(dead["paper_url"], {"row": dead}),
        row=dead,
        repo_edges=dead["repos"],
    )
    assert r2.ok is False
    assert r2.quarantine_reason in {
        "dependency_or_invalid_repo",
        "no_code_repo",
        "no_repo_edge",
    }


# ---------------------------------------------------------------------------
# GitHub metrics
# ---------------------------------------------------------------------------


def test_renamed_repo_follows_stable_repo_id(bulk_env, frozen_clock):
    """Rename: request path differs from html_url; stable repo_id retained."""
    from signal_ingest.adapters.bulk import github as github_adapter
    from signal_ingest.adapters.bulk.persist import upsert_repo_metric
    from signal_ingest.crawl.types import FetchOutcomeKind
    from signal_ingest.storage import connect

    gh = load_bulk_json("github_repos.json")
    # github_owner_repo matches github.com paths (not api.github.com/repos/…).
    raw = raw_json(
        "https://github.com/example/renamed-lib",
        gh["renamed"],
        request_url="https://github.com/example/old-name",
    )
    result = github_adapter.parse_observation(raw)
    assert result.ok is True
    assert result.payload["repo_id"] == "333003"
    assert result.payload["renamed_from"] == "example/old-name"
    assert result.payload["repo_url"] == "https://github.com/example/renamed-lib"

    # discover dedupes by repo id across old/new URLs
    items, _, _ = github_adapter.discover_repos(
        [
            {"repo_id": 333003, "repo_url": "https://github.com/example/old-name"},
            {"repo_id": 333003, "repo_url": "https://github.com/example/renamed-lib"},
        ]
    )
    assert len(items) == 1

    settings = bulk_env
    with connect(settings) as conn:
        snap = result.enrich_stars["snapshot"]
        upsert_repo_metric(conn, snap)
        conn.commit()
        from signal_ingest.adapters.bulk import latest_repo_metric

        latest = latest_repo_metric(conn, repo_id="333003")
        assert latest is not None
        assert int(latest["stars"]) == 12


def test_true_zero_stars_accepted(frozen_clock):
    """True observed zero stars is valid (not fabricated, not quarantined)."""
    from signal_ingest.adapters.bulk import github as github_adapter
    from signal_ingest.adapters.bulk.eligibility import paper_join_complete

    gh = load_bulk_json("github_repos.json")
    result = github_adapter.parse_observation(
        raw_json(
            "https://api.github.com/repos/example/synthetic-attention",
            gh["zero_stars"],
        )
    )
    assert result.ok is True
    assert result.payload["github_stars"] == 0
    assert paper_join_complete(
        title="Synthetic Attention for Fixture Pipelines",
        authors=["A. Researcher"],
        paper_url="https://arxiv.org/abs/2401.00001",
        published_date="2024-01-15T18:30:00Z",
        github_url="https://github.com/example/synthetic-attention",
        github_stars=0,
        primary_repo_ok=True,
    )


def test_private_or_deleted_repo_quarantine_no_fake_zero(frozen_clock):
    """Private flag / Not Found → quarantine; never invent stars=0."""
    from signal_ingest.adapters.bulk import github as github_adapter
    from signal_ingest.crawl.types import FetchOutcomeKind

    gh = load_bulk_json("github_repos.json")

    private = github_adapter.parse_observation(
        raw_json("https://api.github.com/repos/example/secret", gh["private_flag"])
    )
    assert private.ok is False
    assert private.quarantine_reason == "repo_private_or_deleted"
    assert "github_stars" not in (private.payload or {})

    deleted = github_adapter.parse_observation(
        raw_empty(
            "https://api.github.com/repos/example/gone",
            outcome=FetchOutcomeKind.NOT_FOUND,
            status_code=404,
        )
    )
    assert deleted.ok is False
    assert deleted.quarantine_reason == "repo_private_or_deleted"

    msg = github_adapter.parse_observation(
        raw_json(
            "https://api.github.com/repos/example/gone2",
            gh["not_found_message"],
            status_code=404,
        )
    )
    assert msg.ok is False
    assert msg.quarantine_reason == "repo_private_or_deleted"


@pytest.mark.asyncio
async def test_304_cache_revalidates_cached_star_count(bulk_env, frozen_clock):
    """304 with cached body revalidates stargazers_count (no invented body)."""
    from signal_ingest.adapters.bulk import github as github_adapter
    from signal_ingest.config import Mode
    from signal_ingest.crawl import FetchPolicy, HttpPool, PoolLimits, fetch
    from signal_ingest.crawl.types import FetchOutcomeKind
    from tests.integration.b1_http import start_b1_http_server

    gh = load_bulk_json("github_repos.json")

    first = github_adapter.parse_observation(
        raw_json(
            "https://api.github.com/repos/example/shared-suite",
            gh["shared_suite"],
            etag='"repo-etag-v1"',
        )
    )
    assert first.ok and first.payload["github_stars"] == 7

    revalidated = raw_json(
        "https://api.github.com/repos/example/shared-suite",
        gh["shared_suite"],
        outcome=FetchOutcomeKind.NOT_MODIFIED,
        status_code=304,
        etag='"repo-etag-v1"',
    )
    second = github_adapter.parse_observation(revalidated)
    assert second.ok is True
    assert second.payload["github_stars"] == 7
    assert second.evidence["outcome"] == "not_modified"

    # Transport-level: 304 without cached body stays invalid (B1 invariant reused).
    server = start_b1_http_server()
    pool = HttpPool(
        PoolLimits(
            max_active=2,
            max_pending=8,
            per_origin_concurrency=2,
            per_origin_min_interval_s=0.0,
            connect_timeout_s=0.5,
            read_timeout_s=0.5,
            overall_timeout_s=1.0,
            max_compressed_bytes=64 * 1024,
            max_decoded_bytes=128 * 1024,
            max_redirects=5,
        )
    )
    policy = FetchPolicy(mode=Mode.FIXTURE)
    try:
        primed = await fetch(server.url("/etag-resource"), policy, pool=pool)
        assert primed.outcome is FetchOutcomeKind.OK
        bare = await fetch(
            server.url("/etag-resource"),
            policy,
            pool=pool,
            cached_etag=primed.etag or '"b1-etag-v1"',
            cached_body=None,
        )
        assert bare.outcome is FetchOutcomeKind.INVALID
        with_cache = await fetch(
            server.url("/etag-resource"),
            policy,
            pool=pool,
            cached_etag=primed.etag or '"b1-etag-v1"',
            cached_body=primed.body,
            cached_meta={
                "content_type": primed.content_type,
                "content_sha256": primed.content_sha256,
            },
        )
        assert with_cache.outcome is FetchOutcomeKind.NOT_MODIFIED
        assert with_cache.body == primed.body
    finally:
        await pool.aclose()
        server.stop()


# ---------------------------------------------------------------------------
# Checkpoint replay + eligible join
# ---------------------------------------------------------------------------


def test_checkpoint_replay_one_row_per_identity(bulk_env, frozen_clock):
    """Insert-before-offset + truncated replacement page → one row per logical ID."""
    from signal_ingest.adapters.bulk import SOURCE_YC, discover_and_enqueue, list_candidates
    from signal_ingest.adapters.bulk import yc as yc_adapter
    from signal_ingest.adapters.bulk.persist import upsert_candidate
    from signal_ingest.storage import connect

    fixture = load_bulk_json("checkpoint_partition.json")
    settings = bulk_env
    run_id = _run(settings)

    with connect(settings) as conn:
        v1 = discover_and_enqueue(
            conn,
            source_id=SOURCE_YC,
            payload=fixture["revision_v1"]["companies"],
            partition=fixture["revision_v1"]["partition"],
            run_id=run_id,
            revision="cp-v1",
        )
        assert v1["created"] == 2

        # Persist candidates for v1 identities.
        for row in fixture["revision_v1"]["companies"]:
            result = yc_adapter.parse_observation(
                raw_json(row["url"], {"row": row}), row=row
            )
            assert result.ok
            upsert_candidate(
                conn,
                source_id=SOURCE_YC,
                record_type="STARTUP",
                external_id=result.external_id,
                payload=result.payload,
                evidence=result.evidence,
                status="accepted",
                run_id=run_id,
            )
        conn.commit()

        v2 = discover_and_enqueue(
            conn,
            source_id=SOURCE_YC,
            payload=fixture["revision_v2_insert_before_and_truncate"]["companies"],
            partition=fixture["revision_v2_insert_before_and_truncate"]["partition"],
            run_id=run_id,
            revision="cp-v2",
        )
        # New zeta created; alpha already known → skipped on same semantic fetch key
        # when URL/id collide across revisions; candidate upsert keeps one identity.
        assert v2["discovered"] == 2

        for row in fixture["revision_v2_insert_before_and_truncate"]["companies"]:
            result = yc_adapter.parse_observation(
                raw_json(row["url"], {"row": row}), row=row
            )
            assert result.ok
            upsert_candidate(
                conn,
                source_id=SOURCE_YC,
                record_type="STARTUP",
                external_id=result.external_id,
                payload=result.payload,
                evidence=result.evidence,
                status="accepted",
                run_id=run_id,
            )
        conn.commit()

        rows = list_candidates(conn, source_id=SOURCE_YC, record_type="STARTUP")
        by_id = {
            r["external_id"]: r
            for r in rows
            if r["external_id"] in {"yc-cp-a", "yc-cp-b", "yc-cp-z"}
        }
        assert set(by_id) == {"yc-cp-a", "yc-cp-b", "yc-cp-z"}
        # Replay of alpha updated in place (team_size 5 → 6), still one row.
        assert by_id["yc-cp-a"]["payload"]["content"]["data"]["employeeCount"] == 6
        assert len(by_id) == 3


@pytest.mark.asyncio
async def test_eligible_papers_only_after_metric_join(bulk_env, frozen_clock):
    """Eligible count stays 0 until enrich_stars completes paper→repo→stars join."""
    from copy import deepcopy

    from signal_ingest.adapters.bulk import (
        SOURCE_GITHUB,
        SOURCE_PAPERS,
        STAGE_ENRICH_STARS,
        STAGE_FETCH,
        STAGE_PARSE,
        count_eligible_papers,
        discover_and_enqueue,
        drain_bulk_once,
        get_candidate,
        latest_repo_metric,
        list_candidates,
    )
    from signal_ingest.storage import connect

    fixture = load_bulk_json("papers.json")
    paper = deepcopy(
        next(p for p in fixture["papers"] if p.get("arxiv_id", "").startswith("2404.00004"))
    )
    # Fixture-mode stars via enrich loopback (do not pre-join with `stars`).
    paper["repos"] = [
        {
            "repo_url": "https://github.com/example/shared-suite",
            "is_official": True,
            "repo_id": "222002",
            "fixture_repo": {
                "id": 222002,
                "full_name": "example/shared-suite",
                "html_url": "https://github.com/example/shared-suite",
                "private": False,
                "stargazers_count": 0,
            },
        }
    ]
    settings = bulk_env
    run_id = _run(settings)
    pool = http_pool()
    try:
        with connect(settings) as conn:
            enq = discover_and_enqueue(
                conn,
                source_id=SOURCE_PAPERS,
                payload=[paper],
                partition="join-0",
                run_id=run_id,
                revision="join-rev",
            )
            assert enq["created"] == 1

            fetch_r = await drain_bulk_once(
                conn,
                worker_id="b2-join-fetch",
                pool=pool,
                source_id=SOURCE_PAPERS,
                stage=STAGE_FETCH,
                settings=settings,
            )
            assert fetch_r is not None and fetch_r["ok"] is True

            parse_r = await drain_bulk_once(
                conn,
                worker_id="b2-join-parse",
                pool=pool,
                source_id=SOURCE_PAPERS,
                stage=STAGE_PARSE,
                settings=settings,
            )
            assert parse_r is not None and parse_r["ok"] is True
            assert parse_r.get("eligible") is False
            assert parse_r.get("enrich_task_id"), "parse must enqueue enrich_stars"
            external_id = parse_r["external_id"]

            pending = get_candidate(
                conn,
                source_id=SOURCE_PAPERS,
                record_type="RESEARCH_PAPER",
                external_id=external_id,
            )
            assert pending is not None
            assert pending["status"] == "pending"
            assert "github_stars" not in (pending["payload"].get("content") or {})
            assert (
                count_eligible_papers(
                    list_candidates(
                        conn, source_id=SOURCE_PAPERS, record_type="RESEARCH_PAPER"
                    )
                )
                == 0
            )

            enrich_r = await drain_bulk_once(
                conn,
                worker_id="b2-join-enrich",
                pool=pool,
                source_id=SOURCE_GITHUB,
                stage=STAGE_ENRICH_STARS,
                settings=settings,
            )
            assert enrich_r is not None and enrich_r["ok"] is True
            assert enrich_r.get("quarantined") is not True
            assert enrich_r.get("stars") == 0
            assert enrich_r.get("eligible") is True
            assert enrich_r.get("repo_id") == "222002"

            accepted = get_candidate(
                conn,
                source_id=SOURCE_PAPERS,
                record_type="RESEARCH_PAPER",
                external_id=external_id,
            )
            assert accepted is not None
            assert accepted["status"] == "accepted"
            assert accepted["payload"]["content"]["github_stars"] == 0
            assert (
                count_eligible_papers(
                    list_candidates(
                        conn, source_id=SOURCE_PAPERS, record_type="RESEARCH_PAPER"
                    )
                )
                == 1
            )
            metric = latest_repo_metric(conn, repo_id="222002")
            assert metric is not None
            assert int(metric["stars"]) == 0
            assert metric["repo_url"] == "https://github.com/example/shared-suite"
    finally:
        await pool.aclose()


@pytest.mark.asyncio
async def test_private_repo_enrich_writes_quarantine_row(bulk_env, frozen_clock):
    """Private repo on enrich_stars path → quarantines row + candidate quarantined."""
    from copy import deepcopy

    from signal_ingest.adapters.bulk import (
        SOURCE_GITHUB,
        SOURCE_PAPERS,
        STAGE_ENRICH_STARS,
        STAGE_FETCH,
        STAGE_PARSE,
        discover_and_enqueue,
        drain_bulk_once,
        get_candidate,
    )
    from signal_ingest.storage import connect

    fixture = load_bulk_json("papers.json")
    paper = deepcopy(
        next(p for p in fixture["papers"] if p.get("paper_id") == "paper-shared-b")
    )
    paper["repos"] = [
        {
            "repo_url": "https://github.com/example/secret",
            "is_official": True,
            "repo_id": "444004",
            "fixture_repo": {
                "id": 444004,
                "full_name": "example/secret",
                "html_url": "https://github.com/example/secret",
                "private": True,
                "stargazers_count": 99,
            },
        }
    ]
    settings = bulk_env
    run_id = _run(settings)
    pool = http_pool()
    try:
        with connect(settings) as conn:
            assert (
                discover_and_enqueue(
                    conn,
                    source_id=SOURCE_PAPERS,
                    payload=[paper],
                    partition="priv-0",
                    run_id=run_id,
                    revision="priv-rev",
                )["created"]
                == 1
            )
            assert (
                await drain_bulk_once(
                    conn,
                    worker_id="b2-priv-fetch",
                    pool=pool,
                    source_id=SOURCE_PAPERS,
                    stage=STAGE_FETCH,
                    settings=settings,
                )
            )["ok"] is True
            parse_r = await drain_bulk_once(
                conn,
                worker_id="b2-priv-parse",
                pool=pool,
                source_id=SOURCE_PAPERS,
                stage=STAGE_PARSE,
                settings=settings,
            )
            assert parse_r["ok"] is True
            assert parse_r.get("enrich_task_id")
            external_id = parse_r["external_id"]

            enrich_r = await drain_bulk_once(
                conn,
                worker_id="b2-priv-enrich",
                pool=pool,
                source_id=SOURCE_GITHUB,
                stage=STAGE_ENRICH_STARS,
                settings=settings,
            )
            assert enrich_r is not None and enrich_r["ok"] is True
            assert enrich_r.get("quarantined") is True
            assert enrich_r.get("reason") == "repo_private_or_deleted"

            q = conn.execute(
                """
                SELECT reason, record_type, external_id, source_id
                FROM quarantines
                WHERE reason = %s AND external_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                ("repo_private_or_deleted", external_id),
            ).fetchone()
            assert q is not None, "expected quarantines row from enrich_stars private repo"
            assert q[1] == "RESEARCH_PAPER"
            assert q[3] == SOURCE_GITHUB

            cand = get_candidate(
                conn,
                source_id=SOURCE_PAPERS,
                record_type="RESEARCH_PAPER",
                external_id=external_id,
            )
            assert cand is not None
            assert cand["status"] == "quarantined"
            assert "github_stars" not in (cand["payload"].get("content") or {})
    finally:
        await pool.aclose()


def test_discover_and_enqueue_uses_b1_queue_semantic_keys(bulk_env):
    """Bulk discover wires into the same B1 persist_discovery_page / semantic_key queue."""
    from signal_ingest.adapters.bulk import SOURCE_PRODUCTS, discover_and_enqueue
    from signal_ingest.crawl.queue import get_checkpoint, semantic_key
    from signal_ingest.storage import connect

    fixture = load_bulk_json("products.json")
    settings = bulk_env
    run_id = _run(settings)
    payload = [r for r in fixture["products"] if r["product_id"] == "prod-orbit"]

    with connect(settings) as conn:
        out = discover_and_enqueue(
            conn,
            source_id=SOURCE_PRODUCTS,
            payload=payload,
            partition="q0",
            run_id=run_id,
            revision="prod-rev-1",
        )
        assert out["created"] == 1
        assert out["partition_key"].startswith("prod-rev-1:") or ":" in out["partition_key"]
        cp = get_checkpoint(
            conn,
            source_id=SOURCE_PRODUCTS,
            stage="discover",
            partition_key=out["partition_key"],
        )
        assert cp is not None
        assert cp["cursor_opaque"]

        key = semantic_key(
            SOURCE_PRODUCTS, "fetch", external_id="prod-orbit", version="1"
        )
        row = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE semantic_key = %s", (key,)
        ).fetchone()
        assert row is not None and int(row[0]) == 1

        # Replay same revision/partition does not duplicate tasks.
        again = discover_and_enqueue(
            conn,
            source_id=SOURCE_PRODUCTS,
            payload=payload,
            partition="q0",
            run_id=run_id,
            revision="prod-rev-1",
        )
        assert again["created"] == 0
        assert again["skipped"] >= 1


# ---------------------------------------------------------------------------
# R3-C bulk
# ---------------------------------------------------------------------------


def test_r3c_ai_scope_uses_word_boundaries():
    """'ai' must not match email/retail; 'ml' must not match html."""
    from signal_ingest.adapters.bulk.mapping import is_explicit_ai

    assert is_explicit_ai(description="Email marketing for retail brands") is False
    assert is_explicit_ai(tags=["HTML tooling"], industry="Retail") is False
    assert is_explicit_ai(tags=["AI"]) is True
    assert is_explicit_ai(industry="AI-powered CRM") is True
    assert is_explicit_ai(description="Applied machine learning for logistics") is True
    assert is_explicit_ai(categories=["B2B", "ML Ops"]) is True


def test_r3c_same_product_name_different_owners_not_merged():
    """Two vendors' 'Assistant' stay two products; same owner plan rows still collapse."""
    from signal_ingest.adapters.bulk import products as products_adapter
    from signal_ingest.adapters.bulk.mapping import collapse_product_plans

    rows = [
        {"name": "Assistant", "owner": "Acme AI", "url": "https://acme.example/assistant"},
        {"name": "Assistant", "owner": "Beta Labs", "url": "https://beta.example/assistant"},
        {"name": "Assistant", "owner": "Acme  ai", "plan": "Pro", "price": 20},
    ]
    collapsed = collapse_product_plans(rows)
    assert len(collapsed) == 2
    owners = sorted(r["owner"] for r in collapsed)
    assert owners == ["Acme AI", "Beta Labs"]
    items, _, _ = products_adapter.discover_from_payload(rows[:2], partition="r3c")
    assert len({i.external_id for i in items}) == 2


def test_r3c_company_field_without_evidence_quarantined(frozen_clock):
    """A bare `company` value is not ownership evidence (no default True)."""
    from signal_ingest.adapters.bulk import products as products_adapter

    row = {
        "product_id": "prod-r3c-bare",
        "productName": "Bare",
        "company": "Unproven Co",
        "url": "https://fixtures.local/products/bare",
        "pricing_url": "https://fixtures.local/products/bare/pricing",
        "pricing": "paid",
    }
    result = products_adapter.parse_observation(raw_json(row["url"], {"row": row}), row=row)
    assert result.ok is False
    assert result.quarantine_reason == "missing_ownership_evidence"
    evidenced = {**row, "company_evidenced": True}
    ok = products_adapter.parse_observation(raw_json(row["url"], {"row": evidenced}), row=evidenced)
    assert ok.ok is True, ok.quarantine_reason


def test_r3c_pricing_word_boundary_and_zero_price():
    """'product' is not 'pro'; $0 / 0.00 are FREE, not PAID."""
    from signal_ingest.adapters.bulk.mapping import classify_pricing
    from signal_ingest.models.records import PricingModel

    assert classify_pricing(plans=[{"name": "Product listing"}]) is None
    assert classify_pricing(plans=[{"name": "Starter", "price": "$0"}]) is PricingModel.FREE
    assert classify_pricing(plans=[{"name": "Basic", "price": "0.00"}]) is PricingModel.FREE
    assert classify_pricing(plans=[{"name": "Basic $0/mo"}]) is PricingModel.FREE
    assert (
        classify_pricing(plans=[{"name": "Basic", "price": "0.00"}, {"name": "Pro", "price": "$12"}])
        is PricingModel.FREEMIUM
    )
    assert classify_pricing(plans=[{"name": "Team", "price": "1,299.00"}]) is PricingModel.PAID


def test_r3c_github_url_normalization_case_and_paths():
    """Case/.git/tree/blob/query/fragment variants are one repo; gists and orgs are not repos."""
    from signal_ingest.adapters.bulk.mapping import normalize_github_url, select_primary_repo

    canonical = "https://github.com/foo/bar"
    for variant in (
        "https://github.com/Foo/Bar",
        "https://github.com/foo/bar.git",
        "http://www.github.com/foo/bar/tree/main/src",
        "github.com/foo/bar/blob/main/README.md?plain=1#L3",
        "https://github.com/foo/bar).",
    ):
        assert normalize_github_url(variant) == canonical, variant
    for bad in (
        "https://gist.github.com/foo/0123abcd",
        "https://github.com/orgs/foo/repositories",
        "https://api.github.com/repos/foo/bar",
        "https://github.com/foo",
        "https://notgithub.com/foo/bar",
    ):
        assert normalize_github_url(bad) is None, bad
    edge, reason = select_primary_repo(
        [{"repo_url": "https://github.com/Foo/Bar"}, {"repo_url": "https://github.com/foo/bar.git"}]
    )
    assert reason is None and edge["repo_url"] == canonical


def test_r3c_api_request_url_is_not_a_rename(frozen_clock):
    """Requesting api.github.com/repos/o/r for the same repo sets no renamed_from."""
    from signal_ingest.adapters.bulk import github as github_adapter

    gh = load_bulk_json("github_repos.json")
    same = github_adapter.parse_observation(
        raw_json(
            "https://api.github.com/repos/example/shared-suite",
            gh["shared_suite"],
            request_url="https://api.github.com/repos/Example/Shared-Suite",
        )
    )
    assert same.ok and same.payload["renamed_from"] is None
    renamed = github_adapter.parse_observation(
        raw_json(
            "https://api.github.com/repos/example/renamed-lib",
            gh["renamed"],
            request_url="https://api.github.com/repos/example/old-name",
        )
    )
    assert renamed.ok and renamed.payload["renamed_from"] == "example/old-name"


def test_r3c_github_rate_limit_403_schedules_retry_and_token_header(monkeypatch, frozen_clock):
    """403 + x-ratelimit-remaining: 0 → RETRY_AT at reset; plain 403 stays DENIED."""
    from datetime import datetime, timezone

    from signal_ingest.adapters.bulk import pipeline
    from signal_ingest.crawl.types import FetchOutcomeKind, RawObservation

    reset = 1_795_400_000  # after the frozen clock (2026-11-21T12:00Z)
    limited = RawObservation(
        request_url="https://api.github.com/repos/foo/bar",
        final_url="https://api.github.com/repos/foo/bar",
        status_code=403,
        outcome=FetchOutcomeKind.DENIED,
        reason="HTTP 403",
        headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(reset)},
    )
    out = pipeline._github_rate_limit_as_retry(limited)
    assert out.outcome is FetchOutcomeKind.RETRY_AT
    assert out.retry_at == datetime.fromtimestamp(reset, tz=timezone.utc)

    plain = RawObservation(
        request_url=limited.request_url,
        final_url=limited.final_url,
        status_code=403,
        outcome=FetchOutcomeKind.DENIED,
        reason="HTTP 403",
        headers={"x-ratelimit-remaining": "41"},
    )
    assert pipeline._github_rate_limit_as_retry(plain).outcome is FetchOutcomeKind.DENIED

    for name in ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_PAT_TOKEN", "GITHUB_PERSONAL_ACCESS_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    assert "Authorization" not in (pipeline._github_headers("https://api.github.com/repos/a/b") or {})
    monkeypatch.setenv("GITHUB_TOKEN", "test-token-r3c")
    assert pipeline._github_headers("https://api.github.com/repos/a/b")["Authorization"] == (
        "Bearer test-token-r3c"
    )
    # Token is never attached to non-GitHub hosts.
    assert pipeline._github_headers("https://fixtures.local/papers/x") is None


def test_r3c_archive_stars_need_observation_time_and_date_only_precision(frozen_clock):
    """Archive stars without starsObservedAt are not accepted; date-only keeps day precision."""
    from signal_ingest.adapters.bulk import papers as papers_adapter

    row = {
        "arxiv_id": "2404.09001",
        "title": "Archive Stars Paper",
        "authors": ["G. Gamma"],
        "paper_url": "https://arxiv.org/abs/2404.09001",
        "published_date": "2024-04-03",
    }
    edge = {"repo_url": "https://github.com/example/archive", "is_official": True, "stars": 55}
    untimed = papers_adapter.parse_observation(raw_json(row["paper_url"], row), row=row, repo_edges=[edge])
    assert untimed.ok
    assert "github_stars" not in untimed.payload["content"]
    assert untimed.eligible is False and untimed.enrich_stars is not None
    assert untimed.payload["meta"]["published_date_precision"] == "day"
    assert untimed.payload["meta"]["published_date_raw"] == "2024-04-03"

    timed_edge = {**edge, "starsObservedAt": "2026-11-21T08:00:00Z"}
    timed = papers_adapter.parse_observation(
        raw_json(row["paper_url"], row), row=row, repo_edges=[timed_edge]
    )
    assert timed.payload["content"]["github_stars"] == 55
    assert timed.payload["meta"]["starsObservedAt"] == "2026-11-21T08:00:00Z"

    exact = {**row, "published_date": "2024-04-03T10:00:00Z"}
    r = papers_adapter.parse_observation(raw_json(row["paper_url"], exact), row=exact, repo_edges=[edge])
    assert r.payload["meta"]["published_date_precision"] == "instant"


@pytest.mark.asyncio
async def test_r3c_two_papers_sharing_repo_both_get_stars_in_one_run(bulk_env, frozen_clock):
    """Shared-repo papers each get an enrich task and both become accepted in one run;
    a new run gets a fresh star-lookup key (stars refresh across runs)."""
    from copy import deepcopy

    from signal_ingest.adapters.bulk import (
        SOURCE_GITHUB,
        SOURCE_PAPERS,
        STAGE_ENRICH_STARS,
        STAGE_FETCH,
        STAGE_PARSE,
        discover_and_enqueue,
        drain_bulk_once,
        get_candidate,
    )
    from signal_ingest.adapters.bulk import pipeline
    from signal_ingest.storage import connect

    fixture = load_bulk_json("papers.json")
    papers = []
    for pid in ("paper-shared-a", "paper-shared-b"):
        paper = deepcopy(next(p for p in fixture["papers"] if p.get("paper_id") == pid))
        paper["repos"][0]["fixture_repo"] = load_bulk_json("github_repos.json")["shared_suite"]
        papers.append(paper)
    settings = bulk_env
    run_id = _run(settings)
    pool = http_pool()
    try:
        with connect(settings) as conn:
            enq = discover_and_enqueue(
                conn, source_id=SOURCE_PAPERS, payload=papers, partition="r3c-share",
                run_id=run_id, revision="r3c-share-rev",
            )
            assert enq["created"] == 2
            for _ in range(2):
                r = await drain_bulk_once(conn, worker_id="r3c-f", pool=pool,
                                          source_id=SOURCE_PAPERS, stage=STAGE_FETCH, settings=settings)
                assert r and r["ok"]
            enrich_ids = []
            for _ in range(2):
                r = await drain_bulk_once(conn, worker_id="r3c-p", pool=pool,
                                          source_id=SOURCE_PAPERS, stage=STAGE_PARSE, settings=settings)
                assert r and r["ok"]
                enrich_ids.append(r.get("enrich_task_id"))
            assert all(enrich_ids) and len(set(enrich_ids)) == 2
            for _ in range(2):
                r = await drain_bulk_once(conn, worker_id="r3c-e", pool=pool,
                                          source_id=SOURCE_GITHUB, stage=STAGE_ENRICH_STARS,
                                          settings=settings)
                assert r and r["ok"] and r.get("stars") == 7
            for ext in ("2404.00004", "2404.00005"):
                cand = get_candidate(conn, source_id=SOURCE_PAPERS, record_type="RESEARCH_PAPER",
                                     external_id=ext)
                assert cand is not None and cand["status"] == "accepted", ext
                assert cand["payload"]["content"]["github_stars"] == 7
                assert cand["payload"]["meta"]["starsObservedAt"]
            pending = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE source_id = %s AND stage = %s AND status <> 'done'",
                (SOURCE_GITHUB, STAGE_ENRICH_STARS),
            ).fetchone()
            assert int(pending[0]) == 0

            # Later run: same paper/repo gets a new star-lookup task (not deduped forever).
            parse_task = conn.execute(
                "SELECT id FROM tasks WHERE source_id = %s AND stage = %s LIMIT 1",
                (SOURCE_PAPERS, STAGE_PARSE),
            ).fetchone()
            run2 = _run(settings)
            from signal_ingest.crawl.types import ClaimedTask

            fake = ClaimedTask(
                **{
                    **{f: None for f in ClaimedTask.__dataclass_fields__},
                    "id": parse_task[0], "run_id": run2, "source_id": SOURCE_PAPERS,
                    "stage": STAGE_PARSE, "payload": {},
                }
            )
            new_id = pipeline._enqueue_enrich_stars(
                conn, fake,
                enrich={"repo_url": "https://github.com/example/shared-suite", "repo_id": "222002"},
                candidate_external_id="2404.00004",
            )
            assert new_id is not None
            conn.rollback()
    finally:
        await pool.aclose()
