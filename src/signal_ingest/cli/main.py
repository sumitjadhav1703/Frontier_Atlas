"""signal-ingest CLI (Typer)."""

from __future__ import annotations

import json
from typing import Optional
from uuid import UUID

import typer

from signal_ingest.config.settings import Mode, load_settings
from signal_ingest.storage.db import init_db

app = typer.Typer(
    name="signal-ingest",
    help="AI Signal ingestion pipeline",
    no_args_is_help=True,
)


def _configure_logging() -> None:
    import logging
    import os
    import sys

    root = logging.getLogger()
    if root.handlers:
        return
    # Real stderr: stdout stays pure JSON for scripts and CliRunner tests.
    handler = logging.StreamHandler(sys.__stderr__)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(handler)
    root.setLevel(os.environ.get("SIGNAL_INGEST_LOG_LEVEL", "INFO").upper())
    logging.getLogger("httpx").setLevel(logging.WARNING)


@app.callback()
def _root(
    ctx: typer.Context,
    mode: Optional[str] = typer.Option(
        None,
        "--mode",
        help="Execution mode: fixture | live (default: env/fixture).",
    ),
) -> None:
    """Load settings and install the network guard when enabled."""
    _configure_logging()
    overrides: dict[str, object] = {}
    if mode is not None:
        overrides["mode"] = Mode(mode)
    settings = load_settings(**overrides)
    ctx.ensure_object(dict)
    ctx.obj["settings"] = settings


@app.command("init-db")
def init_db_cmd(ctx: typer.Context) -> None:
    """Apply migrations for the active mode DSN."""
    settings = ctx.obj["settings"]
    applied = init_db(settings)
    if applied:
        typer.echo(f"Applied migrations ({settings.mode.value}): {', '.join(applied)}")
    else:
        typer.echo(f"Migrations already applied ({settings.mode.value}).")


@app.command("crawl")
def crawl_cmd(
    ctx: typer.Context,
    source: str = typer.Option(..., "--source", help="Source ID (e.g. bulk.yc)"),
    run_id: Optional[str] = typer.Option(None, "--run", help="Existing run UUID"),
    partition: str = typer.Option("default", "--partition"),
) -> None:
    """Discover + enqueue from fixture payloads (fixture mode)."""
    from signal_ingest.pipeline.crawl import crawl_source

    settings = ctx.obj["settings"]
    result = crawl_source(
        source_id=source,
        settings=settings,
        run_id=UUID(run_id) if run_id else None,
        partition=partition,
    )
    typer.echo(json.dumps(result, indent=2, default=str))


@app.command("work")
def work_cmd(
    ctx: typer.Context,
    stage: str = typer.Option(..., "--stage", help="fetch|parse|enrich_stars|any"),
    run_id: str = typer.Option(..., "--run"),
    worker_id: str = typer.Option("worker-1", "--worker-id"),
    max_tasks: int = typer.Option(100, "--max-tasks"),
    source: Optional[str] = typer.Option(None, "--source"),
    concurrency: int = typer.Option(1, "--concurrency", min=1, help="Concurrent task slots."),
    drain: bool = typer.Option(
        False,
        "--drain",
        help="Wait for scheduled retries/expired leases until the run has no ready/leased "
        "tasks, or --max-tasks is reached.",
    ),
    lease_seconds: int = typer.Option(60, "--lease-seconds", min=1),
) -> None:
    """Claim and process tasks for a run under the shared raw root."""
    from signal_ingest.pipeline.work import work_stage

    settings = ctx.obj["settings"]
    result = work_stage(
        run_id=run_id,
        stage=stage,
        settings=settings,
        worker_id=worker_id,
        max_tasks=max_tasks,
        source_id=source,
        concurrency=concurrency,
        drain=drain,
        lease_seconds=lease_seconds,
    )
    typer.echo(json.dumps(result, indent=2, default=str))


@app.command("requeue-failed")
def requeue_failed_cmd(
    ctx: typer.Context,
    run_id: str = typer.Option(..., "--run"),
) -> None:
    """Give dead-lettered (failed) tasks of a run a fresh attempt budget."""
    from uuid import UUID

    from signal_ingest.crawl.queue import requeue_failed
    from signal_ingest.storage.db import connect

    with connect(ctx.obj["settings"]) as conn:
        count = requeue_failed(conn, run_id=UUID(run_id))
    typer.echo(json.dumps({"run_id": run_id, "requeued": count}))


@app.command("resume")
def resume_cmd(
    ctx: typer.Context,
    run_id: str = typer.Option(..., "--run"),
    worker_id: str = typer.Option("worker-resume", "--worker-id"),
    stage: str = typer.Option("any", "--stage"),
    max_tasks: int = typer.Option(500, "--max-tasks"),
) -> None:
    """Release stale leases and continue ready work for a run."""
    from signal_ingest.pipeline.resume import resume_run

    settings = ctx.obj["settings"]
    result = resume_run(
        run_id=run_id,
        settings=settings,
        worker_id=worker_id,
        stage=stage,
        max_tasks=max_tasks,
    )
    typer.echo(json.dumps(result, indent=2, default=str))


@app.command("audit")
def audit_cmd(
    ctx: typer.Context,
    run_id: str = typer.Option(..., "--run"),
) -> None:
    """Summarize counts, lineage, quarantines and content_key coverage."""
    from signal_ingest.pipeline.audit import audit_run

    settings = ctx.obj["settings"]
    result = audit_run(run_id=run_id, settings=settings)
    typer.echo(json.dumps(result, indent=2, default=str))
    raise typer.Exit(code=0 if result.get("ok") else 1)


@app.command("export")
def export_cmd(
    ctx: typer.Context,
    run_id: str = typer.Option(..., "--run"),
    as_of: str = typer.Option(..., "--as-of", help="UTC cutoff ISO-8601"),
    spreadsheet_id: Optional[str] = typer.Option(
        None,
        "--spreadsheet-id",
        help="Optional mock spreadsheet id (fixture uses MockSheetsTransport)",
    ),
) -> None:
    """Project six tabs to a mode-segregated local export (+ mock Sheets in fixture)."""
    from signal_ingest.export import run_export

    settings = ctx.obj["settings"]
    result = run_export(
        run_id=run_id,
        as_of=as_of,
        settings=settings,
        spreadsheet_id=spreadsheet_id,
    )
    typer.echo(json.dumps(result, indent=2, default=str))


@app.command("bench")
def bench_cmd(
    ctx: typer.Context,
    write: bool = typer.Option(
        True,
        "--write/--no-write",
        help="Write artifacts/gates/b7_benchmark.json",
    ),
) -> None:
    """Run synthetic 1k/10k × 1/4 worker benchmark (not live throughput)."""
    from signal_ingest.bench import run_benchmark, write_benchmark_artifact

    _ = ctx.obj["settings"]
    report = run_benchmark()
    if write:
        path = write_benchmark_artifact(report)
        typer.echo(f"wrote {path}")
    typer.echo(json.dumps(report, indent=2, default=str))


@app.command("qualify")
def qualify_cmd(
    ctx: typer.Context,
    track: str = typer.Option(
        "all",
        "--track",
        help="all|monitors|bulk|canary|seeds",
    ),
    update_inventory: bool = typer.Option(
        True,
        "--update-inventory/--no-update-inventory",
        help="Write candidate→qualified|disabled on config/sources/*.yaml",
    ),
    init_database: bool = typer.Option(
        True,
        "--init-db/--no-init-db",
        help="Apply live DB migrations before probes",
    ),
) -> None:
    """B8 live qualification: monitors, bulk pilots, LLM canary (independent tracks)."""
    from signal_ingest.config.settings import Mode
    from signal_ingest.live.orchestrate import run_live_qualification
    from signal_ingest.live.settings import load_live_settings

    # Force live namespace even if shell defaulted to fixture.
    settings = load_live_settings()
    ctx.obj["settings"] = settings
    if settings.mode is not Mode.LIVE:
        raise typer.BadParameter("qualify requires live mode")
    result = run_live_qualification(
        settings=settings,
        track=track,  # type: ignore[arg-type]
        update_inventory=update_inventory,
        init_database=init_database,
    )
    typer.echo(json.dumps(result, indent=2, default=str))
    canary = result.get("canary") or {}
    # Exit 2 when LLM canary cannot succeed — monitors/bulk manifests may still exist.
    if track in {"all", "canary"} and not canary.get("live_llm_success"):
        raise typer.Exit(code=2)


@app.command("pilot")
def pilot_cmd(
    ctx: typer.Context,
    bulk: bool = typer.Option(True, "--bulk/--no-bulk"),
    monitors: bool = typer.Option(False, "--monitors/--no-monitors"),
) -> None:
    """Run live bulk pilots and/or monitor probes without claiming LLM success."""
    from signal_ingest.live.orchestrate import run_live_qualification
    from signal_ingest.live.settings import load_live_settings

    settings = load_live_settings()
    ctx.obj["settings"] = settings
    if bulk and monitors:
        track = "all"
    elif monitors:
        track = "monitors"
    else:
        track = "bulk"
    # Pilot skips canary by using track monitors/bulk; for both, run sequentially.
    if bulk and monitors:
        mon = run_live_qualification(
            settings=settings, track="monitors", init_database=True
        )
        bul = run_live_qualification(
            settings=settings, track="bulk", init_database=False
        )
        result = {"monitors": mon.get("monitors"), "bulk": bul.get("bulk")}
    else:
        result = run_live_qualification(
            settings=settings, track=track, init_database=True  # type: ignore[arg-type]
        )
    typer.echo(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    app()
