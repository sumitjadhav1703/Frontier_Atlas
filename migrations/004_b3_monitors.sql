-- B3 monitors: per-source run ledger for news/jobs freshness scans.

CREATE TABLE IF NOT EXISTS monitor_runs (
    id UUID PRIMARY KEY,
    run_id UUID REFERENCES runs (id),
    source_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    cutoff_at TIMESTAMPTZ NOT NULL,
    health TEXT NOT NULL
        CHECK (health IN (
            'healthy',
            'healthy_empty',
            'partial',
            'rate_limited',
            'failed'
        )),
    pagination_complete BOOLEAN NOT NULL DEFAULT FALSE,
    feed_ordered BOOLEAN,
    descending_proven BOOLEAN,
    capped_feed BOOLEAN NOT NULL DEFAULT FALSE,
    scan_lower TIMESTAMPTZ,
    scan_upper TIMESTAMPTZ,
    discovered_count INTEGER NOT NULL DEFAULT 0 CHECK (discovered_count >= 0),
    fetched_count INTEGER NOT NULL DEFAULT 0 CHECK (fetched_count >= 0),
    accepted_count INTEGER NOT NULL DEFAULT 0 CHECK (accepted_count >= 0),
    rejected_count INTEGER NOT NULL DEFAULT 0 CHECK (rejected_count >= 0),
    pending_count INTEGER NOT NULL DEFAULT 0 CHECK (pending_count >= 0),
    limitation TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_monitor_runs_source_started
    ON monitor_runs (source_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_monitor_runs_run
    ON monitor_runs (run_id);

CREATE INDEX IF NOT EXISTS idx_monitor_runs_health
    ON monitor_runs (health, source_id);
