-- B1 crawler: checkpoints, leased tasks, content-addressed observations.

CREATE TABLE IF NOT EXISTS source_checkpoints (
    source_id TEXT NOT NULL,
    stage TEXT NOT NULL DEFAULT 'discover',
    partition_key TEXT NOT NULL DEFAULT '',
    cursor_opaque TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (source_id, stage, partition_key)
);

CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    run_id UUID REFERENCES runs (id),
    source_id TEXT NOT NULL,
    stage TEXT NOT NULL DEFAULT 'fetch',
    semantic_key TEXT NOT NULL,
    external_id TEXT,
    url TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ready'
        CHECK (status IN (
            'ready', 'leased', 'done', 'failed',
            'denied', 'blocked', 'not_found', 'invalid'
        )),
    attempt_count INTEGER NOT NULL DEFAULT 0
        CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 5
        CHECK (max_attempts >= 1),
    next_attempt TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner TEXT,
    lease_expires TIMESTAMPTZ,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    parent_task_id UUID REFERENCES tasks (id),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tasks_semantic_key UNIQUE (semantic_key)
);

CREATE INDEX IF NOT EXISTS idx_tasks_ready
    ON tasks (source_id, stage, next_attempt)
    WHERE status IN ('ready', 'leased');

CREATE INDEX IF NOT EXISTS idx_tasks_run ON tasks (run_id);

CREATE TABLE IF NOT EXISTS observations (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks (id),
    run_id UUID REFERENCES runs (id),
    source_id TEXT NOT NULL,
    request_url TEXT NOT NULL,
    final_url TEXT NOT NULL,
    status_code INTEGER,
    outcome TEXT NOT NULL
        CHECK (outcome IN (
            'ok', 'retry_at', 'denied', 'not_found',
            'invalid', 'blocked', 'not_modified'
        )),
    content_type TEXT,
    content_key TEXT,
    content_sha256 TEXT,
    content_length INTEGER,
    fetched_at TIMESTAMPTZ NOT NULL,
    etag TEXT,
    last_modified TEXT,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_observations_task UNIQUE (task_id)
);

CREATE INDEX IF NOT EXISTS idx_observations_source_fetched
    ON observations (source_id, fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_observations_sha
    ON observations (content_sha256)
    WHERE content_sha256 IS NOT NULL;
