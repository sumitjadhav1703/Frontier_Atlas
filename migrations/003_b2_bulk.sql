-- B2 bulk: candidates, relationships, repo metrics, quarantines.

CREATE TABLE IF NOT EXISTS candidates (
    id UUID PRIMARY KEY,
    run_id UUID REFERENCES runs (id),
    source_id TEXT NOT NULL,
    record_type TEXT NOT NULL
        CHECK (record_type IN (
            'STARTUP', 'PRODUCT', 'RESEARCH_PAPER', 'JOB', 'NEWS'
        )),
    external_id TEXT NOT NULL,
    semantic_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending', 'accepted', 'quarantined', 'ineligible', 'superseded'
        )),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    observation_id UUID REFERENCES observations (id),
    collected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_candidates_semantic_key UNIQUE (semantic_key)
);

CREATE INDEX IF NOT EXISTS idx_candidates_source_status
    ON candidates (source_id, status, record_type);

CREATE INDEX IF NOT EXISTS idx_candidates_external
    ON candidates (source_id, external_id);

CREATE TABLE IF NOT EXISTS relationships (
    id UUID PRIMARY KEY,
    run_id UUID REFERENCES runs (id),
    relation_type TEXT NOT NULL
        CHECK (relation_type IN (
            'OWNS', 'IMPLEMENTED_BY', 'ALIASED_AS', 'SAME_AS'
        )),
    from_kind TEXT NOT NULL,
    from_key TEXT NOT NULL,
    to_kind TEXT NOT NULL,
    to_key TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    observation_id UUID REFERENCES observations (id),
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'accepted'
        CHECK (status IN ('accepted', 'rejected', 'quarantined', 'pending')),
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_relationships_edge UNIQUE (
        relation_type, from_kind, from_key, to_kind, to_key
    )
);

CREATE INDEX IF NOT EXISTS idx_relationships_from
    ON relationships (from_kind, from_key, relation_type);

CREATE INDEX IF NOT EXISTS idx_relationships_to
    ON relationships (to_kind, to_key, relation_type);

CREATE TABLE IF NOT EXISTS repo_metrics (
    id UUID PRIMARY KEY,
    repo_id TEXT NOT NULL,
    repo_url TEXT NOT NULL,
    stars INTEGER NOT NULL CHECK (stars >= 0),
    observed_at TIMESTAMPTZ NOT NULL,
    etag TEXT,
    content_sha256 TEXT,
    observation_id UUID REFERENCES observations (id),
    source_id TEXT NOT NULL DEFAULT 'bulk.github',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_repo_metrics_snapshot UNIQUE (repo_id, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_repo_metrics_repo_observed
    ON repo_metrics (repo_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_repo_metrics_url
    ON repo_metrics (repo_url);

CREATE TABLE IF NOT EXISTS quarantines (
    id UUID PRIMARY KEY,
    run_id UUID REFERENCES runs (id),
    source_id TEXT NOT NULL,
    record_type TEXT,
    external_id TEXT,
    reason TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    observation_id UUID REFERENCES observations (id),
    semantic_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quarantines_source_reason
    ON quarantines (source_id, reason, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_quarantines_external
    ON quarantines (source_id, external_id);
