-- B6: deterministic entity resolution crosswalk and mapping log.

CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    namespace TEXT NOT NULL
        CHECK (namespace IN ('startup', 'product')),
    canonical_name TEXT NOT NULL,
    owner_id TEXT REFERENCES entities (id),
    lineage_mode TEXT NOT NULL
        CHECK (lineage_mode IN ('fixture', 'live')),
    comparison_key TEXT NOT NULL,
    evidence_url TEXT,
    seed_revision TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT entities_product_owner_chk CHECK (
        (namespace = 'startup' AND owner_id IS NULL)
        OR (namespace = 'product' AND owner_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_entities_namespace_key
    ON entities (namespace, comparison_key);

CREATE INDEX IF NOT EXISTS idx_entities_owner
    ON entities (owner_id)
    WHERE owner_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_entities_lineage
    ON entities (lineage_mode, namespace);

CREATE TABLE IF NOT EXISTS source_identities (
    id UUID PRIMARY KEY,
    entity_id TEXT NOT NULL REFERENCES entities (id),
    namespace TEXT NOT NULL
        CHECK (namespace IN ('startup', 'product')),
    identity_kind TEXT NOT NULL
        CHECK (identity_kind IN (
            'alias', 'source_id', 'domain', 'comparison_key', 'canonical_name'
        )),
    raw_value TEXT NOT NULL,
    comparison_key TEXT NOT NULL,
    owner_id TEXT REFERENCES entities (id),
    lineage_mode TEXT NOT NULL
        CHECK (lineage_mode IN ('fixture', 'live')),
    evidence_url TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Nullable owner_id: coalesce so product-scoped aliases stay unique per owner.
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_identities_lookup
    ON source_identities (
        namespace,
        identity_kind,
        comparison_key,
        COALESCE(owner_id, '')
    );

CREATE INDEX IF NOT EXISTS idx_source_identities_entity
    ON source_identities (entity_id);

CREATE INDEX IF NOT EXISTS idx_source_identities_raw
    ON source_identities (namespace, raw_value);

CREATE TABLE IF NOT EXISTS resolution_events (
    id UUID PRIMARY KEY,
    observation_key TEXT NOT NULL,
    run_id UUID REFERENCES runs (id),
    entity_type TEXT NOT NULL
        CHECK (entity_type IN ('startup', 'product')),
    raw_name TEXT NOT NULL,
    canonical_name TEXT,
    canonical_id TEXT,
    method TEXT NOT NULL,
    decision TEXT NOT NULL
        CHECK (decision IN (
            'match', 'new', 'abstain', 'unchanged', 'merged', 'unresolved'
        )),
    evidence_url TEXT,
    rule_version TEXT NOT NULL,
    candidate_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    comparison_key TEXT,
    reason TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_resolution_events_observation_key UNIQUE (observation_key)
);

CREATE INDEX IF NOT EXISTS idx_resolution_events_run
    ON resolution_events (run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_resolution_events_canonical
    ON resolution_events (canonical_id)
    WHERE canonical_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_resolution_events_decision
    ON resolution_events (decision, entity_type);
