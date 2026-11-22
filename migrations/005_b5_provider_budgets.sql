-- B5: shared per-provider quota reservations (reserve-before-dispatch).
-- Workers coordinate through PostgreSQL so adding processes cannot multiply quota.

CREATE TABLE IF NOT EXISTS provider_budgets (
    provider TEXT NOT NULL,
    window_kind TEXT NOT NULL
        CHECK (window_kind IN ('day', 'run', 'minute', 'cooldown')),
    window_start TIMESTAMPTZ NOT NULL,
    requests_limit INTEGER NOT NULL CHECK (requests_limit >= 0),
    requests_reserved INTEGER NOT NULL DEFAULT 0 CHECK (requests_reserved >= 0),
    requests_used INTEGER NOT NULL DEFAULT 0 CHECK (requests_used >= 0),
    tokens_limit BIGINT NOT NULL CHECK (tokens_limit >= 0),
    tokens_reserved BIGINT NOT NULL DEFAULT 0 CHECK (tokens_reserved >= 0),
    tokens_used BIGINT NOT NULL DEFAULT 0 CHECK (tokens_used >= 0),
    paid_spend_micros BIGINT NOT NULL DEFAULT 0 CHECK (paid_spend_micros >= 0),
    paid_spend_cap_micros BIGINT NOT NULL DEFAULT 0 CHECK (paid_spend_cap_micros >= 0),
    cooldown_until TIMESTAMPTZ,
    circuit_state TEXT NOT NULL DEFAULT 'closed'
        CHECK (circuit_state IN ('closed', 'open', 'half_open')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (provider, window_kind, window_start)
);

CREATE INDEX IF NOT EXISTS idx_provider_budgets_cooldown
    ON provider_budgets (provider, cooldown_until)
    WHERE cooldown_until IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_provider_budgets_updated
    ON provider_budgets (updated_at DESC);
