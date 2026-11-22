-- B0 minimal schema: runs + typed payloads with lineage mode.
-- Fixture lineage is durable so live promotion can be rejected.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS runs (
    id UUID PRIMARY KEY,
    mode TEXT NOT NULL CHECK (mode IN ('fixture', 'live')),
    status TEXT NOT NULL DEFAULT 'initialized',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cutoff_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_runs_mode_created ON runs (mode, created_at DESC);

CREATE TABLE IF NOT EXISTS records (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs (id),
    record_type TEXT NOT NULL CHECK (
        record_type IN ('STARTUP', 'PRODUCT', 'RESEARCH_PAPER', 'JOB', 'NEWS')
    ),
    payload JSONB NOT NULL,
    lineage_mode TEXT NOT NULL CHECK (lineage_mode IN ('fixture', 'live')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION enforce_record_lineage()
RETURNS TRIGGER AS $$
DECLARE
    run_mode TEXT;
BEGIN
    SELECT mode INTO run_mode FROM runs WHERE id = NEW.run_id;
    IF run_mode IS NULL THEN
        RAISE EXCEPTION 'run % not found', NEW.run_id;
    END IF;
    IF NEW.lineage_mode IS DISTINCT FROM run_mode THEN
        RAISE EXCEPTION 'record lineage_mode % does not match run mode %',
            NEW.lineage_mode, run_mode;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_records_lineage ON records;
CREATE TRIGGER trg_records_lineage
    BEFORE INSERT OR UPDATE ON records
    FOR EACH ROW
    EXECUTE FUNCTION enforce_record_lineage();

CREATE INDEX IF NOT EXISTS idx_records_run ON records (run_id);
CREATE INDEX IF NOT EXISTS idx_records_type ON records (record_type);
CREATE INDEX IF NOT EXISTS idx_records_lineage ON records (lineage_mode);
