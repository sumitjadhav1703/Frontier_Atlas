-- R4: pgvector candidate recall on entities + typed, time-bounded relationships.
-- Embeddings only suggest candidates; deterministic resolution decides merges.

CREATE EXTENSION IF NOT EXISTS vector;

ALTER TABLE entities ADD COLUMN IF NOT EXISTS embedding vector(384);
ALTER TABLE entities ADD COLUMN IF NOT EXISTS embedding_model TEXT;

CREATE INDEX IF NOT EXISTS idx_entities_embedding_hnsw
    ON entities USING hnsw (embedding vector_cosine_ops);

ALTER TABLE relationships
    ADD COLUMN IF NOT EXISTS evidence_observation_id UUID REFERENCES observations (id);
ALTER TABLE relationships ADD COLUMN IF NOT EXISTS valid_from TIMESTAMPTZ;
ALTER TABLE relationships ADD COLUMN IF NOT EXISTS valid_to TIMESTAMPTZ;

-- Replace the B2 inline CHECK (auto-named) with the widened typed set.
ALTER TABLE relationships DROP CONSTRAINT IF EXISTS relationships_relation_type_check;
ALTER TABLE relationships ADD CONSTRAINT relationships_relation_type_check
    CHECK (relation_type IN (
        'OWNS', 'IMPLEMENTED_BY', 'ALIASED_AS', 'SAME_AS',
        'FOUNDED_BY', 'AUTHORED', 'MENTIONS', 'HIRING'
    ));

