# R4 work order: pgvector candidate suggestions + typed relationships (coordinator-owned)

R4 reopens B6 and B7. B8 runs in parallel on disjoint files: `live/*`, `adapters/bulk/*`, `pipeline/crawl.py`, `cli/main.py` and `config/sources`. R4 must not edit those files.

## Environment (done by coordinator)
- pgvector 0.8.7 was built from source for Homebrew PostgreSQL 16.
- `CREATE EXTENSION vector` already exists in signal_fixture, signal_live and template1. The `signal` role can run `CREATE EXTENSION IF NOT EXISTS vector`.
- Python deps are installed: `fastembed==0.9.1`, `pgvector==0.5.1` (psycopg adapter).

## Rules
- Never read, cat, grep, open or print `.env`.
- Embeddings never auto-merge entities. Deterministic resolution decides every merge, so the B6 FP=0 gate holds. Embeddings only suggest.
- Offline tests use an injected deterministic embedder (e.g. hashed bag-of-words into 384 dims). No model download in tests and no network.

## Scope
1. **`migrations/009_r4_vector.sql`**
   - `CREATE EXTENSION IF NOT EXISTS vector;`
   - Add `embedding vector(384)` and `embedding_model text` to the entities table. Find its real name in `migrations/00*.sql`; resolution stores canonical entities there.
   - Add an HNSW index `USING hnsw (embedding vector_cosine_ops)`.
   - Extend the existing `relationships` table (`migrations/003_b2_bulk.sql:32`) with nullable `evidence_observation_id` (FK to the observations table if it has a stable PK), `valid_from timestamptz` and `valid_to timestamptz`.
   - Add a CHECK on the relationship type over the existing types plus FOUNDED_BY, AUTHORED, IMPLEMENTED_BY, OWNS, MENTIONS and HIRING. Inspect the existing values first so nothing already written violates it.
   - The migration must be idempotent and must apply through the existing migration runner (`signal-ingest init-db`).
2. **`src/signal_ingest/resolution/embed.py`**
   - `Embedder` protocol: `embed(texts) -> list[list[float]]`, plus `model_id`.
   - `FastEmbedder`: fastembed `TextEmbedding("BAAI/bge-small-en-v1.5")`, lazily constructed; the live path downloads the model once.
   - `embed_entities(conn, embedder)`: embeds canonical name + description of entities whose embedding is NULL or whose `embedding_model` changed.
   - `suggest_candidates(conn, text, *, k=3, min_similarity=0.85, exclude_entity_id=None)`: an HNSW cosine query returning `[{entity_id, canonical_name, similarity}]`.
3. **Entity Mapping Log**
   - Add a `suggested_candidates` column to the mapping rows in `resolution/mapping.py` and the export projection (`export/project.py` `_mapping_rows`). Write it as compact JSON or `name (0.91); …`.
   - Fill it for abstained/unresolved mentions, and for resolved ones when the top other candidate is above threshold. Leave it empty when no embeddings exist, so fixture exports stay byte-stable unless embeddings were computed. Check B7 export determinism tests.
4. **Near-duplicate news:** a helper that flags a news row whose title embedding has similarity ≥0.95 with another row in the same run. Store it as a flag in the row metadata or export column `nearDuplicateOf`. Never drop the row.
5. **Graph demo**
   - Write `docs/queries/papers_by_yc_startups.sql`, a `WITH RECURSIVE` query over relationships: paper -[IMPLEMENTED_BY]-> repo <-[OWNS]- organization, where the organization is a YC startup. Extend it to multi-hop OWNS chains.
   - Add `resolution/graph_queries.py` to run it.
   - Add a README section, 5–8 lines.

## Tests (verifier), in `tests/gates/test_b6.py` (and `test_b7.py` for the export column)
- The migration applies twice cleanly, and the vector column and HNSW index exist.
- `suggest_candidates` returns the nearest entity with the fake embedder, and excludes self.
- The suggestion never changes a resolution decision: run B6 resolution with and without embeddings and get identical decisions and identical TP/FP/FN metrics (FP=0).
- The mapping log row carries `suggested_candidates` when embeddings exist, and is empty otherwise.
- Near-duplicate titles are flagged and not dropped.
- The recursive CTE returns the expected paper for a seeded fixture graph (paper → repo → org), including a 2-hop OWNS chain.
- The relationship type CHECK rejects an unknown type.
