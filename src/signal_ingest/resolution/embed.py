"""R4 embedding recall: suggest candidate entities, never decide merges."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Protocol, Sequence

import psycopg

_REPO_ROOT = Path(__file__).resolve().parents[3]

EMBEDDING_DIM = 384
DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"


class Embedder(Protocol):
    model_id: str

    def embed(self, texts: Sequence[str]) -> list[list[float]]: ...


class FastEmbedder:
    """fastembed ONNX model; constructed on first embed (downloads once, ~67 MB)."""

    def __init__(self, model_name: str = DEFAULT_MODEL) -> None:
        self.model_id = model_name
        self._model: Any = None

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        if self._model is None:
            from fastembed import TextEmbedding

            # Durable cache: the default $TMPDIR cache can be cleared by macOS.
            self._model = TextEmbedding(self.model_id, cache_dir=os.environ.get("FASTEMBED_CACHE_PATH", str(_REPO_ROOT / "var" / "cache" / "fastembed")))
        return [vec.tolist() for vec in self._model.embed(list(texts))]


def to_vector_literal(vec: Sequence[float]) -> str:
    if len(vec) != EMBEDDING_DIM:
        raise ValueError(f"embedding has {len(vec)} dims, expected {EMBEDDING_DIM}")
    return "[" + ",".join(repr(float(x)) for x in vec) + "]"


def entity_text(canonical_name: str, description: str | None) -> str:
    return f"{canonical_name}. {description}" if description else canonical_name


def embed_entities(
    conn: psycopg.Connection, embedder: Embedder, *, batch_size: int = 256
) -> int:
    """Embed entities with no embedding or one from another model. Returns count."""
    rows = conn.execute(
        """
        SELECT id, canonical_name, metadata->>'description'
        FROM entities
        WHERE embedding IS NULL OR embedding_model IS DISTINCT FROM %s
        ORDER BY id
        """,
        (embedder.model_id,),
    ).fetchall()
    for start in range(0, len(rows), batch_size):
        chunk = rows[start : start + batch_size]
        vectors = embedder.embed([entity_text(name, desc) for _, name, desc in chunk])
        with conn.cursor() as cur:
            cur.executemany(
                "UPDATE entities SET embedding = %s::vector, embedding_model = %s WHERE id = %s",
                [
                    (to_vector_literal(vec), embedder.model_id, entity_id)
                    for (entity_id, _, _), vec in zip(chunk, vectors, strict=True)
                ],
            )
    return len(rows)


def suggest_candidates(
    conn: psycopg.Connection,
    text: str,
    *,
    embedder: Embedder,
    k: int = 3,
    min_similarity: float = 0.85,
    exclude_entity_id: str | None = None,
    namespace: str | None = None,
) -> list[dict[str, Any]]:
    """HNSW cosine top-k over entities embedded by the same model.

    embedder is required so no call path downloads a model implicitly.
    """
    query = to_vector_literal(embedder.embed([text])[0])
    rows = conn.execute(
        """
        SELECT id, canonical_name, 1 - (embedding <=> %(q)s::vector) AS similarity
        FROM entities
        WHERE embedding IS NOT NULL
          AND embedding_model = %(model)s
          AND (%(exclude)s::text IS NULL OR id <> %(exclude)s::text)
          AND (%(ns)s::text IS NULL OR namespace = %(ns)s::text)
        ORDER BY embedding <=> %(q)s::vector
        LIMIT %(k)s
        """,
        {
            "q": query,
            "model": embedder.model_id,
            "exclude": exclude_entity_id,
            "ns": namespace,
            "k": k,
        },
    ).fetchall()
    # Tie-break in Python: a secondary SQL sort key would bypass the HNSW index.
    return [
        {"entity_id": eid, "canonical_name": name, "similarity": float(sim)}
        for eid, name, sim in sorted(rows, key=lambda r: (-float(r[2]), r[0]))
        if float(sim) >= min_similarity
    ]


def format_suggestions(suggestions: Sequence[dict[str, Any]]) -> str:
    return "; ".join(f"{s['canonical_name']} ({s['similarity']:.2f})" for s in suggestions)


def flag_near_duplicate_news(
    conn: psycopg.Connection,
    embedder: Embedder,
    run_id: Any,
    *,
    threshold: float = 0.95,
) -> int:
    """Mark later same-run NEWS rows whose title is >= threshold cosine to an earlier one.

    Writes payload.meta.nearDuplicateOf = earlier row's durable id; rows are never dropped.
    """
    import numpy as np

    # Re-flagging (e.g. a new model) starts clean, like annotate_mapping_suggestions.
    conn.execute(
        """
        UPDATE candidates SET payload = jsonb_set(payload, '{meta}', (payload->'meta') - 'nearDuplicateOf')
        WHERE run_id = %s AND record_type = 'NEWS' AND payload->'meta' ? 'nearDuplicateOf'
        """,
        (str(run_id),),
    )
    rows = conn.execute(
        """
        SELECT id, COALESCE(NULLIF(external_id, ''), semantic_key), payload->'content'->>'title'
        FROM candidates
        WHERE run_id = %s AND record_type = 'NEWS' AND status = 'accepted'
          AND COALESCE(payload->'content'->>'title', '') <> ''
        ORDER BY semantic_key
        """,
        (str(run_id),),
    ).fetchall()
    if len(rows) < 2:
        return 0
    mat = np.asarray(embedder.embed([title for _, _, title in rows]), dtype=float)
    mat /= np.clip(np.linalg.norm(mat, axis=1, keepdims=True), 1e-12, None)
    # shortcut: dense n x n similarity, fine for per-run news counts; use HNSW past ~20k rows.
    sims = mat @ mat.T
    flagged = 0
    for i in range(1, len(rows)):
        earlier = np.flatnonzero(sims[i, :i] >= threshold)
        if earlier.size:
            conn.execute(
                """
                UPDATE candidates
                SET payload = jsonb_set(
                    payload, '{meta}',
                    COALESCE(payload->'meta', '{}'::jsonb)
                        || jsonb_build_object('nearDuplicateOf', %s::text)
                )
                WHERE id = %s
                """,
                (rows[int(earlier[0])][1], rows[i][0]),
            )
            flagged += 1
    return flagged


__all__ = [
    "DEFAULT_MODEL",
    "EMBEDDING_DIM",
    "Embedder",
    "FastEmbedder",
    "embed_entities",
    "entity_text",
    "flag_near_duplicate_news",
    "format_suggestions",
    "suggest_candidates",
    "to_vector_literal",
]
