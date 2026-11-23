"""Run the B6 deterministic resolver over one run's accepted candidates."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

from signal_ingest.resolution.mapping import record_resolution_event
from signal_ingest.resolution.register import SeedRegister
from signal_ingest.resolution.resolve import resolve
from signal_ingest.resolution.seeds import DEFAULT_FIXTURE_SEEDS_PATH
from signal_ingest.resolution.store import persist_seed_register
from signal_ingest.resolution.types import Namespace, ResolveCandidate

LIVE_SEEDS_PATH = DEFAULT_FIXTURE_SEEDS_PATH.parent / "seeds.live.json"


def run_mentions(conn: psycopg.Connection, run_id: UUID | str, *, mode: str) -> list[ResolveCandidate]:
    """Startup names (STARTUP entityName), product owner names (PRODUCT startupName) and
    job employers (JOB company).

    YC startups and OpenRouter owners carry a trusted source id; other owners (AgiPicks) use the name path.
    """
    rows = conn.execute(
        """
        SELECT record_type, source_id, external_id, semantic_key,
               payload->'content'->>'entityName',
               COALESCE(payload->'content'->>'startupName', payload->'content'->>'company'),
               COALESCE(payload->'content'->>'ownerEvidenceUrl', payload->'source'->>'url'),
               payload->'meta'->>'yc_id',
               NULLIF(split_part(payload->'meta'->>'product_id', '/', 1), '')
        FROM candidates
        WHERE run_id = %s AND status = 'accepted' AND record_type IN ('STARTUP', 'PRODUCT', 'JOB')
        ORDER BY record_type, semantic_key
        """,
        (str(run_id),),
    ).fetchall()
    out: list[ResolveCandidate] = []
    for record_type, source_id, external_id, semantic_key, entity, owner, url, yc_id, owner_key in rows:
        raw, field = (entity, "entityName") if record_type == "STARTUP" else (
            (owner, "company") if record_type == "JOB" else (owner, "startupName")
        )
        if not (raw or "").strip():
            continue
        is_yc = record_type == "STARTUP" and str(source_id).endswith(".yc")
        # OpenRouter owners: the model id prefix is the directory's trusted owner key.
        is_or = record_type == "PRODUCT" and str(source_id).endswith(".openrouter") and owner_key
        system, trusted = ("yc", yc_id or external_id) if is_yc else (
            ("openrouter", str(owner_key).casefold()) if is_or else (None, None)
        )
        out.append(
            ResolveCandidate(
                raw_name=raw,
                namespace=Namespace.STARTUP,
                mode=mode,
                source_item_id=semantic_key,
                field=field,
                evidence_url=url,
                # YC rows carry the trusted company id the live seeds were keyed on.
                source_id=trusted,
                source_system=system,
                run_id=str(run_id),
            )
        )
    return out


def resolve_run(
    conn: psycopg.Connection,
    run_id: UUID | str,
    *,
    mode: str,
    seeds_path: Path | None = None,
    embedder: Any = None,
) -> dict[str, Any]:
    """Persist the seed register, resolve every mention, log each decision once.

    Same rules as B6: no fuzzy merge. Embeddings (optional) only annotate suggestions.
    """
    path = seeds_path or (LIVE_SEEDS_PATH if mode == "live" else DEFAULT_FIXTURE_SEEDS_PATH)
    register = SeedRegister.from_path(path, mode=mode)
    seeded = persist_seed_register(conn, register)
    decisions: Counter[str] = Counter()
    mentions = run_mentions(conn, run_id, mode=mode)
    for mention in mentions:
        decision = resolve(mention, register)
        record_resolution_event(conn, decision, run_id=run_id)
        decisions[decision.decision.value] += 1
    result: dict[str, Any] = {
        "run_id": str(run_id),
        "seeds_path": str(path),
        "seed_entities": seeded,
        "mentions": len(mentions),
        "decisions": dict(sorted(decisions.items())),
    }
    if embedder is not None:
        from signal_ingest.resolution.embed import embed_entities
        from signal_ingest.resolution.mapping import annotate_mapping_suggestions

        result["embedded"] = embed_entities(conn, embedder)
        result["annotated"] = annotate_mapping_suggestions(conn, embedder, run_id=run_id)
    return result


__all__ = ["LIVE_SEEDS_PATH", "resolve_run", "run_mentions"]
