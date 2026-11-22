"""Persist seed identities and resolution outcomes into PostgreSQL."""

from __future__ import annotations

import uuid
from typing import Iterable
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from signal_ingest.resolution.normalize import comparison_key, domain_key
from signal_ingest.resolution.register import SeedRegister
from signal_ingest.resolution.types import (
    IdentityKind,
    Namespace,
    SeedIdentity,
)


def upsert_entity(
    conn: psycopg.Connection,
    identity: SeedIdentity,
    *,
    lineage_mode: str,
    seed_revision: str | None = None,
) -> None:
    key = comparison_key(identity.canonical_name)
    conn.execute(
        """
        INSERT INTO entities (
            id, namespace, canonical_name, owner_id, lineage_mode,
            comparison_key, evidence_url, seed_revision, metadata
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (id) DO UPDATE SET
            canonical_name = EXCLUDED.canonical_name,
            owner_id = EXCLUDED.owner_id,
            comparison_key = EXCLUDED.comparison_key,
            evidence_url = EXCLUDED.evidence_url,
            seed_revision = EXCLUDED.seed_revision,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        """,
        (
            identity.id,
            identity.namespace.value,
            identity.canonical_name,
            identity.owner_id,
            lineage_mode,
            key,
            identity.evidence_url or None,
            seed_revision,
            Jsonb(identity.metadata),
        ),
    )


def upsert_source_identity(
    conn: psycopg.Connection,
    *,
    entity_id: str,
    namespace: Namespace,
    identity_kind: IdentityKind,
    raw_value: str,
    comparison_key_value: str,
    owner_id: str | None,
    lineage_mode: str,
    evidence_url: str | None = None,
) -> UUID:
    row = conn.execute(
        """
        SELECT id FROM source_identities
        WHERE namespace = %s
          AND identity_kind = %s
          AND comparison_key = %s
          AND COALESCE(owner_id, '') = COALESCE(%s, '')
        """,
        (
            namespace.value,
            identity_kind.value,
            comparison_key_value,
            owner_id,
        ),
    ).fetchone()
    if row is not None:
        return UUID(str(row[0]))

    sid = uuid.uuid4()
    conn.execute(
        """
        INSERT INTO source_identities (
            id, entity_id, namespace, identity_kind, raw_value,
            comparison_key, owner_id, lineage_mode, evidence_url
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT DO NOTHING
        """,
        (
            sid,
            entity_id,
            namespace.value,
            identity_kind.value,
            raw_value,
            comparison_key_value,
            owner_id,
            lineage_mode,
            evidence_url,
        ),
    )
    again = conn.execute(
        """
        SELECT id FROM source_identities
        WHERE namespace = %s
          AND identity_kind = %s
          AND comparison_key = %s
          AND COALESCE(owner_id, '') = COALESCE(%s, '')
        """,
        (
            namespace.value,
            identity_kind.value,
            comparison_key_value,
            owner_id,
        ),
    ).fetchone()
    if again is None:
        raise RuntimeError(
            f"failed to upsert source_identity for {entity_id}/{raw_value}"
        )
    return UUID(str(again[0]))


def persist_seed_register(
    conn: psycopg.Connection,
    register: SeedRegister,
) -> int:
    """Write all seed entities + aliases/domains/source_ids. Returns entity count.

    Startups are inserted before products so owner_id FKs succeed.
    Collision keys are still persisted per-entity; resolution abstains via
    the in-memory index, not via unique-index failure.
    """
    lineage = register.lineage_mode
    revision = register.revision
    startups = [
        i for i in register.document.identities if i.namespace is Namespace.STARTUP
    ]
    products = [
        i for i in register.document.identities if i.namespace is Namespace.PRODUCT
    ]
    for identity in (*startups, *products):
        upsert_entity(
            conn,
            identity,
            lineage_mode=lineage,
            seed_revision=revision,
        )
        _persist_identity_surfaces(conn, identity, lineage_mode=lineage)
    return len(register.document.identities)


def _persist_identity_surfaces(
    conn: psycopg.Connection,
    identity: SeedIdentity,
    *,
    lineage_mode: str,
) -> None:
    owner = identity.owner_id
    # Canonical name
    upsert_source_identity(
        conn,
        entity_id=identity.id,
        namespace=identity.namespace,
        identity_kind=IdentityKind.CANONICAL_NAME,
        raw_value=identity.canonical_name,
        comparison_key_value=comparison_key(identity.canonical_name),
        owner_id=owner,
        lineage_mode=lineage_mode,
        evidence_url=identity.evidence_url or None,
    )
    for alias in identity.aliases:
        akey = comparison_key(alias)
        if not akey:
            continue
        # Skip alias rows that would violate the unique lookup against another
        # entity; collisions are resolved by abstention in the register.
        conflict = conn.execute(
            """
            SELECT entity_id FROM source_identities
            WHERE namespace = %s
              AND identity_kind = %s
              AND comparison_key = %s
              AND COALESCE(owner_id, '') = COALESCE(%s, '')
              AND entity_id IS DISTINCT FROM %s
            """,
            (
                identity.namespace.value,
                IdentityKind.ALIAS.value,
                akey,
                owner,
                identity.id,
            ),
        ).fetchone()
        if conflict is not None:
            continue
        upsert_source_identity(
            conn,
            entity_id=identity.id,
            namespace=identity.namespace,
            identity_kind=IdentityKind.ALIAS,
            raw_value=alias,
            comparison_key_value=akey,
            owner_id=owner,
            lineage_mode=lineage_mode,
            evidence_url=identity.evidence_url or None,
        )
    for system, value in identity.source_ids.items():
        raw = f"{system}:{value}"
        upsert_source_identity(
            conn,
            entity_id=identity.id,
            namespace=identity.namespace,
            identity_kind=IdentityKind.SOURCE_ID,
            raw_value=raw,
            comparison_key_value=raw.casefold(),
            owner_id=None,
            lineage_mode=lineage_mode,
            evidence_url=identity.evidence_url or None,
        )
    if identity.namespace is Namespace.STARTUP:
        for domain in identity.domains:
            dkey = domain_key(domain)
            if not dkey:
                continue
            conflict = conn.execute(
                """
                SELECT entity_id FROM source_identities
                WHERE namespace = %s
                  AND identity_kind = %s
                  AND comparison_key = %s
                  AND COALESCE(owner_id, '') = ''
                  AND entity_id IS DISTINCT FROM %s
                """,
                (
                    Namespace.STARTUP.value,
                    IdentityKind.DOMAIN.value,
                    dkey,
                    identity.id,
                ),
            ).fetchone()
            if conflict is not None:
                continue
            upsert_source_identity(
                conn,
                entity_id=identity.id,
                namespace=Namespace.STARTUP,
                identity_kind=IdentityKind.DOMAIN,
                raw_value=domain,
                comparison_key_value=dkey,
                owner_id=None,
                lineage_mode=lineage_mode,
                evidence_url=identity.evidence_url or None,
            )


def iter_entities(
    conn: psycopg.Connection,
    *,
    namespace: str | None = None,
) -> Iterable[tuple]:
    if namespace:
        rows = conn.execute(
            """
            SELECT id, namespace, canonical_name, owner_id, lineage_mode,
                   comparison_key, evidence_url
            FROM entities
            WHERE namespace = %s
            ORDER BY id
            """,
            (namespace,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, namespace, canonical_name, owner_id, lineage_mode,
                   comparison_key, evidence_url
            FROM entities
            ORDER BY id
            """
        ).fetchall()
    return rows


__all__ = [
    "iter_entities",
    "persist_seed_register",
    "upsert_entity",
    "upsert_source_identity",
]
