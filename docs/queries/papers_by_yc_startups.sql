-- Papers implemented by repositories that a YC startup owns, directly or through
-- an OWNS chain: paper -[IMPLEMENTED_BY]-> repo <-[OWNS]- org <-[OWNS]- ... <-[OWNS]- YC startup.
-- A YC startup is a startup entity with a `yc:` source identity; an owner node key may be
-- the entity id or any of its comparison keys (alias, name, domain, source id).
-- Runs as-is in psql; signal_ingest.resolution.graph_queries executes this file.
WITH RECURSIVE live_edges AS (
    SELECT relation_type, from_kind, from_key, to_kind, to_key
    FROM relationships
    WHERE status = 'accepted'
      AND (valid_from IS NULL OR valid_from <= NOW())
      AND (valid_to IS NULL OR valid_to > NOW())
),
owners AS (
    -- hop 1: direct owner of a repository that implements a paper
    SELECT impl.from_key AS paper_key,
           impl.to_key AS repository_key,
           own.from_kind AS owner_kind,
           own.from_key AS owner_key,
           1 AS hops,
           ARRAY[own.from_kind || ':' || own.from_key] AS owns_path
    FROM live_edges impl
    JOIN live_edges own
      ON own.relation_type = 'OWNS'
     AND own.to_kind = impl.to_kind
     AND own.to_key = impl.to_key
    WHERE impl.relation_type = 'IMPLEMENTED_BY'
      AND impl.from_kind = 'paper'
      AND impl.to_kind = 'repository'
    UNION ALL
    -- hop n+1: whoever owns the current owner (cycle-safe, depth-capped)
    SELECT o.paper_key, o.repository_key, e.from_kind, e.from_key, o.hops + 1,
           e.from_kind || ':' || e.from_key || o.owns_path
    FROM owners o
    JOIN live_edges e
      ON e.relation_type = 'OWNS'
     AND e.to_kind = o.owner_kind
     AND e.to_key = o.owner_key
    WHERE o.hops < 6
      AND NOT (e.from_kind || ':' || e.from_key) = ANY (o.owns_path)
),
yc_startups AS (
    SELECT DISTINCT e.id AS entity_id, e.canonical_name, k.node_key
    FROM entities e
    JOIN source_identities yc
      ON yc.entity_id = e.id
     AND yc.identity_kind = 'source_id'
     AND yc.raw_value LIKE 'yc:%'
    CROSS JOIN LATERAL (
        SELECT e.id AS node_key
        UNION SELECT e.comparison_key
        UNION SELECT si.comparison_key FROM source_identities si WHERE si.entity_id = e.id
    ) k
    WHERE e.namespace = 'startup'
)
SELECT DISTINCT ON (o.paper_key, o.repository_key, y.entity_id)
       o.paper_key,
       o.repository_key,
       y.entity_id AS startup_entity_id,
       y.canonical_name AS startup_name,
       o.hops,
       o.owns_path
FROM owners o
JOIN yc_startups y ON y.node_key = o.owner_key
ORDER BY o.paper_key, o.repository_key, y.entity_id, o.hops;
