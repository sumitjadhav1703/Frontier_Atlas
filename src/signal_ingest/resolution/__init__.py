"""B6 deterministic startup/product entity resolution."""

from signal_ingest.resolution.mapping import (
    decision_to_mapping_log,
    mapping_event_key,
    mapping_observation_key,
    record_mapping_event,
    record_resolution_event,
)
from signal_ingest.resolution.normalize import (
    COMPARISON_KEY_VERSION,
    comparison_key,
    domain_key,
)
from signal_ingest.resolution.register import (
    SeedRegister,
    load_fixture_register,
    load_seed_register,
)
from signal_ingest.resolution.resolve import resolve
from signal_ingest.resolution.seeds import (
    DEFAULT_FIXTURE_SEEDS_PATH,
    assert_not_fixture_seed_id,
    load_seed_document,
    load_seeds_for_mode,
)
from signal_ingest.resolution.store import (
    persist_seed_register,
    upsert_entity,
    upsert_source_identity,
)
from signal_ingest.resolution.types import (
    DecisionKind,
    FixtureSeedRejectedError,
    IdentityKind,
    MappingLogRecord,
    Namespace,
    ResolveCandidate,
    ResolveDecision,
    SeedIdentity,
    SeedRegisterDocument,
    SeedRegisterError,
)
from signal_ingest.resolution.versions import (
    RULE_VERSION,
    SEED_FORMAT_VERSION,
)

__all__ = [
    "COMPARISON_KEY_VERSION",
    "DEFAULT_FIXTURE_SEEDS_PATH",
    "DecisionKind",
    "FixtureSeedRejectedError",
    "IdentityKind",
    "MappingLogRecord",
    "Namespace",
    "RULE_VERSION",
    "ResolveCandidate",
    "ResolveDecision",
    "SEED_FORMAT_VERSION",
    "SeedIdentity",
    "SeedRegister",
    "SeedRegisterDocument",
    "SeedRegisterError",
    "assert_not_fixture_seed_id",
    "comparison_key",
    "decision_to_mapping_log",
    "domain_key",
    "load_fixture_register",
    "load_seed_document",
    "load_seed_register",
    "load_seeds_for_mode",
    "mapping_event_key",
    "mapping_observation_key",
    "persist_seed_register",
    "record_mapping_event",
    "record_resolution_event",
    "resolve",
    "upsert_entity",
    "upsert_source_identity",
]
