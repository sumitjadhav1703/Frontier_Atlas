"""Deterministic resolve(candidate) → match | new | abstain."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from signal_ingest.resolution.mapping import mapping_observation_key
from signal_ingest.resolution.normalize import (
    comparison_key,
    domain_key,
    is_platform_host,
)
from signal_ingest.resolution.register import SeedRegister, _KeyBucket
from signal_ingest.resolution.seeds import assert_not_fixture_seed_id
from signal_ingest.resolution.types import (
    DecisionKind,
    FixtureSeedRejectedError,
    Namespace,
    ResolveCandidate,
    ResolveDecision,
    SeedIdentity,
)
from signal_ingest.resolution.versions import RULE_VERSION


def resolve(
    candidate: ResolveCandidate | dict[str, Any],
    register: SeedRegister | None = None,
    *,
    mode: Any = None,
    seed_register: SeedRegister | None = None,
    create_id: str | None = None,
) -> ResolveDecision:
    """Resolve a raw name against the seed register.

    Match order:
      1. trusted source_id (unique)
      2. domain + ownership evidence (unique host claimant)
      3. unique, distinctive approved alias/canonical (exact comparison_key)
      4. unique loose name (descriptive_key) *with* seed-linked corroboration
      5. trusted unseen (source_id / ownership) → new
      6. else abstain

    Collisions (shared host, homonym keys, ambiguous aliases) → abstain.
    mode=live rejects fixture seed IDs/aliases.
    Fuzzy similarity is never auto-merged.
    """
    reg = register if register is not None else seed_register
    if reg is None:
        raise TypeError("register is required")

    if isinstance(candidate, dict):
        data = dict(candidate)
        if mode is not None and "mode" not in data:
            data["mode"] = _mode_value(mode)
        candidate = ResolveCandidate.model_validate(data)
    elif mode is not None:
        candidate = candidate.model_copy(update={"mode": _mode_value(mode)})

    key = comparison_key(candidate.raw_name)
    obs_key = mapping_observation_key(
        source_item_id=candidate.source_item_id
        or candidate.observation_id
        or "",
        field=candidate.field,
        raw_value=candidate.raw_name,
        rule_version=reg.rule_version or RULE_VERSION,
    )

    try:
        reg.assert_live_admissible(mode=candidate.mode)
    except FixtureSeedRejectedError as exc:
        return _abstain(
            candidate,
            key=key,
            method="fixture_seed_rejected",
            reason=str(exc),
            candidates=(),
            observation_key=obs_key,
            rule_version=reg.rule_version,
        )

    # Explicit fixture ids on the candidate are rejected in live mode.
    for cited in (candidate.seed_id, candidate.canonical_id):
        if not cited:
            continue
        try:
            assert_not_fixture_seed_id(cited, mode=candidate.mode)
        except FixtureSeedRejectedError as exc:
            return _abstain(
                candidate,
                key=key,
                method="fixture_seed_rejected",
                reason=str(exc),
                candidates=(cited,),
                observation_key=obs_key,
                rule_version=reg.rule_version,
            )

    if candidate.namespace is Namespace.PRODUCT and not candidate.owner_id:
        return _abstain(
            candidate,
            key=key,
            method="missing_owner",
            reason="product resolution requires owner_id",
            candidates=(),
            observation_key=obs_key,
            rule_version=reg.rule_version,
        )

    # --- 1. trusted source ID -------------------------------------------------
    if candidate.source_id:
        try:
            assert_not_fixture_seed_id(candidate.source_id, mode=candidate.mode)
        except FixtureSeedRejectedError as exc:
            return _abstain(
                candidate,
                key=key,
                method="fixture_seed_rejected",
                reason=str(exc),
                candidates=(),
                observation_key=obs_key,
                rule_version=reg.rule_version,
            )
        bucket = reg.lookup_source_id(
            candidate.source_id, system=candidate.source_system
        )
        decision = _from_bucket(
            bucket,
            candidate=candidate,
            key=key,
            method="trusted_source_id",
            register=reg,
            observation_key=obs_key,
        )
        if decision is not None:
            return decision

    # --- 2. domain + ownership evidence --------------------------------------
    if candidate.domain and candidate.ownership_evidenced:
        bucket = reg.lookup_domain(candidate.domain)
        decision = _from_bucket(
            bucket,
            candidate=candidate,
            key=key,
            method="domain_ownership",
            register=reg,
            observation_key=obs_key,
            require_namespace=Namespace.STARTUP,
        )
        if decision is not None:
            return decision
    elif candidate.domain and not candidate.ownership_evidenced:
        bucket = reg.lookup_domain(candidate.domain)
        if bucket is not None and bucket.is_collision:
            return _abstain(
                candidate,
                key=key,
                method="shared_host_collision",
                reason=(
                    f"domain {domain_key(candidate.domain)!r} hosts multiple "
                    f"entities: {sorted(bucket.entity_ids)}"
                ),
                candidates=tuple(sorted(bucket.entity_ids)),
                observation_key=obs_key,
                rule_version=reg.rule_version,
            )
        if bucket is not None and bucket.unique_id is not None:
            # Unique domain without ownership is not enough alone, but an
            # approved alias/name match below may still succeed.
            pass

    # --- 3. unique approved alias --------------------------------------------
    alias_bucket = reg.lookup_alias(
        candidate.raw_name,
        namespace=candidate.namespace,
        owner_id=candidate.owner_id,
    )
    weak = (
        alias_bucket is not None
        and alias_bucket.unique_id is not None
        and reg.is_weak_alias(
            candidate.raw_name,
            alias_bucket.unique_id,
            namespace=candidate.namespace,
            owner_id=candidate.owner_id,
        )
    )
    # A weak (non-distinctive) exact hit falls through to step 4.
    if not weak:
        decision = _from_bucket(
            alias_bucket,
            candidate=candidate,
            key=key,
            method="approved_alias",
            register=reg,
            observation_key=obs_key,
        )
        if decision is not None:
            return decision

    # --- 4. unique normalized name with corroboration ------------------------
    name_bucket = reg.lookup_name(
        candidate.raw_name,
        namespace=candidate.namespace,
        owner_id=candidate.owner_id,
    )
    if name_bucket is not None and name_bucket.is_collision:
        return _abstain(
            candidate,
            key=key,
            method="name_collision",
            reason=(
                f"comparison key {key!r} collides across "
                f"{sorted(name_bucket.entity_ids)}"
            ),
            candidates=tuple(sorted(name_bucket.entity_ids)),
            observation_key=obs_key,
            rule_version=reg.rule_version,
        )
    if (
        name_bucket is not None
        and name_bucket.unique_id is not None
        and _corroborates(candidate, name_bucket.unique_id, reg)
    ):
        decision = _from_bucket(
            name_bucket,
            candidate=candidate,
            key=key,
            method="normalized_name_corroborated",
            register=reg,
            observation_key=obs_key,
        )
        if decision is not None:
            return decision
    if name_bucket is not None and name_bucket.unique_id is not None:
        return _abstain(
            candidate,
            key=key,
            method="name_without_corroboration",
            reason="normalized name match requires corroboration",
            candidates=(name_bucket.unique_id,),
            observation_key=obs_key,
            rule_version=reg.rule_version,
        )

    # --- 5. create new trusted unseen ----------------------------------------
    # Trusted source_id that did not match any seed, or explicit allow_create
    # with ownership evidence, yields a stable new id (no fuzzy merge).
    if _trusted_unseen(candidate) or (
        candidate.allow_create and _has_corroboration(candidate)
    ):
        new_id = create_id or _stable_new_id(candidate, key)
        try:
            assert_not_fixture_seed_id(new_id, mode=candidate.mode)
        except FixtureSeedRejectedError as exc:
            return _abstain(
                candidate,
                key=key,
                method="fixture_seed_rejected",
                reason=str(exc),
                candidates=(),
                observation_key=obs_key,
                rule_version=reg.rule_version,
            )
        return ResolveDecision(
            decision=DecisionKind.NEW,
            canonical_id=new_id,
            canonical_name=candidate.raw_name.strip(),
            method="trusted_unseen",
            rule_version=reg.rule_version or RULE_VERSION,
            candidate_ids=(),
            raw_name=candidate.raw_name,
            comparison_key=key,
            namespace=candidate.namespace,
            reason="trusted previously unseen organization",
            evidence_url=candidate.evidence_url,
            observation_key=obs_key,
        )

    # --- 6. abstain ----------------------------------------------------------
    return _abstain(
        candidate,
        key=key,
        method="no_unique_match",
        reason="no unique trusted match",
        candidates=(),
        observation_key=obs_key,
        rule_version=reg.rule_version,
    )


def _mode_value(mode: Any) -> str:
    if hasattr(mode, "value"):
        return str(mode.value)
    return str(mode)


def _corroborates(
    candidate: ResolveCandidate, entity_id: str, register: SeedRegister
) -> bool:
    """Evidence that links the candidate to *this* seed, not just any evidence."""
    identity = register.get(entity_id)
    if identity is None:
        return False
    if candidate.source_id:
        bucket = register.lookup_source_id(
            candidate.source_id, system=candidate.source_system
        )
        if bucket is not None and bucket.unique_id == entity_id:
            return True
    hosts = [candidate.domain]
    if candidate.ownership_evidenced:
        hosts.append(candidate.evidence_url)
    for host in hosts:
        if not host or identity.namespace is not Namespace.STARTUP:
            continue
        # Exact host only; platform suffixes are not indexed, so one
        # github.io/vercel.app tenant never corroborates another.
        bucket = register.lookup_domain(host)
        if bucket is not None and bucket.unique_id == entity_id:
            return True
    return bool(
        candidate.ownership_evidenced
        and candidate.owner_id
        and candidate.owner_id == identity.owner_id
    )


def _has_corroboration(candidate: ResolveCandidate) -> bool:
    """Generic trust for creating a *new* entity (no seed to link to)."""
    if candidate.source_id:
        return True
    if candidate.domain and candidate.ownership_evidenced:
        return True
    if candidate.ownership_evidenced and candidate.owner_id:
        return True
    if candidate.evidence_url and candidate.ownership_evidenced:
        return True
    return False


def _trusted_unseen(candidate: ResolveCandidate) -> bool:
    """Source-backed identity that did not hit the register → eligible for new."""
    return bool(candidate.source_id)


def _from_bucket(
    bucket: _KeyBucket | None,
    *,
    candidate: ResolveCandidate,
    key: str,
    method: str,
    register: SeedRegister,
    observation_key: str,
    require_namespace: Namespace | None = None,
) -> ResolveDecision | None:
    if bucket is None or not bucket.entity_ids:
        return None
    if bucket.is_collision:
        return _abstain(
            candidate,
            key=key,
            method=f"{method}_collision",
            reason=f"ambiguous candidates: {sorted(bucket.entity_ids)}",
            candidates=tuple(sorted(bucket.entity_ids)),
            observation_key=observation_key,
            rule_version=register.rule_version,
        )
    entity_id = bucket.unique_id
    assert entity_id is not None
    try:
        assert_not_fixture_seed_id(entity_id, mode=candidate.mode)
    except FixtureSeedRejectedError as exc:
        return _abstain(
            candidate,
            key=key,
            method="fixture_seed_rejected",
            reason=str(exc),
            candidates=(entity_id,),
            observation_key=observation_key,
            rule_version=register.rule_version,
        )
    if candidate.mode == "live" and register.lineage_mode == "fixture":
        return _abstain(
            candidate,
            key=key,
            method="fixture_seed_rejected",
            reason="mode=live rejects fixture seed aliases",
            candidates=(entity_id,),
            observation_key=observation_key,
            rule_version=register.rule_version,
        )
    identity = register.get(entity_id)
    if identity is None:
        return _abstain(
            candidate,
            key=key,
            method="missing_identity",
            reason=f"seed id {entity_id!r} not in register",
            candidates=(entity_id,),
            observation_key=observation_key,
            rule_version=register.rule_version,
        )
    if require_namespace is not None and identity.namespace is not require_namespace:
        return None
    if (
        candidate.namespace is Namespace.PRODUCT
        and identity.namespace is Namespace.PRODUCT
        and candidate.owner_id
        and identity.owner_id
        and candidate.owner_id != identity.owner_id
    ):
        return _abstain(
            candidate,
            key=key,
            method="cross_owner_product",
            reason=(
                f"product {identity.id!r} owned by {identity.owner_id!r}, "
                f"not {candidate.owner_id!r}"
            ),
            candidates=(entity_id,),
            observation_key=observation_key,
            rule_version=register.rule_version,
        )
    return _match(
        identity,
        candidate=candidate,
        key=key,
        method=method,
        observation_key=observation_key,
        rule_version=register.rule_version,
    )


def _match(
    identity: SeedIdentity,
    *,
    candidate: ResolveCandidate,
    key: str,
    method: str,
    observation_key: str,
    rule_version: str,
) -> ResolveDecision:
    return ResolveDecision(
        decision=DecisionKind.MATCH,
        canonical_id=identity.id,
        canonical_name=identity.canonical_name,
        method=method,
        rule_version=rule_version or RULE_VERSION,
        candidate_ids=(identity.id,),
        raw_name=candidate.raw_name,
        comparison_key=key,
        namespace=candidate.namespace,
        evidence_url=candidate.evidence_url or identity.evidence_url or None,
        observation_key=observation_key,
    )


def _abstain(
    candidate: ResolveCandidate,
    *,
    key: str,
    method: str,
    reason: str,
    candidates: tuple[str, ...],
    observation_key: str,
    rule_version: str,
) -> ResolveDecision:
    return ResolveDecision(
        decision=DecisionKind.ABSTAIN,
        canonical_id=None,
        canonical_name=None,
        method=method,
        rule_version=rule_version or RULE_VERSION,
        candidate_ids=candidates,
        raw_name=candidate.raw_name,
        comparison_key=key,
        namespace=candidate.namespace,
        reason=reason,
        evidence_url=candidate.evidence_url,
        observation_key=observation_key,
    )


def _stable_new_id(candidate: ResolveCandidate, key: str) -> str:
    """Source-backed ID that keeps the system namespace and separators.

    Percent-encoding is injective, so github:ab/cd ≠ github:abc/d and
    yc:acme-co ≠ yc:acme. Case folds, matching register source-id lookups.
    """
    prefix = "live" if candidate.mode == "live" else "derived"
    ns = candidate.namespace.value
    if candidate.source_id:
        sid = candidate.source_id.strip()
        system = (candidate.source_system or "").strip()
        if system and not sid.casefold().startswith(system.casefold() + ":"):
            sid = f"{system}:{sid}"
        return f"{prefix}.{ns}.{_slug(sid)}"
    if candidate.namespace is Namespace.PRODUCT and candidate.owner_id:
        return f"{prefix}.{ns}.{_slug(candidate.owner_id)}.{_slug(key)}"
    if candidate.domain and not is_platform_host(candidate.domain):
        # Two unseen orgs with the same name must not share an ID.
        return f"{prefix}.{ns}.domain:{_slug(domain_key(candidate.domain))}"
    return f"{prefix}.{ns}.{_slug(key)}"


def _slug(value: str) -> str:
    return quote(value.casefold(), safe=":/-_~")


__all__ = ["resolve"]
