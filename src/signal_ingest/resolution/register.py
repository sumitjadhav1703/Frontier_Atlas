"""In-memory seed register with collision-aware indexes."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from signal_ingest.resolution.normalize import (
    comparison_key,
    descriptive_key,
    domain_key,
    is_platform_host,
)
from signal_ingest.resolution.seeds import (
    DEFAULT_FIXTURE_SEEDS_PATH,
    load_seed_document,
    load_seeds_for_mode,
)
from signal_ingest.resolution.types import (
    FixtureSeedRejectedError,
    Namespace,
    SeedIdentity,
    SeedRegisterDocument,
    SeedRegisterError,
)
from signal_ingest.resolution.versions import RULE_VERSION


@dataclass
class _KeyBucket:
    """Entity IDs sharing one comparison/source/domain key."""

    entity_ids: set[str] = field(default_factory=set)

    def add(self, entity_id: str) -> None:
        self.entity_ids.add(entity_id)

    @property
    def unique_id(self) -> str | None:
        if len(self.entity_ids) == 1:
            return next(iter(self.entity_ids))
        return None

    @property
    def is_collision(self) -> bool:
        return len(self.entity_ids) > 1


@dataclass
class SeedRegister:
    """Collision-aware lookup tables for deterministic resolution."""

    document: SeedRegisterDocument
    by_id: dict[str, SeedIdentity]
    # startup: key → bucket; product: (owner_id, key) → bucket
    name_index: dict[tuple[str, ...], _KeyBucket]  # descriptive_key (loose)
    alias_index: dict[tuple[str, ...], _KeyBucket]  # comparison_key (exact)
    source_id_index: dict[str, _KeyBucket]  # "system:value" only
    domain_index: dict[str, _KeyBucket]  # exact host; platform hosts excluded
    # Exact alias scopes that are only the canonical minus "AI"/"Labs"
    # ("Inflection" for "Inflection AI"): not distinctive, need corroboration.
    weak_alias_index: dict[tuple[str, ...], set[str]] = field(default_factory=dict)
    rule_version: str = RULE_VERSION

    @classmethod
    def from_document(cls, document: SeedRegisterDocument) -> SeedRegister:
        by_id = {item.id: item for item in document.identities}
        name_index: dict[tuple[str, ...], _KeyBucket] = {}
        alias_index: dict[tuple[str, ...], _KeyBucket] = {}
        source_id_index: dict[str, _KeyBucket] = {}
        domain_index: dict[str, _KeyBucket] = {}
        weak_alias_index: dict[tuple[str, ...], set[str]] = {}

        def _bucket(
            index: dict[tuple[str, ...] | str, _KeyBucket],
            key: tuple[str, ...] | str,
            entity_id: str,
        ) -> None:
            bucket = index.get(key)  # type: ignore[arg-type]
            if bucket is None:
                bucket = _KeyBucket()
                index[key] = bucket  # type: ignore[index]
            bucket.add(entity_id)

        for item in document.identities:
            key = comparison_key(item.canonical_name)
            if not key:
                raise SeedRegisterError(
                    f"empty comparison key for seed {item.id!r}"
                )
            loose = descriptive_key(item.canonical_name)
            _bucket(name_index, _name_scope(item.namespace, item.owner_id, loose), item.id)
            scope = _name_scope(item.namespace, item.owner_id, key)
            _bucket(alias_index, scope, item.id)
            if item.namespace is Namespace.STARTUP and _plain_word(item.canonical_name):
                weak_alias_index.setdefault(scope, set()).add(item.id)

            for alias in item.aliases:
                akey = comparison_key(alias)
                if not akey:
                    continue
                ascope = _name_scope(item.namespace, item.owner_id, akey)
                _bucket(alias_index, ascope, item.id)
                if akey == loose != key or (
                    item.namespace is Namespace.STARTUP and _plain_word(alias)
                ):
                    weak_alias_index.setdefault(ascope, set()).add(item.id)
                # Loose alias keys also collide in the name index.
                lscope = _name_scope(item.namespace, item.owner_id, descriptive_key(alias))
                _bucket(name_index, lscope, item.id)

            for system, value in item.source_ids.items():
                raw = str(value).strip()
                if not raw or not str(system).strip():
                    continue
                # Namespaced only: a bare id from one system never matches another.
                _bucket(source_id_index, _source_key(system, raw), item.id)

            if item.namespace is Namespace.STARTUP:
                for domain in item.domains:
                    dkey = domain_key(domain)
                    # A bare platform suffix (github.io, vercel.app) names no owner.
                    if dkey and not is_platform_host(dkey):
                        _bucket(domain_index, dkey, item.id)

        return cls(
            document=document,
            by_id=by_id,
            name_index=name_index,
            alias_index=alias_index,
            source_id_index=source_id_index,
            domain_index=domain_index,
            weak_alias_index=weak_alias_index,
            rule_version=document.rule_version or RULE_VERSION,
        )

    @classmethod
    def from_path(
        cls,
        path: Path | str | None = None,
        *,
        mode: str = "fixture",
    ) -> SeedRegister:
        target = Path(path) if path is not None else DEFAULT_FIXTURE_SEEDS_PATH
        # Loading a labeled fixture file for offline tests uses document lineage;
        # live mode still refuses fixture registers via load_seeds_for_mode.
        if mode == "live":
            doc = load_seeds_for_mode("live", path=target)
        else:
            doc = load_seed_document(target)
        return cls.from_document(doc)

    @classmethod
    def load(cls, path: Path | str | None = None) -> SeedRegister:
        return cls.from_path(path, mode="fixture")

    @classmethod
    def load_fixture(cls, path: Path | None = None) -> SeedRegister:
        return cls.from_path(path or DEFAULT_FIXTURE_SEEDS_PATH, mode="fixture")

    def __len__(self) -> int:
        return len(self.by_id)

    @property
    def count(self) -> int:
        return len(self.by_id)

    @property
    def identities(self) -> list[SeedIdentity]:
        return list(self.document.identities)

    @property
    def lineage_mode(self) -> str:
        return self.document.lineage_mode

    @property
    def revision(self) -> str:
        return self.document.revision

    def get(self, entity_id: str) -> SeedIdentity | None:
        return self.by_id.get(entity_id)

    def require(self, entity_id: str) -> SeedIdentity:
        item = self.by_id.get(entity_id)
        if item is None:
            raise KeyError(entity_id)
        if self.lineage_mode == "fixture":
            # Caller in live mode must not receive fixture identities.
            pass
        return item

    def lookup_source_id(
        self, value: str, *, system: str | None = None
    ) -> _KeyBucket | None:
        if not system:
            system, sep, value = value.partition(":")
            if not sep:
                return None  # un-namespaced ids are not comparable across systems
        return self.source_id_index.get(_source_key(system, value))

    def lookup_domain(self, host_or_url: str) -> _KeyBucket | None:
        return self.domain_index.get(domain_key(host_or_url))

    def lookup_alias(
        self,
        raw: str,
        *,
        namespace: Namespace,
        owner_id: str | None = None,
    ) -> _KeyBucket | None:
        key = comparison_key(raw)
        if not key:
            return None
        scope = _name_scope(namespace, owner_id, key)
        return self.alias_index.get(scope)

    def lookup_name(
        self,
        raw: str,
        *,
        namespace: Namespace,
        owner_id: str | None = None,
    ) -> _KeyBucket | None:
        key = descriptive_key(raw)
        if not key:
            return None
        scope = _name_scope(namespace, owner_id, key)
        return self.name_index.get(scope)

    def is_weak_alias(
        self,
        raw: str,
        entity_id: str,
        *,
        namespace: Namespace,
        owner_id: str | None = None,
    ) -> bool:
        """Exact hit that is not distinctive enough to merge without corroboration."""
        key = comparison_key(raw)
        scope = _name_scope(namespace, owner_id, key)
        if entity_id in self.weak_alias_index.get(scope, ()):
            return True
        # Very short Latin startup keys ("wb", "x") are acronym-like.
        return namespace is Namespace.STARTUP and key.isascii() and len(key) < 3


    def assert_live_admissible(self, *, mode: str) -> None:
        if mode == "live" and self.lineage_mode == "fixture":
            raise FixtureSeedRejectedError(
                "mode=live rejects fixture seed register "
                f"(revision={self.revision!r})"
            )


def _source_key(system: str, value: str) -> str:
    return f"{system.strip().casefold()}:{value.strip().casefold()}"


def _name_scope(
    namespace: Namespace,
    owner_id: str | None,
    key: str,
) -> tuple[str, ...]:
    if namespace is Namespace.PRODUCT:
        if not owner_id:
            # Unscoped product lookups are invalid; use empty owner sentinel
            # so they never collide with a real owner bucket.
            return ("product", "", key)
        return ("product", owner_id, key)
    return ("startup", key)


def load_fixture_register(path: Path | None = None) -> SeedRegister:
    return SeedRegister.load_fixture(path)


def load_seed_register(path: Path | str | None = None) -> SeedRegister:
    """Adapter entry: load a SeedRegister from path (fixture mode)."""
    return SeedRegister.from_path(path, mode="fixture")


__all__ = [
    "SeedRegister",
    "load_fixture_register",
    "load_seed_document",
    "load_seed_register",
]


def _plain_word(raw: str) -> bool:
    """Seed surface form that is one short ordinary-looking word ("Weave",
    "Scale", "Writer", "resolve"). Judged on the seed's spelling, not the input's.

    Such names collide with everyday text, so an exact hit alone is not enough.
    CamelCase, digits or dots ("LeadGenius", "Semantics3", "Copy.ai") stay
    distinctive. shortcut: length heuristic, not a dictionary; swap in a word
    list if recall on short brand names matters more than precision.
    """
    word = raw.strip()
    if not word.isascii() or not word.isalpha() or len(word) > 7:
        return False
    inner = word[1:]
    return inner.islower() or word.isupper() and len(word) > 4
