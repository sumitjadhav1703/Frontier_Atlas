"""Extraction cache keyed by raw/segments/provider/model/prompt/schema/policy versions."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from signal_ingest.llm.types import ExtractResult, Segment
from signal_ingest.llm.versions import (
    EXTRACTION_POLICY_VERSION,
    PROMPT_VERSION,
    SCHEMA_VERSION,
    SEGMENTER_VERSION,
)


def build_cache_key(
    *,
    raw_hash: str,
    segments_hash: str,
    provider: str,
    model: str,
    prompt_version: str = PROMPT_VERSION,
    schema_version: str = SCHEMA_VERSION,
    policy_version: str = EXTRACTION_POLICY_VERSION,
    segmenter_version: str = SEGMENTER_VERSION,
    scope: str = "",
) -> str:
    """`scope` = what was asked (entity, record type, fields): same page, other
    question, other key."""
    material = "|".join(
        [
            scope,
            raw_hash,
            segments_hash,
            provider,
            model,
            prompt_version,
            schema_version,
            policy_version,
            segmenter_version,
        ]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


class ExtractionCache:
    """In-memory cache for offline/fake runs; swap for durable store later."""

    def __init__(self) -> None:
        self._store: dict[str, ExtractResult] = {}

    def get(self, key: str) -> ExtractResult | None:
        hit = self._store.get(key)
        if hit is None:
            return None
        return hit.model_copy(deep=True)

    def put(self, key: str, result: ExtractResult) -> None:
        self._store[key] = result.model_copy(deep=True)

    def __contains__(self, key: str) -> bool:
        return key in self._store

    def clear(self) -> None:
        self._store.clear()


def hash_segments(segments: list[Segment]) -> str:
    from signal_ingest.llm.segments import segments_hash

    return segments_hash(segments)


def stable_json_hash(obj: Any) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
