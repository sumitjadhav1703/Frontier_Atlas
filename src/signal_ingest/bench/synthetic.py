"""Lazy synthetic candidate generators for scale measurements."""

from __future__ import annotations

import hashlib
import itertools
from collections.abc import Iterator
from typing import Any


def synthetic_keys(n: int, *, prefix: str = "syn") -> Iterator[str]:
    """Yield n stable synthetic identity keys without materializing the full list."""
    for i in range(n):
        yield f"{prefix}:{i:08d}"


def synthetic_payload(key: str) -> dict[str, Any]:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return {
        "schemaVersion": "1.0",
        "source": {"name": "SyntheticBench", "url": f"https://fixtures.local/bench/{digest}"},
        "content": {"entityName": f"Bench Entity {digest}", "data": {"employeeCount": None}},
        "collectedAt": "2026-11-21T12:00:00Z",
        "_synthetic": True,
        "_lineage_mode": "fixture",
        "durable_key": key,
    }


def process_synthetic_item(key: str) -> dict[str, Any]:
    """CPU-light stand-in for fetch+parse+project of one candidate."""
    payload = synthetic_payload(key)
    # Cheap deterministic work: hash + flatten-ish walk.
    material = "|".join(
        [
            payload["source"]["url"],
            payload["content"]["entityName"],
            payload["durable_key"],
        ]
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return {
        "key": key,
        "digest": digest,
        "ok": True,
        "bytes": len(material),
    }


def batched(iterable: Iterator[str], size: int) -> Iterator[list[str]]:
    while True:
        batch = list(itertools.islice(iterable, size))
        if not batch:
            return
        yield batch


__all__ = [
    "batched",
    "process_synthetic_item",
    "synthetic_keys",
    "synthetic_payload",
]
