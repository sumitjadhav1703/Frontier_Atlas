"""Deterministic offline token estimator for budget math (no live countTokens)."""

from __future__ import annotations

import math
from typing import Any

from signal_ingest.llm.versions import TOKENIZER_VERSION


def estimate_tokens(payload: bytes | str) -> int:
    """Conservative UTF-8 byte estimator: ceil(nbytes / 4), minimum 1 for non-empty.

    Matches TOKENIZER_VERSION. Used for FakeProvider asserts and chunk planning until
    a live provider supplies countTokens at B5/B8.
    """
    if isinstance(payload, str):
        data = payload.encode("utf-8")
    else:
        data = payload
    if not data:
        return 0
    return max(1, math.ceil(len(data) / 4))


def estimate_tokens_obj(obj: Any) -> int:
    """Estimate tokens of a JSON-serializable object via canonical serialization."""
    from signal_ingest.llm.serialize import serialize_json

    return estimate_tokens(serialize_json(obj))


def tokenizer_version() -> str:
    return TOKENIZER_VERSION
