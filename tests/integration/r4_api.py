"""R4 test helpers: deterministic offline embedder + lazy production imports.

The fake embedder is a hashed bag-of-words into 384 dims. It never downloads a
model and never touches the network. Synthetic; not for live export.
"""

from __future__ import annotations

import hashlib
import inspect
import math
import re
from typing import Any

from tests.integration.b6_api import MissingB6APIError, _import, require_attr

DIM = 384
_TOKEN = re.compile(r"\w+", re.UNICODE)


class FakeEmbedder:
    """Deterministic Embedder stand-in. Counts every text it embeds."""

    def __init__(self, model_id: str = "fake-hash-bow-384") -> None:
        self.model_id = model_id
        self.calls: list[str] = []

    def embed(self, texts: Any) -> list[list[float]]:
        out: list[list[float]] = []
        for text in texts:
            self.calls.append(str(text))
            vec = [0.0] * DIM
            for tok in _TOKEN.findall(str(text).casefold()):
                h = int.from_bytes(hashlib.sha256(tok.encode("utf-8")).digest()[:4], "big")
                vec[h % DIM] += 1.0
            norm = math.sqrt(sum(v * v for v in vec)) or 1.0
            out.append([v / norm for v in vec])
        return out


def cosine(a: str, b: str) -> float:
    va, vb = FakeEmbedder().embed([a, b])
    return sum(x * y for x, y in zip(va, vb, strict=True))


def embed_mod() -> Any:
    return _import("signal_ingest.resolution.embed")


def graph_mod() -> Any:
    return _import("signal_ingest.resolution.graph_queries")


def embed_entities(conn: Any, embedder: Any) -> Any:
    return require_attr(embed_mod(), "embed_entities")(conn, embedder)


def suggest_candidates(conn: Any, text: str, embedder: Any, **kwargs: Any) -> list[dict[str, Any]]:
    """Call suggest_candidates; the contract leaves the query embedder's channel open."""
    fn = require_attr(embed_mod(), "suggest_candidates")
    if "embedder" in inspect.signature(fn).parameters:
        kwargs["embedder"] = embedder
    return list(fn(conn, text, **kwargs))


__all__ = [
    "DIM",
    "FakeEmbedder",
    "MissingB6APIError",
    "cosine",
    "embed_entities",
    "embed_mod",
    "graph_mod",
    "require_attr",
    "suggest_candidates",
]
