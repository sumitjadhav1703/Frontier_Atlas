"""Import helpers for B6 entity resolution surfaces.

Thin wrappers over `signal_ingest.resolution` so gates fail clearly when the
B6 stack is incomplete. Oracle: planner B6 + production package exports.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from tests.support import FIXTURES

RESOLUTION_FIXTURES = FIXTURES / "resolution"
SEEDS_PATH = RESOLUTION_FIXTURES / "seeds.json"
HOLDOUT_PATH = RESOLUTION_FIXTURES / "holdout_negatives.json"
POSITIVES_PATH = RESOLUTION_FIXTURES / "positives.json"
CASES_PATH = RESOLUTION_FIXTURES / "cases.json"
PROD_SEEDS_PATH = Path(__file__).resolve().parents[2] / "config" / "seeds.fixture.json"


class MissingB6APIError(AssertionError):
    """Required B6 API is not importable."""


def _import(path: str) -> Any:
    try:
        return importlib.import_module(path)
    except ImportError as exc:
        raise MissingB6APIError(f"Cannot import {path}: {exc}") from exc


def require_attr(module: Any, *names: str) -> Any:
    tried = []
    for name in names:
        tried.append(f"{getattr(module, '__name__', module)}.{name}")
        if hasattr(module, name):
            return getattr(module, name)
    raise MissingB6APIError("Missing API: " + ", ".join(tried))


def load_resolution_pkg() -> Any:
    return _import("signal_ingest.resolution")


def comparison_key(raw: str) -> str:
    pkg = load_resolution_pkg()
    return require_attr(pkg, "comparison_key")(raw)


def load_seed_register(path: Path | str | None = None) -> Any:
    """Load SeedRegister from path (gate fixtures by default)."""
    pkg = load_resolution_pkg()
    SeedRegister = require_attr(pkg, "SeedRegister")
    target = Path(path) if path is not None else SEEDS_PATH
    return SeedRegister.from_path(target, mode="fixture")


def make_candidate(**kwargs: Any) -> Any:
    pkg = load_resolution_pkg()
    ResolveCandidate = require_attr(pkg, "ResolveCandidate")
    Namespace = require_attr(pkg, "Namespace")

    payload = dict(kwargs)
    # Accept entity_type alias → namespace
    if "namespace" not in payload and "entity_type" in payload:
        payload["namespace"] = payload.pop("entity_type")
    else:
        payload.pop("entity_type", None)

    ns = payload.get("namespace", "startup")
    if isinstance(ns, str):
        payload["namespace"] = Namespace(ns)

    # Drop unknown keys that tests may pass for documentation
    allowed = set(getattr(ResolveCandidate, "model_fields", {}).keys())
    if allowed:
        payload = {k: v for k, v in payload.items() if k in allowed}
    return ResolveCandidate(**payload)


def resolve(
    candidate: Any,
    *,
    mode: str | None = None,
    register: Any = None,
    **kwargs: Any,
) -> Any:
    """resolve(candidate, register) — mode lives on the candidate."""
    pkg = load_resolution_pkg()
    resolve_fn = require_attr(pkg, "resolve")
    reg = register if register is not None else load_seed_register()

    if isinstance(candidate, dict):
        if mode is not None:
            candidate = {**candidate, "mode": mode}
        candidate = make_candidate(**candidate)
    elif mode is not None and hasattr(candidate, "model_copy"):
        candidate = candidate.model_copy(update={"mode": mode})
    elif mode is not None and hasattr(candidate, "mode"):
        try:
            candidate.mode = mode
        except Exception:
            pass

    return resolve_fn(candidate, reg, **kwargs)


def decision_value(result: Any) -> str:
    if result is None:
        raise MissingB6APIError("resolve returned None")
    raw = getattr(result, "decision", None)
    if raw is None and isinstance(result, dict):
        raw = result.get("decision")
    if raw is None:
        raise MissingB6APIError(f"Decision missing on result: {type(result)!r}")
    if hasattr(raw, "value"):
        raw = raw.value
    text = str(raw).strip().lower()
    for token in ("match", "new", "abstain", "rejected", "reject"):
        if token in text:
            return "rejected" if token in {"rejected", "reject"} else token
    return text


def canonical_id_of(result: Any) -> str | None:
    for attr in ("canonical_id", "canonicalID", "canonicalId"):
        if hasattr(result, attr):
            val = getattr(result, attr)
            return None if val is None else str(val)
        if isinstance(result, dict) and attr in result:
            val = result[attr]
            return None if val is None else str(val)
    return None


def canonical_name_of(result: Any) -> str | None:
    for attr in ("canonical_name", "canonicalName"):
        if hasattr(result, attr):
            val = getattr(result, attr)
            return None if val is None else str(val)
        if isinstance(result, dict) and attr in result:
            val = result[attr]
            return None if val is None else str(val)
    return None


def mapping_event_key(**kwargs: Any) -> str:
    pkg = load_resolution_pkg()
    fn = require_attr(pkg, "mapping_observation_key")
    # Production uses raw_value; accept raw_name from tests.
    if "raw_value" not in kwargs and "raw_name" in kwargs:
        kwargs = dict(kwargs)
        kwargs["raw_value"] = kwargs.pop("raw_name")
    kwargs.pop("raw_name", None)
    return str(fn(**kwargs))


def record_mapping_event(conn: Any, decision: Any, **kwargs: Any) -> Any:
    pkg = load_resolution_pkg()
    fn = require_attr(pkg, "record_resolution_event")
    return fn(conn, decision, **kwargs)


def evaluate_metrics(pairs: list[dict[str, Any]], *, register: Any = None) -> dict[str, Any]:
    """TP/FP/FN report. Positives → TP/FN; holdout negatives → FP on false merges."""
    pkg = load_resolution_pkg()
    for name in ("evaluate_resolution", "resolution_metrics", "score_holdout", "compute_metrics"):
        if hasattr(pkg, name):
            return dict(getattr(pkg, name)(pairs, register=register))

    reg = register if register is not None else load_seed_register()
    tp = fp = fn = abstentions = 0
    positive_n = 0
    negative_n = 0
    for pair in pairs:
        kind = pair.get("kind")
        if kind == "positive":
            positive_n += 1
            fields = {
                k: v
                for k, v in pair.items()
                if k not in {"kind", "expected_id", "expected_name"}
            }
            cand = make_candidate(**fields)
            result = resolve(cand, register=reg)
            dec = decision_value(result)
            cid = canonical_id_of(result)
            if dec == "match" and cid == pair.get("expected_id"):
                tp += 1
            elif dec == "match" and cid != pair.get("expected_id"):
                fp += 1
                fn += 1
            else:
                if dec == "abstain":
                    abstentions += 1
                fn += 1
        elif kind == "negative":
            negative_n += 1
            left = make_candidate(**pair["left"])
            right = make_candidate(**pair["right"])
            r1 = resolve(left, register=reg)
            r2 = resolve(right, register=reg)
            id1, id2 = canonical_id_of(r1), canonical_id_of(r2)
            d1, d2 = decision_value(r1), decision_value(r2)
            if d1 == "abstain":
                abstentions += 1
            if d2 == "abstain":
                abstentions += 1
            if id1 is not None and id1 == id2 and d1 == "match" and d2 == "match":
                fp += 1
        else:
            raise ValueError(f"unknown pair kind: {kind!r}")

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "abstention": abstentions,
        "sample_size": len(pairs),
        "positive_n": positive_n,
        "negative_n": negative_n,
        "recall_denominator": tp + fn,
    }
