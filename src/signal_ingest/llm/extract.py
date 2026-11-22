"""Grounded extract(candidate, provider): thin wrapper over the LangGraph extraction graph."""

from __future__ import annotations

from signal_ingest.llm.budget import ComputedBudget
from signal_ingest.llm.cache import ExtractionCache
from signal_ingest.llm.graph import run_extraction_graph
from signal_ingest.llm.provider import Provider
from signal_ingest.llm.types import ExtractCandidate, ExtractResult


async def extract(
    candidate: ExtractCandidate,
    provider: Provider,
    *,
    budget: ComputedBudget | None = None,
    cache: ExtractionCache | None = None,
    max_depth: int = 4,
) -> ExtractResult:
    """Run grounded LLM extraction with chunking, validation, merge, and cache."""
    return await run_extraction_graph(
        candidate, provider, budget=budget, cache=cache, max_depth=max_depth
    )
