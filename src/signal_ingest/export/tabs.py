"""Exact six Sheet tab names and header contracts (docs/contracts.md)."""

from __future__ import annotations

from typing import Final

# Assessment / contracts.md tab titles — exact strings for gate checks.
TAB_STARTUPS: Final = "Startups"
TAB_PRODUCTS: Final = "Products"
TAB_RESEARCH_PAPERS: Final = "Research Papers"
TAB_JOBS: Final = "Jobs"
TAB_NEWS: Final = "News"
TAB_ENTITY_MAPPING_LOG: Final = "Entity Mapping Log"

TAB_NAMES: Final[tuple[str, ...]] = (
    TAB_STARTUPS,
    TAB_PRODUCTS,
    TAB_RESEARCH_PAPERS,
    TAB_JOBS,
    TAB_NEWS,
    TAB_ENTITY_MAPPING_LOG,
)

RECORD_TYPE_TO_TAB: Final[dict[str, str]] = {
    "STARTUP": TAB_STARTUPS,
    "PRODUCT": TAB_PRODUCTS,
    "RESEARCH_PAPER": TAB_RESEARCH_PAPERS,
    "JOB": TAB_JOBS,
    "NEWS": TAB_NEWS,
}

# Flattened header order per tab (dot-separated nested keys).
TAB_HEADERS: Final[dict[str, tuple[str, ...]]] = {
    TAB_STARTUPS: (
        "schemaVersion",
        "source.name",
        "source.url",
        "content.entityName",
        "content.data.employeeCount",
        "collectedAt",
        "durableIdentity",
        "asOf",
        "lineageMode",
        "runId",
    ),
    TAB_PRODUCTS: (
        "schemaVersion",
        "source.name",
        "source.url",
        "content.startupName",
        "content.productName",
        "content.pricingModel",
        "content.productUrl",
        "content.ownerEvidenceUrl",
        "content.pricingEvidenceUrl",
        "collectedAt",
        "durableIdentity",
        "asOf",
        "lineageMode",
        "runId",
    ),
    TAB_RESEARCH_PAPERS: (
        "schemaVersion",
        "source.name",
        "source.url",
        "content.title",
        "content.authors",
        "content.paper_url",
        "content.github_url",
        "content.github_stars",
        "content.published_date",
        "content.repoId",
        "content.starsObservedAt",
        "collectedAt",
        "durableIdentity",
        "asOf",
        "lineageMode",
        "runId",
    ),
    TAB_JOBS: (
        "schemaVersion",
        "source.name",
        "source.url",
        "content.company",
        "content.title",
        "content.date",
        "content.is_remote",
        "content.role_family",
        "content.listingUrl",
        "content.employerJobUrl",
        "content.bodyHash",
        "content.bodyLength",
        "collectedAt",
        "durableIdentity",
        "asOf",
        "lineageMode",
        "runId",
    ),
    TAB_NEWS: (
        "schemaVersion",
        "source.name",
        "source.url",
        "content.title",
        "content.date",
        "content.text",
        "content.article_url",
        "content.bodyHash",
        "content.bodyLength",
        "content.dateExtractionMethod",
        "collectedAt",
        "durableIdentity",
        "asOf",
        "lineageMode",
        "runId",
    ),
    TAB_ENTITY_MAPPING_LOG: (
        "entityType",
        "rawName",
        "canonicalName",
        "canonicalID",
        "method",
        "decision",
        "evidenceURL",
        "runID",
        "ruleVersion",
        "asOf",
        "lineageMode",
    ),
}

__all__ = [
    "RECORD_TYPE_TO_TAB",
    "TAB_ENTITY_MAPPING_LOG",
    "TAB_HEADERS",
    "TAB_JOBS",
    "TAB_NAMES",
    "TAB_NEWS",
    "TAB_PRODUCTS",
    "TAB_RESEARCH_PAPERS",
    "TAB_STARTUPS",
]
