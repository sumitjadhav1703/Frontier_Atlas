"""Typed record models preserving employer field casing."""

from signal_ingest.models.records import (
    JOB,
    NEWS,
    PRODUCT,
    RESEARCH_PAPER,
    STARTUP,
    JobRecord,
    NewsRecord,
    PricingModel,
    ProductRecord,
    RecordType,
    ResearchPaperRecord,
    StartupRecord,
    parse_record,
)

__all__ = [
    "JOB",
    "NEWS",
    "PRODUCT",
    "RESEARCH_PAPER",
    "STARTUP",
    "JobRecord",
    "NewsRecord",
    "PricingModel",
    "ProductRecord",
    "RecordType",
    "ResearchPaperRecord",
    "StartupRecord",
    "parse_record",
]
