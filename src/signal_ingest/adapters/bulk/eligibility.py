"""Counting eligibility for bulk verticals."""

from __future__ import annotations

from typing import Any


def startup_eligible(payload: dict[str, Any], *, ai_included: bool) -> bool:
    """STARTUP counts when AI inclusion is explicit and required identity fields exist."""
    if not ai_included:
        return False
    content = payload.get("content") or {}
    name = content.get("entityName")
    source = payload.get("source") or {}
    return bool(name and source.get("name") and source.get("url") and payload.get("collectedAt"))


def product_eligible(payload: dict[str, Any], *, owner_evidenced: bool, pricing_evidenced: bool) -> bool:
    """PRODUCT counts only when both owner and pricing are evidenced."""
    if not (owner_evidenced and pricing_evidenced):
        return False
    content = payload.get("content") or {}
    source = payload.get("source") or {}
    return bool(
        content.get("startupName")
        and content.get("pricingModel")
        and content.get("productName")
        and source.get("name")
        and source.get("url")
        and payload.get("collectedAt")
    )


def paper_join_complete(
    *,
    title: str | None,
    authors: list[str] | None,
    paper_url: str | None,
    published_date: Any,
    github_url: str | None,
    github_stars: int | None,
    primary_repo_ok: bool,
) -> bool:
    """
    RESEARCH_PAPER counts only after complete paper→primary-repo→stars join.
    True zero stars is valid; missing stars is not.
    """
    if not primary_repo_ok:
        return False
    if not title or not authors or not paper_url or published_date is None:
        return False
    if not github_url:
        return False
    if github_stars is None:
        return False
    if int(github_stars) < 0:
        return False
    return True


def paper_eligible_from_candidate(payload: dict[str, Any], *, primary_repo_ok: bool) -> bool:
    content = payload.get("content") or {}
    return paper_join_complete(
        title=content.get("title"),
        authors=content.get("authors"),
        paper_url=content.get("paper_url"),
        published_date=content.get("published_date"),
        github_url=content.get("github_url"),
        github_stars=content.get("github_stars"),
        primary_repo_ok=primary_repo_ok,
    )


def count_eligible_papers(candidates: list[dict[str, Any]]) -> int:
    """Eligible paper count: status=accepted with complete join only."""
    n = 0
    for row in candidates:
        if row.get("status") != "accepted":
            continue
        if row.get("record_type") != "RESEARCH_PAPER":
            continue
        payload = row.get("payload") or {}
        if paper_eligible_from_candidate(
            payload,
            primary_repo_ok=bool((payload.get("meta") or {}).get("primary_repo_ok")),
        ):
            n += 1
    return n
