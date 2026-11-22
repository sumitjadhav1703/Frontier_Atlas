"""Deterministic field maps shared across bulk adapters."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse, urlunparse

from signal_ingest.models.records import PricingModel

# Explicit AI inclusion: category/tag/description tokens (case-insensitive).
_AI_TAG_TOKENS = frozenset(
    {
        "ai",
        "artificial intelligence",
        "machine learning",
        "ml",
        "deep learning",
        "generative ai",
        "llm",
        "nlp",
        "computer vision",
        "reinforcement learning",
    }
)

_RANGE_RE = re.compile(
    r"^\s*\d+\s*[-–—to]+\s*\d+\s*$",
    re.IGNORECASE,
)
_ARXIV_ID_RE = re.compile(
    r"(?:arxiv(?:\.org)?/(?:abs|pdf)/)?(?P<id>\d{4}\.\d{4,5})(?:v(?P<ver>\d+))?",
    re.IGNORECASE,
)
_LEGACY_ARXIV_RE = re.compile(
    r"(?:arxiv(?:\.org)?/(?:abs|pdf)/)?(?P<id>[a-z\-]+(?:\.[A-Z]{2})?/\d{7})(?:v(?P<ver>\d+))?",
    re.IGNORECASE,
)
_GITHUB_SEGMENT_RE = re.compile(r"^[a-z0-9_.-]+$")
# First path segments on github.com that are not repository owners.
_GITHUB_NON_OWNER = frozenset(
    {
        "orgs", "organizations", "users", "topics", "collections", "sponsors",
        "marketplace", "settings", "features", "apps", "enterprise", "explore",
        "login", "join", "about", "pricing", "search", "notifications", "issues",
        "pulls", "trending", "site", "customer-stories", "readme", "events",
    }
)
_TRAILING_PUNCT = ".,;:!?)]}>\"'`"
# Word-boundary AI matcher: "ai" must not match "email"/"retail", "ml" not "html".
_AI_TOKEN_RE = re.compile(
    r"\b(?:"
    + "|".join(re.escape(t).replace(r"\ ", r"\s+") for t in sorted(_AI_TAG_TOKENS, key=len, reverse=True))
    + r")\b",
    re.IGNORECASE,
)
_PAID_WORD_RE = re.compile(r"\b(?:paid|pro|premium)\b")
_AMOUNT_RE = re.compile(r"(\d+(?:,\d{3})*(?:\.\d+)?)")
_CURRENCY_AMOUNT_RE = re.compile(
    r"(?:[$€£]|\busd\b)\s*(\d+(?:,\d{3})*(?:\.\d+)?)|(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:\busd\b|[$€£])"
)

# Framework / dependency repo heuristics (reject as primary without official evidence).
_DEP_NAME_HINTS = frozenset(
    {
        "pytorch",
        "tensorflow",
        "keras",
        "transformers",
        "numpy",
        "scipy",
        "scikit-learn",
        "sklearn",
        "pandas",
        "jax",
        "flax",
        "huggingface",
        "datasets",
        "tokenizers",
        "accelerate",
        "lightning",
        "diffusers",
    }
)


def content_revision(payload: Any) -> str:
    """Stable snapshot hash for checkpoint binding."""
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def checkpoint_partition_key(*, revision: str, partition: str) -> str:
    """Bind checkpoint to source revision/hash + partition."""
    return f"{revision}:{partition}"


def parse_employee_count(value: Any) -> int | None:
    """
    Nonnegative integer if explicitly available.
    Ranges and missing values stay null (range text remains in raw evidence).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        if value.is_integer() and value >= 0:
            return int(value)
        return None
    text = str(value).strip()
    if not text:
        return None
    if _RANGE_RE.match(text) or "+" in text or "-" in text or "–" in text:
        # Explicit range-only → null (do not invent a midpoint).
        if not text.isdigit():
            return None
    try:
        n = int(text.replace(",", ""))
    except ValueError:
        return None
    return n if n >= 0 else None


def is_explicit_ai(
    *,
    tags: Any = None,
    categories: Any = None,
    description: str | None = None,
    industry: str | None = None,
) -> bool:
    """AI inclusion only when a category/tag or source description is explicit."""
    tokens: list[str] = []
    for group in (tags, categories):
        if group is None:
            continue
        if isinstance(group, str):
            tokens.append(group)
        elif isinstance(group, (list, tuple, set)):
            tokens.extend(str(x) for x in group)
    for extra in (description, industry):
        if extra:
            tokens.append(extra)
    return any(_AI_TOKEN_RE.search(token) for token in tokens if token.strip())


def _price_amount(value: Any) -> float | None:
    """Numeric price from 29 / "29" / "$0" / "0.00" / "free"; None when absent or unparseable."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().lower()
    if not text:
        return None
    if text == "free":
        return 0.0
    m = _AMOUNT_RE.search(text)
    return float(m.group(1).replace(",", "")) if m else None


def _blob_amount(blob: str) -> float | None:
    m = _CURRENCY_AMOUNT_RE.search(blob)
    if not m:
        return None
    return float((m.group(1) or m.group(2)).replace(",", ""))


def classify_pricing(
    *,
    pricing: str | None = None,
    plans: list[dict[str, Any]] | None = None,
    has_free_tier: bool | None = None,
    has_paid_tier: bool | None = None,
    trial_only: bool | None = None,
    enterprise_only: bool | None = None,
) -> PricingModel | None:
    """
    Pricing policy from contracts.md.
    Temporary free trial with paid → PAID (not FREEMIUM).
    Permanent free + paid → FREEMIUM.
    """
    label = (pricing or "").strip().lower()
    plan_rows = plans or []

    free = has_free_tier
    paid = has_paid_tier
    trial = trial_only
    enterprise = enterprise_only

    for plan in plan_rows:
        name = str(plan.get("name") or plan.get("tier") or "").lower()
        ptype = str(plan.get("type") or plan.get("pricing") or "").lower()
        blob = f"{name} {ptype}"
        if "enterprise" in blob:
            enterprise = True if enterprise is None else enterprise
        if "trial" in blob:
            trial = True if trial is None else trial
        amount = _price_amount(plan.get("price"))
        if amount is None:
            amount = _blob_amount(blob)
        if ptype in {"free", "forever free"} or name in {"free", "hobby"} or amount == 0:
            if "trial" not in blob:
                free = True if free is None else free
        if (amount is None or amount > 0) and (
            ptype in {"paid", "pro", "premium", "starter", "plus"} or _PAID_WORD_RE.search(blob)
        ):
            if "trial" not in blob:
                paid = True if paid is None else paid
        if amount is not None and amount > 0:
            if "trial" not in blob:
                paid = True if paid is None else paid
            else:
                trial = True if trial is None else trial
                paid = True if paid is None else paid

    if label in {"freemium"}:
        return PricingModel.FREEMIUM
    if label in {"free", "forever-free", "forever_free"}:
        return PricingModel.FREE
    if label in {"enterprise"}:
        return PricingModel.ENTERPRISE
    if label in {"paid", "subscription"}:
        # Trial-only free access with paid still maps to PAID.
        if trial and not free:
            return PricingModel.PAID
        if free and paid:
            return PricingModel.FREEMIUM
        return PricingModel.PAID
    if label in {"trial", "free-trial", "free_trial"}:
        return PricingModel.PAID

    if enterprise and not free and not paid:
        return PricingModel.ENTERPRISE
    if trial and paid and not free:
        return PricingModel.PAID
    if free and paid:
        return PricingModel.FREEMIUM
    if free and not paid:
        return PricingModel.FREE
    if paid and not free:
        return PricingModel.PAID
    if enterprise:
        return PricingModel.ENTERPRISE
    return None


def collapse_product_plans(
    rows: list[dict[str, Any]],
    *,
    product_key_fields: tuple[str, ...] = ("product_id",),
) -> list[dict[str, Any]]:
    """Collapse plan/locale/relaunch rows into one product per logical identity."""
    grouped: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for row in rows:
        key = _product_identity(row, product_key_fields)
        if key not in grouped:
            grouped[key] = {
                **row,
                "plans": list(row.get("plans") or ([row] if _looks_like_plan(row) else [])),
            }
            order.append(key)
        else:
            existing = grouped[key]
            plans = list(existing.get("plans") or [])
            if _looks_like_plan(row):
                plans.append(row)
            elif row.get("plans"):
                plans.extend(row["plans"])
            existing["plans"] = plans
            # Prefer evidenced owner/pricing from later rows without inventing.
            for field in ("owner", "company", "startupName", "pricing", "pricingModel"):
                if not existing.get(field) and row.get(field):
                    existing[field] = row[field]
    return [grouped[k] for k in order]


def _norm_text(value: Any) -> str:
    return " ".join(str(value or "").split()).casefold()


def _product_identity(row: dict[str, Any], fields: tuple[str, ...]) -> str:
    """Stable product ID, else (owner, name), else canonical URL — never name alone."""
    for f in fields:
        val = row.get(f)
        if val:
            return f"id:{str(val).strip().lower()}"
    owner = _norm_text(
        row.get("owner") or row.get("company") or row.get("startupName") or row.get("vendor")
    )
    name = _norm_text(row.get("productName") or row.get("name") or row.get("product_name"))
    if owner and name:
        return f"owner:{owner}::{name}"
    url = row.get("product_url") or row.get("url")
    if url:
        return f"url:{normalize_url(str(url))}"
    return f"owner:{owner}::{name}"


def _looks_like_plan(row: dict[str, Any]) -> bool:
    return any(k in row for k in ("plan", "tier", "price", "billing"))


def versionless_arxiv_id(value: str | None) -> str | None:
    """Preserve versionless paper ID (strip vN)."""
    if not value:
        return None
    text = value.strip()
    m = _ARXIV_ID_RE.search(text)
    if m:
        return m.group("id")
    m = _LEGACY_ARXIV_RE.search(text)
    if m:
        return m.group("id")
    # Already versionless bare id
    if re.fullmatch(r"\d{4}\.\d{4,5}", text):
        return text
    return None


def arxiv_version(value: str | None) -> int | None:
    if not value:
        return None
    m = _ARXIV_ID_RE.search(value) or _LEGACY_ARXIV_RE.search(value)
    if m and m.group("ver"):
        return int(m.group("ver"))
    return None


def parse_aware_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_github_url(url: str | None) -> str | None:
    """Canonical https://github.com/<owner>/<repo>, lowercased; None for non-repo links.

    Strips .git, /tree|/blob/... subpaths, query, fragment and trailing punctuation.
    Rejects gist.github.com, api.github.com and non-owner paths such as /orgs/x.
    """
    if not url:
        return None
    text = str(url).strip().strip(_TRAILING_PUNCT)
    if "://" not in text:
        text = "https://" + text
    parsed = urlparse(text)
    if (parsed.hostname or "").lower() not in {"github.com", "www.github.com"}:
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        return None
    owner = parts[0].lower()
    repo = parts[1].lower().rstrip(_TRAILING_PUNCT)
    if repo.endswith(".git"):
        repo = repo[:-4]
    if owner in _GITHUB_NON_OWNER or not repo:
        return None
    if not (_GITHUB_SEGMENT_RE.match(owner) and _GITHUB_SEGMENT_RE.match(repo)):
        return None
    return f"https://github.com/{owner}/{repo}"


def github_owner_repo(url: str | None) -> tuple[str, str] | None:
    norm = normalize_github_url(url)
    if not norm:
        return None
    parts = urlparse(norm).path.strip("/").split("/")
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


def normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    scheme = (parsed.scheme or "https").lower()
    host = (parsed.hostname or "").lower()
    path = parsed.path.rstrip("/") or ""
    return urlunparse((scheme, host, path, "", "", ""))


def looks_like_dependency_repo(
    *,
    repo_url: str | None,
    name: str | None = None,
    is_official: bool | None = None,
    framework: bool | None = None,
) -> bool:
    """Reject framework/dependency repos absent primary implementation evidence."""
    if is_official is True:
        return False
    if framework is True:
        return True
    candidates = []
    if name:
        candidates.append(name.lower())
    pair = github_owner_repo(repo_url)
    if pair:
        candidates.append(pair[1].lower())
        candidates.append(pair[0].lower())
    for c in candidates:
        base = c.replace("_", "-")
        if base in _DEP_NAME_HINTS or c in _DEP_NAME_HINTS:
            return True
    return False


def select_primary_repo(
    edges: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str | None]:
    """
    Prefer author-declared / official association.
    Reject ambiguous multi-repo ties and dependency repos without primary evidence.
    Returns (edge, reject_reason).
    """
    if not edges:
        return None, "no_repo_edge"

    usable: list[dict[str, Any]] = []
    for edge in edges:
        url = normalize_github_url(
            edge.get("repo_url") or edge.get("github_url") or edge.get("url")
        )
        if not url:
            continue
        if looks_like_dependency_repo(
            repo_url=url,
            name=edge.get("repo_name") or edge.get("name"),
            is_official=edge.get("is_official") or edge.get("official"),
            framework=edge.get("framework") or edge.get("is_framework"),
        ):
            continue
        usable.append({**edge, "repo_url": url})

    if not usable:
        return None, "dependency_or_invalid_repo"

    official = [
        e
        for e in usable
        if e.get("is_official") is True or e.get("official") is True
    ]
    pool = official if official else usable

    # Ambiguous: multiple distinct repos with equal claim and no single official.
    distinct = {e["repo_url"] for e in pool}
    if len(distinct) > 1:
        if not official:
            return None, "ambiguous_multi_repo"
        # Multiple official claims still ambiguous.
        if len({e["repo_url"] for e in official}) > 1:
            return None, "ambiguous_multi_repo"
        pool = official

    # Deterministic: prefer official, then shortest URL, then lexical.
    pool_sorted = sorted(
        pool,
        key=lambda e: (
            0 if (e.get("is_official") or e.get("official")) else 1,
            len(e["repo_url"]),
            e["repo_url"],
        ),
    )
    return pool_sorted[0], None
