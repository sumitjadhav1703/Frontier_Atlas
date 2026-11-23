"""Deterministic field maps shared across bulk adapters."""

from __future__ import annotations

import hashlib
import html as html_lib
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


UNMAPPED_PRICING_LABELS = frozenset(
    {"contact-sales", "contact_sales", "contact sales", "contact", "open-source", "open_source", "opensource"}
)

OWNER_HTML_RULE_VERSION = "owner-html-v4"
_JSONLD_RE = re.compile(
    r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_ORG_TYPES = frozenset({"organization", "corporation", "localbusiness", "ngo", "onlinebusiness"})
_PUBLISHER_HOLDERS = frozenset({"website", "softwareapplication", "webapplication"})
# Product/WebPage nodes count as publisher holders only when their own URL is on the homepage domain.
_PUBLISHER_HOLDERS_DOMAIN_ONLY = frozenset({"product", "webpage"})
_TAG_RE = re.compile(r"<[^>]+>")
_INVISIBLE_RE = re.compile(
    r"<!--.*?-->|<(script|style|noscript|template)\b[^>]*>.*?</\1\s*>",
    re.IGNORECASE | re.DOTALL,
)
_BLOCK_TAG_RE = re.compile(
    r"</?(?:p|div|section|article|aside|header|footer|nav|main|li|ul|ol|br|h[1-6]|table|tr|td|span)\b[^>]*>",
    re.IGNORECASE,
)
_COPYRIGHT_RE = re.compile(
    r"(?:©|\(c\)|\bcopyright\b)\s*©?\s*"
    r"(?:(?:19|20)\d{2}\s*(?:[-–—]\s*(?:(?:19|20)\d{2}|present)\s*)?[,.]?\s*)?"
    r"(?P<holder>[A-Z0-9][\w&.,'’\- ]{1,80}?)"
    r"\s*(?:[.|·•]\s*)?(?=all rights|\||·|•|<|$|\n|\s{2,}|\. )",
    re.IGNORECASE,
)
_LEGAL_SUFFIXES = (
    "inc", "llc", "ltd", "limited", "gmbh", "corp", "corporation", "co", "plc", "ag", "sa",
    "sas", "srl", "bv", "oy", "ab", "pty", "pte", "sl", "spa", "kk", "llp", "lp",
)
# "Co" must end the word: "Acme Co-pilot Ltd" keeps "Co-pilot".
_LEGAL_SUFFIX_RE = re.compile(
    r"^(?P<name>.*?\b(?:" + "|".join(_LEGAL_SUFFIXES) + r")\b\.?)(?=\s|,|$)",
    re.IGNORECASE,
)
_HOLDER_REJECT_RE = re.compile(
    r"\b(?:by|powered|template|templates|theme|themes|design|designed|built|made)\b|all rights",
    re.IGNORECASE,
)
_PLATFORM_DENY = frozenset(
    {
        "webflow", "framer", "wix", "squarespace", "wordpress", "automattic", "shopify",
        "vercel", "netlify", "godaddy", "carrd", "notion", "bubble", "ghost", "hubspot",
        "google", "facebook", "meta", "themeforest", "elementor", "cloudflare",
    }
)
_FOLD_STOP = frozenset({"the", "and", "of", "a"} | set(_LEGAL_SUFFIXES))
_TLD_EXTRACT = None


def registrable_domain(url_or_host: str | None) -> str | None:
    """eTLD+1 via tldextract's bundled PSL; non-PSL hosts (fixtures, IPs) use the last two labels."""
    global _TLD_EXTRACT
    if not url_or_host:
        return None
    text = str(url_or_host).strip()
    host = (urlparse(text if "://" in text else f"//{text}").hostname or "").lower().rstrip(".")
    if not host:
        return None
    if _TLD_EXTRACT is None:
        import tldextract

        _TLD_EXTRACT = tldextract.TLDExtract(
            suffix_list_urls=(), cache_dir=None, include_psl_private_domains=True
        )
    ext = _TLD_EXTRACT(host)
    top = ext.top_domain_under_public_suffix
    if top:
        return top.lower()
    if re.fullmatch(r"[\d.]+|\[?[0-9a-f:]+\]?", host):
        return host
    return ".".join(host.split(".")[-2:])


def _fold_tokens(text: str | None) -> list[str]:
    """Case/accent-folded alphanumeric tokens with legal suffixes and stopwords removed."""
    import unicodedata

    raw = unicodedata.normalize("NFKD", str(text or ""))
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch)).casefold()
    return [t for t in re.split(r"[^0-9a-z]+", raw) if t and t not in _FOLD_STOP]


def _domain_label(page_url: str | None) -> str:
    reg = registrable_domain(page_url) or ""
    return reg.split(".", 1)[0] if reg else ""


_GENERIC_TOKENS = frozenset(
    {
        "ai", "app", "apps", "labs", "lab", "tech", "get", "try", "use", "hq", "io", "studio",
        "writer", "cloud", "gpt", "chat", "bot", "the", "inc", "co", "llc", "ltd", "tools",
        "tool", "data", "pro", "hub", "ly",
    }
)


def owner_plausible(holder: str, *, page_url: str | None, product_name: str | None) -> bool:
    """Label rule for owners with no URL evidence (URL-less JSON-LD nodes, footer holders).

    Accept iff the folded holder equals the homepage domain label, or one is a prefix of the
    other with >= 4 shared chars; or a non-generic product-name token (>= 3 chars) is in the
    holder and also in the domain label. Platform credits are always rejected.
    """
    tokens = _fold_tokens(holder)
    if not tokens or _HOLDER_REJECT_RE.search(holder) or set(tokens) & _PLATFORM_DENY:
        return False
    label = re.sub(r"[^0-9a-z]", "", _domain_label(page_url))
    if not label:
        return False
    compact = "".join(tokens)
    if compact == label:
        return True
    shorter, longer = sorted((compact, label), key=len)
    if len(shorter) >= 4 and longer.startswith(shorter):
        return True
    product = {t for t in _fold_tokens(product_name) if len(t) >= 3 and t not in _GENERIC_TOKENS}
    return any(t in label for t in product & set(tokens))


def _node_hosts(node: dict[str, Any]) -> list[str]:
    vals: list[Any] = [node.get("url"), node.get("@id")]
    same = node.get("sameAs")
    vals.extend(same if isinstance(same, list) else [same])
    return [str(v) for v in vals if isinstance(v, str) and ("://" in v or v.startswith("//"))]


def _on_domain(node: dict[str, Any], domain: str | None) -> bool:
    return bool(domain) and any(registrable_domain(u) == domain for u in _node_hosts(node))


def _jsonld_nodes(html: str) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []

    def walk(obj: Any) -> None:
        if isinstance(obj, list):
            for x in obj:
                walk(x)
        elif isinstance(obj, dict):
            nodes.append(obj)
            if isinstance(obj.get("@graph"), list):
                walk(obj["@graph"])

    for block in _JSONLD_RE.findall(html):
        try:
            walk(json.loads(block.strip()))
        except (ValueError, TypeError):
            continue
    return nodes


def _types(node: dict[str, Any]) -> set[str]:
    raw = node.get("@type")
    vals = raw if isinstance(raw, list) else [raw]
    return {str(v).strip().lower() for v in vals if v}


def _org_name(node: Any) -> str | None:
    if not isinstance(node, dict):
        return None
    for key in ("legalName", "name"):
        val = node.get(key)
        if isinstance(val, str) and val.strip():
            return " ".join(val.split())
    return None


def _visible_text(fragment: str) -> str:
    text = _BLOCK_TAG_RE.sub("\n", fragment)
    return html_lib.unescape(_TAG_RE.sub(" ", text))


_PRODUCT_OF_RE = re.compile(r"(?:^|[,\s])(?:a\s+)?product\s+of\s+(?P<owner>.+)$", re.IGNORECASE)


def clean_owner_name(name: str) -> str:
    """Trim page-title and address noise from a found owner string (excerpt keeps the raw text)."""
    out = html_lib.unescape(name).replace("\u2122", "").replace("\u00ae", "").replace("\u00a9", "")
    out = re.sub(r"^\s*(?:copyright|\(c\))\s*(?:\d{4}(?:\s*[-\u2013]\s*\d{4})?)?\s*", "", out, flags=re.IGNORECASE)
    m = _PRODUCT_OF_RE.search(out)
    if m:
        out = m.group("owner")
    out = re.split(r"\s+[|\u2014\u2013-]\s+", out, maxsplit=1)[0]  # "Brand - tagline", "Brand | tagline"
    out = re.split(r",\s*\d", out, maxsplit=1)[0]  # ", 165 Page St, ..." street address
    out = re.sub(r"\s+\d{4}$", "", out)  # trailing year
    return " ".join(out.split()).strip(" ,;:")


def owner_from_html(
    html: str | bytes | None,
    *,
    page_url: str | None = None,
    product_name: str | None = None,
) -> dict[str, Any] | None:
    """Deterministic owner from a vendor homepage (rule OWNER_HTML_RULE_VERSION).

    JSON-LD: the publisher/author/provider of a WebSite/SoftwareApplication node, or an
    Organization/Corporation node (legalName before name). A node counts when it (or the
    WebSite it publishes) sits on the homepage's registrable domain, or its name overlaps
    the domain label / product name. Footer: a ©/(c)/Copyright holder from <footer> text,
    else the last visible text block, under the same overlap rule and platform denylist.
    Returns {value, method, pointer, excerpt, rule_version} or None.
    """
    if not html:
        return None
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    domain = registrable_domain(page_url)

    def ok(name: str, node: dict[str, Any] | None = None, *, linked: bool = False) -> bool:
        if _HOLDER_REJECT_RE.search(name) or set(_fold_tokens(name)) & _PLATFORM_DENY:
            return False
        if node is not None and _node_hosts(node):
            # Domain-first: a node that carries URLs must carry one on the homepage domain.
            return _on_domain(node, domain)
        return linked or owner_plausible(name, page_url=page_url, product_name=product_name)

    def found(name: str, method: str, pointer: str, excerpt: str) -> dict[str, Any]:
        return {
            "value": clean_owner_name(name) or name, "method": method, "pointer": pointer, "excerpt": excerpt,
            "rule_version": OWNER_HTML_RULE_VERSION, "homepage_domain": domain,
        }

    nodes = _jsonld_nodes(text)
    by_id = {str(n["@id"]): n for n in nodes if n.get("@id")}
    for node in nodes:
        types = _types(node)
        holder_on_domain = _on_domain(node, domain)
        if not (types & _PUBLISHER_HOLDERS or (types & _PUBLISHER_HOLDERS_DOMAIN_ONLY and holder_on_domain)):
            continue
        for key in ("publisher", "author", "provider", "creator"):
            ref = node.get(key)
            ref = ref[0] if isinstance(ref, list) and ref else ref
            if isinstance(ref, dict) and ref.get("@id") and not _org_name(ref):
                ref = by_id.get(str(ref["@id"]))
            if isinstance(ref, dict) and "person" not in _types(ref):
                name = _org_name(ref)
                if name and ok(name, ref, linked=holder_on_domain):
                    return found(name, "json_ld", f"$.{sorted(types)[0]}.{key}", name)
    for node in nodes:
        if _types(node) & _ORG_TYPES:
            name = _org_name(node)
            if name and ok(name, node):
                key = "legalName" if node.get("legalName") else "name"
                return found(name, "json_ld", f"$.Organization.{key}", name)
    visible = _INVISIBLE_RE.sub(" ", text)
    footers = re.findall(r"<footer\b.*?</footer>", visible, re.IGNORECASE | re.DOTALL)
    if footers:
        chunks = [("footer", _visible_text(f)) for f in footers]
    else:
        body = re.split(r"<body\b[^>]*>", visible, maxsplit=1, flags=re.IGNORECASE)[-1]
        lines = [ln.strip() for ln in _visible_text(body).split("\n") if ln.strip()]
        chunks = [("tail_block", lines[-1])] if lines else []
    for pointer, plain in chunks:
        for m in _COPYRIGHT_RE.finditer(plain):
            holder = " ".join(m.group("holder").split()).strip(" ,.-|")
            legal = _LEGAL_SUFFIX_RE.match(holder)
            if legal:
                holder = legal.group("name").strip()
            if len(holder) < 2 or re.fullmatch(r"[\d\s\-–—]+", holder):
                continue
            if not ok(holder):
                continue
            return found(holder, "footer_copyright", pointer, " ".join(m.group(0).split()))
    return None


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
    # contracts.md: "contact us" alone or an open-source label is insufficient evidence.
    if label in UNMAPPED_PRICING_LABELS:
        return None
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
