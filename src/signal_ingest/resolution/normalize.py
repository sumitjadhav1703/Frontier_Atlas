"""Comparison-key normalization for startup/product names."""

from __future__ import annotations

import re
import unicodedata

import tldextract

from signal_ingest.resolution.versions import COMPARISON_KEY_VERSION

# Locale-aware legal / entity-type suffixes stripped from the *end* of a name.
# Longer phrases first so "limited liability company" wins over "company".
_LEGAL_SUFFIXES: tuple[str, ...] = (
    "limited liability company",
    "limited liability partnership",
    "public limited company",
    "incorporated",
    "corporation",
    "company",
    "limited",
    "l.l.c.",
    "l.l.c",
    "llc",
    "l.l.p.",
    "l.l.p",
    "llp",
    "pllc",
    "plc",
    "ltd.",
    "ltd",
    "inc.",
    "inc",
    "corp.",
    "corp",
    "co.",
    "co",
    "gmbh",
    "ag",
    "s.a.",
    "sa",
    "sas",
    "bv",
    "nv",
    "oy",
    "ab",
    "kk",
    "pty",
    "pvt",
    "pc",
    "lp",
)

_SUFFIX_PATTERN = re.compile(
    r"(?:^|\s+)(?:"
    + "|".join(re.escape(s) for s in _LEGAL_SUFFIXES)
    + r")\.?\s*$",
    re.IGNORECASE,
)

# Descriptive (non-legal) suffixes stripped for comparison only. A match that
# needs this strip is weaker evidence; resolve() then requires corroboration.
_DESCRIPTIVE_SUFFIXES: tuple[str, ...] = ("ai", "labs", "lab")
_DESCRIPTIVE_PATTERN = re.compile(
    r"\s+(?:" + "|".join(_DESCRIPTIVE_SUFFIXES) + r")$", re.IGNORECASE
)

# Offline PSL snapshot bundled with tldextract; private domains keep
# foo.github.io / bar.vercel.app distinct registrable domains.
_TLD = tldextract.TLDExtract(
    suffix_list_urls=(), cache_dir=None, include_psl_private_domains=True
)

# Keep letters (any script), numbers; drop punctuation/symbols/separators.
_STRIP_NON_ALNUM = re.compile(r"[^\w]+", re.UNICODE)
_UNDERSCORE = re.compile(r"_+")


def comparison_key(raw: str) -> str:
    """NFKC → casefold → fold accents → punct→space → strip legal suffixes.

    Examples:
      "OpenAI, Inc." / "Open AI" / "openai" → "openai"; "Café" → "cafe"
      "A.I." / "AI" → "ai"  (callers must abstain on multi-entity collisions)
    """
    return _key(raw, descriptive=False)


def descriptive_key(raw: str) -> str:
    """Looser key: also strips trailing descriptive words (AI, Labs, Lab).

    "Mistral AI" / "Mistral" / "Mistral Labs" → "mistral". Comparison only:
    a match that holds only on this key needs corroboration in resolve().
    Not a replacement for comparison_key ("Open AI" → "open" here).
    """
    return _key(raw, descriptive=True)


def _key(raw: str, *, descriptive: bool) -> str:
    if raw is None:
        raise TypeError("raw name is required")
    text = unicodedata.normalize("NFKC", str(raw)).casefold().strip()
    # Fold accents: NFKD then drop combining marks ("Café" → "cafe").
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )
    text = unicodedata.normalize("NFKC", text)
    if not text:
        return ""

    # Punctuation to spaces so suffix tokens stay word-bounded.
    spaced = _STRIP_NON_ALNUM.sub(" ", text)
    spaced = _UNDERSCORE.sub(" ", spaced)
    spaced = re.sub(r"\s+", " ", spaced).strip()

    # Strip trailing legal (and optionally descriptive) suffixes repeatedly,
    # never down to an empty key ("AI Corp" stays "ai").
    prev = None
    while prev != spaced and spaced:
        prev = spaced
        for pattern in (_SUFFIX_PATTERN, _DESCRIPTIVE_PATTERN) if descriptive else (_SUFFIX_PATTERN,):
            stripped = re.sub(r"\s+", " ", pattern.sub("", spaced)).strip()
            if stripped:
                spaced = stripped

    # Collapse remaining whitespace/punct for Open AI ↔ OpenAI.
    compact = _STRIP_NON_ALNUM.sub("", spaced)
    compact = compact.replace("_", "")
    return compact


def domain_key(host_or_url: str) -> str:
    """Normalize a host or URL to a lowercase host (no scheme/port/www)."""
    text = unicodedata.normalize("NFKC", str(host_or_url)).casefold().strip()
    if "://" in text:
        text = text.split("://", 1)[1]
    for sep in ("/", "?", "#"):
        text = text.split(sep, 1)[0]
    text = text.rsplit("@", 1)[-1]  # drop userinfo
    if not text.startswith("["):
        text = text.split(":", 1)[0]  # drop port
    text = text.rstrip(".")
    if text.startswith("www."):
        text = text[4:]
    return text


def is_platform_host(host_or_url: str) -> bool:
    """True when the host is a bare public suffix (github.io, vercel.app, com).

    Such a host has no registrable domain, so it can never identify one owner.
    """
    host = domain_key(host_or_url)
    return not host or not _TLD(host).top_domain_under_public_suffix


__all__ = [
    "COMPARISON_KEY_VERSION",
    "comparison_key",
    "domain_key",
    "descriptive_key",
    "is_platform_host",
]
