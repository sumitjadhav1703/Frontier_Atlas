"""URL normalization, destination safety, and robots policy."""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

from signal_ingest.config.settings import Mode

# Documented tracking parameters stripped during normalization.
TRACKING_PARAMS = frozenset(
    {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "utm_id",
        "utm_reader",
        "utm_name",
        "fbclid",
        "gclid",
        "gclsrc",
        "dclid",
        "msclkid",
        "mc_cid",
        "mc_eid",
        "_ga",
        "_gl",
        "igshid",
        "mkt_tok",
        "yclid",
        "wickedid",
    }
)

_CHALLENGE_MARKERS = (
    "cf-browser-verification",
    "challenge-platform",
    "just a moment",
    "attention required! | cloudflare",
    "enable javascript and cookies to continue",
    "captcha-delivery",
    "g-recaptcha",
    "hcaptcha",
    "ddos-guard",
    "checking your browser before accessing",
)


class PolicyError(ValueError):
    """URL or robots policy rejection."""


@dataclass
class UrlPolicy:
    """Normalize and validate crawl destinations."""

    mode: Mode = Mode.FIXTURE
    allow_http_loopback: bool = True
    user_agent: str = "signal-ingest/0.1 (+assessment)"

    def normalize(self, url: str, *, base: str | None = None) -> str:
        raw = url.strip()
        if base:
            raw = urljoin(base, raw)
        parts = urlsplit(raw)
        if not parts.scheme or not parts.netloc:
            raise PolicyError(f"URL missing scheme/host: {url!r}")
        scheme = parts.scheme.lower()
        host = parts.hostname
        if host is None:
            raise PolicyError(f"URL missing host: {url!r}")
        host_l = host.lower()
        # Drop default ports; preserve non-default.
        port = parts.port
        if port and not (
            (scheme == "http" and port == 80)
            or (scheme == "https" and port == 443)
        ):
            netloc = f"{host_l}:{port}"
        else:
            netloc = host_l
        # IPv6 bracket form
        if ":" in host_l and not host_l.startswith("["):
            try:
                ipaddress.IPv6Address(host_l)
                if port and not (
                    (scheme == "http" and port == 80)
                    or (scheme == "https" and port == 443)
                ):
                    netloc = f"[{host_l}]:{port}"
                else:
                    netloc = f"[{host_l}]"
            except ValueError:
                pass
        userinfo = ""
        if parts.username is not None:
            userinfo = parts.username
            if parts.password is not None:
                userinfo += f":{parts.password}"
            userinfo += "@"
        # Query: drop fragment always; strip tracking params; keep identity params.
        query_pairs = [
            (k, v)
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k.lower() not in TRACKING_PARAMS
        ]
        query = urlencode(query_pairs, doseq=True)
        path = parts.path or "/"
        return urlunsplit((scheme, userinfo + netloc, path, query, ""))

    def strip_auth_if_cross_origin(self, url: str, previous: str | None) -> str:
        parts = urlsplit(url)
        if not parts.username and parts.password is None:
            return url
        if previous is None:
            return url
        prev = urlsplit(previous)
        same_origin = (
            parts.scheme.lower() == prev.scheme.lower()
            and (parts.hostname or "").lower() == (prev.hostname or "").lower()
            and (parts.port or _default_port(parts.scheme))
            == (prev.port or _default_port(prev.scheme))
        )
        if same_origin:
            return url
        # Strip userinfo on cross-origin redirect.
        host = (parts.hostname or "").lower()
        port = parts.port
        if port and not (
            (parts.scheme == "http" and port == 80)
            or (parts.scheme == "https" and port == 443)
        ):
            netloc = f"{host}:{port}"
        else:
            netloc = host
        return urlunsplit((parts.scheme.lower(), netloc, parts.path or "/", parts.query, ""))

    def validate_destination(self, url: str) -> str:
        """Validate scheme and host safety; return normalized URL."""
        normalized = self.normalize(url)
        parts = urlsplit(normalized)
        scheme = parts.scheme.lower()
        host = parts.hostname
        if host is None:
            raise PolicyError("missing host")
        loopback = _is_loopback_host(host)
        # Fixture transport: loopback only — never DNS/connect to public hosts.
        if self.mode is Mode.FIXTURE:
            if not (self.allow_http_loopback and loopback):
                raise PolicyError(
                    f"fixture mode rejects non-loopback destination: {host}"
                )
            if scheme not in {"http", "https"}:
                raise PolicyError(f"unsupported scheme for fixture transport: {scheme}")
            return normalized
        if scheme != "https":
            raise PolicyError(f"only HTTPS public destinations allowed; got {scheme}")
        if loopback:
            raise PolicyError("loopback hosts require fixture transport")
        if _is_unsafe_host(host):
            raise PolicyError(f"private/unsafe destination rejected: {host}")
        return normalized

    def validate_redirect_target(self, url: str, *, previous: str | None) -> str:
        stripped = self.strip_auth_if_cross_origin(url, previous)
        return self.validate_destination(stripped)


@dataclass
class RobotsCache:
    """Per-origin robots.txt cache using stdlib RobotFileParser."""

    user_agent: str = "signal-ingest/0.1 (+assessment)"
    _parsers: dict[str, RobotFileParser] = field(default_factory=dict)
    _fetched: dict[str, bool] = field(default_factory=dict)

    def origin_of(self, url: str) -> str:
        parts = urlsplit(url)
        return f"{parts.scheme}://{parts.netloc}"

    def robots_url(self, url: str) -> str:
        return f"{self.origin_of(url)}/robots.txt"

    def remember(self, origin: str, body: bytes | None, *, status_code: int | None) -> None:
        parser = RobotFileParser()
        parser.set_url(f"{origin}/robots.txt")
        if status_code == 200 and body is not None:
            text = body.decode("utf-8", errors="replace")
            parser.parse(text.splitlines())
        else:
            # Missing/denied robots → allow all (common crawl convention).
            parser.parse([])
        self._parsers[origin] = parser
        self._fetched[origin] = True

    def is_allowed(self, url: str) -> bool:
        origin = self.origin_of(url)
        parser = self._parsers.get(origin)
        if parser is None:
            # Not yet fetched → caller must fetch robots first; default allow.
            return True
        return parser.can_fetch(self.user_agent, url)

    def needs_fetch(self, url: str) -> bool:
        return self.origin_of(url) not in self._fetched


def looks_like_challenge(body: bytes, content_type: str | None, status_code: int) -> bool:
    """Detect challenge/interstitial pages served as HTTP 200."""
    if status_code != 200:
        return False
    ctype = (content_type or "").lower()
    if "html" not in ctype and ctype:
        return False
    sample = body[:8192].decode("utf-8", errors="replace").lower()
    return any(marker in sample for marker in _CHALLENGE_MARKERS)


def extract_alternate_feed_hint(body: bytes, base_url: str) -> str | None:
    """Best-effort alternate feed/link from HTML challenge or page head."""
    text = body[:65536].decode("utf-8", errors="replace")
    # <link rel="alternate" type="application/rss+xml|atom+xml" href="...">
    pattern = re.compile(
        r'<link[^>]+rel=["\']alternate["\'][^>]*>',
        re.IGNORECASE,
    )
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
    type_re = re.compile(
        r'type=["\'](application/(?:rss|atom)\+xml|text/xml)["\']',
        re.IGNORECASE,
    )
    for tag in pattern.findall(text):
        if not type_re.search(tag):
            continue
        m = href_re.search(tag)
        if m:
            return urljoin(base_url, m.group(1))
    return None


def _default_port(scheme: str) -> int | None:
    if scheme == "http":
        return 80
    if scheme == "https":
        return 443
    return None


def _is_loopback_host(host: str) -> bool:
    normalized = host.strip().lower().strip("[]")
    if normalized in {"localhost", "127.0.0.1", "::1"}:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _is_unsafe_host(host: str) -> bool:
    normalized = host.strip().lower().strip("[]")
    if normalized in {"localhost"}:
        return True
    try:
        ip = ipaddress.ip_address(normalized)
    except ValueError:
        # Hostname: reject obvious local names; DNS rebinding checked after resolve.
        if normalized.endswith(".local") or normalized.endswith(".localhost"):
            return True
        return False
    return bool(
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


# DNS64/NAT64 well-known prefix — synthetic mapped addrs, not LAN destinations.
_NAT64_WELL_KNOWN = ipaddress.ip_network("64:ff9b::/96")


def _resolved_ip_unsafe(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """True when an address is a private/local destination (not NAT64 synthetic)."""
    if isinstance(ip, ipaddress.IPv6Address) and ip in _NAT64_WELL_KNOWN:
        return False
    return bool(
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def resolved_addresses_unsafe(host: str) -> bool:
    """Return True if any resolved address is private/unsafe (post-DNS check)."""
    import socket

    from signal_ingest.config.network import NetworkGuardError, network_guard_enabled

    # Under the default guard, never issue public DNS — treat as unsafe.
    if network_guard_enabled() and not _is_loopback_host(host):
        return True

    try:
        infos = socket.getaddrinfo(host, None)
    except (OSError, NetworkGuardError):
        return True
    saw_public = False
    for info in infos:
        addr = info[4][0]
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            continue
        if _resolved_ip_unsafe(ip):
            # DNS rebinding: any true private/local address fails closed.
            return True
        saw_public = True
    # No usable public address → unsafe.
    return not saw_public
