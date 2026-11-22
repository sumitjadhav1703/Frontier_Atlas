"""Socket/DNS guard: allow only loopback by default."""

from __future__ import annotations

import ipaddress
import socket
from typing import Any

_installed = False
_orig_connect = socket.socket.connect
_orig_getaddrinfo = socket.getaddrinfo


class NetworkGuardError(PermissionError):
    """Raised when a non-loopback network attempt is blocked."""


def _is_loopback_host(host: str | None) -> bool:
    if host is None:
        return False
    normalized = host.strip().lower().strip("[]")
    if normalized in {"localhost", "127.0.0.1", "::1", "0.0.0.0", "::"}:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _guarded_getaddrinfo(
    host: str | bytes | None,
    port: Any,
    family: int = 0,
    type: int = 0,
    proto: int = 0,
    flags: int = 0,
) -> list[tuple[Any, ...]]:
    host_s = host.decode() if isinstance(host, bytes) else host
    if host_s is not None and not _is_loopback_host(str(host_s)):
        raise NetworkGuardError(
            f"network guard blocked DNS/resolve for non-loopback host: {host_s!r}"
        )
    return _orig_getaddrinfo(host, port, family, type, proto, flags)


def _guarded_connect(self: socket.socket, address: Any) -> None:
    host: str | None
    if isinstance(address, tuple) and address:
        host = str(address[0])
    else:
        host = None
    if host is not None and not _is_loopback_host(host):
        raise NetworkGuardError(
            f"network guard blocked connect to non-loopback address: {address!r}"
        )
    return _orig_connect(self, address)


def install_network_guard() -> None:
    """Patch socket connect/getaddrinfo to allow only loopback."""
    global _installed
    if _installed:
        return
    socket.getaddrinfo = _guarded_getaddrinfo  # type: ignore[assignment]
    socket.socket.connect = _guarded_connect  # type: ignore[method-assign, assignment]
    _installed = True


def uninstall_network_guard() -> None:
    """Restore original socket connect/getaddrinfo."""
    global _installed
    if not _installed:
        return
    socket.getaddrinfo = _orig_getaddrinfo  # type: ignore[assignment]
    socket.socket.connect = _orig_connect  # type: ignore[method-assign, assignment]
    _installed = False


def network_guard_enabled() -> bool:
    return _installed
