from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit

from .settings import BrowserSettings


class BrowserNetworkPolicy:
    def __init__(self, settings: BrowserSettings) -> None:
        self._allowed_hosts = settings.allowed_hosts
        self._allowed_cidrs = tuple(
            ipaddress.ip_network(value, strict=False) for value in settings.allowed_cidrs
        )

    async def validate(self, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("A navigation URL is required")
        if value == "about:blank":
            return value
        uri = urlsplit(value)
        if (
            uri.scheme.lower() not in {"http", "https"}
            or uri.hostname is None
            or uri.username is not None
            or uri.password is not None
        ):
            raise PermissionError("Only HTTP(S) URLs without userinfo and about:blank are allowed")
        try:
            host = uri.hostname.encode("idna").decode("ascii").lower()
        except UnicodeError as exc:
            raise PermissionError("Navigation target could not be validated") from exc
        if host in self._allowed_hosts:
            return value
        addresses = await asyncio.to_thread(self._resolve, host)
        if not addresses:
            raise PermissionError("Navigation target could not be validated")
        for address in addresses:
            if not self._allowed(address) and not self._matches_allowlist(address):
                raise PermissionError("Navigation to a private or local address is blocked")
        return value

    async def is_allowed(self, value: str) -> bool:
        try:
            await self.validate(value)
            return True
        except (OSError, UnicodeError, ValueError, PermissionError):
            return False

    @staticmethod
    def _resolve(host: str) -> tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, ...]:
        try:
            entries = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
            return tuple(dict.fromkeys(ipaddress.ip_address(entry[4][0]) for entry in entries))
        except (OSError, ValueError) as exc:
            raise PermissionError("Navigation target could not be validated") from exc

    def _matches_allowlist(self, address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
        return any(
            address.version == network.version and address in network
            for network in self._allowed_cidrs
        )

    @staticmethod
    def _allowed(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
        return not (
            address.is_unspecified
            or address.is_loopback
            or address.is_link_local
            or address.is_private
            or address.is_multicast
        )
