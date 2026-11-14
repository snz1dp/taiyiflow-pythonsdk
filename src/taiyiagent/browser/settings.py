from __future__ import annotations

import ipaddress
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from urllib.parse import urlsplit


class BrowserMode(StrEnum):
    DISABLED = "disabled"
    HEADLESS = "headless"
    REMOTE_CDP = "remote_cdp"


@dataclass(frozen=True, slots=True)
class BrowserSettings:
    mode: BrowserMode = BrowserMode.DISABLED
    executable_path: Path | None = None
    launch_arguments: tuple[str, ...] = ()
    action_timeout: float = 15.0
    max_pages_per_session: int = 10
    max_read_text_characters: int = 8000
    max_console_messages: int = 200
    max_screenshot_bytes: int = 10 * 1024 * 1024
    execute_js_enabled: bool = False
    allowed_hosts: frozenset[str] = field(default_factory=frozenset)
    allowed_cidrs: frozenset[str] = field(default_factory=frozenset)
    remote_endpoint: str | None = field(default=None, repr=False)
    remote_headers: Mapping[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "mode", BrowserMode(self.mode))
        object.__setattr__(self, "launch_arguments", tuple(self.launch_arguments))
        object.__setattr__(
            self,
            "allowed_hosts",
            frozenset(host.encode("idna").decode("ascii").lower() for host in self.allowed_hosts),
        )
        for cidr in self.allowed_cidrs:
            try:
                ipaddress.ip_network(cidr, strict=False)
            except ValueError as exc:
                raise ValueError("Invalid CIDR allowlist entry") from exc
        headers = dict(self.remote_headers)
        for name, value in headers.items():
            if not name.strip() or "\r" in name or "\n" in name or "\r" in value or "\n" in value:
                raise ValueError("Invalid remote header name or value")
        object.__setattr__(self, "remote_headers", MappingProxyType(headers))
        if not 0 < self.action_timeout <= 120:
            raise ValueError("Browser action timeout must be between 1ms and 2 minutes")
        if not 1 <= self.max_pages_per_session <= 50:
            raise ValueError("Browser resource limits are outside supported bounds")
        if not 1 <= self.max_read_text_characters <= 100_000:
            raise ValueError("Browser resource limits are outside supported bounds")
        if not 1 <= self.max_console_messages <= 200:
            raise ValueError("Browser resource limits are outside supported bounds")
        if not 1024 <= self.max_screenshot_bytes <= 20 * 1024 * 1024:
            raise ValueError("Browser resource limits are outside supported bounds")
        if self.mode is BrowserMode.REMOTE_CDP:
            self._validate_remote_endpoint()
        elif self.remote_endpoint is not None or headers:
            raise ValueError("Remote CDP settings require REMOTE_CDP mode")

    @classmethod
    def disabled(cls) -> BrowserSettings:
        return cls()

    def redacted_remote_host(self) -> str:
        if self.remote_endpoint is None:
            return ""
        uri = urlsplit(self.remote_endpoint)
        try:
            port = f":{uri.port}" if uri.port is not None else ""
        except ValueError:
            port = ""
        return f"{uri.scheme}://{uri.hostname or 'invalid'}{port}"

    def _validate_remote_endpoint(self) -> None:
        if self.remote_endpoint is None or not self.remote_endpoint.strip():
            raise ValueError("A remote CDP endpoint is required")
        uri = urlsplit(self.remote_endpoint)
        if (
            uri.scheme.lower() not in {"http", "https", "ws", "wss"}
            or uri.hostname is None
            or uri.username is not None
            or uri.password is not None
        ):
            raise ValueError("Remote CDP endpoint must be HTTP(S)/WS(S) without userinfo")
