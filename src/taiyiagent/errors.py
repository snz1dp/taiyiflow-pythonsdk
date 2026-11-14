from __future__ import annotations

import re
from typing import Any

_SENSITIVE = re.compile(
    r"(?i)(__TOKEN__=|authorization\s*[:=]\s*bearer\s+|api[_-]?key\s*[:=]\s*)[^\s&;,]+"
)


def redact(value: object) -> str:
    return _SENSITIVE.sub(lambda match: match.group(1) + "***", str(value))


class TaiyiFlowError(Exception):
    def __init__(self, message: str = "") -> None:
        super().__init__(redact(message))


class ConfigurationError(TaiyiFlowError, ValueError):
    pass


class TransportError(TaiyiFlowError):
    pass


class ProtocolError(TaiyiFlowError):
    pass


class AuthenticationError(TransportError):
    pass


class ApiError(TransportError):
    def __init__(
        self,
        status_code: int,
        detail: Any = None,
        *,
        method: str | None = None,
        path: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.detail = detail
        self.method = method
        self.path = path
        location = f" for {method} {path}" if method and path else ""
        super().__init__(f"REST error {status_code}{location}")

    @property
    def detail_code(self) -> str | None:
        if isinstance(self.detail, str):
            return self.detail
        if isinstance(self.detail, dict):
            code = self.detail.get("code", self.detail.get("detail"))
            return code if isinstance(code, str) else None
        return None


class SessionClosedError(TaiyiFlowError):
    pass


class SessionConflictError(TaiyiFlowError):
    pass


class ToolExecutionError(TaiyiFlowError):
    pass


class ApprovalTimeoutError(ToolExecutionError):
    pass


class RecoveryError(TaiyiFlowError):
    pass


class OutboxError(RecoveryError):
    pass


class ReportConflictError(RecoveryError):
    pass
