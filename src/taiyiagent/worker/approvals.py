from __future__ import annotations

from collections.abc import Awaitable
from typing import Protocol

from ..errors import ToolExecutionError
from ..hooks import PermissionDecision, PermissionHook, PermissionPolicy, maybe_await
from ..models import ToolCall
from .config import ApprovalChannel


class ApprovalAudit(Protocol):
    def record(
        self,
        session_id: str,
        tool_call: ToolCall,
        decision: PermissionDecision,
    ) -> Awaitable[None] | None: ...


class WorkerApprovalHook(PermissionHook):
    def __init__(
        self,
        policy: PermissionPolicy,
        channel: ApprovalChannel,
        *,
        approval_handler: PermissionHook | None = None,
        approval_audit: ApprovalAudit | None = None,
    ) -> None:
        self._policy = policy
        self._channel = channel
        self._approval_handler = approval_handler
        self._approval_audit = approval_audit

    async def on_permission(self, session_id: str, tool_call: ToolCall) -> PermissionDecision:
        decision = self._policy.decide(tool_call)
        if decision is not PermissionDecision.ASK:
            return decision
        if self._approval_handler is None:
            message = (
                "Platform approval is unavailable; configure a platform-backed approval handler"
                if self._channel is ApprovalChannel.PLATFORM
                else "Local approval callback is not configured"
            )
            raise ToolExecutionError(message)
        if self._channel is ApprovalChannel.LOCAL and self._approval_audit is None:
            raise ToolExecutionError("Local approval decisions require a platform approval audit")
        result = PermissionDecision(
            await maybe_await(self._approval_handler.on_permission(session_id, tool_call))
        )
        if result is PermissionDecision.ASK:
            raise ToolExecutionError("Approval handler must return ALLOW or DENY")
        if self._channel is ApprovalChannel.LOCAL:
            assert self._approval_audit is not None
            await maybe_await(self._approval_audit.record(session_id, tool_call, result))
        return result
