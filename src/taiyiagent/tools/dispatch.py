from __future__ import annotations

import asyncio
import logging

from ..errors import redact
from ..hooks import (
    PermissionDecision,
    PermissionHook,
    PermissionPolicy,
    ToolExecutionHook,
    maybe_await,
)
from ..models import ToolCall, ToolResult
from ..plugins.manager import PluginManager, ToolExecutionContext
from .approval import Approval, ApprovalBroker

log = logging.getLogger(__name__)


class ToolDispatcher:
    def __init__(
        self,
        plugins: PluginManager,
        *,
        permission_policy: PermissionPolicy = PermissionPolicy.CONFIRM,
        permission_hook: PermissionHook | None = None,
        permission_timeout: float = 300.0,
        approval_broker: ApprovalBroker | None = None,
    ) -> None:
        if permission_timeout < 0:
            raise ValueError("permission_timeout must be non-negative")
        self.plugins = plugins
        self.permission_policy = permission_policy
        self.permission_hook = permission_hook
        self.permission_timeout = permission_timeout
        self.approvals = approval_broker or ApprovalBroker()

    async def dispatch(
        self,
        context: ToolExecutionContext,
        call: ToolCall,
        *,
        tool_hooks: list[ToolExecutionHook] | None = None,
        permission_hooks: list[PermissionHook] | None = None,
    ) -> ToolResult:
        tool_hooks = tool_hooks or []
        permission_hooks = permission_hooks or []
        if context.allowed_tools is not None and call.name not in context.allowed_tools:
            return ToolResult.fail(call.id, "Tool is not present in the capability snapshot")
        if context.request_mode in {"ask", "plan"} and PermissionPolicy.is_mutating(call.name):
            return ToolResult.fail(
                call.id, f"Mutating tools are disabled in {context.request_mode} mode"
            )

        decision = await self._check_permission(context.session_id, call, permission_hooks)
        if decision.decision is PermissionDecision.DENY:
            return ToolResult.fail(call.id, decision.reason or "Permission denied")

        current = call
        for hook in tool_hooks:
            try:
                modified = await maybe_await(hook.before(context.session_id, current))
                if modified is None:
                    return ToolResult.fail(call.id, "Tool call cancelled by before hook")
                current = modified
            except Exception as exc:
                return ToolResult.fail(call.id, f"Before hook failed: {redact(exc)}")

        result: ToolResult | None = None
        for hook in tool_hooks:
            try:
                result = await maybe_await(hook.takeover(context.session_id, current))
                if result is not None:
                    break
            except Exception as exc:
                return ToolResult.fail(call.id, f"Takeover hook failed: {redact(exc)}")
        if result is None:
            result = await self.plugins.execute(context, current)

        for hook in tool_hooks:
            try:
                modified_result = await maybe_await(hook.after(context.session_id, current, result))
                if modified_result is not None:
                    result = modified_result
            except Exception:
                log.exception("Tool after hook failed")
        return result

    async def _check_permission(
        self,
        session_id: str,
        call: ToolCall,
        hooks: list[PermissionHook],
    ) -> Approval:
        if self.permission_hook is None:
            approval = await self._apply_decision(
                session_id, call, self.permission_policy.decide(call)
            )
            if approval.decision is PermissionDecision.DENY:
                return approval
        hooks = ([self.permission_hook] if self.permission_hook is not None else []) + hooks
        for hook in hooks:
            try:
                raw_decision = maybe_await(hook.on_permission(session_id, call))
                if self.permission_timeout > 0:
                    decision = await asyncio.wait_for(raw_decision, self.permission_timeout)
                else:
                    decision = await raw_decision
            except TimeoutError:
                return Approval(PermissionDecision.DENY, "Permission check timed out")
            except Exception as exc:
                return Approval(PermissionDecision.DENY, f"Permission check failed: {redact(exc)}")
            approval = await self._apply_decision(session_id, call, decision)
            if approval.decision is PermissionDecision.DENY:
                return approval
        return Approval(PermissionDecision.ALLOW)

    async def _apply_decision(
        self, session_id: str, call: ToolCall, decision: PermissionDecision
    ) -> Approval:
        if decision is PermissionDecision.DENY:
            return Approval(PermissionDecision.DENY, "Permission denied by hook")
        if decision is PermissionDecision.ALLOW:
            return Approval(PermissionDecision.ALLOW)
        try:
            return await self.approvals.wait_for(session_id, call.id, self.permission_timeout)
        except TimeoutError:
            return Approval(PermissionDecision.DENY, "Permission check timed out")
        except Exception as exc:
            return Approval(PermissionDecision.DENY, redact(exc))
