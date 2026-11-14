from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import Any

from .models import StreamEvent, ToolCall, ToolResult


async def maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


class StreamHook:
    async def on_chunk(self, session_id: str, chunk: str) -> None:
        pass

    async def on_event(self, session_id: str, event: StreamEvent) -> None:
        pass

    async def on_error(self, session_id: str, error: BaseException) -> None:
        pass

    async def on_close(self, session_id: str, reason: str | None) -> None:
        pass


class ToolExecutionHook:
    async def before(self, session_id: str, tool_call: ToolCall) -> ToolCall | None:
        return tool_call

    async def after(self, session_id: str, tool_call: ToolCall, result: ToolResult) -> ToolResult:
        return result

    async def takeover(self, session_id: str, tool_call: ToolCall) -> ToolResult | None:
        return None


class PermissionDecision(StrEnum):
    ALLOW = "allow"
    DENY = "deny"
    ASK = "ask"


class PermissionHook:
    async def on_permission(self, session_id: str, tool_call: ToolCall) -> PermissionDecision:
        return PermissionPolicy.READONLY.decide(tool_call)


class SessionHook:
    async def on_start(self, session_id: str) -> None:
        pass

    async def on_finish(self, session_id: str) -> None:
        pass

    async def on_interrupt(self, session_id: str, reason: str) -> None:
        pass

    async def on_close(self, session_id: str) -> None:
        pass


class PermissionPolicy(StrEnum):
    AUTO = "auto"
    CONFIRM = "confirm"
    READONLY = "readonly"

    def decide(self, tool_call: ToolCall) -> PermissionDecision:
        if self is PermissionPolicy.AUTO or not self.is_mutating(tool_call.name):
            return PermissionDecision.ALLOW
        return (
            PermissionDecision.ASK if self is PermissionPolicy.CONFIRM else PermissionDecision.DENY
        )

    @staticmethod
    def is_mutating(tool_name: str) -> bool:
        readonly_prefixes = {
            "taiyiflow-in-chrome__": {"tabs_context", "read_text", "read_console"},
            "taiyiflow-computer__": {"displays_context", "windows_context", "screenshot"},
        }
        for prefix, readonly in readonly_prefixes.items():
            if tool_name.startswith(prefix):
                return tool_name.removeprefix(prefix) not in readonly
        return tool_name in MUTATING_TOOLS


MUTATING_TOOLS = {
    "shell",
    "bash",
    "execute_command",
    "shell_exec",
    "process_stop",
    "process_cleanup",
    "task_stop",
    "task_delete",
    "task_cleanup",
    "todo_write",
    "schedule_create",
    "schedule_update",
    "schedule_delete",
    "schedule_toggle",
    "skill_create",
    "skill_upload",
    "skill_delete",
    "file_edit",
    "document_unstructured",
    "write_file",
}

StreamCallback = Callable[[str, StreamEvent], Awaitable[None] | None]
