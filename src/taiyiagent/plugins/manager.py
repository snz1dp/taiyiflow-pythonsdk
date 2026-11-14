from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from ..errors import redact
from ..hooks import PermissionPolicy
from ..models import ChatRequest, ToolCall, ToolResult
from ..transport.config import FlowMetadata

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    name: str
    description: str
    input_schema: Mapping[str, Any]
    required_feature: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.description.strip():
            raise ValueError("Tool name and description are required")
        if not isinstance(self.input_schema, Mapping):
            raise ValueError("Tool input_schema must be an object")
        object.__setattr__(self, "input_schema", deepcopy(dict(self.input_schema)))

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": deepcopy(dict(self.input_schema)),
        }


@dataclass(frozen=True, slots=True)
class ToolExecutionContext:
    session_id: str
    run_id: str | None = None
    task_id: str | None = None
    request_mode: str | None = None
    flow_id: str | None = None
    message_id: str | None = None
    chat_request: ChatRequest | None = None
    allowed_tools: frozenset[str] | None = None
    workspace: Path | None = None
    cancellation_requested: Callable[[], bool] | None = None
    capabilities: Mapping[str, Any] | None = None


ToolHandler = Callable[
    [ToolExecutionContext, ToolCall], ToolResult | Any | Awaitable[ToolResult | Any]
]
SessionCleanup = Callable[[str], None | Awaitable[None]]


class CloseablePlugin(Protocol):
    async def aclose(self) -> None: ...


class PluginManager:
    def __init__(self) -> None:
        self._handlers: dict[str, ToolHandler] = {}
        self._definitions: dict[str, ToolDefinition] = {}
        self._closeables: list[CloseablePlugin] = []
        self._session_cleanups: list[SessionCleanup] = []

    def register(
        self,
        tool_name: str,
        handler: ToolHandler,
        *,
        definition: ToolDefinition | None = None,
    ) -> None:
        if not tool_name.strip():
            raise ValueError("tool_name is required")
        if definition is not None and definition.name != tool_name:
            raise ValueError("Tool definition name must match tool_name")
        self._handlers[tool_name] = handler
        if definition is None:
            self._definitions.pop(tool_name, None)
        else:
            self._definitions[tool_name] = definition

    def get_definition(self, tool_name: str) -> ToolDefinition | None:
        return self._definitions.get(tool_name)

    @property
    def definitions(self) -> tuple[ToolDefinition, ...]:
        return tuple(self._definitions.values())

    def automatic_tools(
        self,
        metadata: FlowMetadata | None,
        request_mode: str | None,
        allowed_tools: frozenset[str] | None,
        permission_policy: PermissionPolicy,
    ) -> list[dict[str, Any]]:
        return [
            definition.to_json()
            for definition in sorted(self._definitions.values(), key=lambda item: item.name)
            if _visible(definition, metadata, request_mode, allowed_tools, permission_policy)
        ]

    def explicit_tools(
        self,
        requested: list[Any] | None,
        metadata: FlowMetadata | None,
        request_mode: str | None,
        allowed_tools: frozenset[str] | None,
        permission_policy: PermissionPolicy,
    ) -> list[Any]:
        result: list[Any] = []
        for candidate in requested or []:
            if isinstance(candidate, str):
                name = candidate
            elif isinstance(candidate, dict) and isinstance(candidate.get("name"), str):
                name = candidate["name"]
            else:
                continue
            local = self.get_definition(name)
            if local is None:
                if not self.has_handler(name):
                    result.append(deepcopy(candidate))
                continue
            if _visible(local, metadata, request_mode, allowed_tools, permission_policy):
                result.append(candidate if isinstance(candidate, str) else local.to_json())
        return result

    def register_closeable(self, plugin: CloseablePlugin) -> None:
        self._closeables.append(plugin)

    def register_session_cleanup(self, cleanup: SessionCleanup) -> None:
        self._session_cleanups.append(cleanup)

    async def close_session(self, session_id: str) -> None:
        for cleanup in self._session_cleanups:
            try:
                result = cleanup(session_id)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                log.exception("Session plugin cleanup failed")

    def unregister(self, tool_name: str) -> None:
        self._handlers.pop(tool_name, None)
        self._definitions.pop(tool_name, None)

    def has_handler(self, tool_name: str) -> bool:
        return tool_name in self._handlers

    @property
    def tool_names(self) -> frozenset[str]:
        return frozenset(self._handlers)

    async def execute(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        handler = self._handlers.get(call.name)
        if handler is None:
            return ToolResult.fail(call.id, f"No handler for tool: {call.name}")
        try:
            result = handler(context, call)
            if inspect.isawaitable(result):
                result = await result
            if isinstance(result, ToolResult):
                return result
            return ToolResult.ok(call.id, result)
        except Exception as exc:
            log.exception("Tool execution failed: %s", call.name)
            return ToolResult.fail(call.id, redact(exc))

    async def aclose(self) -> None:
        for plugin in reversed(self._closeables):
            try:
                await plugin.aclose()
            except Exception:
                log.exception("Plugin cleanup failed")
        self._closeables.clear()


def _visible(
    definition: ToolDefinition,
    metadata: FlowMetadata | None,
    request_mode: str | None,
    allowed_tools: frozenset[str] | None,
    permission_policy: PermissionPolicy,
) -> bool:
    if allowed_tools is not None and definition.name not in allowed_tools:
        return False
    if definition.required_feature is not None and (
        metadata is None or not metadata.supports(definition.required_feature)
    ):
        return False
    readonly_request = request_mode in {"ask", "plan"}
    return not (readonly_request or permission_policy is PermissionPolicy.READONLY) or (
        not PermissionPolicy.is_mutating(definition.name)
    )
