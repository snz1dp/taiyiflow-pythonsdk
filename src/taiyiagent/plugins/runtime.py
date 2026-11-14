from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Protocol

from ..errors import redact
from ..models import ToolCall, ToolResult
from ..transport.capabilities import ScheduleClient
from ..transport.config import FlowMetadata
from ..transport.rest import RestClient
from .definitions import tool_definition
from .manager import PluginManager, ToolExecutionContext

_SCHEDULE_FIELDS = frozenset(
    {
        "name",
        "description",
        "schedule_type",
        "run_at",
        "interval_seconds",
        "cron_expr",
        "prompt",
        "request_mode",
        "working_directory",
        "session_mode",
        "session_id",
        "group_id",
        "timezone",
        "effective_start_at",
        "effective_end_at",
        "timeout_seconds",
        "mcp_server_ids",
        "skill_names",
    }
)
_SCHEDULE_PROPERTIES: dict[str, Any] = {
    "name": {"type": "string"},
    "description": {"type": "string"},
    "schedule_type": {"type": "string", "enum": ["once", "interval", "cron"]},
    "run_at": {"type": "string"},
    "interval_seconds": {"type": "integer", "minimum": 60},
    "cron_expr": {"type": "string"},
    "prompt": {"type": "string"},
    "request_mode": {"type": "string", "enum": ["agent", "ask", "plan", "command"]},
    "working_directory": {"type": "string"},
    "session_mode": {"type": "string", "enum": ["always_new", "new_once", "existing"]},
    "session_id": {"type": "string"},
    "group_id": {"type": "string"},
    "timezone": {"type": "string"},
    "effective_start_at": {"type": "string"},
    "effective_end_at": {"type": "string"},
    "timeout_seconds": {"type": "integer", "minimum": 1},
    "mcp_server_ids": {"type": "array", "items": {"type": "string"}},
    "skill_names": {"type": "array", "items": {"type": "string"}},
}


class ScheduleTools:
    def __init__(self, rest: RestClient) -> None:
        self.schedules = ScheduleClient(rest)

    async def list(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        return await self._run(
            call,
            lambda: self.schedules.list(
                filters={
                    "offset": 0,
                    "limit": 100,
                    "is_template": call.arguments_dict.get("is_template") is True,
                    "flow_id": _flow_id(context),
                }
            ),
        )

    async def create(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            _validate_schedule(call.arguments_dict)
            payload = _schedule_payload(call.arguments_dict)
            payload["flow_id"] = _flow_id(context)
        except Exception as exc:
            return ToolResult.fail(call.id, redact(exc))
        return await self._run(call, lambda: self.schedules.create(payload))

    async def update(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            schedule_id = _required(call.arguments_dict, "schedule_id")
            payload = _schedule_payload(call.arguments_dict)
            if "schedule_type" in payload:
                _validate_schedule(payload)
            if not payload:
                raise ValueError("No schedule fields were supplied")
            payload["flow_id"] = _flow_id(context)
        except Exception as exc:
            return ToolResult.fail(call.id, redact(exc))
        return await self._run(call, lambda: self.schedules.update(schedule_id, payload))

    async def delete(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            schedule_id = _required(call.arguments_dict, "schedule_id")
            await self.schedules.delete(schedule_id)
            return ToolResult.ok(call.id, {"schedule_id": schedule_id, "deleted": True})
        except Exception as exc:
            return ToolResult.fail(call.id, redact(exc))

    async def toggle(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            schedule_id = _required(call.arguments_dict, "schedule_id")
            disabled = call.arguments_dict.get("disabled")
            if not isinstance(disabled, bool):
                raise ValueError("disabled must be boolean")
            operation = self.schedules.disable if disabled else self.schedules.enable
            result = await operation(schedule_id)
            if result is None:
                result = {"schedule_id": schedule_id, "disabled": disabled}
            return ToolResult.ok(call.id, result)
        except Exception as exc:
            return ToolResult.fail(call.id, redact(exc))

    async def runs(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            schedule_id = _required(call.arguments_dict, "schedule_id")
            limit = call.arguments_dict.get("limit", 10)
            if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
                raise ValueError("limit must be between 1 and 100")
        except Exception as exc:
            return ToolResult.fail(call.id, redact(exc))
        return await self._run(
            call, lambda: self.schedules.runs(schedule_id, offset=0, limit=limit)
        )

    @staticmethod
    async def _run(call: ToolCall, operation: Callable[[], Awaitable[Any]]) -> ToolResult:
        try:
            return ToolResult.ok(call.id, await operation())
        except Exception as exc:
            return ToolResult.fail(call.id, redact(exc))

    @staticmethod
    def register(manager: PluginManager, rest: RestClient) -> ScheduleTools:
        plugin = ScheduleTools(rest)
        entries = (
            (
                "schedule_list",
                "List platform schedules visible to the current user.",
                {"is_template": {"type": "boolean"}},
                (),
                plugin.list,
            ),
            (
                "schedule_create",
                "Create a platform schedule.",
                _SCHEDULE_PROPERTIES,
                ("name", "schedule_type", "prompt"),
                plugin.create,
            ),
            (
                "schedule_update",
                "Update selected fields of a platform schedule.",
                {"schedule_id": {"type": "string"}, **_SCHEDULE_PROPERTIES},
                ("schedule_id",),
                plugin.update,
            ),
            (
                "schedule_delete",
                "Delete a platform schedule.",
                {"schedule_id": {"type": "string"}},
                ("schedule_id",),
                plugin.delete,
            ),
            (
                "schedule_toggle",
                "Enable or disable a platform schedule.",
                {"schedule_id": {"type": "string"}, "disabled": {"type": "boolean"}},
                ("schedule_id", "disabled"),
                plugin.toggle,
            ),
            (
                "schedule_runs",
                "List execution records for a schedule.",
                {
                    "schedule_id": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                },
                ("schedule_id",),
                plugin.runs,
            ),
        )
        for name, description, properties, required, handler in entries:
            manager.register(
                name,
                handler,
                definition=tool_definition(name, description, properties, required=required),
            )
        return plugin


class SessionContextPlugin:
    def __init__(self, metadata_provider: Callable[[], Awaitable[FlowMetadata]]) -> None:
        self.metadata_provider = metadata_provider

    async def read(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            metadata = await self.metadata_provider()
            return ToolResult.ok(
                call.id,
                {
                    "flow_id": context.flow_id,
                    "session_id": context.session_id,
                    "run_id": context.run_id,
                    "task_id": context.task_id,
                    "message_id": context.message_id,
                    "request_mode": context.request_mode,
                    "capabilities": deepcopy(dict(context.capabilities or {})),
                    "allowed_tools": sorted(context.allowed_tools or ()),
                    "flow_metadata": metadata.raw,
                },
            )
        except Exception as exc:
            return ToolResult.fail(call.id, redact(exc))

    @staticmethod
    def register(
        manager: PluginManager, metadata_provider: Callable[[], Awaitable[FlowMetadata]]
    ) -> SessionContextPlugin:
        plugin = SessionContextPlugin(metadata_provider)
        manager.register(
            "session_context",
            plugin.read,
            definition=tool_definition(
                "session_context",
                "Read trusted session identity, subtask capabilities, and current Flow metadata.",
                {},
            ),
        )
        return plugin


class TodoPlugin:
    def __init__(self) -> None:
        self._sessions: dict[str, list[dict[str, Any]]] = {}

    async def write(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            if not context.session_id.strip():
                raise ValueError("sessionId is required")
            items = call.arguments_dict.get("items")
            if not isinstance(items, list) or len(items) > 100:
                raise ValueError("items must be an array with at most 100 entries")
            normalized: list[dict[str, Any]] = []
            characters = 0
            for item in items:
                if not isinstance(item, dict):
                    raise ValueError("Each Todo item must be an object")
                title = item.get("title", "")
                notes = item.get("notes", "")
                status = item.get("status", "not_started")
                if not isinstance(title, str) or not 1 <= len(title.strip()) <= 500:
                    raise ValueError("Each Todo title must contain 1 to 500 characters")
                if not isinstance(notes, str) or len(notes.strip()) > 4000:
                    raise ValueError("Todo notes exceed the length limit")
                if status not in {"not_started", "in_progress", "completed"}:
                    raise ValueError("Unsupported Todo status")
                characters += len(title.strip()) + len(notes.strip())
                if characters > 100_000:
                    raise ValueError("Todo memory limit exceeded")
                normalized.append(
                    {"title": title.strip(), "status": status, "notes": notes.strip()}
                )
            key = f"{context.flow_id or ''}\0{context.session_id}"
            if key not in self._sessions and len(self._sessions) >= 100:
                raise ValueError("Todo session limit exceeded")
            self._sessions[key] = normalized
            return ToolResult.ok(
                call.id,
                {"items": deepcopy(normalized), "count": len(normalized), "transient": True},
            )
        except Exception as exc:
            return ToolResult.fail(call.id, redact(exc))

    def close_session(self, session_id: str) -> None:
        suffix = f"\0{session_id}"
        for key in [key for key in self._sessions if key.endswith(suffix)]:
            del self._sessions[key]

    async def aclose(self) -> None:
        self._sessions.clear()

    @staticmethod
    def register(manager: PluginManager) -> TodoPlugin:
        plugin = TodoPlugin()
        manager.register(
            "todo_write",
            plugin.write,
            definition=tool_definition(
                "todo_write",
                "Replace the transient in-memory Todo list for this session.",
                {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "title": {"type": "string"},
                                "status": {
                                    "type": "string",
                                    "enum": ["not_started", "in_progress", "completed"],
                                },
                                "notes": {"type": "string"},
                            },
                            "required": ["title"],
                        },
                    }
                },
                required=("items",),
            ),
        )
        manager.register_session_cleanup(plugin.close_session)
        manager.register_closeable(plugin)
        return plugin


@dataclass(frozen=True, slots=True)
class InteractionQuestion:
    id: str
    prompt: str
    options: tuple[str, ...] = ()
    multi_select: bool = False


@dataclass(frozen=True, slots=True)
class InteractionImage:
    media_type: str
    base64_data: str


@dataclass(frozen=True, slots=True)
class UserInteractionRequest:
    flow_id: str | None
    session_id: str
    run_id: str | None
    task_id: str | None
    questions: tuple[InteractionQuestion, ...]
    images: tuple[InteractionImage, ...]
    timeout_seconds: int


@dataclass(frozen=True, slots=True)
class UserInteractionResponse:
    answers: Mapping[str, tuple[str, ...]]
    cancelled: bool = False


class UserInteractionHandler(Protocol):
    def ask(
        self, request: UserInteractionRequest, cancellation_requested: Callable[[], bool]
    ) -> Awaitable[UserInteractionResponse]: ...


class AskUserPlugin:
    def __init__(self, handler: UserInteractionHandler) -> None:
        self.handler = handler

    async def ask(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        pending: asyncio.Future[UserInteractionResponse] | None = None
        monitor: asyncio.Task[bool] | None = None
        try:
            questions, images, timeout = _parse_interaction(call.arguments_dict)
            request = UserInteractionRequest(
                context.flow_id,
                context.session_id,
                context.run_id,
                context.task_id,
                questions,
                images,
                timeout,
            )
            cancelled = context.cancellation_requested or (lambda: False)
            pending = asyncio.ensure_future(self.handler.ask(request, cancelled))
            monitor = asyncio.create_task(_watch_cancellation(cancelled))
            done, _ = await asyncio.wait(
                {pending, monitor}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if pending not in done:
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
                if monitor in done and monitor.result():
                    return ToolResult.fail(call.id, "User interaction cancelled")
                return ToolResult.fail(call.id, "User interaction timed out")
            try:
                response = pending.result()
            except asyncio.CancelledError:
                return ToolResult.fail(call.id, "User interaction cancelled")
            answers = _validate_answers(response, questions)
            return ToolResult.ok(call.id, {"cancelled": response.cancelled, "answers": answers})
        except asyncio.CancelledError:
            if pending is not None:
                pending.cancel()
            raise
        except Exception as exc:
            return ToolResult.fail(call.id, redact(exc))
        finally:
            if monitor is not None:
                monitor.cancel()
            if pending is not None and not pending.done():
                pending.cancel()

    @staticmethod
    def register(manager: PluginManager, handler: UserInteractionHandler | None) -> None:
        if handler is None:
            return
        plugin = AskUserPlugin(handler)
        manager.register(
            "ask_user",
            plugin.ask,
            definition=tool_definition(
                "ask_user",
                "Ask structured questions through the injected user interaction handler.",
                {
                    "questions": {"type": "array", "items": {"type": "object"}},
                    "images": {"type": "array", "items": {"type": "object"}},
                    "timeout_seconds": {"type": "integer"},
                },
                required=("questions",),
            ),
        )


async def _watch_cancellation(cancelled: Callable[[], bool]) -> bool:
    wakeup = asyncio.Event()
    while not cancelled():
        try:
            await asyncio.wait_for(wakeup.wait(), 0.05)
        except TimeoutError:
            continue
    return True


def _parse_interaction(
    arguments: Mapping[str, Any],
) -> tuple[tuple[InteractionQuestion, ...], tuple[InteractionImage, ...], int]:
    raw_questions = arguments.get("questions")
    if not isinstance(raw_questions, list) or not 1 <= len(raw_questions) <= 10:
        raise ValueError("ask_user requires 1 to 10 questions")
    questions: list[InteractionQuestion] = []
    identifiers: set[str] = set()
    for item in raw_questions:
        if not isinstance(item, dict):
            raise ValueError("Invalid question")
        identifier = item.get("id")
        prompt = item.get("prompt")
        if (
            not isinstance(identifier, str)
            or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", identifier) is None
            or identifier in identifiers
            or not isinstance(prompt, str)
            or not 1 <= len(prompt.strip()) <= 4000
        ):
            raise ValueError("Invalid or duplicate question")
        identifiers.add(identifier)
        raw_options = item.get("options", [])
        if not isinstance(raw_options, list) or len(raw_options) > 20:
            raise ValueError("Too many question options")
        if any(not isinstance(value, str) or len(value) > 500 for value in raw_options):
            raise ValueError("Question option is too long")
        questions.append(
            InteractionQuestion(
                identifier, prompt.strip(), tuple(raw_options), item.get("multi_select") is True
            )
        )
    raw_images = arguments.get("images", [])
    if not isinstance(raw_images, list) or len(raw_images) > 4:
        raise ValueError("Too many interaction images")
    images: list[InteractionImage] = []
    image_characters = 0
    for item in raw_images:
        if not isinstance(item, dict):
            raise ValueError("Unsupported interaction image")
        media_type = item.get("media_type")
        data = item.get("base64_data")
        if (
            not isinstance(media_type, str)
            or re.fullmatch(r"image/(png|jpeg|webp)", media_type) is None
            or not isinstance(data, str)
            or not data.strip()
        ):
            raise ValueError("Unsupported interaction image")
        image_characters += len(data)
        if image_characters > 2_000_000:
            raise ValueError("Interaction images are too large")
        images.append(InteractionImage(media_type, data))
    timeout = arguments.get("timeout_seconds", 300)
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= 1800:
        raise ValueError("Invalid interaction timeout")
    return tuple(questions), tuple(images), timeout


def _validate_answers(
    response: UserInteractionResponse, questions: tuple[InteractionQuestion, ...]
) -> dict[str, list[str]]:
    valid_ids = {question.id for question in questions}
    answers: dict[str, list[str]] = {}
    characters = 0
    for identifier, values in response.answers.items():
        if identifier not in valid_ids or len(values) > 20:
            raise ValueError(
                "Interaction response contains an unknown question or too many answers"
            )
        normalized: list[str] = []
        for value in values:
            if not isinstance(value, str) or len(value) > 4000:
                raise ValueError("Interaction answer exceeds the length limit")
            characters += len(value)
            if characters > 64_000:
                raise ValueError("Interaction response is too large")
            normalized.append(value)
        answers[identifier] = normalized
    return answers


def _schedule_payload(arguments: Mapping[str, Any]) -> dict[str, Any]:
    return {key: deepcopy(arguments[key]) for key in _SCHEDULE_FIELDS if key in arguments}


def _validate_schedule(arguments: Mapping[str, Any]) -> None:
    schedule_type = _required(arguments, "schedule_type")
    if schedule_type == "once":
        _required(arguments, "run_at")
    elif schedule_type == "interval":
        interval = arguments.get("interval_seconds")
        if isinstance(interval, bool) or not isinstance(interval, int) or interval < 60:
            raise ValueError("interval_seconds must be at least 60")
    elif schedule_type == "cron":
        if len(_required(arguments, "cron_expr").split()) != 5:
            raise ValueError("cron_expr must contain five fields")
    else:
        raise ValueError("schedule_type must be once, interval, or cron")


def _required(arguments: Mapping[str, Any], name: str) -> str:
    value = arguments.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")
    return value.strip()


def _flow_id(context: ToolExecutionContext) -> str:
    if context.flow_id is None or not context.flow_id.strip():
        raise ValueError("Schedule tools require a trusted flowId")
    return context.flow_id
