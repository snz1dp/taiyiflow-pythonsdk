from __future__ import annotations

import asyncio
import copy
import json
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, TypeAlias

from ..errors import redact
from ..models import ChatRequest, ToolCall
from ..plugins import PluginManager, ToolExecutionContext
from ..transport import CollaborationTaskClient, RestClient

JsonObject: TypeAlias = dict[str, Any]
SubtaskDispatcher: TypeAlias = Callable[["SubtaskTask"], Awaitable[JsonObject]]
SubtaskCanceller: TypeAlias = Callable[["SubtaskTask"], Awaitable[None]]

_REQUEST_MODES = {"ask": 0, "plan": 1, "agent": 2}
_SUCCESS_STATES = {"success", "succeeded", "completed"}
_FAILURE_STATES = {"failed", "cancelled", "expired"}


@dataclass(frozen=True, slots=True)
class SubtaskCapabilitySnapshot:
    reasoning: bool | None = None
    internet_search: bool | None = None
    knowledge_scopes: tuple[str, ...] | str | None = None
    knowledge_docs: tuple[str, ...] | str | None = None
    requested_skill_names: tuple[str, ...] = ()
    client_skill_packages: tuple[JsonObject, ...] = ()
    client_mcp_server_names: tuple[str, ...] = ()
    allowed_tools: frozenset[str] | None = frozenset()

    @classmethod
    def from_request(cls, request: ChatRequest | None) -> SubtaskCapabilitySnapshot:
        if request is None:
            return cls()
        return cls(
            reasoning=request.reasoning_enabled,
            internet_search=request.internet_search,
            knowledge_scopes=_strings_or_text(request.knowledge_scopes),
            knowledge_docs=_strings_or_text(request.knowledge_docs),
            requested_skill_names=_string_tuple(request.requested_skill_names),
            client_skill_packages=tuple(
                copy.deepcopy(request.client_skill_packages)
                if isinstance(request.client_skill_packages, list)
                else []
            ),
            client_mcp_server_names=_mcp_names(request.client_mcp_servers),
            allowed_tools=(
                frozenset(_tool_names(request.available_tools))
                if request.available_tools is not None
                else None
            ),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> SubtaskCapabilitySnapshot:
        features = value.get("features")
        source = copy.deepcopy(dict(features)) if isinstance(features, Mapping) else {}
        mcp_names = _string_tuple(source.pop("client_mcp_server_names", None))
        for name in (
            "reasoning",
            "internet_search",
            "knowledge_scopes",
            "knowledge_docs",
            "requested_skill_names",
            "client_skill_packages",
            "available_tools",
            "client_mcp_servers",
        ):
            if name in value:
                source[name] = copy.deepcopy(value[name])
        snapshot = cls.from_request(ChatRequest.model_validate(source))
        if mcp_names:
            snapshot = cls(
                reasoning=snapshot.reasoning,
                internet_search=snapshot.internet_search,
                knowledge_scopes=snapshot.knowledge_scopes,
                knowledge_docs=snapshot.knowledge_docs,
                requested_skill_names=snapshot.requested_skill_names,
                client_skill_packages=snapshot.client_skill_packages,
                client_mcp_server_names=mcp_names,
                allowed_tools=snapshot.allowed_tools,
            )
        return snapshot

    def to_features(self) -> JsonObject:
        result: JsonObject = {
            "requested_skill_names": list(self.requested_skill_names),
            "client_mcp_server_names": list(self.client_mcp_server_names),
        }
        if self.reasoning is not None:
            result["reasoning"] = self.reasoning
        if self.internet_search is not None:
            result["internet_search"] = self.internet_search
        if self.knowledge_scopes is not None:
            result["knowledge_scopes"] = _wire_strings(self.knowledge_scopes)
        if self.knowledge_docs is not None:
            result["knowledge_docs"] = _wire_strings(self.knowledge_docs)
        return result

    def available_tools(self) -> list[str] | None:
        return sorted(self.allowed_tools) if self.allowed_tools is not None else None

    def apply(self, request: ChatRequest) -> None:
        request.reasoning_enabled = self.reasoning
        request.internet_search = self.internet_search
        request.knowledge_scopes = _wire_strings(self.knowledge_scopes)
        request.knowledge_docs = _wire_strings(self.knowledge_docs)
        request.requested_skill_names = list(self.requested_skill_names)
        request.client_skill_packages = copy.deepcopy(list(self.client_skill_packages))
        tools = self.available_tools()
        request.available_tools = tools if tools is None else [{"name": name} for name in tools]


class SubtaskMcpResolver(Protocol):
    def resolve(
        self,
        flow_id: str,
        server_names: frozenset[str],
    ) -> Awaitable[dict[str, JsonObject]] | dict[str, JsonObject]: ...


@dataclass(frozen=True, slots=True)
class SubtaskTask:
    task_id: str
    version: int
    payload: JsonObject

    @classmethod
    def from_platform(cls, value: Mapping[str, Any]) -> SubtaskTask:
        task_id = _text(value.get("id")) or _text(value.get("task_id"))
        if task_id is None:
            raise ValueError("Subtask id is required")
        version = value.get("version", 0)
        if not isinstance(version, int) or isinstance(version, bool):
            version = 0
        return cls(task_id, version, copy.deepcopy(dict(value)))

    @property
    def status(self) -> str | None:
        return _text(self.payload.get("status"))


@dataclass(slots=True)
class _SubtaskSpec:
    index: int
    prompt: str
    flow_id: str
    request_mode: str
    target_client_id: str | None
    target_device_id: str | None
    delivery_policy: str
    timeout: float
    idempotency_key: str
    child_session_id: str
    child_run_id: str


class SubtaskOrchestrator:
    def __init__(
        self,
        rest: RestClient,
        plugins: PluginManager,
        dispatcher: SubtaskDispatcher | None = None,
        *,
        canceller: SubtaskCanceller | None = None,
        local_client_id: str | None = None,
        local_device_id: str | None = None,
        max_tasks: int = 16,
        max_parallelism: int = 4,
        timeout: float = 3600.0,
        poll_interval: float = 0.25,
    ) -> None:
        if max_tasks <= 0 or max_parallelism <= 0 or timeout <= 0 or poll_interval <= 0:
            raise ValueError("Subtask limits must be positive")
        self._tasks = CollaborationTaskClient(rest)
        self.dispatcher = dispatcher
        self.canceller = canceller
        self.local_client_id = local_client_id
        self.local_device_id = local_device_id
        self.max_tasks = max_tasks
        self.max_parallelism = min(max_parallelism, max_tasks)
        self.timeout = timeout
        self.poll_interval = poll_interval
        plugins.register("run_subtask", self.execute)

    async def execute(self, context: ToolExecutionContext, call: ToolCall) -> JsonObject:
        try:
            arguments = call.arguments_dict
            specs = self._specs(context, arguments)
            parallelism = self._parallelism(arguments, len(specs))
        except ValueError as exc:
            return {"results": [], "count": 0, "successful": 0, "error": str(exc)}
        semaphore = asyncio.Semaphore(parallelism)
        created: list[SubtaskTask] = []
        runners = [
            asyncio.create_task(self._run_one(context, call, spec, created, semaphore))
            for spec in specs
        ]
        try:
            results = await asyncio.gather(*runners)
        except asyncio.CancelledError:
            await self._stop(runners, created)
            raise
        return {
            "results": sorted(results, key=lambda item: item["index"]),
            "count": len(results),
            "successful": sum(item.get("status") == "success" for item in results),
        }

    def _specs(
        self,
        context: ToolExecutionContext,
        arguments: Mapping[str, Any],
    ) -> list[_SubtaskSpec]:
        _require_wait(arguments)
        raw_tasks = arguments.get("subtasks")
        if raw_tasks is None:
            raw_tasks = [arguments]
        if not isinstance(raw_tasks, list) or not raw_tasks:
            raise ValueError("subtasks must be a non-empty array")
        if len(raw_tasks) > self.max_tasks:
            raise ValueError(f"At most {self.max_tasks} subtasks are allowed")
        parent_mode = context.request_mode or "agent"
        if parent_mode not in _REQUEST_MODES:
            raise ValueError("Parent request mode is invalid")
        specs: list[_SubtaskSpec] = []
        for index, raw in enumerate(raw_tasks):
            if isinstance(raw, str):
                raw = {"prompt": raw}
            if not isinstance(raw, dict):
                raise ValueError("Each subtask must be a prompt or object")
            _require_wait(raw)
            prompt = _text(raw.get("prompt")) or _text(raw.get("input"))
            if prompt is None:
                raise ValueError("Subtask prompt is required")
            request_mode = (
                _text(raw.get("request_mode"))
                or _text(arguments.get("request_mode"))
                or parent_mode
            )
            if request_mode not in _REQUEST_MODES:
                raise ValueError("Subtask request mode is invalid")
            if _REQUEST_MODES[request_mode] > _REQUEST_MODES[parent_mode]:
                raise ValueError("Subtask request mode cannot exceed the parent mode")
            flow_id = (
                _text(raw.get("flow_id")) or _text(arguments.get("flow_id")) or context.flow_id
            )
            if flow_id is None:
                raise ValueError("Subtask flow_id is required")
            timeout = _timeout(raw, arguments, self.timeout)
            specs.append(
                _SubtaskSpec(
                    index=index,
                    prompt=prompt,
                    flow_id=flow_id,
                    request_mode=request_mode,
                    target_client_id=_text(raw.get("target_client_id"))
                    or _text(arguments.get("target_client_id"))
                    or self.local_client_id,
                    target_device_id=_text(raw.get("target_device_id"))
                    or _text(arguments.get("target_device_id"))
                    or self.local_device_id,
                    delivery_policy=_text(raw.get("delivery_policy"))
                    or _text(arguments.get("delivery_policy"))
                    or "local_default",
                    timeout=timeout,
                    idempotency_key=uuid.uuid4().hex,
                    child_session_id=uuid.uuid4().hex,
                    child_run_id=uuid.uuid4().hex,
                )
            )
        return specs

    def _parallelism(self, arguments: Mapping[str, Any], count: int) -> int:
        if arguments.get("serial") is True:
            return 1
        value = arguments.get("max_parallelism", min(count, self.max_parallelism))
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError("max_parallelism must be positive")
        if value > self.max_parallelism:
            raise ValueError("max_parallelism exceeds configured maximum")
        return min(value, count)

    async def _run_one(
        self,
        context: ToolExecutionContext,
        call: ToolCall,
        spec: _SubtaskSpec,
        created: list[SubtaskTask],
        semaphore: asyncio.Semaphore,
    ) -> JsonObject:
        async with semaphore:
            source_run_id = context.run_id or call.run_id
            if not source_run_id:
                return _failure(spec.index, "failed", "A persisted parent run is required")
            payload = self._create_payload(context, call, spec, source_run_id)
            task: SubtaskTask | None
            try:
                response = await self._tasks.create(payload)
                if not isinstance(response, Mapping):
                    return _failure(spec.index, "unknown", "Platform returned an invalid task")
                task = SubtaskTask.from_platform(response)
            except Exception:
                task = await self._reconcile(spec, source_run_id)
                if task is None:
                    return _failure(
                        spec.index,
                        "unknown",
                        "Subtask creation outcome is unknown; manual reconciliation is required",
                    )
            local_payload = copy.deepcopy(payload)
            local_payload.update(task.payload)
            local_payload.update(
                {
                    "id": task.task_id,
                    "task_id": task.task_id,
                    "version": task.version,
                    "command_payload": payload["command_payload"],
                }
            )
            task = SubtaskTask.from_platform(local_payload)
            created.append(task)
            try:
                async with asyncio.timeout(spec.timeout):
                    if self._is_local(spec):
                        if self.dispatcher is None:
                            raise ValueError("No local subtask executor is configured")
                        result = await self.dispatcher(task)
                    else:
                        result = await self._await_remote(task)
            except TimeoutError:
                await self._cancel(task)
                return _failure(
                    spec.index, "timeout", "Subtask did not finish before its deadline", task=task
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._cancel(task)
                return _failure(spec.index, "failed", redact(exc), task=task)
            result.setdefault("index", spec.index)
            result.setdefault("task_id", task.task_id)
            return result

    def _create_payload(
        self,
        context: ToolExecutionContext,
        call: ToolCall,
        spec: _SubtaskSpec,
        source_run_id: str,
    ) -> JsonObject:
        capabilities = SubtaskCapabilitySnapshot.from_request(context.chat_request)
        command: JsonObject = {
            "input_value": spec.prompt,
            "flow_id": spec.flow_id,
            "request_mode": spec.request_mode,
            "parent_id": source_run_id,
            "features": capabilities.to_features(),
        }
        tools = capabilities.available_tools()
        if tools is not None:
            command["available_tools"] = tools
        command["client_skill_packages"] = copy.deepcopy(list(capabilities.client_skill_packages))
        payload: JsonObject = {
            "command_payload": command,
            "task_kind": "dialogue_subtask",
            "delivery_policy": spec.delivery_policy,
            "source_session_id": context.session_id,
            "source_run_id": source_run_id,
            "child_session_id": spec.child_session_id,
            "child_run_id": spec.child_run_id,
            "correlation_id": uuid.uuid4().hex,
            "idempotency_key": spec.idempotency_key,
            "batch_id": uuid.uuid4().hex,
        }
        message_id = context.message_id or call.message_id
        if message_id:
            payload["source_message_id"] = message_id
        if context.task_id:
            payload["parent_task_id"] = context.task_id
        if spec.target_client_id:
            payload["target_client_id"] = spec.target_client_id
        if spec.target_device_id:
            payload["target_device_id"] = spec.target_device_id
        return payload

    async def _reconcile(self, spec: _SubtaskSpec, source_run_id: str) -> SubtaskTask | None:
        try:
            response = await self._tasks.list(
                filters={
                    "source_run_id": source_run_id,
                    "task_kind": "dialogue_subtask",
                    "active_only": False,
                    "limit": 100,
                }
            )
        except Exception:
            return None
        for value in _task_list(response):
            if value.get("idempotency_key") == spec.idempotency_key or (
                value.get("child_session_id") == spec.child_session_id
                and value.get("child_run_id") == spec.child_run_id
            ):
                try:
                    return SubtaskTask.from_platform(value)
                except ValueError:
                    return None
        return None

    def _is_local(self, spec: _SubtaskSpec) -> bool:
        return spec.delivery_policy == "local_default" and (
            (self.local_client_id is not None and spec.target_client_id == self.local_client_id)
            or (self.local_device_id is not None and spec.target_device_id == self.local_device_id)
        )

    async def _await_remote(self, task: SubtaskTask) -> JsonObject:
        while True:
            current = await self._tasks.get(task.task_id)
            if not isinstance(current, Mapping):
                raise ValueError("Subtask status response must be an object")
            task = SubtaskTask.from_platform(current)
            if task.status in _SUCCESS_STATES:
                return {
                    "task_id": task.task_id,
                    "status": "success",
                    "result": copy.deepcopy(task.payload.get("result_payload")),
                }
            if task.status in _FAILURE_STATES:
                return {
                    "task_id": task.task_id,
                    "status": task.status or "failed",
                    "error": _text(task.payload.get("error_message")) or f"Subtask {task.status}",
                }
            await asyncio.sleep(self.poll_interval)

    async def _stop(
        self,
        runners: list[asyncio.Task[JsonObject]],
        created: list[SubtaskTask],
    ) -> None:
        for runner in runners:
            runner.cancel()
        await asyncio.gather(*runners, return_exceptions=True)
        if created:
            await asyncio.gather(*(self._cancel(task) for task in created), return_exceptions=True)

    async def _cancel(self, task: SubtaskTask) -> None:
        operations: list[Awaitable[Any]] = [
            self._tasks.cancel(
                task.task_id,
                uuid.uuid4().hex,
                expected_version=task.version or None,
            )
        ]
        if self.canceller is not None:
            operations.append(self.canceller(task))
        await asyncio.gather(*operations, return_exceptions=True)


def _task_list(response: Any) -> list[JsonObject]:
    value = response
    if isinstance(value, Mapping):
        value = value.get("items", value.get("tasks", []))
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _failure(
    index: int,
    status: str,
    error: str,
    *,
    task: SubtaskTask | None = None,
) -> JsonObject:
    return {
        "index": index,
        "task_id": task.task_id if task else None,
        "status": status,
        "error": error,
    }


def _require_wait(value: Mapping[str, Any]) -> None:
    if value.get("wait_for_completion") is False:
        raise ValueError("Detached subtasks are not supported; wait_for_completion must be true")


def _timeout(raw: Mapping[str, Any], root: Mapping[str, Any], maximum: float) -> float:
    value = raw.get("timeout_seconds", root.get("timeout_seconds", min(300.0, maximum)))
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError("timeout_seconds must be positive")
    if value > maximum:
        raise ValueError("Subtask timeout exceeds configured maximum")
    return float(value)


def _text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(dict.fromkeys(item.strip() for item in value if _text(item) is not None))


def _strings_or_text(value: Any) -> tuple[str, ...] | str | None:
    if isinstance(value, str):
        return value
    strings = _string_tuple(value)
    return strings if isinstance(value, list) else None


def _wire_strings(value: tuple[str, ...] | str | None) -> list[str] | str | None:
    return list(value) if isinstance(value, tuple) else value


def _tool_names(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    names: list[str] = []
    for item in value:
        name = _text(item.get("name")) if isinstance(item, Mapping) else _text(item)
        if name is not None:
            names.append(name)
    return tuple(names)


def _mcp_names(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return ()
    if not isinstance(value, Mapping):
        return ()
    return tuple(name for name in value if isinstance(name, str) and name.strip())
