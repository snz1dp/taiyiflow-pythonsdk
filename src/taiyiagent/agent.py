from __future__ import annotations

import asyncio
import hashlib
import os
import threading
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any, TypeVar

import httpx

from .browser import BrowserRuntime, BrowserSettings, register_browser_tools
from .errors import ApiError, ConfigurationError, ProtocolError, ToolExecutionError
from .hooks import PermissionHook, PermissionPolicy, SessionHook, StreamHook, ToolExecutionHook
from .models import (
    ChatRequest,
    EventType,
    RecoveryPolicy,
    SessionState,
    StreamEvent,
    StreamProtocol,
    ToolCall,
)
from .plugins import (
    MediaUnderstandingPlugin,
    PluginManager,
    ToolDefinition,
    ToolExecutionContext,
    ToolHandler,
    UserInteractionHandler,
    register_builtin_plugins,
)
from .session import AgentSession, SessionManager
from .skills import (
    SkillManager,
    SkillQuery,
    SkillSelection,
    SkillSupplier,
    SkillTools,
    validate_skill_package_payloads,
)
from .tools import (
    FileReportOutbox,
    InMemoryReportOutbox,
    ReportStatus,
    ToolDispatcher,
    ToolResultReporter,
)
from .transport import (
    ExecutionController,
    RestClient,
    SseStreamClient,
    StreamConnectionError,
    StreamProtocolResolver,
    WebSocketStreamClient,
)
from .transport.capabilities import (
    CollaborationTaskClient,
    ConfigClient,
    FileClient,
    FlowMetadataClient,
    McpServerClient,
    MemoryClient,
    ScheduleClient,
    SessionClient,
    SkillClient,
)
from .transport.config import FlowMetadata, ServiceConfig


class TaiyiAgent:
    def __init__(
        self,
        *,
        base_url: str,
        flow_id: str | None = None,
        api_key: str | None = None,
        access_token: Callable[[], str] | None = None,
        credential_headers: Mapping[str, str] | None = None,
        timeout: float = 30.0,
        connect_timeout: float = 10.0,
        event_queue_size: int = 256,
        max_sessions: int = 100,
        stream_protocol: StreamProtocol | str = StreamProtocol.AUTO,
        recovery_policy: RecoveryPolicy | str = RecoveryPolicy.MANUAL,
        recovery_max_attempts: int = 2,
        recovery_min_backoff: float = 0.5,
        recovery_max_backoff: float = 5.0,
        auto_restart_on_execution_loss: bool = True,
        state_dir: str | Path | None = None,
        outbox: InMemoryReportOutbox | None = None,
        permission_policy: PermissionPolicy | str = PermissionPolicy.CONFIRM,
        permission_hook: PermissionHook | None = None,
        permission_timeout: float = 300.0,
        workspace: str | Path | None = None,
        allowed_shell_commands: list[str] | tuple[str, ...] = (),
        enable_builtin_tools: bool = True,
        enable_media_tools: bool | None = None,
        user_interaction_handler: UserInteractionHandler | None = None,
        skill_supplier: SkillSupplier | None = None,
        default_skill_names: list[str] | tuple[str, ...] = (),
        skill_manager: SkillManager | None = None,
        browser_settings: BrowserSettings | None = None,
        client_id: str | None = None,
        device_id: str | None = None,
        identity_path: str | Path | None = None,
        http_client: httpx.AsyncClient | None = None,
        on_unauthorized: Callable[[], Awaitable[None]] | None = None,
        stream_hooks: list[StreamHook] | None = None,
        tool_hooks: list[ToolExecutionHook] | None = None,
        permission_hooks: list[PermissionHook] | None = None,
        session_hooks: list[SessionHook] | None = None,
    ) -> None:
        if flow_id is not None and not flow_id.strip():
            raise ConfigurationError("flow_id must not be blank")
        if max_sessions <= 0:
            raise ConfigurationError("max_sessions must be positive")
        if recovery_max_attempts <= 0:
            raise ConfigurationError("recovery_max_attempts must be positive")
        if recovery_min_backoff < 0 or recovery_max_backoff < recovery_min_backoff:
            raise ConfigurationError("Recovery backoff bounds are invalid")
        if state_dir is not None and outbox is not None:
            raise ConfigurationError("state_dir and outbox are mutually exclusive")
        self.base_url = base_url.rstrip("/")
        self.flow_id = flow_id.strip() if flow_id is not None else None
        self.workspace = Path(workspace or Path.cwd()).expanduser().absolute()
        self.connect_timeout = connect_timeout
        self.event_queue_size = event_queue_size
        self.recovery_policy = _enum_value(RecoveryPolicy, recovery_policy)
        self.recovery_max_attempts = recovery_max_attempts
        self.recovery_min_backoff = recovery_min_backoff
        self.recovery_max_backoff = recovery_max_backoff
        self.auto_restart_on_execution_loss = auto_restart_on_execution_loss
        self.permission_policy = _enum_value(PermissionPolicy, permission_policy)
        self.client_id = client_id or _load_or_create_client_id(identity_path)
        self.device_id = device_id or self.client_id
        self._credential_headers = dict(credential_headers or {})
        self._access_token = access_token or (lambda: api_key or "")
        self._stream_hooks = list(stream_hooks or [])
        self._tool_hooks = list(tool_hooks or [])
        self._permission_hooks = list(permission_hooks or [])
        self._session_hooks = list(session_hooks or [])
        self._closed = False
        self.rest = RestClient(
            self.base_url,
            api_key,
            access_token=access_token,
            timeout=timeout,
            credential_headers=credential_headers,
            client=http_client,
            on_unauthorized=on_unauthorized,
        )
        self.execution = ExecutionController(self.rest, self.client_id, self.device_id)
        self.session_api = SessionClient(self.rest)
        self.skills = SkillClient(self.rest)
        self.memories = MemoryClient(self.rest)
        self.config = ConfigClient(self.rest)
        self.flow_metadata_api = FlowMetadataClient(self.rest)
        self.files = FileClient(self.rest)
        self._service_config: ServiceConfig | None = None
        self._service_config_lock = asyncio.Lock()
        self._flow_metadata: dict[str, FlowMetadata] = {}
        self._flow_metadata_locks: dict[str, asyncio.Lock] = {}
        self._flow_metadata_guard = asyncio.Lock()
        self.schedules = ScheduleClient(self.rest)
        self.collaboration_tasks = CollaborationTaskClient(self.rest)
        self.mcp_servers = McpServerClient(self.rest)
        self.protocol = StreamProtocolResolver(
            self.rest, _enum_value(StreamProtocol, stream_protocol)
        )
        self.sessions = SessionManager(self, max_sessions)
        self.plugins = PluginManager()
        self.skill_supplier = skill_supplier
        self.default_skill_names = tuple(default_skill_names)
        media_tools_enabled = (
            enable_builtin_tools if enable_media_tools is None else enable_media_tools
        )
        if enable_builtin_tools:
            register_builtin_plugins(
                self.plugins,
                self.workspace,
                allowed_shell_commands,
                rest=self.rest,
                metadata_provider=self.get_flow_metadata,
                user_interaction_handler=user_interaction_handler,
                enable_media=media_tools_enabled,
            )
        elif media_tools_enabled:
            MediaUnderstandingPlugin.register(self.plugins, self.rest)
        self.browser_runtime: BrowserRuntime | None = register_browser_tools(
            self.plugins, self.rest, browser_settings or BrowserSettings.disabled()
        )
        if skill_supplier is not None and skill_manager is not None:
            SkillTools(skill_supplier, skill_manager, self.default_skill_names).register(
                self.plugins
            )
        self.tools = ToolDispatcher(
            self.plugins,
            permission_policy=self.permission_policy,
            permission_hook=permission_hook,
            permission_timeout=permission_timeout,
        )
        self.outbox = outbox or (
            FileReportOutbox(state_dir) if state_dir is not None else InMemoryReportOutbox()
        )
        self.tool_reporter = ToolResultReporter(self.rest, outbox=self.outbox)

    def session(self, session_id: str | None = None, **hooks: Any) -> AgentSession:
        if self._closed:
            raise ConfigurationError("Agent has been closed")
        return self.sessions.create(session_id, **hooks)

    def register_tool(
        self,
        name: str,
        handler: ToolHandler,
        *,
        definition: ToolDefinition | None = None,
    ) -> TaiyiAgent:
        self.plugins.register(name, handler, definition=definition)
        return self

    def on_stream(self, hook: StreamHook) -> TaiyiAgent:
        self._stream_hooks.append(hook)
        return self

    def on_tool(self, hook: ToolExecutionHook) -> TaiyiAgent:
        self._tool_hooks.append(hook)
        return self

    def on_permission(self, hook: PermissionHook) -> TaiyiAgent:
        self._permission_hooks.append(hook)
        return self

    def on_session(self, hook: SessionHook) -> TaiyiAgent:
        self._session_hooks.append(hook)
        return self

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.sessions.interrupt_all()
        await self.plugins.aclose()
        await self.rest.aclose()

    async def __aenter__(self) -> TaiyiAgent:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def resolve_flow_id(self) -> str:
        if self.flow_id is not None:
            return self.flow_id
        config = await self.get_service_config()
        if self.flow_id is None:
            self.flow_id = config.default_flow_id()
        return self.flow_id

    async def get_service_config(self, *, refresh: bool = False) -> ServiceConfig:
        if self._service_config is not None and not refresh:
            return self._service_config
        async with self._service_config_lock:
            if self._service_config is None or refresh:
                self._service_config = await self.config.get_service_config()
            return self._service_config

    async def get_flow_metadata(
        self, flow_id_or_name: str | None = None, *, refresh: bool = False
    ) -> FlowMetadata:
        flow_id = flow_id_or_name or await self.resolve_flow_id()
        cached = self._flow_metadata.get(flow_id)
        if cached is not None and not refresh:
            return cached
        async with self._flow_metadata_guard:
            lock = self._flow_metadata_locks.setdefault(flow_id, asyncio.Lock())
        async with lock:
            cached = self._flow_metadata.get(flow_id)
            if cached is not None and not refresh:
                return cached
            metadata = await self.flow_metadata_api.get(flow_id)
            self._flow_metadata[flow_id] = metadata
            return metadata

    def workspace_for(self, flow_id: str, session_id: str) -> Path:
        identity = f"{flow_id}\0{session_id}".encode()
        digest = bytearray(hashlib.md5(identity, usedforsecurity=False).digest())
        directory = str(uuid.UUID(bytes=bytes(digest), version=3))
        root = self.workspace.resolve()
        workspace = (root / directory).resolve()
        if workspace == root or root not in workspace.parents:
            raise ConfigurationError("Session workspace escapes the configured root")
        workspace.mkdir(parents=True, exist_ok=True)
        return workspace

    async def _prepare_tool_supply(self, session: AgentSession, request: ChatRequest) -> None:
        flow_id = await self.resolve_flow_id()
        if request.execution_mode in {"attach", "resume"}:
            request.available_tools = deepcopy(session.run_context.available_tools)
            request.client_skill_packages = deepcopy(session.run_context.client_skill_packages)
            request.requested_skill_names = list(session.run_context.requested_skill_names)
            session.run_context.last_request = request.model_copy(deep=True)
            return
        metadata = await self.get_flow_metadata(flow_id) if self.plugins.definitions else None
        allowed_tools = _request_tool_names(request)
        if request.available_tools is None:
            supply: list[Any] = self.plugins.automatic_tools(
                metadata, request.request_type, allowed_tools, self.permission_policy
            )
        else:
            supply = self.plugins.explicit_tools(
                request.available_tools,
                metadata,
                request.request_type,
                allowed_tools,
                self.permission_policy,
            )
        session.run_context.available_tools = deepcopy(supply)
        request.available_tools = supply
        if self.skill_supplier is not None and request.client_skill_packages is None:
            query = SkillQuery(
                flow_id=flow_id,
                session_id=session.session_id,
                request_mode=request.request_type,
                principal=f"{flow_id}\0{session.session_id}",
                available_tools=_request_tool_names(request),
            )
            selection = SkillSelection(
                requested_names=tuple(request.requested_skill_names or ()),
                default_names=self.default_skill_names,
            )
            snapshot = await self.skill_supplier.supply(query, selection)
            request.client_skill_packages = snapshot.as_wire()
            request.requested_skill_names = list(snapshot.requested_skill_names)
            session.run_context.skill_diagnostics = list(snapshot.diagnostics)
        if request.client_skill_packages is not None:
            if not isinstance(request.client_skill_packages, list):
                raise ValueError("client_skill_packages must be an array")
            validate_skill_package_payloads(request.client_skill_packages)
        packages = (
            request.client_skill_packages if isinstance(request.client_skill_packages, list) else []
        )
        session.run_context.client_skill_packages = deepcopy(packages)
        session.run_context.requested_skill_names = list(request.requested_skill_names or [])
        session.run_context.last_request = request.model_copy(deep=True)

    async def _open_stream(self, request: ChatRequest) -> AsyncIterator[StreamEvent]:
        session = self.sessions.get(request.session_id) if request.session_id else None
        flow_id = await self.resolve_flow_id()
        if session is not None:
            await self._prepare_tool_supply(session, request)
        protocol = await self.protocol.resolve()

        async def websocket_events() -> AsyncIterator[StreamEvent]:
            client = WebSocketStreamClient(
                self.base_url,
                self._access_token,
                flow_id,
                credential_headers=self._credential_headers,
                connect_timeout=self.connect_timeout,
            )
            async for event in client.stream(request):
                yield event

        async def sse_events() -> AsyncIterator[StreamEvent]:
            client = SseStreamClient(self.rest, flow_id)
            async for event in client.stream(
                request,
                on_session_id=lambda value: self._resolve_session_identity(session, value),
            ):
                yield event

        if protocol == StreamProtocol.SSE:
            return sse_events()
        if protocol == StreamProtocol.WEBSOCKET and not self.protocol.automatic:
            return websocket_events()

        async def automatic_events() -> AsyncIterator[StreamEvent]:
            try:
                async for event in websocket_events():
                    yield event
            except StreamConnectionError:
                async for event in sse_events():
                    yield event

        return automatic_events()

    def _resolve_session_identity(self, session: AgentSession | None, resolved_id: str) -> None:
        if session is None or not resolved_id or session.session_id == resolved_id:
            return
        previous_id = session.session_id
        self.sessions.update_id(previous_id, resolved_id, session)
        if session.run_context.last_request is not None:
            session.run_context.last_request.session_id = resolved_id

    async def _session_status(self, session_id: str) -> dict[str, Any] | None:
        flow_id = await self.resolve_flow_id()
        try:
            response = await self.session_api.get(flow_id, session_id)
        except ApiError as exc:
            if exc.status_code == 404:
                return None
            raise
        if not isinstance(response, dict):
            raise ProtocolError("Session status response must be an object")
        return response

    async def _new_session_id(self) -> str:
        return await self.session_api.create_session_id()

    async def _dispatch_tool_event(self, session: AgentSession, event: StreamEvent) -> None:
        if event.event not in {EventType.CLIENT_TOOL, EventType.MCP_TOOL, EventType.BUILT_IN}:
            return
        if session.state in {
            SessionState.PAUSING,
            SessionState.PAUSED,
            SessionState.CANCELLED,
            SessionState.INTERRUPTED,
        }:
            return
        session._tool_execution_observed = True
        session._state = SessionState.TOOL_CALLING
        flow_id = await self.resolve_flow_id()
        try:
            try:
                call = ToolCall.from_event(
                    event,
                    default_flow_id=flow_id,
                    default_session_id=session.session_id,
                )
            except ValueError as exc:
                raise ProtocolError("Invalid client tool event") from exc
            call.run_id = call.run_id or session.run_context.run_id
            context = ToolExecutionContext(
                session_id=session.session_id,
                run_id=call.run_id,
                request_mode=session.run_context.request_type,
                flow_id=flow_id,
                message_id=call.message_id,
                chat_request=session.run_context.last_request,
                allowed_tools=_request_tool_names(session.run_context.last_request),
                workspace=self.workspace_for(flow_id, session.session_id),
                cancellation_requested=lambda: (
                    session._closed
                    or session.state in {SessionState.CANCELLED, SessionState.INTERRUPTED}
                ),
                capabilities={
                    "requested_skill_names": list(session.run_context.requested_skill_names),
                    "skill_diagnostics": list(session.run_context.skill_diagnostics),
                },
            )
            result = await self.tools.dispatch(
                context,
                call,
                tool_hooks=[*self._tool_hooks, *session._tool_hooks],
                permission_hooks=[*self._permission_hooks, *session._permission_hooks],
            )
            payload = self.tool_reporter.build_payload(call, result)
            session._pending_tool_results[call.id] = payload
            outcome = await self.tool_reporter.report_payload(payload)
            if outcome.terminal:
                session._pending_tool_results.pop(call.id, None)
            if outcome.status is not ReportStatus.REPORTED:
                if not outcome.persisted:
                    session._automatic_recovery_safe = False
                await session._notify_stream_error(
                    ToolExecutionError(f"Failed to report tool result for call {call.id}")
                )
        finally:
            if session.state is SessionState.TOOL_CALLING:
                session._state = SessionState.STREAMING


def _request_tool_names(request: ChatRequest | None) -> frozenset[str] | None:
    if request is None or request.available_tools is None:
        return None
    names: set[str] = set()
    for item in request.available_tools:
        if isinstance(item, str) and item.strip():
            names.add(item)
        elif isinstance(item, Mapping):
            name = item.get("name")
            if isinstance(name, str) and name.strip():
                names.add(name)
    return frozenset(names)


EnumT = TypeVar("EnumT")


def _enum_value(enum_type: type[EnumT], value: EnumT | str) -> EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value).lower())  # type: ignore[call-arg]
    except ValueError as exc:
        raise ConfigurationError(f"Unsupported {enum_type.__name__}: {value}") from exc


_CLIENT_IDENTITY_LOCK = threading.Lock()


def _load_or_create_client_id(identity_path: str | Path | None) -> str:
    path = Path(identity_path) if identity_path else Path.home() / ".taiyiflow" / "client-id"
    try:
        with _CLIENT_IDENTITY_LOCK:
            if path.exists():
                value = path.read_text(encoding="utf-8").strip()
                if value:
                    return value
            path.parent.mkdir(parents=True, exist_ok=True)
            value = str(uuid.uuid4())
            temporary = path.with_name(f".{path.name}.{uuid.uuid4()}.tmp")
            try:
                temporary.write_text(value, encoding="utf-8")
                os.chmod(temporary, 0o600)
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
            return value
    except OSError as exc:
        raise ConfigurationError(f"Unable to persist client identity at {path}") from exc
