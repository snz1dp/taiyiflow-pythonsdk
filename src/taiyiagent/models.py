from __future__ import annotations

from copy import deepcopy
from enum import IntEnum, StrEnum
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field


class WireModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    def to_wire(self) -> dict[str, Any]:
        return self.model_dump(mode="json", by_alias=True, exclude_none=True)

    @classmethod
    def from_wire(cls, value: dict[str, Any]) -> Self:
        return cls.model_validate(value)


class EventType(StrEnum):
    MESSAGE = "message"
    HEARTBEAT = "heartbeat"
    ERROR = "error"
    CLOSE = "close"
    CUSTOM = "custom"
    BYTES = "bytes"
    TOOL_CALL = "tool_call"
    FILE_WRITTEN = "file_written"
    CLIENT_TOOL = "client_tool"
    MCP_TOOL = "mcp_tool"
    BUILT_IN = "built_in"


class CloseCode(IntEnum):
    NORMAL = 1000
    INVALID_PATH = 4000
    AUTH_FAILED = 4001
    INVALID_FIRST_FRAME = 4002
    PERMISSION_DENIED = 4003
    FLOW_UNAVAILABLE = 4004
    SESSION_CONFLICT = 4009

    @property
    def description(self) -> str:
        return CLOSE_DESCRIPTIONS[int(self)]

    @property
    def is_error(self) -> bool:
        return self >= 4000


CLOSE_DESCRIPTIONS = {
    1000: "Normal closure",
    4000: "Invalid WebSocket endpoint path",
    4001: "Authentication failed",
    4002: "First frame timeout or invalid request",
    4003: "Permission, quota, or model key denied",
    4004: "Flow not found or unavailable",
    4009: "Session execution conflict",
}


def describe_close(code: int, reason: str | None = None) -> str:
    description = CLOSE_DESCRIPTIONS.get(code, f"Unknown close code: {code}")
    return f"{description}: {reason}" if reason else description


class StreamProtocol(StrEnum):
    AUTO = "auto"
    SSE = "sse"
    WEBSOCKET = "websocket"


class RecoveryPolicy(StrEnum):
    MANUAL = "manual"
    AUTO = "auto"


class SessionState(StrEnum):
    IDLE = "idle"
    CONNECTING = "connecting"
    STREAMING = "streaming"
    TOOL_CALLING = "tool_calling"
    PAUSING = "pausing"
    PAUSED = "paused"
    RESUMING = "resuming"
    CANCELLED = "cancelled"
    DISCONNECTED = "disconnected"
    RESTARTING = "restarting"
    DONE = "done"
    INTERRUPTED = "interrupted"
    ERROR = "error"


class StreamEvent(WireModel):
    event: str
    data: Any = None

    def is_type(self, event_type: EventType) -> bool:
        return self.event == event_type.value

    @property
    def is_message(self) -> bool:
        return self.is_type(EventType.MESSAGE)

    @property
    def is_heartbeat(self) -> bool:
        return self.is_type(EventType.HEARTBEAT)

    @property
    def is_error(self) -> bool:
        return self.is_type(EventType.ERROR)

    @property
    def is_close(self) -> bool:
        return self.is_type(EventType.CLOSE)

    @property
    def is_tool(self) -> bool:
        return self.event in {
            EventType.TOOL_CALL,
            EventType.CLIENT_TOOL,
            EventType.MCP_TOOL,
            EventType.BUILT_IN,
        }

    @property
    def message_text(self) -> str | None:
        if isinstance(self.data, dict):
            chunk = self.data.get("chunk")
            if isinstance(chunk, str):
                return chunk
        return None

    @property
    def error_message(self) -> str | None:
        if not isinstance(self.data, dict):
            return None
        error = self.data.get("error")
        if error is None:
            return None
        return error if isinstance(error, str) else str(error)


class ChatRequest(WireModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    input_value: str | None = None
    session_id: str | None = None
    tweaks: dict[str, Any] | None = None
    run_id: str | None = None
    execution_mode: Literal["start", "attach", "resume"] | None = None
    execution_attempt_id: str | None = None
    after_event_id: str | None = None
    subscription_id: str | None = None
    checkpoint_id: str | None = None
    checkpoint_version: int | None = Field(default=None, ge=1)
    tool_results: list[dict[str, Any]] | None = None
    submission_id: str | None = None
    message_id: str | None = None
    input_files: list[str] | None = Field(default=None, alias="files")
    request_type: Literal["ask", "agent", "plan"] | None = None
    parent_id: str | None = None
    reasoning_enabled: bool | None = Field(default=None, alias="reasoning")
    internet_search: bool | None = None
    knowledge_scopes: list[str] | str | None = None
    knowledge_docs: list[str] | str | None = None
    requested_skill_names: list[str] | None = None
    available_tools: list[Any] | None = None
    client_mcp_servers: dict[str, Any] | str | None = None
    client_id: str | None = None
    device_id: str | None = None
    client_type: Literal["web", "device", "cli", "desktop"] | None = None
    input_type: str | None = None
    output_type: str | None = None
    output_component: str | None = None
    async_: bool | None = Field(default=None, alias="async")
    long_term_memory_enabled: bool | None = None
    cross_session_memory_enabled: bool | None = None
    relation_sessions: list[str] | None = None
    client_context_dir: str | None = None
    current_context_dir: str | None = None
    client_ip: str | None = None
    is_voice_dialogue: bool | None = None
    agent_auto_loop_enabled: bool | None = None
    agent_auto_loop_max_rounds: int | None = Field(default=None, ge=0)
    image_generation_enabled: bool | None = None
    runtime_strategy: Literal["default", "hermes"] | None = None
    command_execution_target: Literal["server", "client"] | None = None
    max_iterations: int | None = Field(default=None, gt=0)
    agent_continue_windows: int | None = Field(default=None, ge=0)
    agent_app_id: str | None = None
    agent_app_owner_id: str | None = None
    agent_inputs: dict[str, Any] | None = None
    group_id: str | None = None
    master_flow_id: str | None = None
    client_skill_packages: dict[str, Any] | list[dict[str, Any]] | str | None = None
    client_instructions: dict[str, Any] | str | None = None
    model_key: str | None = Field(default=None, alias="model")


class ToolCall(WireModel):
    id: str = ""
    name: str
    arguments: Any = None
    type: str | None = None
    flow_id: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    message_id: str | None = None

    @property
    def arguments_dict(self) -> dict[str, Any]:
        if self.arguments is None:
            return {}
        if isinstance(self.arguments, dict):
            return self.arguments
        raise ValueError("Tool arguments must be an object")

    @classmethod
    def from_event(
        cls,
        event: StreamEvent,
        *,
        default_flow_id: str,
        default_session_id: str,
    ) -> Self:
        if not isinstance(event.data, dict):
            raise ValueError("Tool event data must be an object")
        function = event.data.get("function") or event.data.get("command")
        if not isinstance(function, dict):
            raise ValueError("Tool event is missing function or command")
        context = event.data.get("context")
        if not isinstance(context, dict):
            context = {}
        default_name = "shell_exec" if event.event == EventType.BUILT_IN else "unknown"
        return cls(
            id=str(function.get("id") or ""),
            name=str(function.get("name") or default_name),
            arguments=function.get("arguments"),
            type=event.event,
            flow_id=str(event.data.get("flow_id") or default_flow_id),
            session_id=str(context.get("session_id") or default_session_id),
            run_id=context.get("run_id"),
            message_id=context.get("message_id"),
        )


class ToolResult(WireModel):
    tool_call_id: str
    success: bool
    result: Any = None
    error: str | None = None

    @classmethod
    def ok(cls, tool_call_id: str, result: Any = None) -> Self:
        return cls(tool_call_id=tool_call_id, success=True, result=result)

    @classmethod
    def fail(cls, tool_call_id: str, error: str) -> Self:
        return cls(tool_call_id=tool_call_id, success=False, error=error)


class AgentLoopControlResult(WireModel):
    session_id: str
    run_id: str
    status: str
    request_id: str
    checkpoint_id: str | None = None
    subtask_cancel_requested: int = 0
    subtask_cancel_pending: int = 0
    subtask_cancel_failed: bool = False


class ResumableCheckpoint(WireModel):
    id: str
    flow_id: str
    session_id: str
    run_id: str
    origin_client_id: str | None = None
    origin_device_id: str | None = None
    status: str
    version: int
    resume_stage: str | None = None
    pause_reason: str | None = None
    requires_user_resume: bool = False
    waiting_call_ids: list[str] = Field(default_factory=list)
    pending_call_ids: list[str] = Field(default_factory=list)
    uncertain_call_ids: list[str] = Field(default_factory=list)
    last_submission_id: str | None = None
    last_submission_hash: str | None = None
    updated_at: str | None = None
    expires_at: str | None = None

    @property
    def has_uncertain_side_effects(self) -> bool:
        return bool(self.pending_call_ids or self.uncertain_call_ids)


class RunContext(WireModel):
    run_id: str | None = None
    checkpoint_id: str | None = None
    checkpoint_version: int | None = None
    after_event_id: str | None = None
    execution_attempt_id: str | None = None
    last_input_value: str | None = None
    last_tweaks: dict[str, Any] = Field(default_factory=dict)
    last_input_files: list[str] = Field(default_factory=list)
    request_type: Literal["ask", "agent", "plan"] | None = None
    recovery_generation: int = 0
    persisted: bool = False
    last_request: ChatRequest | None = None
    available_tools: list[Any] | None = None
    client_skill_packages: list[dict[str, Any]] = Field(default_factory=list)
    requested_skill_names: list[str] = Field(default_factory=list)
    skill_diagnostics: list[str] = Field(default_factory=list)

    def begin(self, request: ChatRequest) -> None:
        self.run_id = request.run_id
        self.checkpoint_id = request.checkpoint_id
        self.checkpoint_version = request.checkpoint_version
        self.after_event_id = request.after_event_id
        self.execution_attempt_id = request.execution_attempt_id
        self.last_input_value = request.input_value
        self.last_tweaks = dict(request.tweaks or {})
        self.last_input_files = list(request.input_files or [])
        self.request_type = request.request_type
        self.client_skill_packages = deepcopy(
            request.client_skill_packages if isinstance(request.client_skill_packages, list) else []
        )
        self.requested_skill_names = list(request.requested_skill_names or [])
        self.skill_diagnostics = []
        self.last_request = request.model_copy(deep=True)
        self.persisted = False

    def next_recovery_generation(self) -> int:
        self.recovery_generation += 1
        return self.recovery_generation

    def update_control(self, result: AgentLoopControlResult, expected_generation: int) -> bool:
        if self.recovery_generation != expected_generation:
            return False
        self.run_id = result.run_id or self.run_id
        self.checkpoint_id = result.checkpoint_id or self.checkpoint_id
        return True

    def update_checkpoint(self, checkpoint: ResumableCheckpoint, expected_generation: int) -> bool:
        if self.recovery_generation != expected_generation:
            return False
        self.run_id = checkpoint.run_id
        self.checkpoint_id = checkpoint.id
        self.checkpoint_version = checkpoint.version
        self.persisted = True
        return True

    def adopt_execution(
        self,
        run_id: str | None,
        execution_attempt_id: str | None,
        expected_generation: int,
    ) -> bool:
        if (
            self.recovery_generation != expected_generation
            or not run_id
            or (self.run_id is not None and self.run_id != run_id)
        ):
            return False
        self.run_id = run_id
        if execution_attempt_id:
            self.execution_attempt_id = execution_attempt_id
        return True

    def update(self, event: StreamEvent) -> None:
        if not isinstance(event.data, dict):
            return
        event_run_id = _nonblank(event.data.get("run_id"))
        if self.run_id and event_run_id and self.run_id != event_run_id:
            return
        if event_run_id:
            self.run_id = event_run_id
        attempt_id = _nonblank(event.data.get("execution_attempt_id"))
        if attempt_id:
            self.execution_attempt_id = attempt_id
        event_id = _nonblank(event.data.get("event_id"))
        if event_id:
            self.after_event_id = event_id
            self.persisted = True
        checkpoint_id = _nonblank(event.data.get("checkpoint_id"))
        if checkpoint_id:
            self.checkpoint_id = checkpoint_id
        version = event.data.get("checkpoint_version")
        if isinstance(version, int) and not isinstance(version, bool):
            self.checkpoint_version = version


def _nonblank(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None
