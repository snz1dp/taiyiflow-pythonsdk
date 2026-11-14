from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Literal, cast

from ..agent import TaiyiAgent
from ..collaboration import TaskDispatch
from ..errors import ConfigurationError, ToolExecutionError
from ..hooks import maybe_await
from ..models import ChatRequest, SessionState
from ..session import AgentSession, SessionStream
from ..transport import JsonValue
from .subtasks import SubtaskCapabilitySnapshot, SubtaskMcpResolver
from .tasks import ProgressReporter, TaskCancelledError


@dataclass
class _SessionExecution:
    session: AgentSession
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class _SessionControl:
    def __init__(self, session: AgentSession) -> None:
        self._session = session
        self._resume = asyncio.Event()
        self._valid = True

    async def pause(self) -> None:
        if self._valid:
            await self._session.pause()

    async def resume(self) -> None:
        if self._valid:
            self._resume.set()

    async def cancel(self) -> None:
        if self._valid:
            try:
                await self._session.cancel()
            finally:
                self._resume.set()

    def invalidate(self) -> None:
        self._valid = False
        self._resume.set()

    async def wait_resume(self) -> None:
        await self._resume.wait()
        self._resume.clear()


class AgentSessionTaskHandler:
    def __init__(
        self,
        agent_factory: Callable[[str], TaiyiAgent],
        *,
        on_control: Callable[[TaskDispatch, _SessionControl, bool], None] | None = None,
        mcp_resolver: SubtaskMcpResolver | None = None,
    ) -> None:
        self._agent_factory = agent_factory
        self._agents: dict[str, TaiyiAgent] = {}
        self._sessions: dict[str, _SessionExecution] = {}
        self._on_control = on_control
        self._mcp_resolver = mcp_resolver

    async def execute(self, task: TaskDispatch, progress: ProgressReporter) -> JsonValue:
        if not task.flow_id.strip() or not task.input_value.strip():
            raise ConfigurationError("Agent session task requires flow_id and input_value")
        agent = self._agents.get(task.flow_id)
        if agent is None:
            agent = self._agent_factory(task.flow_id)
            self._agents[task.flow_id] = agent
        if task.session_id:
            key = f"{task.flow_id}\0{task.session_id}"
            execution = self._sessions.get(key)
            if execution is None:
                execution = _SessionExecution(agent.session(task.session_id))
                self._sessions[key] = execution
        else:
            execution = _SessionExecution(agent.session())
        async with execution.lock:
            return await self._run(execution.session, task)

    async def _run(self, session: AgentSession, task: TaskDispatch) -> JsonValue:
        control = _SessionControl(session)
        if self._on_control is not None:
            self._on_control(task, control, True)
        text: list[str] = []
        stream: SessionStream | None = None
        try:
            request = await self._chat_request(task)
            stream = session.chat(request)
            while True:
                async for event in stream:
                    if event.message_text is not None:
                        text.append(event.message_text)
                    if event.is_error:
                        raise ToolExecutionError(
                            f"Agent session failed: {event.error_message or 'unknown error'}"
                        )
                if session.state is SessionState.PAUSED:
                    await control.wait_resume()
                    state = _current_state(session)
                    if state is SessionState.CANCELLED:
                        raise TaskCancelledError
                    stream = await session.resume()
                    continue
                if session.state in {SessionState.CANCELLED, SessionState.INTERRUPTED}:
                    raise TaskCancelledError
                if session.state is not SessionState.DONE:
                    raise ToolExecutionError(f"Agent session ended in state {session.state.value}")
                return {
                    "session_id": session.session_id,
                    "run_id": session.run_context.run_id,
                    "text": "".join(text),
                }
        finally:
            control.invalidate()
            if stream is not None:
                await stream.aclose()
            if self._on_control is not None:
                self._on_control(task, control, False)

    async def _chat_request(self, task: TaskDispatch) -> ChatRequest:
        if task.task_kind != "dialogue_subtask":
            return ChatRequest(input_value=task.input_value)
        if not task.child_run_id or not task.child_session_id or not task.source_run_id:
            raise ConfigurationError(
                "Dialogue subtask requires child_run_id, child_session_id and source_run_id"
            )
        if task.request_mode not in {"ask", "plan", "agent"}:
            raise ConfigurationError("Dialogue subtask request_mode is invalid")
        command_value = task.payload.get("command_payload", task.payload.get("command"))
        command = command_value if isinstance(command_value, Mapping) else task.payload
        snapshot = SubtaskCapabilitySnapshot.from_mapping(command)
        request = ChatRequest(
            input_value=task.input_value,
            run_id=task.child_run_id,
            parent_id=task.source_run_id,
            request_type=cast(Literal["ask", "plan", "agent"], task.request_mode),
        )
        snapshot.apply(request)
        if snapshot.client_mcp_server_names:
            if self._mcp_resolver is None:
                raise ConfigurationError("Dialogue subtask MCP resolver is not configured")
            allowed = frozenset(snapshot.client_mcp_server_names)
            resolved = await maybe_await(self._mcp_resolver.resolve(task.flow_id, allowed))
            if not isinstance(resolved, Mapping):
                raise ConfigurationError("Subtask MCP resolver must return an object")
            request.client_mcp_servers = {
                name: copy.deepcopy(dict(config))
                for name, config in resolved.items()
                if name in allowed and isinstance(config, Mapping)
            }
        return request

    async def aclose(self) -> None:
        agents = list(self._agents.values())
        self._agents.clear()
        self._sessions.clear()
        for agent in agents:
            await agent.aclose()


def _current_state(session: AgentSession) -> SessionState:
    return session.state
