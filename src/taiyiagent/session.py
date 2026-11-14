from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from .errors import (
    ApiError,
    ProtocolError,
    RecoveryError,
    ReportConflictError,
    SessionClosedError,
    SessionConflictError,
)
from .hooks import PermissionHook, SessionHook, StreamHook, ToolExecutionHook, maybe_await
from .models import (
    AgentLoopControlResult,
    ChatRequest,
    RecoveryPolicy,
    ResumableCheckpoint,
    RunContext,
    SessionState,
    StreamEvent,
)
from .tools import CheckpointToolResultsAdapter, OutboxEntry, OutboxStatus, OutboxType
from .tools.outbox import entry_id

if TYPE_CHECKING:
    from .agent import TaiyiAgent

log = logging.getLogger(__name__)

_ACTIVE_RUN_STATUSES = {
    "running",
    "resuming",
    "network_freezing",
    "pause_requested",
    "pausing",
    "waiting",
}
_COMPLETED_RUN_STATUSES = {"completed", "complete", "done", "success", "succeeded"}
_FAILED_RUN_STATUSES = {"failed", "error", "expired", "producer_lost"}


@dataclass(frozen=True, slots=True)
class _QueueItem:
    event: StreamEvent | None = None
    close_reason: str | None = None
    error: BaseException | None = None

    @property
    def terminal(self) -> bool:
        return self.event is None


class SessionEventQueue:
    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("event_queue_size must be positive")
        self._queue: asyncio.Queue[_QueueItem] = asyncio.Queue(capacity)
        self.dropped_heartbeats = 0

    async def put_event(self, event: StreamEvent) -> None:
        if event.is_heartbeat:
            if self._queue.full():
                self.dropped_heartbeats += 1
                return
            self._queue.put_nowait(_QueueItem(event=event))
            return
        await self._put_preserving_events(_QueueItem(event=event))

    async def close(self, reason: str) -> None:
        await self._put_preserving_events(_QueueItem(close_reason=reason))

    async def fail(self, error: BaseException) -> None:
        await self._put_preserving_events(_QueueItem(error=error))

    async def get(self) -> _QueueItem:
        return await self._queue.get()

    def close_nowait(self, reason: str) -> None:
        while not self._queue.empty():
            self._queue.get_nowait()
        self._queue.put_nowait(_QueueItem(close_reason=reason))

    async def _put_preserving_events(self, item: _QueueItem) -> None:
        while self._queue.full():
            if not self._drop_heartbeat():
                break
        await self._queue.put(item)

    def _drop_heartbeat(self) -> bool:
        retained: list[_QueueItem] = []
        dropped = False
        while not self._queue.empty():
            item = self._queue.get_nowait()
            if not dropped and item.event is not None and item.event.is_heartbeat:
                self.dropped_heartbeats += 1
                dropped = True
            else:
                retained.append(item)
        for item in retained:
            self._queue.put_nowait(item)
        return dropped


class SessionStream(AsyncIterator[StreamEvent]):
    def __init__(self, session: AgentSession, *, notify_start: bool) -> None:
        self._session = session
        self._notify_start = notify_start
        self._task: asyncio.Task[None] | None = None
        self._done = asyncio.Event()

    def _start(self) -> None:
        self._task = asyncio.create_task(
            self._session._produce(self), name=f"taiyiagent:{self._session.session_id}"
        )

    def __aiter__(self) -> SessionStream:
        return self

    async def __anext__(self) -> StreamEvent:
        while True:
            item = await self._session._events.get()
            if item.event is not None:
                return item.event
            self._done.set()
            if item.error is not None:
                raise item.error
            raise StopAsyncIteration

    async def wait(self, timeout: float | None = None) -> bool:  # noqa: ASYNC109
        if self._task is None:
            return True
        try:
            await asyncio.wait_for(asyncio.shield(self._task), timeout)
        except TimeoutError:
            return False
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise
            return True
        return True

    async def aclose(self) -> None:
        await self._session.interrupt()


class AgentSession:
    def __init__(
        self,
        agent: TaiyiAgent,
        session_id: str,
        *,
        stream_hooks: list[StreamHook] | None = None,
        tool_hooks: list[ToolExecutionHook] | None = None,
        permission_hooks: list[PermissionHook] | None = None,
        session_hooks: list[SessionHook] | None = None,
    ) -> None:
        self._agent = agent
        self._session_id = session_id
        self._stream_hooks = stream_hooks or []
        self._tool_hooks = tool_hooks or []
        self._permission_hooks = permission_hooks or []
        self._session_hooks = session_hooks or []
        self._events = SessionEventQueue(agent.event_queue_size)
        self._state = SessionState.IDLE
        self._context = RunContext()
        self._stream: SessionStream | None = None
        self._terminal_notified = False
        self._closed = False
        self._pending_tool_results: dict[str, dict[str, Any]] = {}
        self._tool_execution_observed = False
        self._automatic_recovery_safe = True
        self._active_resume_entry: OutboxEntry | None = None
        self._active_resume_persisted = False
        self._terminal_stream_error: Exception | None = None

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def run_context(self) -> RunContext:
        return self._context

    @property
    def dropped_heartbeats(self) -> int:
        return self._events.dropped_heartbeats

    def chat(
        self,
        message: str | ChatRequest,
        *,
        tweaks: dict[str, Any] | None = None,
        input_files: list[str] | None = None,
        request_type: Literal["ask", "agent", "plan"] | None = None,
    ) -> SessionStream:
        self._require_restart_state(
            {
                SessionState.IDLE,
                SessionState.DONE,
                SessionState.CANCELLED,
                SessionState.INTERRUPTED,
                SessionState.ERROR,
                SessionState.DISCONNECTED,
            }
        )
        if isinstance(message, ChatRequest):
            if tweaks is not None or input_files is not None or request_type is not None:
                raise ValueError("ChatRequest cannot be combined with chat field overrides")
            request = message.model_copy(deep=True)
        else:
            request = ChatRequest(input_value=message, tweaks=tweaks, request_type=request_type)
            request.input_files = input_files
        request.session_id = self._session_id
        self._apply_identity(request)
        request.execution_mode = "start"
        request.message_id = request.message_id or str(uuid.uuid4())
        request.subscription_id = request.subscription_id or str(uuid.uuid4())
        self._context.begin(request)
        self._reset_execution_tracking()
        return self._start_stream(request, SessionState.CONNECTING, notify_start=True)

    async def attach(self) -> SessionStream:
        self._require_recoverable_state()
        if self._context.run_id is None:
            raise RecoveryError("No run is available to attach")
        request = self._build_attach_request()
        return self._start_stream(request, SessionState.RESUMING, notify_start=False)

    async def resume(self, checkpoint: ResumableCheckpoint | None = None) -> SessionStream:
        previous = self._require_recoverable_state()
        flow_id = await self._agent.resolve_flow_id()
        if checkpoint is None:
            if self._context.run_id is None and self._context.checkpoint_id is None:
                raise RecoveryError("No run or checkpoint is available to resume")
            self._state = SessionState.RESUMING
            try:
                checkpoints = await self._agent.execution.list_resumable(flow_id, self.session_id)
                checkpoint = self._select_checkpoint(checkpoints)
            except Exception as exc:
                self._state = previous
                raise RecoveryError("Unable to discover a resumable checkpoint") from exc
            if checkpoint is None:
                self._state = previous
                raise RecoveryError("No resumable checkpoint found")
        self._validate_checkpoint(checkpoint)
        generation = self._context.next_recovery_generation()
        self._context.update_checkpoint(checkpoint, generation)
        request = self._build_resume_request(checkpoint, flow_id)
        return self._start_stream(
            request, SessionState.RESUMING, notify_start=False, increment_generation=False
        )

    async def recover_report(self, entry: OutboxEntry) -> SessionStream | None:
        if self._closed:
            raise SessionClosedError("Session has been closed")
        if self._state is not SessionState.IDLE:
            raise SessionConflictError("Stored reports can only be recovered by an idle session")
        flow_id = await self._agent.resolve_flow_id()
        if (
            entry.type is not OutboxType.CHECKPOINT_TOOL_RESULTS
            or entry.status is OutboxStatus.DEAD_LETTER
            or entry.flow_id != flow_id
            or entry.session_id != self.session_id
            or not entry.run_id
            or not entry.checkpoint_id
        ):
            raise RecoveryError("Stored checkpoint report identity is invalid")
        generation = self._context.next_recovery_generation()
        if not self._context.adopt_execution(entry.run_id, None, generation):
            raise RecoveryError("Stored checkpoint report run is invalid")
        self._state = SessionState.RESUMING
        try:
            checkpoints = await self._agent.execution.list_resumable(flow_id, self.session_id)
            checkpoint = next(
                (item for item in checkpoints if item.id == entry.checkpoint_id), None
            )
            if checkpoint is None:
                self._agent.outbox.mark_dead_letter(entry.id, "checkpoint-not-resumable")
                raise RecoveryError(f"Checkpoint is not resumable: {entry.checkpoint_id}")
            adapter = CheckpointToolResultsAdapter(self._agent.outbox)
            adapter.reconcile(entry, checkpoint, flow_id, self.session_id)
            if self._agent.outbox.find(entry.id) is None:
                self._state = SessionState.DONE
                return None
            self._context.update_checkpoint(checkpoint, generation)
            request = self._build_resume_request(checkpoint, flow_id)
            return self._start_stream(
                request, SessionState.RESUMING, notify_start=False, increment_generation=False
            )
        except Exception:
            if not self._terminal_notified:
                self._state = SessionState.ERROR
            raise

    async def retry(self) -> SessionStream:
        self._require_restart_state(
            {
                SessionState.DONE,
                SessionState.ERROR,
                SessionState.CANCELLED,
                SessionState.DISCONNECTED,
                SessionState.INTERRUPTED,
            }
        )
        if self._context.last_input_value is None:
            raise RecoveryError("No previous input is available")
        input_value = (
            "请继续完成上面的任务" if self._context.persisted else self._context.last_input_value
        )
        return self._start_from_template(input_value)

    async def repeat(self) -> SessionStream:
        if self._state is SessionState.PAUSED:
            await self.cancel()
        self._require_restart_state(
            {
                SessionState.DONE,
                SessionState.ERROR,
                SessionState.CANCELLED,
                SessionState.DISCONNECTED,
                SessionState.INTERRUPTED,
            }
        )
        if self._context.last_input_value is None:
            raise RecoveryError("No previous input is available")
        return self._start_from_template(self._context.last_input_value)

    def approve(self, call_id: str) -> bool:
        return self._agent.tools.approvals.approve(call_id)

    def deny(self, call_id: str, reason: str | None = None) -> bool:
        return self._agent.tools.approvals.deny(call_id, reason)

    async def wait(self, timeout: float | None = None) -> bool:  # noqa: ASYNC109
        if self._stream is None:
            return True
        return await self._stream.wait(timeout)

    async def pause(self) -> AgentLoopControlResult:
        previous = self._state
        if previous not in {
            SessionState.CONNECTING,
            SessionState.STREAMING,
            SessionState.TOOL_CALLING,
        }:
            raise SessionConflictError(f"Session cannot be paused: {previous.value}")
        self._state = SessionState.PAUSING
        generation = self._context.recovery_generation
        try:
            result = await self._agent.execution.pause(self.session_id, self._context.run_id)
        except Exception:
            if self._state is SessionState.PAUSING:
                self._state = previous
            raise
        if self._context.update_control(result, generation) and result.status == "paused":
            self._stop_local(SessionState.PAUSED, "Paused")
            await self._notify_stream_close("Paused")
        return result

    async def cancel(self) -> AgentLoopControlResult:
        current = self._state
        if current not in {
            SessionState.CONNECTING,
            SessionState.STREAMING,
            SessionState.TOOL_CALLING,
            SessionState.PAUSING,
            SessionState.PAUSED,
            SessionState.RESUMING,
            SessionState.DISCONNECTED,
            SessionState.RESTARTING,
        }:
            raise SessionConflictError(f"Session cannot be cancelled: {current.value}")
        result = await self._agent.execution.cancel(self.session_id, self._context.run_id)
        self._context.update_control(result, self._context.recovery_generation)
        self._stop_local(SessionState.CANCELLED, "Cancelled by user")
        await self._notify_interrupt("Cancelled by user")
        await self._notify_stream_close("Cancelled by user")
        return result

    async def interrupt(self) -> None:
        if self._terminal_notified:
            return
        self._stop_local(SessionState.INTERRUPTED, "Execution interrupted")
        await self._notify_interrupt("Execution interrupted")
        await self._notify_finish()
        await self._notify_stream_close("Execution interrupted")

    async def aclose(self) -> None:
        if self._closed:
            return
        await self.interrupt()
        self._closed = True
        self._agent.sessions.remove(self)
        await self._agent.plugins.close_session(self.session_id)
        await self._notify_close()

    def _start_from_template(self, input_value: str) -> SessionStream:
        request = ChatRequest(
            input_value=input_value,
            session_id=self.session_id,
            tweaks=dict(self._context.last_tweaks),
            request_type=self._context.request_type,
            requested_skill_names=list(self._context.requested_skill_names),
            client_skill_packages=deepcopy(self._context.client_skill_packages),
            run_id=str(uuid.uuid4()),
            execution_mode="start",
            message_id=str(uuid.uuid4()),
            subscription_id=str(uuid.uuid4()),
        )
        request.input_files = list(self._context.last_input_files)
        self._apply_identity(request)
        self._context.begin(request)
        self._reset_execution_tracking()
        return self._start_stream(request, SessionState.CONNECTING, notify_start=True)

    def _start_stream(
        self,
        request: ChatRequest,
        state: SessionState,
        *,
        notify_start: bool,
        increment_generation: bool = True,
    ) -> SessionStream:
        if self._closed:
            raise SessionClosedError("Session has been closed")
        self._agent.sessions.register(self)
        if increment_generation:
            self._context.next_recovery_generation()
        self._context.last_request = request.model_copy(deep=True)
        self._state = state
        self._events = SessionEventQueue(self._agent.event_queue_size)
        self._terminal_notified = False
        self._terminal_stream_error = None
        self._stream = SessionStream(self, notify_start=notify_start)
        self._stream._start()
        return self._stream

    def _reset_execution_tracking(self) -> None:
        self._pending_tool_results.clear()
        self._tool_execution_observed = False
        self._automatic_recovery_safe = True
        self._active_resume_entry = None
        self._active_resume_persisted = False

    def _require_recoverable_state(self) -> SessionState:
        if self._closed:
            raise SessionClosedError("Session has been closed")
        if self._state not in {
            SessionState.PAUSED,
            SessionState.DISCONNECTED,
            SessionState.ERROR,
        }:
            raise SessionConflictError(f"Session cannot be recovered: {self._state.value}")
        return self._state

    def _require_restart_state(self, allowed: set[SessionState]) -> None:
        if self._closed:
            raise SessionClosedError("Session has been closed")
        if self._state not in allowed:
            raise SessionConflictError(f"Session cannot start a new run: {self._state.value}")

    def _apply_identity(self, request: ChatRequest) -> None:
        request.client_id = self._agent.client_id
        request.device_id = self._agent.device_id
        request.client_type = "cli"

    async def _produce(self, stream: SessionStream) -> None:
        request = self._context.last_request
        try:
            if request is None:
                raise RuntimeError("Session request has not been initialized")
            if stream._notify_start:
                await self._notify_start()
            while self._stream is stream and not self._terminal_notified:
                server_closed = False
                self._context.last_request = request.model_copy(deep=True)
                try:
                    if request.execution_mode == "attach":
                        await self._agent.tool_reporter.flush_now()
                        self._prune_acknowledged_results()
                    events = await self._agent._open_stream(request)
                    self._state = SessionState.STREAMING
                    async for event in events:
                        if self._stream is not stream or self._terminal_notified:
                            return
                        self._acknowledge_resume_submission()
                        self._context.update(event)
                        await self._notify_stream_event(event)
                        server_closed = server_closed or event.is_close
                        if event.is_error:
                            self._terminal_stream_error = ProtocolError(
                                f"Agent error: {event.error_message or 'Unknown error'}"
                            )
                        await self._events.put_event(event)
                        await self._agent._dispatch_tool_event(self, event)
                    if self._stream is not stream or self._terminal_notified:
                        return
                    if self._terminal_stream_error is not None:
                        await self._fail(self._terminal_stream_error)
                        return
                    if server_closed:
                        await self._finish(SessionState.DONE, "Execution completed")
                        return
                    request = await self._disconnected(
                        stream, "Stream ended before the server close event"
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if self._stream is not stream or self._terminal_notified:
                        return
                    self._fail_resume_submission(exc)
                    if (
                        self._nonrecoverable(exc)
                        or self._agent.recovery_policy is RecoveryPolicy.MANUAL
                    ):
                        await self._fail(exc)
                        return
                    await self._notify_stream_error(exc)
                    request = await self._recover(stream, exc)
        except asyncio.CancelledError:
            if self._stream is stream and not self._terminal_notified:
                self._stop_local(SessionState.INTERRUPTED, "Execution interrupted")
                await self._notify_interrupt("Execution interrupted")
                await self._notify_finish()
                await self._notify_stream_close("Execution interrupted")
        except Exception as exc:
            if self._stream is stream and not self._terminal_notified:
                await self._fail(exc)
        finally:
            stream._done.set()

    async def _disconnected(self, stream: SessionStream, reason: str) -> ChatRequest:
        if self._active_resume_entry is not None:
            self._fail_resume_submission(
                RecoveryError("resume-stream-ended-before-acknowledgement")
            )
        self._state = SessionState.DISCONNECTED
        await self._notify_stream_close(reason)
        if self._agent.recovery_policy is RecoveryPolicy.MANUAL:
            await self._finish(SessionState.DISCONNECTED, reason)
            raise _RecoveryFinished
        if not self._automatic_recovery_safe:
            await self._fail(RecoveryError("Automatic recovery is unsafe after report failure"))
            raise _RecoveryFinished
        return await self._recover(stream, None)

    async def _recover(
        self, stream: SessionStream, disconnect_cause: Exception | None
    ) -> ChatRequest:
        self._state = SessionState.DISCONNECTED
        last_error = disconnect_cause
        for attempt in range(1, self._agent.recovery_max_attempts + 1):
            delay = min(
                self._agent.recovery_max_backoff,
                self._agent.recovery_min_backoff * (2 ** (attempt - 1)),
            )
            if delay:
                await asyncio.sleep(delay)
            if self._stream is not stream or self._terminal_notified:
                raise _RecoveryFinished
            try:
                await self._agent.protocol.refresh()
                flow_id = await self._agent.resolve_flow_id()
                checkpoints = await self._agent.execution.list_resumable(flow_id, self.session_id)
                checkpoint = self._select_checkpoint(checkpoints)
                session = await self._agent._session_status(self.session_id)
                generation = self._context.recovery_generation
                if checkpoint is not None and self._usable_checkpoint(checkpoint):
                    self._context.update_checkpoint(checkpoint, generation)
                    if checkpoint.requires_user_resume:
                        await self._finish(SessionState.PAUSED, "Execution requires user resume")
                        raise _RecoveryFinished
                    return self._build_resume_request(checkpoint, flow_id)
                run_status = self._text(session, "run_status") if session else None
                if (
                    self._context.run_id is None
                    and session is not None
                    and (session.get("executing") is True or run_status in _ACTIVE_RUN_STATUSES)
                ):
                    self._context.adopt_execution(
                        self._text(session, "executing_run_id"),
                        self._text(session, "execution_attempt_id"),
                        generation,
                    )
                if self._original_run_active(session):
                    return self._build_attach_request(
                        self._text(session, "execution_attempt_id") if session else None
                    )
                if run_status in _COMPLETED_RUN_STATUSES:
                    await self._finish(SessionState.DONE, "Run completed on server")
                    raise _RecoveryFinished
                if run_status in {"cancelled", "cancel_requested"}:
                    self._stop_local(SessionState.CANCELLED, "Cancelled on server")
                    await self._notify_interrupt("Cancelled on server")
                    await self._notify_stream_close("Cancelled on server")
                    raise _RecoveryFinished
                checkpoint_lost = checkpoint is not None and checkpoint.status == "expired"
                execution_lost = (
                    session is None or run_status in _FAILED_RUN_STATUSES or checkpoint_lost
                )
                if execution_lost and self._can_restart(checkpoint):
                    return await self._restart_after_execution_loss(session is None)
                if execution_lost:
                    raise RecoveryError("Server execution is no longer safely recoverable")
                last_error = RecoveryError(
                    f"Server execution state is not yet recoverable: {run_status or 'unknown'}"
                )
            except _RecoveryFinished:
                raise
            except Exception as exc:
                if self._nonrecoverable(exc):
                    await self._fail(exc)
                    raise _RecoveryFinished from exc
                last_error = exc
        error = RecoveryError("Recovery attempts exhausted")
        if last_error is not None:
            error.__cause__ = last_error
        await self._fail(error)
        raise _RecoveryFinished

    async def _restart_after_execution_loss(self, session_missing: bool) -> ChatRequest:
        if session_missing:
            resolved_id = await self._agent._new_session_id()
            self._agent.sessions.update_id(self.session_id, resolved_id, self)
        input_value = self._context.last_input_value
        if input_value is None:
            raise RecoveryError("No previous input is available for safe restart")
        request = ChatRequest(
            input_value=input_value,
            session_id=self.session_id,
            tweaks=dict(self._context.last_tweaks),
            request_type=self._context.request_type,
            requested_skill_names=list(self._context.requested_skill_names),
            client_skill_packages=deepcopy(self._context.client_skill_packages),
            run_id=str(uuid.uuid4()),
            execution_mode="start",
            message_id=str(uuid.uuid4()),
            subscription_id=str(uuid.uuid4()),
        )
        request.input_files = list(self._context.last_input_files)
        self._apply_identity(request)
        self._context.begin(request)
        self._pending_tool_results.clear()
        self._active_resume_entry = None
        self._state = SessionState.RESTARTING
        return request

    def _build_attach_request(self, execution_attempt_id: str | None = None) -> ChatRequest:
        self._active_resume_entry = None
        request = self._recovery_request("attach")
        request.execution_attempt_id = execution_attempt_id or self._context.execution_attempt_id
        request.after_event_id = self._context.after_event_id
        request.subscription_id = str(uuid.uuid4())
        return request

    def _build_resume_request(self, checkpoint: ResumableCheckpoint, flow_id: str) -> ChatRequest:
        request = self._recovery_request("resume")
        request.checkpoint_id = checkpoint.id
        request.checkpoint_version = checkpoint.version
        adapter = CheckpointToolResultsAdapter(self._agent.outbox)
        try:
            prepared = adapter.prepare(
                checkpoint, flow_id, self.session_id, self._pending_tool_results
            )
            self._active_resume_persisted = True
        except ReportConflictError:
            self._active_resume_entry = None
            raise
        except Exception:
            prepared = adapter.direct(
                checkpoint, flow_id, self.session_id, self._pending_tool_results
            )
            self._active_resume_persisted = False
            log.error("Unable to persist checkpoint tool results for session %s", self.session_id)
        self._active_resume_entry = prepared.entry
        if prepared.has_results:
            request.tool_results = prepared.tool_results
            request.submission_id = prepared.entry.submission_id if prepared.entry else None
        request.subscription_id = str(uuid.uuid4())
        return request

    def _recovery_request(self, mode: Literal["attach", "resume"]) -> ChatRequest:
        request = ChatRequest(
            input_value="",
            session_id=self.session_id,
            run_id=self._context.run_id,
            execution_mode=mode,
        )
        self._apply_identity(request)
        request.available_tools = deepcopy(self._context.available_tools)
        request.client_skill_packages = deepcopy(self._context.client_skill_packages)
        request.requested_skill_names = list(self._context.requested_skill_names)
        return request

    def _select_checkpoint(
        self, checkpoints: list[ResumableCheckpoint]
    ) -> ResumableCheckpoint | None:
        if self._context.checkpoint_id is not None:
            return next(
                (item for item in checkpoints if item.id == self._context.checkpoint_id), None
            )
        if self._context.run_id is not None:
            return next((item for item in checkpoints if item.run_id == self._context.run_id), None)
        return checkpoints[0] if len(checkpoints) == 1 else None

    def _validate_checkpoint(self, checkpoint: ResumableCheckpoint) -> None:
        if not checkpoint.id or not checkpoint.run_id:
            raise RecoveryError("Checkpoint id and run id are required")
        if checkpoint.session_id and checkpoint.session_id != self.session_id:
            raise RecoveryError("Checkpoint belongs to another session")
        if self._context.run_id and checkpoint.run_id != self._context.run_id:
            raise RecoveryError("Checkpoint belongs to another run")

    @staticmethod
    def _usable_checkpoint(checkpoint: ResumableCheckpoint | None) -> bool:
        return checkpoint is not None and checkpoint.status in {"ready", "paused", "waiting"}

    def _original_run_active(self, session: dict[str, Any] | None) -> bool:
        if session is None or self._context.run_id is None:
            return False
        if self._text(session, "executing_run_id") != self._context.run_id:
            return False
        return (
            session.get("executing") is True
            or self._text(session, "run_status") in _ACTIVE_RUN_STATUSES
        )

    def _can_restart(self, checkpoint: ResumableCheckpoint | None) -> bool:
        if (
            not self._agent.auto_restart_on_execution_loss
            or self._context.last_input_value is None
            or self._tool_execution_observed
            or not self._automatic_recovery_safe
        ):
            return False
        return checkpoint is None or (
            not checkpoint.waiting_call_ids and not checkpoint.has_uncertain_side_effects
        )

    def _prune_acknowledged_results(self) -> None:
        for call_id, payload in list(self._pending_tool_results.items()):
            run_id = payload.get("run_id")
            if not isinstance(run_id, str):
                continue
            stored_id = entry_id(OutboxType.CLIENT_TOOL_RESULT, run_id + "\0" + call_id)
            stored = self._agent.outbox.find(stored_id)
            if stored is None:
                self._pending_tool_results.pop(call_id, None)

    def _acknowledge_resume_submission(self) -> None:
        entry = self._active_resume_entry
        if entry is None:
            return
        self._active_resume_entry = None
        if self._active_resume_persisted:
            try:
                CheckpointToolResultsAdapter(self._agent.outbox).accepted(entry)
            except Exception:
                log.exception("Unable to clean acknowledged checkpoint submission")

    def _fail_resume_submission(self, error: Exception) -> None:
        entry = self._active_resume_entry
        if entry is None:
            return
        self._active_resume_entry = None
        if not self._active_resume_persisted:
            self._automatic_recovery_safe = False
            log.error("Unpersisted checkpoint tool results could not be acknowledged")
            return
        try:
            CheckpointToolResultsAdapter(self._agent.outbox).failed(entry, error)
        except Exception:
            log.exception("Unable to update checkpoint submission")

    @staticmethod
    def _nonrecoverable(error: Exception) -> bool:
        if isinstance(error, (ProtocolError, ReportConflictError, SessionConflictError)):
            return True
        return isinstance(error, ApiError) and error.status_code in {400, 401, 402, 403, 404, 422}

    @staticmethod
    def _text(value: dict[str, Any] | None, key: str) -> str | None:
        if value is None:
            return None
        item = value.get(key)
        return item if isinstance(item, str) and item.strip() else None

    def _stop_local(self, state: SessionState, reason: str) -> None:
        self._agent.tools.approvals.deny_all(reason, session_id=self.session_id)
        self._terminal_notified = True
        self._state = state
        self._context.next_recovery_generation()
        self._events.close_nowait(reason)
        stream = self._stream
        if (
            stream is not None
            and stream._task is not None
            and stream._task is not asyncio.current_task()
        ):
            stream._task.cancel()
        self._agent.sessions.remove(self)

    async def _finish(self, state: SessionState, reason: str) -> None:
        self._terminal_notified = True
        self._state = state
        if state is not SessionState.PAUSED:
            await self._notify_finish()
        await self._notify_stream_close(reason)
        self._agent.sessions.remove(self)
        await self._events.close(reason)

    async def _fail(self, error: Exception) -> None:
        self._terminal_notified = True
        self._state = SessionState.ERROR
        self._agent.tools.approvals.deny_all("Session execution failed", session_id=self.session_id)
        await self._notify_stream_error(error)
        await self._notify_finish()
        await self._notify_stream_close(str(error))
        self._agent.sessions.remove(self)
        await self._events.fail(error)

    async def _notify_stream_event(self, event: StreamEvent) -> None:
        for hook in self._all_stream_hooks():
            try:
                await maybe_await(hook.on_event(self.session_id, event))
            except Exception:
                log.exception("Stream event hook failed")
        text = event.message_text
        if text is not None:
            for hook in self._all_stream_hooks():
                try:
                    await maybe_await(hook.on_chunk(self.session_id, text))
                except Exception:
                    log.exception("Stream chunk hook failed")

    async def _notify_start(self) -> None:
        for hook in self._all_session_hooks():
            try:
                await maybe_await(hook.on_start(self.session_id))
            except Exception:
                log.exception("Session start hook failed")

    async def _notify_finish(self) -> None:
        for hook in self._all_session_hooks():
            try:
                await maybe_await(hook.on_finish(self.session_id))
            except Exception:
                log.exception("Session finish hook failed")

    async def _notify_interrupt(self, reason: str) -> None:
        for hook in self._all_session_hooks():
            try:
                await maybe_await(hook.on_interrupt(self.session_id, reason))
            except Exception:
                log.exception("Session interrupt hook failed")

    async def _notify_close(self) -> None:
        for hook in self._all_session_hooks():
            try:
                await maybe_await(hook.on_close(self.session_id))
            except Exception:
                log.exception("Session close hook failed")

    async def _notify_stream_error(self, error: BaseException) -> None:
        for hook in self._all_stream_hooks():
            try:
                await maybe_await(hook.on_error(self.session_id, error))
            except Exception:
                log.exception("Stream error hook failed")

    async def _notify_stream_close(self, reason: str) -> None:
        for hook in self._all_stream_hooks():
            try:
                await maybe_await(hook.on_close(self.session_id, reason))
            except Exception:
                log.exception("Stream close hook failed")

    def _all_stream_hooks(self) -> list[StreamHook]:
        return [*self._agent._stream_hooks, *self._stream_hooks]

    def _all_session_hooks(self) -> list[SessionHook]:
        return [*self._agent._session_hooks, *self._session_hooks]


class _RecoveryFinished(Exception):
    pass


class SessionManager:
    def __init__(self, agent: TaiyiAgent, max_sessions: int) -> None:
        self._agent = agent
        self._max_sessions = max_sessions
        self._sessions: dict[str, AgentSession] = {}
        self._lock = asyncio.Lock()

    def create(self, session_id: str | None = None, **hooks: Any) -> AgentSession:
        session = AgentSession(self._agent, session_id or str(uuid.uuid4()), **hooks)
        self.register(session)
        return session

    def register(self, session: AgentSession) -> None:
        existing = self._sessions.get(session.session_id)
        if existing is session:
            return
        if existing is not None:
            raise SessionConflictError(f"Session ID is already registered: {session.session_id}")
        if len(self._sessions) >= self._max_sessions:
            raise SessionConflictError(f"Maximum number of sessions reached: {self._max_sessions}")
        self._sessions[session.session_id] = session

    def get(self, session_id: str) -> AgentSession | None:
        return self._sessions.get(session_id)

    def update_id(self, previous_id: str, resolved_id: str, session: AgentSession) -> None:
        existing = self._sessions.get(resolved_id)
        if existing is not None and existing is not session:
            raise SessionConflictError(f"Session ID is already registered: {resolved_id}")
        if self._sessions.get(previous_id) is not session:
            raise SessionConflictError("Session registration changed during stream startup")
        del self._sessions[previous_id]
        self._sessions[resolved_id] = session
        session._session_id = resolved_id

    def remove(self, session: AgentSession) -> None:
        if self._sessions.get(session.session_id) is session:
            del self._sessions[session.session_id]

    async def interrupt_all(self) -> None:
        await asyncio.gather(
            *(session.interrupt() for session in list(self._sessions.values())),
            return_exceptions=True,
        )

    @property
    def active_session_count(self) -> int:
        return sum(
            session.state
            not in {
                SessionState.DONE,
                SessionState.ERROR,
                SessionState.CANCELLED,
                SessionState.INTERRUPTED,
            }
            for session in self._sessions.values()
        )
