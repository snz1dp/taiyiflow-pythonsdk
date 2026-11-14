from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Protocol

from ..collaboration import TaskCancel, TaskDispatch
from ..errors import ConfigurationError, ReportConflictError, redact
from ..transport import CollaborationTaskClient, JsonObject, JsonValue, RestClient, ScheduleClient
from .auth import WorkerAuth


class TaskExecutionState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REPORT_UNKNOWN = "report_unknown"


class TaskCancelledError(Exception):
    pass


class TaskControl(Protocol):
    async def pause(self) -> None: ...

    async def resume(self) -> None: ...

    async def cancel(self) -> None: ...


ProgressReporter = Callable[[JsonValue], Awaitable[None]]


class WorkerTaskHandler(Protocol):
    def execute(
        self,
        task: TaskDispatch,
        progress: ProgressReporter,
    ) -> Awaitable[JsonValue]: ...


class TaskReporter(Protocol):
    async def running(self, task: TaskDispatch, progress: JsonValue = None) -> None: ...

    async def success(self, task: TaskDispatch, result: JsonValue) -> None: ...

    async def failure(self, task: TaskDispatch, error: str) -> None: ...

    async def cancelled(self, task: TaskDispatch, cancel_request_id: str) -> None: ...


@dataclass
class _Execution:
    key: str
    task: TaskDispatch
    state: TaskExecutionState = TaskExecutionState.QUEUED
    runner: asyncio.Task[None] | None = None
    control: TaskControl | None = None
    handler_started: bool = False
    cancel_requested: bool = False
    pause_requested: bool = False
    cancel_request_id: str = ""
    pending_status: str | None = None
    pending_result: JsonValue = None
    pending_error: str | None = None
    result: JsonValue = None
    error: str | None = None
    completed_at: float = field(default_factory=time.monotonic)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def terminal(self) -> bool:
        return self.state in {
            TaskExecutionState.SUCCESS,
            TaskExecutionState.FAILED,
            TaskExecutionState.CANCELLED,
        }


@dataclass(frozen=True)
class TaskReservation:
    execution: _Execution
    accepted: bool


class TaskExecutor:
    def __init__(
        self,
        handler: WorkerTaskHandler,
        reporter: TaskReporter,
        *,
        registry_capacity: int = 1024,
    ) -> None:
        if registry_capacity <= 0:
            raise ConfigurationError("Task registry capacity must be positive")
        self._handler = handler
        self._reporter = reporter
        self._registry_capacity = registry_capacity
        self._executions: dict[str, _Execution] = {}

    def prepare(self, task: TaskDispatch) -> TaskReservation:
        key = _task_key(task)
        self._evict_terminal()
        current = self._executions.get(key)
        if current is not None and not (current.terminal and task.version > current.task.version):
            return TaskReservation(current, False)
        if len(self._executions) >= self._registry_capacity:
            return TaskReservation(_Execution(key, task), False)
        execution = _Execution(key, task)
        self._executions[key] = execution
        return TaskReservation(execution, True)

    def attach(self, reservation: TaskReservation, runner: asyncio.Task[None]) -> None:
        execution = reservation.execution
        execution.runner = runner
        if execution.cancel_requested:
            runner.cancel()

    def reject(self, reservation: TaskReservation) -> None:
        if reservation.accepted:
            self._executions.pop(reservation.execution.key, None)

    async def execute(self, reservation: TaskReservation) -> TaskExecutionState:
        execution = reservation.execution
        if not reservation.accepted:
            return execution.state
        async with execution.lock:
            cancelled = execution.cancel_requested
            if not cancelled:
                if execution.state is not TaskExecutionState.QUEUED:
                    return execution.state
                execution.handler_started = True
                execution.state = TaskExecutionState.RUNNING
        if cancelled:
            return await self._complete_cancellation(execution)
        try:
            await self._reporter.running(execution.task)
        except asyncio.CancelledError:
            if not execution.cancel_requested:
                raise
            return await self._complete_cancellation(execution)
        except Exception:
            if execution.cancel_requested:
                return await self._complete_cancellation(execution)
            execution.state = TaskExecutionState.REPORT_UNKNOWN
            return execution.state
        try:
            result = await self._handler.execute(
                execution.task,
                lambda progress: self._progress(execution, progress),
            )
        except asyncio.CancelledError:
            if execution.cancel_requested:
                return await self._complete_cancellation(execution)
            raise
        except TaskCancelledError:
            return await self._complete_cancellation(execution)
        except Exception as exc:
            if execution.cancel_requested:
                return await self._complete_cancellation(execution)
            return await self._complete_failure(execution, str(exc))
        if execution.cancel_requested:
            return await self._complete_cancellation(execution)
        return await self._complete_success(execution, result)

    async def execute_detached(self, task: TaskDispatch) -> JsonObject:
        reservation = self.prepare(task)
        execution = reservation.execution
        if not reservation.accepted:
            return _execution_result(execution)

        async def run() -> None:
            await self.execute(reservation)

        runner = asyncio.create_task(run())
        self.attach(reservation, runner)
        await runner
        return _execution_result(execution)

    def state_of(self, task: TaskDispatch) -> TaskExecutionState | None:
        execution = self._executions.get(_task_key(task))
        return execution.state if execution is not None else None

    def register_control(self, task: TaskDispatch, control: TaskControl) -> None:
        execution = self._executions.get(_task_key(task))
        if execution is None or execution.task is not task or execution.terminal:
            return
        execution.control = control

    def unregister_control(self, task: TaskDispatch, control: TaskControl) -> None:
        execution = self._executions.get(_task_key(task))
        if execution is not None and execution.control is control:
            execution.control = None

    async def cancel(self, cancel: TaskCancel) -> bool:
        if not cancel.task_id.strip():
            return False
        execution = self._executions.get(f"task:{cancel.task_id}")
        if execution is None:
            return False
        if (
            cancel.version > 0
            and execution.task.version > 0
            and cancel.version < execution.task.version
        ):
            return False
        if cancel.cancel_request_id and cancel.cancel_request_id == execution.cancel_request_id:
            return True
        if execution.state is TaskExecutionState.CANCELLED:
            return True
        if execution.terminal:
            return False
        if cancel.version > execution.task.version:
            execution.task = replace(execution.task, version=cancel.version)
        execution.cancel_requested = True
        execution.cancel_request_id = cancel.cancel_request_id
        if execution.runner is not None:
            execution.runner.cancel()
        if execution.control is not None:
            try:
                await execution.control.cancel()
            except Exception:
                execution.state = TaskExecutionState.REPORT_UNKNOWN
                return True
        await self._complete_cancellation(execution)
        return True

    async def pause_all(self) -> None:
        for execution in list(self._executions.values()):
            if execution.control is None or execution.state is not TaskExecutionState.RUNNING:
                continue
            try:
                await execution.control.pause()
                execution.pause_requested = True
                execution.state = TaskExecutionState.PAUSED
            except Exception:
                execution.state = TaskExecutionState.REPORT_UNKNOWN

    async def resume_all(self) -> None:
        for execution in list(self._executions.values()):
            if (
                execution.control is None
                or execution.state is not TaskExecutionState.PAUSED
                or execution.cancel_requested
            ):
                continue
            try:
                await execution.control.resume()
                execution.pause_requested = False
                execution.state = TaskExecutionState.RUNNING
            except Exception:
                execution.state = TaskExecutionState.REPORT_UNKNOWN

    async def cancel_all(self, request_prefix: str) -> None:
        for execution in list(self._executions.values()):
            if execution.terminal or execution.task.task_type == "schedule":
                continue
            cancel = TaskCancel(
                task_id=execution.task.task_id,
                task_kind=execution.task.task_kind,
                version=execution.task.version,
                source_principal_type="worker",
                correlation_id="",
                cancel_request_id=f"{request_prefix}:{execution.key}",
                cancel_requested_at="",
                payload={},
            )
            await self.cancel(cancel)

    async def flush_unknown_reports(self) -> None:
        for execution in list(self._executions.values()):
            if execution.state is not TaskExecutionState.REPORT_UNKNOWN:
                continue
            if execution.pending_status == "success":
                await self._complete_success(execution, execution.pending_result)
            elif execution.pending_status == "failed":
                await self._complete_failure(execution, execution.pending_error or "")
            elif execution.pending_status == "cancelled":
                await self._complete_cancellation(execution)

    def reconcile(self, snapshot: JsonObject) -> None:
        task_id = _text(snapshot, "task_id") or _text(snapshot, "id")
        run_record_id = _text(snapshot, "run_record_id")
        key = f"schedule:{run_record_id}" if run_record_id else f"task:{task_id}"
        if not task_id and not run_record_id:
            return
        status = snapshot.get("status")
        states = {
            "success": TaskExecutionState.SUCCESS,
            "failed": TaskExecutionState.FAILED,
            "offline": TaskExecutionState.FAILED,
            "timeout": TaskExecutionState.FAILED,
            "cancelled": TaskExecutionState.CANCELLED,
            "dispatched": TaskExecutionState.REPORT_UNKNOWN,
            "acknowledged": TaskExecutionState.REPORT_UNKNOWN,
            "running": TaskExecutionState.REPORT_UNKNOWN,
            "pending": TaskExecutionState.REPORT_UNKNOWN,
        }
        state = states.get(status) if isinstance(status, str) else None
        if state is None:
            return
        execution = self._executions.get(key)
        version = snapshot.get("version")
        snapshot_version = (
            version if isinstance(version, int) and not isinstance(version, bool) else 0
        )
        if execution is None:
            task = TaskDispatch.from_payload(snapshot, "schedule" if run_record_id else "task")
            execution = _Execution(key, task, state=state)
            self._executions[key] = execution
            return
        if snapshot_version > 0 and execution.task.version > snapshot_version:
            return
        execution.state = state
        if execution.terminal:
            execution.pending_status = None
            execution.pending_result = None
            execution.pending_error = None

    @property
    def registry_size(self) -> int:
        return len(self._executions)

    async def _progress(self, execution: _Execution, progress: JsonValue) -> None:
        if not execution.cancel_requested and execution.state is TaskExecutionState.RUNNING:
            await self._reporter.running(execution.task, progress)

    async def _complete_success(
        self,
        execution: _Execution,
        result: JsonValue,
    ) -> TaskExecutionState:
        async with execution.lock:
            if execution.terminal or execution.cancel_requested:
                return execution.state
            execution.pending_status = "success"
            execution.pending_result = result
            execution.pending_error = None
            execution.result = result
            execution.error = None
            try:
                await self._reporter.success(execution.task, result)
                execution.state = TaskExecutionState.SUCCESS
                execution.pending_status = None
                execution.pending_result = None
            except Exception:
                execution.state = TaskExecutionState.REPORT_UNKNOWN
            execution.completed_at = time.monotonic()
            return execution.state

    async def _complete_failure(
        self,
        execution: _Execution,
        error: str,
    ) -> TaskExecutionState:
        async with execution.lock:
            if execution.terminal or execution.cancel_requested:
                return execution.state
            execution.pending_status = "failed"
            execution.pending_result = None
            execution.pending_error = redact(error) or "Task execution failed"
            execution.error = execution.pending_error
            try:
                await self._reporter.failure(execution.task, execution.pending_error)
                execution.state = TaskExecutionState.FAILED
                execution.pending_status = None
                execution.pending_error = None
            except Exception:
                execution.state = TaskExecutionState.REPORT_UNKNOWN
            execution.completed_at = time.monotonic()
            return execution.state

    async def _complete_cancellation(self, execution: _Execution) -> TaskExecutionState:
        async with execution.lock:
            if execution.terminal:
                return execution.state
            execution.pending_status = "cancelled"
            execution.pending_result = None
            execution.pending_error = None
            try:
                await self._reporter.cancelled(execution.task, execution.cancel_request_id)
                execution.state = TaskExecutionState.CANCELLED
                execution.pending_status = None
            except Exception:
                execution.state = TaskExecutionState.REPORT_UNKNOWN
            execution.completed_at = time.monotonic()
            return execution.state

    def _evict_terminal(self) -> None:
        while len(self._executions) >= self._registry_capacity:
            terminal = [item for item in self._executions.values() if item.terminal]
            if not terminal:
                return
            oldest = min(terminal, key=lambda item: item.completed_at)
            self._executions.pop(oldest.key, None)


class PlatformTaskReporter:
    def __init__(
        self,
        base_url: str,
        auth: WorkerAuth,
        *,
        timeout: float = 30.0,
        rest: RestClient | None = None,
    ) -> None:
        self._auth = auth
        self._rest = rest or RestClient(
            base_url,
            access_token=auth.access_token,
            timeout=timeout,
        )
        self._tasks = CollaborationTaskClient(self._rest)
        self._schedules = ScheduleClient(self._rest)

    async def running(self, task: TaskDispatch, progress: JsonValue = None) -> None:
        if _is_schedule(task):
            await self._report_schedule(task, {"status": "running"})
            return
        await self._report_task(task, "running", progress=progress)

    async def success(self, task: TaskDispatch, result: JsonValue) -> None:
        payload = _object_payload(result, "result")
        if _is_schedule(task):
            await self._report_schedule(
                task,
                {"status": "success", "result_payload": payload},
            )
            return
        await self._report_task(task, "success", result=payload)

    async def failure(self, task: TaskDispatch, error: str) -> None:
        message = redact(error) or "Task execution failed"
        if _is_schedule(task):
            await self._report_schedule(
                task,
                {"status": "failed", "error_message": message},
            )
            return
        await self._report_task(task, "failed", detail=message)

    async def cancelled(self, task: TaskDispatch, cancel_request_id: str) -> None:
        if _is_schedule(task):
            await self._report_schedule(
                task,
                {
                    "status": "cancelled",
                    "status_payload": {"cancel_request_id": cancel_request_id},
                },
            )
            return
        await self._report_task(task, "cancelled", detail=cancel_request_id)

    async def _report_task(
        self,
        task: TaskDispatch,
        status: str,
        *,
        result: JsonObject | None = None,
        progress: JsonValue = None,
        detail: str | None = None,
    ) -> None:
        if not task.task_id.strip():
            raise ConfigurationError("Platform reporting requires task_id")

        async def report() -> None:
            for attempt in range(2):
                snapshot = _object(await self._tasks.get(task.task_id))
                if _matches_terminal(snapshot, status, result, detail):
                    return
                _require_mutable(snapshot, task.task_id, status)
                version = snapshot.get("version")
                expected = (
                    version if isinstance(version, int) and not isinstance(version, bool) else None
                )
                try:
                    if status == "running":
                        await self._tasks.running(task.task_id, progress, expected_version=expected)
                    elif status == "success":
                        await self._tasks.success(task.task_id, result, expected_version=expected)
                    elif status == "failed":
                        await self._tasks.failure(task.task_id, detail, expected_version=expected)
                    elif status == "cancelled":
                        await self._tasks.cancelled(
                            task.task_id, detail or "", expected_version=expected
                        )
                    return
                except Exception as exc:
                    if getattr(exc, "status_code", None) != 409 or attempt == 1:
                        raise

        await self._auth.execute_with_retry(report)

    async def _report_schedule(self, task: TaskDispatch, report: JsonObject) -> None:
        if not task.run_record_id.strip():
            raise ConfigurationError("Schedule reporting requires run_record_id")
        await self._auth.execute_with_retry(
            lambda: _discard(self._schedules.report(task.run_record_id, report))
        )

    async def aclose(self) -> None:
        await self._rest.aclose()


def _execution_result(execution: _Execution) -> JsonObject:
    return {
        "task_id": execution.task.task_id,
        "status": execution.state.value,
        "result": execution.result,
        "error": execution.error,
    }


def _task_key(task: TaskDispatch) -> str:
    if task.task_type == "schedule":
        if not task.run_record_id.strip():
            raise ConfigurationError("Schedule task requires run_record_id")
        return f"schedule:{task.run_record_id}"
    if not task.task_id.strip():
        raise ConfigurationError("Collaboration task requires task_id")
    return f"task:{task.task_id}"


def _text(value: JsonObject, name: str) -> str:
    result = value.get(name)
    return result if isinstance(result, str) else ""


def _object(value: JsonValue) -> JsonObject:
    if not isinstance(value, dict):
        raise ConfigurationError("Platform task snapshot must be an object")
    return value


def _object_payload(value: JsonValue, field: str) -> JsonObject:
    return value if isinstance(value, dict) else {field: value}


def _is_schedule(task: TaskDispatch) -> bool:
    return task.task_type == "schedule"


def _matches_terminal(
    snapshot: JsonObject,
    target: str,
    result: JsonObject | None,
    detail: str | None,
) -> bool:
    status = snapshot.get("status")
    if status not in {"success", "failed", "cancelled", "timeout", "offline"} or status != target:
        return False
    if target == "success":
        return snapshot.get("result_payload") == result
    if target == "failed":
        return snapshot.get("error_message", "") == detail
    if target == "cancelled":
        payload = snapshot.get("status_payload")
        nested = payload.get("cancel_request_id") if isinstance(payload, dict) else None
        return snapshot.get("cancel_request_id", nested or "") == (detail or "")
    return True


def _require_mutable(snapshot: JsonObject, task_id: str, target: str) -> None:
    status = snapshot.get("status")
    if status in {"success", "failed", "cancelled", "timeout", "offline"}:
        raise ReportConflictError(
            f"Platform task {task_id} is already {status}; cannot report {target}"
        )


async def _discard(value: Awaitable[JsonValue]) -> None:
    await value
