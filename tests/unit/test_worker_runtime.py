from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import respx

from taiyiagent import ToolExecutionContext
from taiyiagent.collaboration import TaskCancel, TaskDispatch
from taiyiagent.errors import ApiError, ReportConflictError, ToolExecutionError
from taiyiagent.hooks import PermissionDecision, PermissionHook, PermissionPolicy
from taiyiagent.models import ToolCall, ToolResult
from taiyiagent.transport import JsonValue
from taiyiagent.worker import (
    Ability,
    AgentSessionTaskHandler,
    ApprovalChannel,
    DigitalWorker,
    PlatformTaskReporter,
    TaskExecutionState,
    TaskExecutor,
    WorkerApprovalHook,
    WorkerAuth,
    WorkerIdentity,
    WorkerRegistrationResult,
    WorkerRole,
    WorkerState,
)


class Handler:
    def __init__(self, result: JsonValue = None) -> None:
        self.result = result
        self.calls = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(self, task: TaskDispatch, progress: object) -> JsonValue:
        self.calls += 1
        self.started.set()
        await self.release.wait()
        return self.result


class Reporter:
    def __init__(self, *, fail_running: bool = False, fail_success: int = 0) -> None:
        self.fail_running = fail_running
        self.fail_success = fail_success
        self.running_calls = 0
        self.success_calls = 0
        self.failure_calls = 0
        self.cancel_calls = 0

    async def running(self, task: TaskDispatch, progress: JsonValue = None) -> None:
        self.running_calls += 1
        if self.fail_running:
            raise RuntimeError("running report failed")

    async def success(self, task: TaskDispatch, result: JsonValue) -> None:
        self.success_calls += 1
        if self.success_calls <= self.fail_success:
            raise RuntimeError("success report failed")

    async def failure(self, task: TaskDispatch, error: str) -> None:
        self.failure_calls += 1

    async def cancelled(self, task: TaskDispatch, cancel_request_id: str) -> None:
        self.cancel_calls += 1


class IdentityValidator:
    def __init__(self) -> None:
        self.identity: WorkerIdentity | None = None

    def validate(
        self,
        identity: WorkerIdentity,
        role: WorkerRole,
        abilities: frozenset[Ability],
    ) -> None:
        self.identity = identity


class Registrar:
    async def register(self, request: object) -> WorkerRegistrationResult:
        return WorkerRegistrationResult(WorkerIdentity("registered-worker", "Worker"))


class FakeCollaboration:
    def __init__(self) -> None:
        self.is_connected = False
        self.dispatch_handler: object = None
        self.cancel_handler: object = None
        self.snapshot_handler: object = None
        self.ready_handler: object = None
        self.kicked_handler: object = None
        self.closed = False

    def on_task_dispatch(self, handler: object) -> FakeCollaboration:
        self.dispatch_handler = handler
        return self

    def on_task_cancel(self, handler: object) -> FakeCollaboration:
        self.cancel_handler = handler
        return self

    def on_task_snapshot(self, handler: object) -> FakeCollaboration:
        self.snapshot_handler = handler
        return self

    def on_connection_ready(self, handler: object) -> FakeCollaboration:
        self.ready_handler = handler
        return self

    def on_kicked(self, handler: object) -> FakeCollaboration:
        self.kicked_handler = handler
        return self

    async def start(self) -> None:
        self.is_connected = True
        await self.ready_handler({})

    async def aclose(self) -> None:
        self.closed = True
        self.is_connected = False

    async def dispatch(self, task: TaskDispatch) -> bool:
        return await _maybe_await(self.dispatch_handler(task))


def task(task_id: str = "task-1", *, kind: str = "remote_task", version: int = 1) -> TaskDispatch:
    return TaskDispatch(
        task_id=task_id,
        task_kind=kind,
        task_type="task",
        version=version,
        flow_id="flow-1",
        input_value="hello",
        session_id="session-1",
        child_session_id="",
        child_run_id="",
        source_session_id="",
        source_run_id="",
        source_message_id="",
        schedule_id="",
        run_record_id="",
        request_mode="agent",
        instance_id="",
        payload={},
    )


@pytest.mark.asyncio
async def test_running_report_gates_handler_and_terminal_retry_does_not_rerun() -> None:
    gated_handler = Handler({"ok": True})
    gated = TaskExecutor(gated_handler, Reporter(fail_running=True))
    reservation = gated.prepare(task())
    assert await gated.execute(reservation) is TaskExecutionState.REPORT_UNKNOWN
    assert gated_handler.calls == 0

    handler = Handler({"ok": True})
    handler.release.set()
    reporter = Reporter(fail_success=1)
    executor = TaskExecutor(handler, reporter)
    reservation = executor.prepare(task())
    assert await executor.execute(reservation) is TaskExecutionState.REPORT_UNKNOWN
    assert handler.calls == 1
    await executor.flush_unknown_reports()
    assert executor.state_of(task()) is TaskExecutionState.SUCCESS
    assert reporter.success_calls == 2
    assert handler.calls == 1
    assert executor.prepare(task()).accepted is False


@pytest.mark.asyncio
async def test_cancellation_is_idempotent_and_reports_cancelled() -> None:
    handler = Handler()
    reporter = Reporter()
    executor = TaskExecutor(handler, reporter)
    reservation = executor.prepare(task())
    runner = asyncio.create_task(executor.execute(reservation))
    executor.attach(reservation, runner)
    await handler.started.wait()
    cancel = TaskCancel(
        task_id="task-1",
        task_kind="remote_task",
        version=1,
        source_principal_type="user",
        correlation_id="",
        cancel_request_id="cancel-1",
        cancel_requested_at="",
        payload={},
    )
    assert await executor.cancel(cancel) is True
    assert await executor.cancel(cancel) is True
    assert await runner is TaskExecutionState.CANCELLED
    assert reporter.cancel_calls == 1
    assert executor.state_of(task()) is TaskExecutionState.CANCELLED


@pytest.mark.asyncio
async def test_worker_routes_custom_handler_and_finishes_online(tmp_path: Path) -> None:
    collaboration = FakeCollaboration()
    handler = Handler({"custom": True})
    handler.release.set()
    reporter = Reporter()
    validator = IdentityValidator()
    worker = DigitalWorker(
        base_url="https://example.test",
        auth=WorkerAuth.api_key("secret"),
        workspace=tmp_path / "workspace",
        identity=WorkerIdentity("worker-1", "Worker"),
        identity_validator=validator,
        task_handler=handler,
        task_reporter=reporter,
        collaboration=collaboration,  # type: ignore[arg-type]
    )
    await worker.start()
    assert worker.state is WorkerState.ONLINE
    assert validator.identity == WorkerIdentity("worker-1", "Worker")
    assert await collaboration.dispatch(task()) is True
    await asyncio.wait_for(handler.started.wait(), 1)
    for _ in range(100):
        if worker.task_state(task()) is TaskExecutionState.SUCCESS:
            break
        await asyncio.sleep(0)
    assert worker.task_state(task()) is TaskExecutionState.SUCCESS
    assert handler.calls == 1
    assert reporter.running_calls == 1
    assert reporter.success_calls == 1
    await worker.pause()
    assert worker.state is WorkerState.PAUSED
    assert await collaboration.dispatch(task("paused-task")) is False
    await worker.resume()
    await worker.aclose()
    assert collaboration.closed
    assert worker.state is WorkerState.OFFLINE


@pytest.mark.asyncio
async def test_runtime_limits_active_operations_and_queues_excess_work() -> None:
    from taiyiagent.worker.worker import WorkerRuntime

    runtime = WorkerRuntime(2, queue_capacity=2)
    runtime.online()
    release = asyncio.Event()
    active = 0
    max_active = 0
    started = 0

    async def operation() -> None:
        nonlocal active, max_active, started
        started += 1
        active += 1
        max_active = max(max_active, active)
        await release.wait()
        active -= 1

    tasks = [runtime.submit(operation) for _ in range(4)]
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert started == 2
    assert max_active == 2
    assert runtime.active_tasks == 4
    release.set()
    await asyncio.gather(*tasks)
    await runtime.aclose()


@pytest.mark.asyncio
async def test_queued_task_cancellation_never_starts_handler() -> None:
    reporter = Reporter()
    handler = Handler()
    executor = TaskExecutor(handler, reporter)
    reservation = executor.prepare(task())
    assert reservation is not None
    runner = asyncio.create_task(executor.execute(reservation))
    executor.attach(reservation, runner)

    cancel = TaskCancel(
        task_id="task-1",
        task_kind="remote_task",
        version=1,
        source_principal_type="user",
        correlation_id="",
        cancel_request_id="cancel-1",
        cancel_requested_at="",
        payload={},
    )
    assert await executor.cancel(cancel)
    with pytest.raises(asyncio.CancelledError):
        await runner
    assert reporter.cancel_calls == 1
    assert handler.calls == 0


@pytest.mark.asyncio
async def test_default_session_handler_serializes_same_flow_and_session() -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.session_id = "session-1"
            self.state = "done"
            self.run_context = SimpleNamespace(run_id="run-1")
            self.calls = 0
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        def chat(self, message: str) -> object:
            self.calls += 1

            class Stream:
                def __aiter__(stream_self: object) -> object:
                    return stream_self

                async def __anext__(stream_self: object) -> object:
                    self.started.set()
                    await self.release.wait()
                    raise StopAsyncIteration

                async def aclose(stream_self: object) -> None:
                    return None

            return Stream()

    from taiyiagent.models import SessionState

    session = FakeSession()
    session.state = SessionState.DONE

    class FakeAgent:
        def session(self, session_id: str | None = None) -> FakeSession:
            return session

        async def aclose(self) -> None:
            return None

    handler = AgentSessionTaskHandler(lambda flow_id: FakeAgent())  # type: ignore[return-value]
    first = asyncio.create_task(handler.execute(task(), _ignore))
    await session.started.wait()
    second = asyncio.create_task(handler.execute(task(), _ignore))
    await asyncio.sleep(0)
    assert session.calls == 1
    session.release.set()
    await asyncio.gather(first, second)
    assert session.calls == 2
    await handler.aclose()


@pytest.mark.asyncio
async def test_worker_registers_identity_before_ready(tmp_path: Path) -> None:
    collaboration = FakeCollaboration()
    validator = IdentityValidator()
    worker = DigitalWorker(
        base_url="https://example.test",
        auth=WorkerAuth.api_key("secret"),
        workspace=tmp_path,
        display_name="Worker",
        worker_registrar=Registrar(),
        identity_validator=validator,
        task_handler=Handler(),
        task_reporter=Reporter(),
        collaboration=collaboration,  # type: ignore[arg-type]
    )
    await worker.start()
    assert worker.identity == WorkerIdentity("registered-worker", "Worker")
    assert validator.identity == worker.identity
    await worker.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_worker_agent_refreshes_dynamic_auth_without_replaying_static_401(
    tmp_path: Path,
) -> None:
    class RefreshingAuth(WorkerAuth):
        def __init__(self) -> None:
            super().__init__()
            self.token = "old-token"
            self.refresh_calls = 0

        @property
        def supports_refresh(self) -> bool:
            return True

        def access_token(self) -> str:
            return self.token

        async def _authenticate(self, base_url: str) -> None:
            return None

        async def _refresh(self) -> None:
            self.refresh_calls += 1
            self.token = "new-token"

    dynamic_auth = RefreshingAuth()
    dynamic_worker = DigitalWorker(
        base_url="https://example.test",
        auth=dynamic_auth,
        workspace=tmp_path / "dynamic",
        identity=WorkerIdentity("worker-1", "Worker"),
        identity_validator=IdentityValidator(),
        task_reporter=Reporter(),
    )
    dynamic_agent = dynamic_worker._new_agent("flow-1")
    route = respx.get("https://example.test/api/v1/protected").mock(
        side_effect=lambda request: httpx.Response(
            200 if request.headers.get("Authorization") == "Bearer new-token" else 401,
            json={"ok": True},
        )
    )
    assert await dynamic_agent.rest.get("/api/v1/protected") == {"ok": True}
    assert dynamic_auth.refresh_calls == 1
    assert route.call_count == 2
    await dynamic_agent.aclose()
    await dynamic_worker.aclose()

    static_worker = DigitalWorker(
        base_url="https://example.test",
        auth=WorkerAuth.api_key("static-token"),
        workspace=tmp_path / "static",
        identity=WorkerIdentity("worker-2", "Worker"),
        identity_validator=IdentityValidator(),
        task_reporter=Reporter(),
    )
    static_agent = static_worker._new_agent("flow-1")
    static_route = respx.post("https://example.test/api/v1/protected").respond(401)
    with pytest.raises(ApiError):
        await static_agent.rest.post("/api/v1/protected", {"side_effect": True})
    assert static_route.call_count == 1
    await static_agent.aclose()
    await static_worker.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_platform_reporter_uses_version_and_detects_terminal_conflict() -> None:
    route = respx.get("https://example.test/api/v1/agents/client/collaborations/task-1").mock(
        side_effect=[
            httpx.Response(200, json={"status": "running", "version": 3}),
            httpx.Response(200, json={"status": "failed", "version": 4}),
            httpx.Response(
                200,
                json={
                    "status": "success",
                    "version": 5,
                    "result_payload": {"result": "done"},
                },
            ),
        ]
    )
    patch = respx.patch("https://example.test/api/v1/agents/client/collaborations/task-1").mock(
        return_value=httpx.Response(200, json={"status": "success"})
    )
    reporter = PlatformTaskReporter("https://example.test", WorkerAuth.api_key("secret"))
    await reporter.success(task(), "done")
    assert patch.calls[0].request.content == (
        b'{"status":"success","result_payload":{"result":"done"},"expected_version":3}'
    )
    with pytest.raises(ReportConflictError):
        await reporter.success(task(), "changed")
    await reporter.success(task(), "done")
    assert route.call_count == 3
    assert patch.call_count == 1
    await reporter.aclose()


@pytest.mark.asyncio
async def test_worker_approval_requires_adapter_and_audits_local_decision(tmp_path: Path) -> None:
    hook = WorkerApprovalHook(PermissionPolicy.CONFIRM, ApprovalChannel.PLATFORM)
    with pytest.raises(ToolExecutionError, match="Platform approval"):
        await hook.on_permission("session", ToolCall(name="shell"))

    class Allow(PermissionHook):
        async def on_permission(self, session_id: str, tool_call: ToolCall) -> PermissionDecision:
            return PermissionDecision.ALLOW

    class Audit:
        def __init__(self) -> None:
            self.calls = 0

        async def record(
            self,
            session_id: str,
            tool_call: ToolCall,
            decision: PermissionDecision,
        ) -> None:
            self.calls += 1

    local_without_audit = WorkerApprovalHook(
        PermissionPolicy.CONFIRM,
        ApprovalChannel.LOCAL,
        approval_handler=Allow(),
    )
    with pytest.raises(ToolExecutionError, match="approval audit"):
        await local_without_audit.on_permission("session", ToolCall(name="shell"))
    audit = Audit()
    local = WorkerApprovalHook(
        PermissionPolicy.CONFIRM,
        ApprovalChannel.LOCAL,
        approval_handler=Allow(),
        approval_audit=audit,
    )
    assert await local.on_permission("session", ToolCall(name="shell")) is PermissionDecision.ALLOW
    assert audit.calls == 1

    worker = DigitalWorker(
        base_url="https://example.test",
        auth=WorkerAuth.api_key("secret"),
        workspace=tmp_path,
        identity=WorkerIdentity("worker-1", "Worker"),
        identity_validator=IdentityValidator(),
        approval_handler=Allow(),
        task_reporter=Reporter(),
    )
    agent = worker._new_agent("flow-1")

    async def write_file(context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        return ToolResult(tool_call_id=call.id, success=True, result="written")

    agent.register_tool("write_file", write_file)
    result = await asyncio.wait_for(
        agent.tools.dispatch(
            ToolExecutionContext(session_id="session"),
            ToolCall(id="call-1", name="write_file"),
        ),
        timeout=0.1,
    )
    assert result.success
    assert result.result == "written"
    await agent.aclose()
    await worker.aclose()


async def _ignore(progress: JsonValue) -> None:
    return None


async def _maybe_await(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return bool(await value)  # type: ignore[misc]
