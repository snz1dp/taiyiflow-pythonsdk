import asyncio
from collections.abc import AsyncIterator, Callable
from pathlib import Path

import httpx
import pytest
import respx

from taiyiagent import (
    ChatRequest,
    CheckpointToolResultsAdapter,
    RecoveryError,
    ResumableCheckpoint,
    SessionConflictError,
    SessionState,
    StreamEvent,
    TaiyiAgent,
    ToolResult,
)


def event(name: str, **data: object) -> StreamEvent:
    return StreamEvent(event=name, data=data)


def make_agent(tmp_path: Path, **kwargs: object) -> TaiyiAgent:
    options: dict[str, object] = {
        "base_url": "https://example.test",
        "flow_id": "flow-1",
        "api_key": "secret",
        "stream_protocol": "sse",
        "identity_path": tmp_path / "client-id",
        "enable_builtin_tools": False,
        "permission_policy": "auto",
        "recovery_min_backoff": 0,
        "recovery_max_backoff": 0,
        "recovery_max_attempts": 1,
    }
    options.update(kwargs)
    return TaiyiAgent(**options)  # type: ignore[arg-type]


def patch_stream(
    monkeypatch: pytest.MonkeyPatch,
    agent: TaiyiAgent,
    factory: Callable[[ChatRequest], AsyncIterator[StreamEvent]],
    requests: list[ChatRequest],
) -> None:
    async def open_stream(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        requests.append(request.model_copy(deep=True))
        return factory(request)

    monkeypatch.setattr(agent, "_open_stream", open_stream)


async def initial_hold(request: ChatRequest) -> AsyncIterator[StreamEvent]:
    yield event(
        "message",
        chunk="working",
        run_id="run-1",
        execution_attempt_id="attempt-1",
        event_id="event-1",
    )
    await asyncio.Event().wait()


async def complete(request: ChatRequest) -> AsyncIterator[StreamEvent]:
    yield event("close", run_finished=True)


def checkpoint(status: str = "paused", **overrides: object) -> ResumableCheckpoint:
    values: dict[str, object] = {
        "id": "checkpoint-1",
        "flow_id": "flow-1",
        "session_id": "session-1",
        "run_id": "run-1",
        "status": status,
        "version": 3,
    }
    values.update(overrides)
    return ResumableCheckpoint(**values)  # type: ignore[arg-type]


def mock_control(status: str = "paused") -> None:
    respx.post("https://example.test/api/v1/run/sessions/session-1/pause").mock(
        return_value=httpx.Response(
            202,
            json={
                "session_id": "session-1",
                "run_id": "run-1",
                "status": status,
                "request_id": "request-1",
                "checkpoint_id": "checkpoint-1",
            },
        )
    )
    respx.post("https://example.test/api/v1/run/sessions/session-1/cancel").mock(
        return_value=httpx.Response(
            202,
            json={
                "session_id": "session-1",
                "run_id": "run-1",
                "status": "cancel_requested",
                "request_id": "request-2",
            },
        )
    )


@pytest.mark.asyncio
@respx.mock
async def test_pause_discovers_checkpoint_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_control()
    respx.get("https://example.test/api/v1/run/checkpoints/resumable").mock(
        return_value=httpx.Response(200, json=[checkpoint("ready").model_dump(mode="json")])
    )
    agent = make_agent(tmp_path)
    requests: list[ChatRequest] = []

    def factory(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return initial_hold(request) if request.execution_mode == "start" else complete(request)

    patch_stream(monkeypatch, agent, factory, requests)
    session = agent.session("session-1")
    stream = session.chat("hello")
    await stream.__anext__()
    result = await session.pause()
    assert result.status == "paused"
    assert session.state is SessionState.PAUSED

    resumed = await session.resume()
    assert [item.event async for item in resumed] == ["close"]
    assert session.state is SessionState.DONE
    assert len(requests) == 2
    resume = requests[1]
    assert resume.execution_mode == "resume"
    assert resume.input_value == ""
    assert resume.run_id == "run-1"
    assert resume.checkpoint_id == "checkpoint-1"
    assert resume.checkpoint_version == 3
    assert resume.client_id == agent.client_id
    assert resume.subscription_id
    await agent.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_attach_uses_current_execution_cursor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_control()
    agent = make_agent(tmp_path)
    requests: list[ChatRequest] = []

    def factory(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return initial_hold(request) if request.execution_mode == "start" else complete(request)

    patch_stream(monkeypatch, agent, factory, requests)
    session = agent.session("session-1")
    stream = session.chat("hello")
    await stream.__anext__()
    await session.pause()

    attached = await session.attach()
    assert [item.event async for item in attached] == ["close"]
    attach = requests[1]
    assert attach.execution_mode == "attach"
    assert attach.run_id == "run-1"
    assert attach.execution_attempt_id == "attempt-1"
    assert attach.after_event_id == "event-1"
    assert attach.tool_results is None
    await agent.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_cancelled_session_cannot_be_recovered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_control()
    agent = make_agent(tmp_path)
    requests: list[ChatRequest] = []
    patch_stream(monkeypatch, agent, initial_hold, requests)
    session = agent.session("session-1")
    stream = session.chat("hello")
    await stream.__anext__()
    await session.cancel()
    assert session.state is SessionState.CANCELLED
    with pytest.raises(SessionConflictError):
        await session.resume()
    with pytest.raises(SessionConflictError):
        await session.attach()
    await agent.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_auto_reconnect_attaches_to_active_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    respx.get("https://example.test/api/v1/run/checkpoints/resumable").mock(
        return_value=httpx.Response(200, json=[])
    )
    respx.get("https://example.test/api/v1/agents/flows/flow-1/sessions/session-1").mock(
        return_value=httpx.Response(
            200,
            json={
                "run_status": "running",
                "executing": True,
                "executing_run_id": "run-1",
                "execution_attempt_id": "attempt-2",
            },
        )
    )
    agent = make_agent(tmp_path, recovery_policy="auto")
    requests: list[ChatRequest] = []

    async def initial_eof(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield event(
            "message",
            run_id="run-1",
            execution_attempt_id="attempt-1",
            event_id="event-1",
        )

    def factory(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return initial_eof(request) if request.execution_mode == "start" else complete(request)

    patch_stream(monkeypatch, agent, factory, requests)
    session = agent.session("session-1")
    received = [item async for item in session.chat("hello")]
    assert [item.event for item in received] == ["message", "close"]
    assert session.state is SessionState.DONE
    assert requests[1].execution_mode == "attach"
    assert requests[1].execution_attempt_id == "attempt-2"
    assert requests[1].after_event_id == "event-1"
    await agent.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_auto_reconnect_prefers_checkpoint_and_honors_user_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    respx.get("https://example.test/api/v1/agents/flows/flow-1/sessions/session-1").mock(
        return_value=httpx.Response(
            200,
            json={"run_status": "running", "executing": True, "executing_run_id": "run-1"},
        )
    )
    agent = make_agent(tmp_path, recovery_policy="auto")
    requests: list[ChatRequest] = []

    async def initial_eof(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield event("message", run_id="run-1", event_id="event-1")

    def factory(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return initial_eof(request) if request.execution_mode == "start" else complete(request)

    patch_stream(monkeypatch, agent, factory, requests)
    session = agent.session("session-1")
    first = session.chat("hello")
    respx.get("https://example.test/api/v1/run/checkpoints/resumable").mock(
        return_value=httpx.Response(
            200,
            json=[checkpoint("waiting", waiting_call_ids=["call-1"]).model_dump(mode="json")],
        )
    )
    assert [item.event async for item in first] == ["message", "close"]
    assert requests[1].execution_mode == "resume"
    assert requests[1].tool_results is None

    second_session = agent.session("session-1")
    second_requests: list[ChatRequest] = []
    patch_stream(monkeypatch, agent, factory, second_requests)
    respx.get("https://example.test/api/v1/run/checkpoints/resumable").mock(
        return_value=httpx.Response(
            200,
            json=[checkpoint(requires_user_resume=True).model_dump(mode="json")],
        )
    )
    assert [item.event async for item in second_session.chat("hello")] == ["message"]
    assert second_session.state is SessionState.PAUSED
    assert len(second_requests) == 1
    await agent.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_confirmed_execution_loss_safely_starts_new_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    respx.get("https://example.test/api/v1/run/checkpoints/resumable").mock(
        return_value=httpx.Response(200, json=[])
    )
    respx.get("https://example.test/api/v1/agents/flows/flow-1/sessions/session-1").mock(
        return_value=httpx.Response(200, json={"run_status": "failed", "executing": False})
    )
    agent = make_agent(tmp_path, recovery_policy="auto")
    requests: list[ChatRequest] = []

    async def initial_eof(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield event("message", run_id="run-1", event_id="event-1")

    def factory(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return initial_eof(request) if len(requests) == 0 else complete(request)

    async def open_stream(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        is_first = not requests
        requests.append(request.model_copy(deep=True))
        return initial_eof(request) if is_first else complete(request)

    monkeypatch.setattr(agent, "_open_stream", open_stream)
    session = agent.session("session-1")
    assert [item.event async for item in session.chat("hello")] == ["message", "close"]
    assert session.state is SessionState.DONE
    restart = requests[1]
    assert restart.execution_mode == "start"
    assert restart.input_value == "hello"
    assert restart.run_id not in {None, "run-1"}
    assert restart.message_id != requests[0].message_id
    assert restart.subscription_id != requests[0].subscription_id
    await agent.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_unknown_state_and_uncertain_tools_never_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    status = respx.get("https://example.test/api/v1/agents/flows/flow-1/sessions/session-1").mock(
        return_value=httpx.Response(200, json={"run_status": "mystery", "executing": False})
    )
    respx.get("https://example.test/api/v1/run/checkpoints/resumable").mock(
        return_value=httpx.Response(200, json=[])
    )
    agent = make_agent(tmp_path, recovery_policy="auto")
    requests: list[ChatRequest] = []

    async def initial_eof(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield event("message", run_id="run-1")

    patch_stream(monkeypatch, agent, initial_eof, requests)
    session = agent.session("session-1")
    with pytest.raises(RecoveryError):
        [item async for item in session.chat("hello")]
    assert session.state is SessionState.ERROR
    assert len(requests) == 1
    assert status.call_count == 1

    uncertain = checkpoint("expired", waiting_call_ids=["call-1"], pending_call_ids=["call-2"])
    respx.get("https://example.test/api/v1/run/checkpoints/resumable").mock(
        return_value=httpx.Response(200, json=[uncertain.model_dump(mode="json")])
    )
    status.mock(return_value=httpx.Response(200, json={"run_status": "failed"}))
    other = make_agent(tmp_path / "other", recovery_policy="auto")
    other_requests: list[ChatRequest] = []
    patch_stream(monkeypatch, other, initial_eof, other_requests)
    other_session = other.session("session-1")
    with pytest.raises(RecoveryError):
        [item async for item in other_session.chat("hello")]
    assert other_session.state is SessionState.ERROR
    assert len(other_requests) == 1
    await agent.aclose()
    await other.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_retry_and_repeat_create_explicit_new_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = make_agent(tmp_path)
    requests: list[ChatRequest] = []

    async def initial(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield event("message", run_id="run-1", event_id="event-1")
        yield event("close")

    def factory(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return initial(request) if len(requests) == 1 else complete(request)

    patch_stream(monkeypatch, agent, factory, requests)
    session = agent.session("session-1")
    assert len([item async for item in session.chat("original")]) == 2
    assert [item.event async for item in await session.retry()] == ["close"]
    assert requests[1].execution_mode == "start"
    assert requests[1].input_value == "请继续完成上面的任务"
    assert requests[1].run_id not in {None, "run-1"}
    assert [item.event async for item in await session.repeat()] == ["close"]
    assert requests[2].input_value == "请继续完成上面的任务"
    await agent.aclose()


async def tool_call_and_hold(request: ChatRequest) -> AsyncIterator[StreamEvent]:
    yield event("message", run_id="run-1", event_id="event-1")
    yield event(
        "client_tool",
        function={"id": "call-1", "name": "echo", "arguments": {}},
        context={"session_id": "session-1", "run_id": "run-1", "message_id": "message-1"},
    )
    await asyncio.Event().wait()


async def wait_for_pending_outbox(agent: TaiyiAgent) -> None:
    for _ in range(20):
        if agent.outbox.list_all():
            return
        await asyncio.sleep(0)
    raise AssertionError("Tool result was not persisted")


@pytest.mark.asyncio
@respx.mock
async def test_unacknowledged_tool_result_is_submitted_with_resume_without_reexecution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_control()
    report = respx.post("https://example.test/api/v1/agents/client/call/result").mock(
        return_value=httpx.Response(500)
    )
    agent = make_agent(tmp_path)
    agent.tool_reporter._max_retries = 0
    executions = 0

    def execute(context: object, tool_call: object) -> ToolResult:
        nonlocal executions
        executions += 1
        return ToolResult.ok("call-1", {"temperature": 22})

    agent.register_tool("echo", execute)
    requests: list[ChatRequest] = []

    def factory(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return (
            tool_call_and_hold(request) if request.execution_mode == "start" else complete(request)
        )

    patch_stream(monkeypatch, agent, factory, requests)
    session = agent.session("session-1")
    stream = session.chat("weather")
    await stream.__anext__()
    await stream.__anext__()
    await wait_for_pending_outbox(agent)
    await session.pause()

    resumed = await session.resume(checkpoint("waiting", waiting_call_ids=["call-1"]))
    assert [item.event async for item in resumed] == ["close"]
    resume = requests[1]
    assert resume.execution_mode == "resume"
    assert resume.tool_results is not None
    assert resume.tool_results[0]["call_id"] == "call-1"
    assert resume.tool_results[0]["result"] == {"temperature": 22}
    assert resume.submission_id
    assert executions == 1
    assert report.call_count == 1
    assert agent.outbox.list_all() == []
    await agent.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_attach_flushes_stored_result_before_opening_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_control()
    report = respx.post("https://example.test/api/v1/agents/client/call/result").mock(
        return_value=httpx.Response(500)
    )
    agent = make_agent(tmp_path)
    agent.tool_reporter._max_retries = 0
    agent.register_tool("echo", lambda _context, call: ToolResult.ok(call.id, "done"))
    requests: list[ChatRequest] = []

    def factory(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return (
            tool_call_and_hold(request) if request.execution_mode == "start" else complete(request)
        )

    patch_stream(monkeypatch, agent, factory, requests)
    session = agent.session("session-1")
    stream = session.chat("hello")
    await stream.__anext__()
    await stream.__anext__()
    await wait_for_pending_outbox(agent)
    await session.pause()
    report.mock(return_value=httpx.Response(200, json={}))

    attached = await session.attach()
    assert [item.event async for item in attached] == ["close"]
    assert report.call_count == 2
    assert report.calls[0].request.content == report.calls[1].request.content
    assert requests[1].execution_mode == "attach"
    assert requests[1].tool_results is None
    assert agent.outbox.list_all() == []
    await agent.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_observed_tool_call_prevents_automatic_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    respx.post("https://example.test/api/v1/agents/client/call/result").mock(
        return_value=httpx.Response(200, json={})
    )
    respx.get("https://example.test/api/v1/run/checkpoints/resumable").mock(
        return_value=httpx.Response(200, json=[])
    )
    respx.get("https://example.test/api/v1/agents/flows/flow-1/sessions/session-1").mock(
        return_value=httpx.Response(200, json={"run_status": "failed", "executing": False})
    )
    agent = make_agent(tmp_path, recovery_policy="auto")
    executions = 0

    def execute(context: object, call: object) -> ToolResult:
        nonlocal executions
        executions += 1
        return ToolResult.ok("call-1", "done")

    agent.register_tool("echo", execute)
    requests: list[ChatRequest] = []

    async def tool_call_and_eof(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield event(
            "client_tool",
            function={"id": "call-1", "name": "echo", "arguments": {}},
            context={"session_id": "session-1", "run_id": "run-1"},
        )

    patch_stream(monkeypatch, agent, tool_call_and_eof, requests)
    session = agent.session("session-1")
    with pytest.raises(RecoveryError):
        [item async for item in session.chat("hello")]
    assert session.state is SessionState.ERROR
    assert len(requests) == 1
    assert executions == 1
    await agent.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_idle_session_recovers_stored_checkpoint_without_tool_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = make_agent(tmp_path)
    stored_checkpoint = checkpoint("waiting", waiting_call_ids=["call-1"])
    adapter = CheckpointToolResultsAdapter(agent.outbox)
    prepared = adapter.prepare(
        stored_checkpoint,
        "flow-1",
        "session-1",
        {
            "call-1": {
                "flow_id": "flow-1",
                "session_id": "session-1",
                "run_id": "run-1",
                "call_id": "call-1",
                "success": True,
                "result": "stored-result",
            }
        },
    )
    assert prepared.entry is not None
    respx.get("https://example.test/api/v1/run/checkpoints/resumable").mock(
        return_value=httpx.Response(200, json=[stored_checkpoint.model_dump(mode="json")])
    )
    requests: list[ChatRequest] = []
    patch_stream(monkeypatch, agent, complete, requests)

    session = agent.session("session-1")
    recovered = await session.recover_report(prepared.entry)
    assert recovered is not None
    assert [item.event async for item in recovered] == ["close"]
    assert session.state is SessionState.DONE
    assert requests[0].execution_mode == "resume"
    assert requests[0].tool_results is not None
    assert requests[0].tool_results[0]["result"] == "stored-result"
    assert agent.outbox.list_all() == []
    await agent.aclose()
