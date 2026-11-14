import asyncio
from collections.abc import AsyncIterator, Callable
from pathlib import Path

import pytest

from taiyiagent import (
    SessionHook,
    SessionState,
    StreamEvent,
    StreamHook,
    TaiyiAgent,
    TransportError,
)
from taiyiagent.errors import SessionConflictError
from taiyiagent.models import ChatRequest, StreamProtocol
from taiyiagent.transport import StreamConnectionError


def event(name: str, **data: object) -> StreamEvent:
    return StreamEvent(event=name, data=data)


def make_agent(tmp_path: Path, **kwargs: object) -> TaiyiAgent:
    options: dict[str, object] = {
        "base_url": "https://example.test",
        "flow_id": "flow-id",
        "api_key": "secret",
        "stream_protocol": StreamProtocol.SSE,
        "identity_path": tmp_path / "client-id",
    }
    options.update(kwargs)
    return TaiyiAgent(**options)  # type: ignore[arg-type]


def patch_stream(
    monkeypatch: pytest.MonkeyPatch,
    agent: TaiyiAgent,
    factory: Callable[[ChatRequest], AsyncIterator[StreamEvent]],
) -> None:
    async def open_stream(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return factory(request)

    monkeypatch.setattr(agent, "_open_stream", open_stream)


class RecordingStreamHook(StreamHook):
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def on_event(self, session_id: str, stream_event: StreamEvent) -> None:
        self.calls.append(f"event:{stream_event.event}")

    async def on_chunk(self, session_id: str, chunk: str) -> None:
        self.calls.append(f"chunk:{chunk}")

    async def on_error(self, session_id: str, error: BaseException) -> None:
        self.calls.append(f"error:{error}")

    async def on_close(self, session_id: str, reason: str | None) -> None:
        self.calls.append(f"close:{reason}")


class RecordingSessionHook(SessionHook):
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def on_start(self, session_id: str) -> None:
        self.calls.append("start")

    async def on_finish(self, session_id: str) -> None:
        self.calls.append("finish")

    async def on_interrupt(self, session_id: str, reason: str) -> None:
        self.calls.append(f"interrupt:{reason}")


@pytest.mark.asyncio
async def test_session_streams_events_and_orders_hooks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stream_hook = RecordingStreamHook()
    session_hook = RecordingSessionHook()
    agent = make_agent(tmp_path, stream_hooks=[stream_hook], session_hooks=[session_hook])
    requests: list[ChatRequest] = []

    async def events(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        requests.append(request)
        yield event("message", chunk="hello", run_id="run-1", event_id="event-1")
        yield event("close", reason="done")

    patch_stream(monkeypatch, agent, events)
    session = agent.session("session-1")
    stream = session.chat("hello", input_files=["file.txt"])
    received = [item async for item in stream]
    assert [item.event for item in received] == ["message", "close"]
    assert session.state is SessionState.DONE
    assert session.run_context.run_id == "run-1"
    assert session.run_context.after_event_id == "event-1"
    assert requests[0].execution_mode == "start"
    assert requests[0].input_files == ["file.txt"]
    assert requests[0].client_id == agent.client_id
    assert requests[0].message_id
    assert requests[0].subscription_id
    assert stream_hook.calls == [
        "event:message",
        "chunk:hello",
        "event:close",
        "close:Execution completed",
    ]
    assert session_hook.calls == ["start", "finish"]
    await agent.aclose()


@pytest.mark.asyncio
async def test_sessions_run_independently(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    agent = make_agent(tmp_path)

    release = asyncio.Event()

    async def events(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        await asyncio.sleep(0)
        yield event("message", chunk=request.session_id)
        await release.wait()
        yield event("close")

    patch_stream(monkeypatch, agent, events)
    first = agent.session("first")
    second = agent.session("second")
    first_events, second_events = await asyncio.gather(
        first.chat("one").__anext__(), second.chat("two").__anext__()
    )
    assert first_events.message_text == "first"
    assert second_events.message_text == "second"
    assert first.state is SessionState.STREAMING
    assert second.state is SessionState.STREAMING
    release.set()
    assert await first.wait(0.1)
    assert await second.wait(0.1)
    assert first.state is SessionState.DONE
    assert second.state is SessionState.DONE
    await agent.aclose()


@pytest.mark.asyncio
async def test_active_session_rejects_concurrent_chat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = make_agent(tmp_path)
    entered = asyncio.Event()

    async def events(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        entered.set()
        await asyncio.Event().wait()
        yield event("close")

    patch_stream(monkeypatch, agent, events)
    session = agent.session()
    stream = session.chat("one")
    await entered.wait()
    with pytest.raises(SessionConflictError):
        session.chat("two")
    await stream.aclose()
    assert session.state is SessionState.INTERRUPTED
    await agent.aclose()


@pytest.mark.asyncio
async def test_interrupt_wakes_blocked_consumer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = make_agent(tmp_path)

    async def events(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield event("message", chunk="first")
        await asyncio.Event().wait()
        yield event("close")

    patch_stream(monkeypatch, agent, events)
    session = agent.session()
    stream = session.chat("hello")
    assert (await stream.__anext__()).message_text == "first"
    pending = asyncio.create_task(stream.__anext__())
    await asyncio.sleep(0)
    await stream.aclose()
    with pytest.raises(StopAsyncIteration):
        await pending
    await agent.aclose()


@pytest.mark.asyncio
async def test_close_does_not_wait_for_uncooperative_producer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = make_agent(tmp_path)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def events(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()
        yield event("close")

    patch_stream(monkeypatch, agent, events)
    stream = agent.session().chat("hello")
    await entered.wait()
    await asyncio.wait_for(stream.aclose(), 0.05)
    assert not stream._task.done()
    release.set()
    assert await stream.wait(0.1)
    await agent.aclose()


@pytest.mark.asyncio
async def test_wait_propagates_caller_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = make_agent(tmp_path)

    async def events(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        await asyncio.Event().wait()
        yield event("close")

    patch_stream(monkeypatch, agent, events)
    stream = agent.session().chat("hello")
    waiting = asyncio.create_task(stream.wait())
    await asyncio.sleep(0)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert stream._task is not None and not stream._task.done()
    await stream.aclose()
    await agent.aclose()


@pytest.mark.asyncio
async def test_backpressure_drops_heartbeats_but_preserves_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = make_agent(tmp_path, event_queue_size=1)

    async def events(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield event("heartbeat")
        yield event("heartbeat")
        yield event("custom", call_id="call-1")
        yield event("close")

    patch_stream(monkeypatch, agent, events)
    session = agent.session()
    stream = session.chat("hello")
    await asyncio.sleep(0.01)
    received = [item async for item in stream]
    assert [item.event for item in received] == ["custom", "close"]
    assert session.dropped_heartbeats == 2
    await agent.aclose()


@pytest.mark.asyncio
async def test_stream_errors_reach_consumer_and_hooks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hook = RecordingStreamHook()
    agent = make_agent(tmp_path, stream_hooks=[hook])

    async def events(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield event("message", chunk="partial")
        raise TransportError("connection lost")

    patch_stream(monkeypatch, agent, events)
    session = agent.session()
    stream = session.chat("hello")
    assert (await stream.__anext__()).message_text == "partial"
    with pytest.raises(TransportError, match="connection lost"):
        await stream.__anext__()
    assert session.state is SessionState.ERROR
    assert "error:connection lost" in hook.calls
    await agent.aclose()


@pytest.mark.asyncio
async def test_missing_close_event_does_not_report_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = make_agent(tmp_path)

    async def events(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield event("message", chunk="partial")

    patch_stream(monkeypatch, agent, events)
    session = agent.session()
    assert len([item async for item in session.chat("hello")]) == 1
    assert session.state is SessionState.DISCONNECTED
    await agent.aclose()


@pytest.mark.asyncio
async def test_auto_protocol_falls_back_only_for_connection_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = make_agent(tmp_path, stream_protocol=StreamProtocol.AUTO)
    sse_calls = 0

    async def resolve() -> StreamProtocol:
        return StreamProtocol.WEBSOCKET

    async def websocket_stream(self: object, request: ChatRequest) -> AsyncIterator[StreamEvent]:
        raise StreamConnectionError("connect failed")
        yield event("message")

    async def sse_stream(
        self: object,
        request: ChatRequest,
        on_session_id: Callable[[str], None] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        nonlocal sse_calls
        sse_calls += 1
        yield event("message", chunk="fallback")
        yield event("close")

    monkeypatch.setattr(agent.protocol, "resolve", resolve)
    monkeypatch.setattr("taiyiagent.agent.WebSocketStreamClient.stream", websocket_stream)
    monkeypatch.setattr("taiyiagent.agent.SseStreamClient.stream", sse_stream)
    stream = await agent._open_stream(ChatRequest(input_value="hello"))
    events = [item async for item in stream]
    assert events[0].message_text == "fallback"
    assert sse_calls == 1
    await agent.aclose()


@pytest.mark.asyncio
async def test_established_websocket_failure_never_replays_through_sse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = make_agent(tmp_path, stream_protocol=StreamProtocol.AUTO)
    sse_calls = 0

    async def resolve() -> StreamProtocol:
        return StreamProtocol.WEBSOCKET

    async def websocket_stream(self: object, request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield event("message", chunk="partial")
        raise TransportError("established connection failed")

    async def sse_stream(
        self: object,
        request: ChatRequest,
        on_session_id: Callable[[str], None] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        nonlocal sse_calls
        sse_calls += 1
        yield event("close")

    monkeypatch.setattr(agent.protocol, "resolve", resolve)
    monkeypatch.setattr("taiyiagent.agent.WebSocketStreamClient.stream", websocket_stream)
    monkeypatch.setattr("taiyiagent.agent.SseStreamClient.stream", sse_stream)
    stream = await agent._open_stream(ChatRequest(input_value="hello"))
    with pytest.raises(TransportError, match="established connection failed"):
        async for _ in stream:
            pass
    assert sse_calls == 0
    await agent.aclose()


@pytest.mark.asyncio
async def test_sse_header_resolves_session_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = make_agent(tmp_path, enable_builtin_tools=False)
    registered: list[bool] = []
    session = agent.session("client-session")

    async def sse_stream(
        self: object,
        request: ChatRequest,
        on_session_id: Callable[[str], None] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        assert on_session_id is not None
        on_session_id("server-session")
        registered.append(agent.sessions.get("server-session") is session)
        yield event("close")

    monkeypatch.setattr("taiyiagent.agent.SseStreamClient.stream", sse_stream)
    assert len([item async for item in session.chat("hello")]) == 1
    assert registered == [True]
    assert session.session_id == "server-session"
    assert session.run_context.last_request is not None
    assert session.run_context.last_request.session_id == "server-session"
    await agent.aclose()


@pytest.mark.asyncio
async def test_client_identity_is_reused(tmp_path: Path) -> None:
    first = make_agent(tmp_path)
    second = make_agent(tmp_path)
    assert first.client_id == second.client_id
    assert (tmp_path / "client-id").read_text() == first.client_id
    await first.aclose()
    await second.aclose()


@pytest.mark.asyncio
async def test_client_identity_creation_is_thread_safe(tmp_path: Path) -> None:
    agents = await asyncio.gather(*(asyncio.to_thread(make_agent, tmp_path) for _ in range(8)))
    assert len({agent.client_id for agent in agents}) == 1
    await asyncio.gather(*(agent.aclose() for agent in agents))
