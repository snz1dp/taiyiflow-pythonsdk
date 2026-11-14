import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import respx

from taiyiagent import (
    ChatRequest,
    PermissionDecision,
    PermissionHook,
    PermissionPolicy,
    StreamEvent,
    TaiyiAgent,
    ToolCall,
    ToolExecutionContext,
    ToolExecutionError,
    ToolExecutionHook,
    ToolResult,
)
from taiyiagent.plugins import PluginManager
from taiyiagent.tools import ApprovalBroker, ToolDispatcher, ToolResultReporter
from taiyiagent.transport import RestClient


def call(name: str = "echo", **arguments: object) -> ToolCall:
    return ToolCall(
        id="call-1",
        name=name,
        arguments=arguments,
        flow_id="flow-1",
        session_id="session-1",
        run_id="run-1",
        message_id="message-1",
    )


def context(session_id: str = "session-1") -> ToolExecutionContext:
    return ToolExecutionContext(session_id=session_id, run_id="run-1")


class RecordingHook(ToolExecutionHook):
    def __init__(self, order: list[str], *, takeover: bool = False) -> None:
        self.order = order
        self.takeover_result = takeover

    async def before(self, session_id: str, tool_call: ToolCall) -> ToolCall:
        self.order.append("before")
        modified = tool_call.model_copy(deep=True)
        modified.arguments = {**modified.arguments_dict, "before": True}
        return modified

    async def takeover(self, session_id: str, tool_call: ToolCall) -> ToolResult | None:
        self.order.append("takeover")
        if self.takeover_result:
            return ToolResult.ok(tool_call.id, "taken-over")
        return None

    async def after(self, session_id: str, tool_call: ToolCall, result: ToolResult) -> ToolResult:
        self.order.append("after")
        return ToolResult.ok(result.tool_call_id, {"wrapped": result.result})


@pytest.mark.asyncio
async def test_dispatcher_runs_complete_hook_chain() -> None:
    order: list[str] = []
    plugins = PluginManager()

    async def execute(tool_context: ToolExecutionContext, tool_call: ToolCall) -> ToolResult:
        order.append("execute")
        assert tool_call.arguments_dict["before"] is True
        return ToolResult.ok(tool_call.id, "executed")

    plugins.register("echo", execute)
    dispatcher = ToolDispatcher(plugins, permission_policy=PermissionPolicy.AUTO)
    result = await dispatcher.dispatch(context(), call(), tool_hooks=[RecordingHook(order)])
    assert order == ["before", "takeover", "execute", "after"]
    assert result.result == {"wrapped": "executed"}


@pytest.mark.asyncio
async def test_takeover_skips_plugin_but_runs_after_hook() -> None:
    order: list[str] = []
    plugins = PluginManager()
    plugins.register(
        "echo",
        lambda _context, _call: (_ for _ in ()).throw(AssertionError("must not execute")),
    )
    dispatcher = ToolDispatcher(plugins, permission_policy=PermissionPolicy.AUTO)
    result = await dispatcher.dispatch(
        context(), call(), tool_hooks=[RecordingHook(order, takeover=True)]
    )
    assert order == ["before", "takeover", "after"]
    assert result.result == {"wrapped": "taken-over"}


@pytest.mark.asyncio
async def test_permission_approval_blocks_execution() -> None:
    plugins = PluginManager()
    executed = asyncio.Event()
    plugins.register(
        "write_file",
        lambda _context, _call: executed.set() or ToolResult.ok("call-1", True),
    )
    dispatcher = ToolDispatcher(
        plugins,
        permission_policy=PermissionPolicy.CONFIRM,
        permission_timeout=1,
    )
    pending = asyncio.create_task(dispatcher.dispatch(context(), call("write_file")))
    await asyncio.sleep(0)
    assert not executed.is_set()
    assert dispatcher.approvals.approve("call-1")
    result = await pending
    assert result.success
    assert executed.is_set()
    assert not dispatcher.approvals.approve("missing")


@pytest.mark.asyncio
async def test_permission_hook_override_replaces_local_policy_wait() -> None:
    class Allow(PermissionHook):
        async def on_permission(self, session_id: str, tool_call: ToolCall) -> PermissionDecision:
            return PermissionDecision.ALLOW

    plugins = PluginManager()
    plugins.register("write_file", lambda _context, call: ToolResult.ok(call.id, "written"))
    dispatcher = ToolDispatcher(
        plugins,
        permission_policy=PermissionPolicy.CONFIRM,
        permission_hook=Allow(),
        permission_timeout=0.01,
    )
    result = await dispatcher.dispatch(context(), call("write_file"))
    assert result.success
    assert result.result == "written"


@pytest.mark.asyncio
async def test_permission_timeout_and_hook_denial_fail_closed() -> None:
    plugins = PluginManager()
    plugins.register("write_file", lambda _context, call: ToolResult.ok(call.id))
    dispatcher = ToolDispatcher(
        plugins,
        permission_policy=PermissionPolicy.CONFIRM,
        permission_timeout=0.01,
    )
    result = await dispatcher.dispatch(context(), call("write_file"))
    assert not result.success
    assert result.error == "Permission check timed out"

    class Deny(PermissionHook):
        async def on_permission(self, session_id: str, tool_call: ToolCall) -> PermissionDecision:
            return PermissionDecision.DENY

    allowed = ToolDispatcher(plugins, permission_policy=PermissionPolicy.AUTO)
    denied = await allowed.dispatch(context(), call("write_file"), permission_hooks=[Deny()])
    assert not denied.success
    assert denied.error == "Permission denied by hook"


@pytest.mark.asyncio
async def test_ask_and_plan_modes_reject_mutating_tools() -> None:
    dispatcher = ToolDispatcher(PluginManager(), permission_policy=PermissionPolicy.AUTO)
    result = await dispatcher.dispatch(
        ToolExecutionContext(session_id="session", request_mode="plan"),
        call("write_file"),
    )
    assert not result.success
    assert "disabled" in (result.error or "")


@pytest.mark.asyncio
async def test_duplicate_pending_approval_is_rejected() -> None:
    broker = ApprovalBroker()
    first = asyncio.create_task(broker.wait_for("session", "call", 1))
    await asyncio.sleep(0)
    with pytest.raises(ValueError, match="already pending"):
        await broker.wait_for("session", "call", 1)
    assert broker.deny("call", "duplicate")
    approval = await first
    assert approval.decision is PermissionDecision.DENY
    assert approval.reason == "duplicate"


@pytest.mark.asyncio
@respx.mock
async def test_reporter_uses_protocol_payload_and_retries() -> None:
    route = respx.post("https://example.test/api/v1/agents/client/call/result").mock(
        side_effect=[httpx.Response(500), httpx.Response(200, json={})]
    )
    rest = RestClient("https://example.test", "secret")
    reporter = ToolResultReporter(rest, max_retries=1, initial_retry_delay=0)
    tool_call = ToolCall(
        id="call-1",
        name="echo",
        flow_id="flow-1",
        session_id="session-1",
        run_id="run-1",
        message_id="message-1",
    )
    await reporter.report(tool_call, ToolResult.fail("different", "failed"))
    payload = route.calls[1].request.content.decode()
    assert '"call_id":"call-1"' in payload
    assert '"success":false' in payload
    assert '"error_message":"failed"' in payload
    assert route.call_count == 2
    await rest.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_reporter_treats_paused_call_as_delivered() -> None:
    route = respx.post("https://example.test/api/v1/agents/client/call/result").mock(
        return_value=httpx.Response(400, json={"detail": {"code": "tool_call_paused"}})
    )
    rest = RestClient("https://example.test", "secret")
    reporter = ToolResultReporter(rest, max_retries=0)
    await reporter.report(call(), ToolResult.ok("call-1"))
    assert route.call_count == 1
    await rest.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_reporter_failure_is_not_silent() -> None:
    respx.post("https://example.test/api/v1/agents/client/call/result").mock(
        return_value=httpx.Response(400, json={"detail": "invalid"})
    )
    rest = RestClient("https://example.test", "secret")
    reporter = ToolResultReporter(rest, max_retries=3, initial_retry_delay=0)
    with pytest.raises(ToolExecutionError):
        await reporter.report(call(), ToolResult.ok("call-1"))
    await rest.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_session_tool_event_executes_and_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    route = respx.post("https://example.test/api/v1/agents/client/call/result").mock(
        return_value=httpx.Response(200, json={})
    )
    agent = TaiyiAgent(
        base_url="https://example.test",
        flow_id="flow-1",
        api_key="secret",
        stream_protocol="sse",
        identity_path=tmp_path / "client-id",
        enable_builtin_tools=False,
        permission_policy="auto",
    )
    agent.register_tool("echo", lambda _context, tool_call: ToolResult.ok(tool_call.id, "done"))

    async def events(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield StreamEvent(
            event="client_tool",
            data={
                "function": {"id": "call-1", "name": "echo", "arguments": {}},
                "context": {"session_id": "session-1", "run_id": "run-1"},
            },
        )
        yield StreamEvent(event="close", data={})

    async def open_stream(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return events(request)

    monkeypatch.setattr(agent, "_open_stream", open_stream)
    session = agent.session("session-1")
    events_received = [item async for item in session.chat("hello")]
    assert len(events_received) == 2
    assert route.call_count == 1
    assert b'"run_id":"run-1"' in route.calls[0].request.content
    await agent.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_tool_event_is_visible_before_approval_completes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    route = respx.post("https://example.test/api/v1/agents/client/call/result").mock(
        return_value=httpx.Response(200, json={})
    )
    agent = TaiyiAgent(
        base_url="https://example.test",
        flow_id="flow-1",
        api_key="secret",
        stream_protocol="sse",
        identity_path=tmp_path / "client-id",
        enable_builtin_tools=False,
        permission_policy="confirm",
        permission_timeout=1,
    )
    agent.register_tool(
        "write_file", lambda _context, tool_call: ToolResult.ok(tool_call.id, "done")
    )

    async def events(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield StreamEvent(
            event="client_tool",
            data={
                "function": {"id": "call-1", "name": "write_file", "arguments": {}},
                "context": {"run_id": "run-1"},
            },
        )
        yield StreamEvent(event="close", data={})

    async def open_stream(request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return events(request)

    monkeypatch.setattr(agent, "_open_stream", open_stream)
    session = agent.session("session-1")
    stream = session.chat("hello")
    pending_tool = await stream.__anext__()
    assert pending_tool.event == "client_tool"
    assert route.call_count == 0
    assert session.approve("call-1")
    assert (await stream.__anext__()).event == "close"
    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()
    assert route.call_count == 1
    await agent.aclose()
