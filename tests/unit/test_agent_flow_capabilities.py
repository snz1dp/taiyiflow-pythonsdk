import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from taiyiagent import ChatRequest, PermissionPolicy, TaiyiAgent, ToolDefinition
from taiyiagent.errors import ApiError
from taiyiagent.models import StreamEvent, StreamProtocol, ToolResult

BASE = "https://example.test"


def make_agent(tmp_path: Path, **kwargs: Any) -> TaiyiAgent:
    options: dict[str, Any] = {
        "base_url": BASE,
        "api_key": "secret",
        "stream_protocol": StreamProtocol.SSE,
        "identity_path": tmp_path / "client-id",
        "enable_builtin_tools": False,
    }
    options.update(kwargs)
    return TaiyiAgent(**options)


async def noop_handler(_context: object, call: object) -> ToolResult:
    return ToolResult.ok("call", {"ok": True})


@pytest.mark.asyncio
@respx.mock
async def test_default_flow_is_resolved_once_and_used_by_sse(tmp_path: Path) -> None:
    config_route = respx.get(BASE + "/api/v1/config").mock(
        return_value=httpx.Response(200, json={"default_flow_id": "default-flow"})
    )
    run_route = respx.post(BASE + "/api/v1/run/default-flow/stream").mock(
        return_value=httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content='data: {"event":"close","data":{}}\n\n',
        )
    )
    agent = make_agent(tmp_path)
    try:
        session = agent.session("session-1")
        events = [event async for event in session.chat("hello")]
        assert events[-1].event == "close"
        assert await agent.resolve_flow_id() == "default-flow"
    finally:
        await agent.aclose()
    assert config_route.call_count == 1
    assert run_route.call_count == 1
    assert json.loads(run_route.calls[0].request.content)["available_tools"] == []


@pytest.mark.asyncio
@respx.mock
async def test_tool_supply_filters_and_recovery_reuses_snapshot(tmp_path: Path) -> None:
    metadata_route = respx.get(BASE + "/api/v1/run/flow/metadata").mock(
        return_value=httpx.Response(200, json={"id": "flow", "features": ["search"]})
    )
    agent = make_agent(tmp_path, flow_id="flow", permission_policy=PermissionPolicy.AUTO)
    readonly = ToolDefinition(
        "search",
        "Search content",
        {"type": "object", "properties": {"query": {"type": "string"}}},
        required_feature="search",
    )
    unsupported = ToolDefinition("hidden", "Hidden", {"type": "object"}, "missing")
    write = ToolDefinition("write_file", "Write", {"type": "object"})
    agent.register_tool("search", noop_handler, definition=readonly)
    agent.register_tool("hidden", noop_handler, definition=unsupported)
    agent.register_tool("write_file", noop_handler, definition=write)
    session = agent.session("session-1")
    request = ChatRequest(input_value="hello", session_id="session-1")
    try:
        await agent._prepare_tool_supply(session, request)
        expected = [readonly.to_json(), write.to_json()]
        assert request.available_tools == expected
        assert session.run_context.available_tools == expected

        agent.plugins.unregister("search")
        recovery = ChatRequest(session_id="session-1", execution_mode="attach")
        await agent._prepare_tool_supply(session, recovery)
        assert recovery.available_tools == expected
    finally:
        await agent.aclose()
    assert metadata_route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_readonly_tool_supply_and_explicit_external_candidates(tmp_path: Path) -> None:
    respx.get(BASE + "/api/v1/run/flow/metadata").mock(
        return_value=httpx.Response(200, json={"id": "flow", "features": []})
    )
    agent = make_agent(tmp_path, flow_id="flow")
    local = ToolDefinition("read_file", "Read", {"type": "object"})
    write = ToolDefinition("write_file", "Write", {"type": "object"})
    agent.register_tool("read_file", noop_handler, definition=local)
    agent.register_tool("write_file", noop_handler, definition=write)
    session = agent.session("session-1")
    external = {"name": "platform_tool", "input_schema": {"type": "object"}}
    request = ChatRequest(
        input_value="hello",
        session_id="session-1",
        request_type="plan",
        available_tools=["read_file", "write_file", external],
    )
    try:
        await agent._prepare_tool_supply(session, request)
    finally:
        await agent.aclose()
    assert request.available_tools == ["read_file", external]


@pytest.mark.asyncio
@respx.mock
async def test_metadata_failure_does_not_fall_back_to_all_tools(tmp_path: Path) -> None:
    respx.get(BASE + "/api/v1/run/flow/metadata").mock(
        return_value=httpx.Response(503, json={"detail": "unavailable"})
    )
    agent = make_agent(tmp_path, flow_id="flow")
    definition = ToolDefinition("read_file", "Read", {"type": "object"})
    agent.register_tool("read_file", noop_handler, definition=definition)
    session = agent.session("session-1")
    request = ChatRequest(input_value="hello", session_id="session-1")
    try:
        with pytest.raises(ApiError):
            await agent._prepare_tool_supply(session, request)
    finally:
        await agent.aclose()
    assert session.run_context.available_tools is None


@pytest.mark.asyncio
@respx.mock
async def test_flow_metadata_cache_has_single_flight(tmp_path: Path) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def metadata_handler(_request: httpx.Request) -> httpx.Response:
        entered.set()
        await release.wait()
        return httpx.Response(200, json={"id": "flow", "features": ["future"]})

    route = respx.get(BASE + "/api/v1/run/flow/metadata").mock(side_effect=metadata_handler)
    agent = make_agent(tmp_path, flow_id="flow")
    try:
        first = asyncio.create_task(agent.get_flow_metadata())
        second = asyncio.create_task(agent.get_flow_metadata())
        await entered.wait()
        release.set()
        metadata = await asyncio.gather(first, second)
        assert metadata[0] is metadata[1]
        assert metadata[0].supports("future")
        refreshed = await agent.get_flow_metadata(refresh=True)
        assert refreshed.supports("future")
    finally:
        release.set()
        await agent.aclose()
    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_tool_dispatch_uses_session_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result_route = respx.post(BASE + "/api/v1/agents/client/call/result").mock(
        return_value=httpx.Response(200, json={})
    )
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "note.txt").write_text("root-content", encoding="utf-8")
    agent = make_agent(tmp_path, flow_id="flow", enable_builtin_tools=True, workspace=root)
    session_workspace = agent.workspace_for("flow", "session-1")
    (session_workspace / "note.txt").write_text("session-content", encoding="utf-8")

    async def events(_request: ChatRequest) -> AsyncIterator[StreamEvent]:
        yield StreamEvent(
            event="client_tool",
            data={
                "function": {
                    "id": "call-1",
                    "name": "read_file",
                    "arguments": {"path": "note.txt"},
                },
                "context": {"run_id": "run-1"},
            },
        )
        yield StreamEvent(event="close", data={})

    async def open_stream(_request: ChatRequest) -> AsyncIterator[StreamEvent]:
        return events(_request)

    monkeypatch.setattr(agent, "_open_stream", open_stream)
    try:
        session = agent.session("session-1")
        _ = [event async for event in session.chat("hello")]
    finally:
        await agent.aclose()
    payload = result_route.calls[0].request.content
    assert b"session-content" in payload
    assert b"root-content" not in payload
