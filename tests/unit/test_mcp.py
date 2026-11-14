from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from taiyiagent.errors import ConfigurationError
from taiyiagent.mcp import (
    McpError,
    StdioMcpManager,
    StdioMcpPolicy,
    StdioMcpServerConfig,
    StreamableHttpMcpClient,
)
from taiyiagent.models import ToolCall
from taiyiagent.plugins import PluginManager, ToolExecutionContext


@pytest.mark.asyncio
@respx.mock
async def test_http_mcp_initialize_session_tools_and_registration() -> None:
    methods: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        method = payload["method"]
        methods.append(method)
        if method == "notifications/initialized":
            return httpx.Response(202)
        if method != "initialize":
            assert request.headers["Mcp-Session-Id"] == "session-1"
        if method == "initialize":
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": payload["id"], "result": {}},
                headers={"Mcp-Session-Id": "session-1"},
            )
        if method == "tools/list":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": {
                        "tools": [
                            {
                                "name": "lookup",
                                "description": "Lookup data",
                                "inputSchema": {"type": "object"},
                            }
                        ]
                    },
                },
            )
        assert method == "tools/call"
        assert payload["params"] == {"name": "lookup", "arguments": {"key": "value"}}
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {"content": [{"type": "text", "text": "done"}]},
            },
        )

    route = respx.post("https://mcp.example.test/").mock(side_effect=respond)
    respx.delete("https://mcp.example.test/").respond(200)
    async with httpx.AsyncClient() as http_client:
        client = StreamableHttpMcpClient("https://mcp.example.test/", client=http_client)
        plugins = PluginManager()
        assert await client.register_tools("remote", plugins) == ["c_remote_lookup"]
        assert (await client.list_tools())[0].name == "lookup"
        result = await plugins.execute(
            ToolExecutionContext("session"),
            ToolCall(id="call-1", name="c_remote_lookup", arguments={"key": "value"}),
        )
        assert result.success
        assert result.result["content"][0]["text"] == "done"
        assert client.session_id == "session-1"
        await plugins.aclose()

    assert methods == [
        "initialize",
        "notifications/initialized",
        "tools/list",
        "tools/list",
        "tools/call",
    ]
    assert route.call_count == 5


@pytest.mark.asyncio
@respx.mock
async def test_http_mcp_sse_matches_json_rpc_id() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        if payload["method"] == "notifications/initialized":
            return httpx.Response(202)
        body = (
            'data: {"jsonrpc":"2.0","method":"notifications/progress"}\n\n'
            'data: {"jsonrpc":"2.0","id":999,"result":{}}\n\n'
            f'data: {{"jsonrpc":"2.0","id":{payload["id"]},"result":{{"tools":[]}}}}\n\n'
        )
        return httpx.Response(200, content=body, headers={"Content-Type": "text/event-stream"})

    respx.post("https://mcp.example.test/sse").mock(side_effect=respond)
    client = StreamableHttpMcpClient("https://mcp.example.test/sse")
    try:
        assert await client.list_tools() == []
    finally:
        await client.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_http_mcp_cancel_does_not_replay_call() -> None:
    started = asyncio.Event()
    calls = 0

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        payload = json.loads(request.content)
        method = payload["method"]
        if method == "notifications/initialized":
            return httpx.Response(202)
        if method == "tools/call":
            calls += 1
            started.set()
            await asyncio.Event().wait()
        result = {"tools": []} if method == "tools/list" else {}
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": result})

    route = respx.post("https://mcp.example.test/cancel").mock(side_effect=respond)
    client = StreamableHttpMcpClient("https://mcp.example.test/cancel")
    try:
        await client.list_tools()
        task = asyncio.create_task(client.call_tool("slow", {}))
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        assert calls == 1
    finally:
        await client.aclose()
    assert route.call_count == 3


class FakeWriter:
    def __init__(self) -> None:
        self.messages: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self._buffer = bytearray()
        self.closed = False

    def write(self, content: bytes) -> None:
        self._buffer.extend(content)
        while (newline := self._buffer.find(b"\n")) >= 0:
            frame = bytes(self._buffer[:newline])
            del self._buffer[: newline + 1]
            self.messages.put_nowait(json.loads(frame))

    async def drain(self) -> None:
        await asyncio.sleep(0)

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.messages.put_nowait(None)


class FakeProcess:
    def __init__(self) -> None:
        self.stdin = FakeWriter()
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.returncode: int | None = None
        self._exited = asyncio.Event()
        self.responder: asyncio.Task[None] | None = None

    def terminate(self) -> None:
        self._finish(-15)

    def kill(self) -> None:
        self._finish(-9)

    def _finish(self, returncode: int) -> None:
        if self.returncode is not None:
            return
        self.returncode = returncode
        self.stdin.close()
        self.stdout.feed_eof()
        self.stderr.feed_eof()
        self._exited.set()

    async def wait(self) -> int:
        await self._exited.wait()
        return self.returncode or 0


class FakeProcessFactory:
    def __init__(
        self,
        *,
        framed: bool = False,
        hang_calls: bool = False,
        oversized_list: bool = False,
    ) -> None:
        self.framed = framed
        self.hang_calls = hang_calls
        self.oversized_list = oversized_list
        self.processes: list[FakeProcess] = []
        self.invocations: list[tuple[tuple[str, ...], dict[str, Any]]] = []
        self.call_count = 0
        self.call_started = asyncio.Event()

    async def __call__(self, *command: str, **kwargs: Any) -> FakeProcess:
        self.invocations.append((command, kwargs))
        process = FakeProcess()
        self.processes.append(process)
        process.responder = asyncio.create_task(self._respond(process))
        return process

    async def _respond(self, process: FakeProcess) -> None:
        while (payload := await process.stdin.messages.get()) is not None:
            method = payload["method"]
            if method == "notifications/initialized":
                continue
            if method == "tools/call":
                self.call_count += 1
                self.call_started.set()
                if self.hang_calls:
                    continue
            result: dict[str, Any] = {}
            if method == "tools/list" and self.oversized_list:
                process.stdout.feed_data(b"x" * 2048 + b"\n")
                continue
            if method == "tools/list":
                result = {"tools": [{"name": "local", "inputSchema": {"type": "object"}}]}
            elif method == "tools/call":
                result = {"content": [{"type": "text", "text": "local done"}]}
            response = json.dumps(
                {"jsonrpc": "2.0", "id": payload["id"], "result": result}
            ).encode()
            if method == "tools/list" and self.framed:
                frame = f"Content-Length: {len(response)}\r\n\r\n".encode() + response
                process.stdout.feed_data(frame[:11])
                await asyncio.sleep(0)
                process.stdout.feed_data(frame[11:])
            else:
                process.stdout.feed_data(response + b"\n")


def _policy(workspace: Path, **overrides: Any) -> StdioMcpPolicy:
    values: dict[str, Any] = {
        "allowed_commands": frozenset({"fake-mcp"}),
        "inherited_environment": frozenset(),
        "max_lifetime": 60.0,
    }
    values.update(overrides)
    return StdioMcpPolicy(workspace=workspace, **values)


def _config(**overrides: Any) -> StdioMcpServerConfig:
    values: dict[str, Any] = {"command": "fake-mcp", "args": ("serve",)}
    values.update(overrides)
    return StdioMcpServerConfig(**values)


@pytest.mark.asyncio
async def test_stdio_mcp_frames_registration_and_process_ownership(tmp_path: Path) -> None:
    factory = FakeProcessFactory(framed=True)
    policy = _policy(
        tmp_path,
        allowed_environment=frozenset({"MCP_MODE"}),
    )
    manager = StdioMcpManager(policy, process_factory=factory)
    config = _config(env={"MCP_MODE": "test"})
    plugins = PluginManager()
    assert await manager.register_tools("local", config, plugins) == ["c_local_local"]
    result = await plugins.execute(
        ToolExecutionContext("session"),
        ToolCall(id="call-1", name="c_local_local", arguments={}),
    )
    assert result.success
    assert result.result["content"][0]["text"] == "local done"
    assert manager.instance(config) is manager.instance(config)
    assert manager.instance_count == 1
    command, kwargs = factory.invocations[0]
    assert command == ("fake-mcp", "serve")
    assert kwargs["env"] == {"MCP_MODE": "test"}
    assert kwargs["cwd"] == tmp_path
    await plugins.aclose()
    assert manager.instance_count == 0
    assert factory.processes[0].returncode is not None


@pytest.mark.asyncio
async def test_stdio_mcp_cancel_and_timeout_remove_pending_without_replay(
    tmp_path: Path,
) -> None:
    factory = FakeProcessFactory(hang_calls=True)
    manager = StdioMcpManager(_policy(tmp_path), request_timeout=0.02, process_factory=factory)
    config = _config()
    client = manager.instance(config)
    await client.list_tools()
    task = asyncio.create_task(client.call_tool("local", {}))
    await asyncio.wait_for(factory.call_started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.pending_count == 0
    with pytest.raises(McpError, match="timed out"):
        await client.call_tool("local", {})
    assert client.pending_count == 0
    assert factory.call_count == 2
    await manager.aclose()


@pytest.mark.asyncio
async def test_stdio_mcp_process_exit_fails_pending_and_closes(tmp_path: Path) -> None:
    factory = FakeProcessFactory(hang_calls=True)
    manager = StdioMcpManager(_policy(tmp_path), process_factory=factory)
    client = manager.instance(_config())
    await client.list_tools()
    task = asyncio.create_task(client.call_tool("local", {}))
    await asyncio.wait_for(factory.call_started.wait(), timeout=1)
    factory.processes[0].terminate()
    with pytest.raises(McpError, match="process exited"):
        await task
    assert client.pending_count == 0
    for _ in range(10):
        if manager.instance_count == 0:
            break
        await asyncio.sleep(0)
    assert manager.instance_count == 0
    await manager.aclose()


@pytest.mark.asyncio
async def test_stdio_mcp_frame_limit_closes_process(tmp_path: Path) -> None:
    factory = FakeProcessFactory(oversized_list=True)
    manager = StdioMcpManager(_policy(tmp_path, max_frame_bytes=512), process_factory=factory)
    client = manager.instance(_config())
    with pytest.raises(McpError, match="frame limit exceeded"):
        await client.list_tools()
    assert client.pending_count == 0
    assert factory.processes[0].returncode is not None
    await manager.aclose()


def test_stdio_policy_defaults_and_sensitive_representation(tmp_path: Path) -> None:
    policy = StdioMcpPolicy(workspace=tmp_path)
    with pytest.raises(ConfigurationError, match="command is not allowed"):
        policy.validate(_config())
    with pytest.raises(ConfigurationError, match="environment variable is not allowed"):
        _policy(tmp_path).validate(_config(env={"SECRET": "do-not-use"}))
    config = _config(args=("--token=secret",), env={"SECRET": "sensitive"})
    assert "secret" not in repr(config)
    assert "sensitive" not in repr(config)
    assert "secret" not in str(McpError("request __TOKEN__=secret failed"))


def test_stdio_policy_rejects_symlink_escape(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (workspace / "escape").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ConfigurationError, match="cwd"):
        _policy(workspace).validate(_config(cwd=Path("escape")))


def test_stdio_config_data_uri_and_manager_capacity(tmp_path: Path) -> None:
    config = StdioMcpServerConfig.parse(
        "data:application/json,%7B%22command%22%3A%22fake-mcp%22%2C%22args%22%3A%5B%22serve%22%5D%7D"
    )
    assert config.command == "fake-mcp"
    assert config.args == ("serve",)
    manager = StdioMcpManager(_policy(tmp_path, max_servers=1))
    manager.instance(config)
    with pytest.raises(McpError, match="limit exceeded"):
        manager.instance(_config(args=("other",)))
