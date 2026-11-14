from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import cast
from urllib.parse import unquote_to_bytes

from ..errors import ConfigurationError
from ..plugins.manager import PluginManager
from ..transport.capabilities import JsonObject, JsonValue
from .base import PROTOCOL_VERSION, McpError, McpTool, parse_result, parse_tools, register_tools

ProcessFactory = Callable[..., Awaitable[asyncio.subprocess.Process]]
TerminationListener = Callable[["StdioMcpClient"], None]


@dataclass(frozen=True, slots=True)
class StdioMcpServerConfig:
    command: str
    args: tuple[str, ...] = ()
    env: Mapping[str, str] = field(default_factory=dict)
    cwd: Path | None = None

    def __post_init__(self) -> None:
        if not self.command.strip():
            raise ConfigurationError("Stdio MCP command is required")
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "env", MappingProxyType(dict(self.env)))
        if self.cwd is not None:
            object.__setattr__(self, "cwd", Path(self.cwd))

    @classmethod
    def parse(
        cls, value: str, environment: Mapping[str, str] | None = None
    ) -> StdioMcpServerConfig:
        if not value.strip():
            raise ConfigurationError("Stdio MCP configuration is required")
        if value.startswith("stdio://"):
            tokens = value.removeprefix("stdio://").strip().split()
            if not tokens:
                raise ConfigurationError("stdio:// configuration has no command")
            return cls(tokens[0], tuple(tokens[1:]), environment or {})
        if not value.startswith("data:"):
            raise ConfigurationError("Stdio MCP configuration must use stdio:// or data:")
        metadata, separator, payload = value[5:].partition(",")
        if not separator:
            raise ConfigurationError("Stdio MCP data URI has no payload separator")
        try:
            if "base64" in metadata:
                compact = "".join(payload.split())
                content = base64.b64decode(compact, validate=True)
            else:
                content = unquote_to_bytes(payload)
            decoded = json.loads(content)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, binascii.Error) as exc:
            raise ConfigurationError("Invalid stdio MCP data URI") from exc
        return cls.from_dict(decoded)

    @classmethod
    def from_dict(cls, value: object) -> StdioMcpServerConfig:
        if not isinstance(value, dict):
            raise ConfigurationError("Stdio MCP configuration must be a JSON object")
        if "command" not in value:
            for key in ("server_url", "url", "stdio"):
                nested = value.get(key)
                if isinstance(nested, str):
                    return cls.parse(nested, _environment(value.get("env")))
            raise ConfigurationError("Stdio MCP command is required")
        command = value.get("command")
        args = value.get("args", [])
        if (
            not isinstance(command, str)
            or not isinstance(args, list)
            or not all(isinstance(item, str) for item in args)
        ):
            raise ConfigurationError("Stdio MCP command and args are invalid")
        cwd = value.get("cwd")
        if cwd is not None and not isinstance(cwd, str):
            raise ConfigurationError("Stdio MCP cwd must be a string")
        return cls(command, tuple(args), _environment(value.get("env")), Path(cwd) if cwd else None)

    @property
    def cache_key(self) -> str:
        canonical = json.dumps(
            {
                "command": self.command,
                "args": self.args,
                "env": dict(sorted(self.env.items())),
                "cwd": str(self.cwd) if self.cwd else None,
            },
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(canonical).hexdigest()

    def __repr__(self) -> str:
        return f"StdioMcpServerConfig(args={len(self.args)}, env={len(self.env)})"


def _environment(value: object) -> Mapping[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise ConfigurationError("Stdio MCP env must be a string object")
    return cast(Mapping[str, str], value)


@dataclass(frozen=True, slots=True)
class StdioMcpPolicy:
    workspace: Path
    allowed_commands: frozenset[str] = frozenset()
    allowed_environment: frozenset[str] = frozenset()
    inherited_environment: frozenset[str] = frozenset(
        {"HOME", "LOGNAME", "PATH", "SHELL", "TERM", "USER"}
    )
    max_servers: int = 16
    max_arguments: int = 64
    max_argument_chars: int = 16_384
    max_frame_bytes: int = 1024 * 1024
    output_tail_bytes: int = 65_536
    max_lifetime: float = 30 * 60
    stop_timeout: float = 2.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "workspace", Path(self.workspace).absolute())
        limits = (
            self.max_servers,
            self.max_arguments,
            self.max_argument_chars,
            self.max_frame_bytes,
            self.output_tail_bytes,
        )
        if any(value <= 0 for value in limits) or self.max_lifetime <= 0:
            raise ConfigurationError("Stdio MCP limits and lifetime must be positive")
        if self.stop_timeout < 0:
            raise ConfigurationError("Stdio MCP stop_timeout must not be negative")

    def validate(self, config: StdioMcpServerConfig) -> None:
        if config.command not in self.allowed_commands:
            raise ConfigurationError("Stdio MCP command is not allowed")
        if len(config.args) > self.max_arguments or any(
            len(value) > self.max_argument_chars for value in config.args
        ):
            raise ConfigurationError("Stdio MCP argument limits exceeded")
        if any(name not in self.allowed_environment for name in config.env):
            raise ConfigurationError("Stdio MCP environment variable is not allowed")
        self.working_directory(config)

    def working_directory(self, config: StdioMcpServerConfig) -> Path:
        cwd = config.cwd if config.cwd is not None else self.workspace
        candidate = cwd if cwd.is_absolute() else self.workspace / cwd
        try:
            workspace = self.workspace.resolve(strict=True)
            resolved = candidate.resolve(strict=True)
        except OSError as exc:
            raise ConfigurationError("Stdio MCP cwd is not accessible") from exc
        if not resolved.is_relative_to(workspace) or not resolved.is_dir():
            raise ConfigurationError("Stdio MCP cwd escapes its workspace")
        return resolved

    def environment(self, config: StdioMcpServerConfig) -> dict[str, str]:
        environment = {
            name: os.environ[name] for name in self.inherited_environment if name in os.environ
        }
        environment.update(
            (name, value) for name, value in config.env.items() if name in self.allowed_environment
        )
        return environment


class StdioMcpClient:
    def __init__(
        self,
        config: StdioMcpServerConfig,
        policy: StdioMcpPolicy,
        *,
        request_timeout: float = 30.0,
        process_factory: ProcessFactory = asyncio.create_subprocess_exec,
        on_termination: TerminationListener | None = None,
    ) -> None:
        if request_timeout <= 0:
            raise ConfigurationError("MCP request_timeout must be positive")
        self._config = config
        self._policy = policy
        self._request_timeout = request_timeout
        self._process_factory = process_factory
        self._on_termination = on_termination
        self._request_id = 0
        self._pending: dict[int, asyncio.Future[JsonObject]] = {}
        self._process: asyncio.subprocess.Process | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._stdout_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._lifetime_task: asyncio.Task[None] | None = None
        self._initialize_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._initialized = False
        self._closed = False
        self._cleanup_done = False
        self._termination_notified = False
        self._stderr_tail = bytearray()

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def is_alive(self) -> bool:
        return not self._closed and self._process is not None and self._process.returncode is None

    async def list_tools(self) -> list[McpTool]:
        await self._ensure_initialized()
        return parse_tools(await self._request("tools/list", {}))

    async def call_tool(self, tool_name: str, arguments: JsonObject) -> JsonObject:
        await self._ensure_initialized()
        return await self._request("tools/call", {"name": tool_name, "arguments": arguments})

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        async with self._initialize_lock:
            if self._initialized:
                return
            if self._closed:
                raise McpError("MCP stdio instance is unavailable")
            try:
                await self._start()
                await self._request(
                    "initialize",
                    {
                        "protocolVersion": PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {
                            "name": "taiyiflow-pythonsdk",
                            "version": "1.0.0",
                        },
                    },
                )
                await self._write({"jsonrpc": "2.0", "method": "notifications/initialized"})
                self._initialized = True
            except BaseException:
                await self.aclose()
                raise

    async def _start(self) -> None:
        self._policy.validate(self._config)
        try:
            process = await self._process_factory(
                self._config.command,
                *self._config.args,
                cwd=self._policy.working_directory(self._config),
                env=self._policy.environment(self._config),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except (OSError, ValueError) as exc:
            raise McpError("Unable to start MCP stdio process") from exc
        if process.stdin is None or process.stdout is None:
            await _stop_process(process, self._policy.stop_timeout)
            raise McpError("MCP stdio process has no pipes")
        self._process = process
        self._writer = process.stdin
        self._stdout_task = asyncio.create_task(self._read_stdout(process.stdout))
        if process.stderr is not None:
            self._stderr_task = asyncio.create_task(self._read_stderr(process.stderr))
        self._lifetime_task = asyncio.create_task(self._expire())

    async def _request(self, method: str, params: JsonObject) -> JsonObject:
        if not self.is_alive:
            raise McpError("MCP stdio process is not running")
        self._request_id += 1
        request_id = self._request_id
        response = asyncio.get_running_loop().create_future()
        self._pending[request_id] = response
        try:
            try:
                await self._write(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": method,
                        "params": params,
                    }
                )
            except asyncio.CancelledError:
                await self.aclose()
                raise
            try:
                return await asyncio.wait_for(
                    asyncio.shield(response), timeout=self._request_timeout
                )
            except TimeoutError as exc:
                response.cancel()
                raise McpError(f"MCP {method} timed out") from exc
            except asyncio.CancelledError:
                response.cancel()
                raise
        finally:
            self._pending.pop(request_id, None)

    async def _write(self, payload: JsonObject) -> None:
        async with self._write_lock:
            writer = self._writer
            if not self.is_alive or writer is None:
                raise McpError("MCP stdio process is not running")
            try:
                writer.write(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
                await writer.drain()
            except (OSError, RuntimeError) as exc:
                await self.aclose()
                raise McpError("MCP stdio write failed") from exc

    async def _read_stdout(self, reader: asyncio.StreamReader) -> None:
        buffer = bytearray()
        failure: McpError | None = None
        try:
            while True:
                chunk = await reader.read(65_536)
                if not chunk:
                    break
                buffer.extend(chunk)
                frames = _take_frames(buffer, self._policy.max_frame_bytes)
                for frame in frames:
                    self._handle_frame(frame)
            if buffer.strip():
                self._handle_frame(bytes(buffer))
            if not self._closed:
                failure = McpError("MCP stdio process exited")
        except asyncio.CancelledError:
            raise
        except McpError as exc:
            failure = exc
        except (OSError, RuntimeError, ValueError) as exc:
            failure = McpError("MCP stdout contained an invalid protocol frame")
            failure.__cause__ = exc
        finally:
            if failure is not None:
                await self._shutdown(failure, cancel_reader=False)

    def _handle_frame(self, frame: bytes) -> None:
        try:
            payload = cast(JsonValue, json.loads(frame))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise McpError("Invalid MCP JSON-RPC frame") from exc
        if not isinstance(payload, dict) or "id" not in payload:
            return
        request_id = payload.get("id")
        if isinstance(request_id, bool) or not isinstance(request_id, int):
            return
        response = self._pending.get(request_id)
        if response is None or response.done():
            return
        try:
            response.set_result(parse_result(payload, request_id, "request"))
        except McpError as exc:
            response.set_exception(exc)

    async def _read_stderr(self, reader: asyncio.StreamReader) -> None:
        try:
            while chunk := await reader.read(4096):
                self._stderr_tail.extend(chunk)
                excess = len(self._stderr_tail) - self._policy.output_tail_bytes
                if excess > 0:
                    del self._stderr_tail[:excess]
        except (OSError, RuntimeError):
            return

    async def _expire(self) -> None:
        try:
            await asyncio.sleep(self._policy.max_lifetime)
            await self.aclose()
        except asyncio.CancelledError:
            raise

    async def aclose(self) -> None:
        await self._shutdown(McpError("MCP stdio instance was closed"), cancel_reader=True)

    async def _shutdown(self, error: McpError, *, cancel_reader: bool) -> None:
        async with self._close_lock:
            if self._cleanup_done:
                return
            self._closed = True
            self._initialized = False
            for response in self._pending.values():
                if not response.done():
                    response.set_exception(error)
            self._pending.clear()
            current = asyncio.current_task()
            for task in (self._lifetime_task, self._stderr_task):
                if task is not None and task is not current:
                    task.cancel()
            process = self._process
            if process is not None:
                await _stop_process(process, self._policy.stop_timeout)
            writer = self._writer
            if writer is not None:
                writer.close()
            if cancel_reader and self._stdout_task is not None and self._stdout_task is not current:
                self._stdout_task.cancel()
            tasks = [
                task
                for task in (self._stdout_task, self._stderr_task, self._lifetime_task)
                if task is not None and task is not current
            ]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._cleanup_done = True
        if not self._termination_notified:
            self._termination_notified = True
            if self._on_termination is not None:
                self._on_termination(self)


class StdioMcpManager:
    def __init__(
        self,
        policy: StdioMcpPolicy,
        *,
        request_timeout: float = 30.0,
        process_factory: ProcessFactory = asyncio.create_subprocess_exec,
    ) -> None:
        self._policy = policy
        self._request_timeout = request_timeout
        self._process_factory = process_factory
        self._instances: dict[str, StdioMcpClient] = {}
        self._closed = False

    @property
    def instance_count(self) -> int:
        return len(self._instances)

    def instance(self, config: StdioMcpServerConfig) -> StdioMcpClient:
        if self._closed:
            raise McpError("Stdio MCP manager is closed")
        self._policy.validate(config)
        key = config.cache_key
        existing = self._instances.get(key)
        if existing is not None and not existing._closed:
            return existing
        if existing is None and len(self._instances) >= self._policy.max_servers:
            raise McpError("Stdio MCP server limit exceeded")
        created = StdioMcpClient(
            config,
            self._policy,
            request_timeout=self._request_timeout,
            process_factory=self._process_factory,
            on_termination=lambda client: self._remove(key, client),
        )
        self._instances[key] = created
        return created

    def _remove(self, key: str, client: StdioMcpClient) -> None:
        if self._instances.get(key) is client:
            self._instances.pop(key, None)

    async def list_tools(self, config: StdioMcpServerConfig) -> list[McpTool]:
        return await self.instance(config).list_tools()

    async def call_tool(
        self, config: StdioMcpServerConfig, tool_name: str, arguments: JsonObject
    ) -> JsonObject:
        return await self.instance(config).call_tool(tool_name, arguments)

    async def register_tools(
        self,
        server_name: str,
        config: StdioMcpServerConfig,
        plugin_manager: PluginManager,
    ) -> list[str]:
        client = self.instance(config)

        class BoundClient:
            async def list_tools(self) -> list[McpTool]:
                return await client.list_tools()

            async def call_tool(self, tool_name: str, arguments: JsonObject) -> JsonObject:
                return await client.call_tool(tool_name, arguments)

            async def aclose(self) -> None:
                await client.aclose()

        registered = await register_tools(BoundClient(), server_name, plugin_manager)
        plugin_manager.register_closeable(self)
        return registered

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        instances = list(self._instances.values())
        self._instances.clear()
        if instances:
            await asyncio.gather(
                *(instance.aclose() for instance in instances), return_exceptions=True
            )


def _take_frames(buffer: bytearray, limit: int) -> list[bytes]:
    frames: list[bytes] = []
    while buffer:
        if buffer.startswith((b"Content-Length:", b"content-length:")):
            header_end = buffer.find(b"\r\n\r\n")
            separator_length = 4
            if header_end < 0:
                header_end = buffer.find(b"\n\n")
                separator_length = 2
            if header_end < 0:
                if len(buffer) > min(limit, 8192):
                    raise McpError("MCP stdout header limit exceeded")
                break
            header = bytes(buffer[:header_end]).decode("ascii", errors="strict")
            lengths = [
                int(line.split(":", 1)[1].strip())
                for line in header.replace("\r", "").split("\n")
                if line.lower().startswith("content-length:")
            ]
            if len(lengths) != 1 or lengths[0] < 0 or lengths[0] > limit:
                raise McpError("MCP stdout frame limit exceeded")
            body_start = header_end + separator_length
            body_end = body_start + lengths[0]
            if len(buffer) < body_end:
                break
            frames.append(bytes(buffer[body_start:body_end]))
            del buffer[:body_end]
            continue
        newline = buffer.find(b"\n")
        if newline < 0:
            if len(buffer) > limit:
                raise McpError("MCP stdout frame limit exceeded")
            break
        frame = bytes(buffer[:newline]).strip()
        if len(frame) > limit:
            raise McpError("MCP stdout frame limit exceeded")
        del buffer[: newline + 1]
        if frame:
            frames.append(frame)
    return frames


async def _stop_process(process: asyncio.subprocess.Process, wait_timeout: float) -> None:
    if process.returncode is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
    try:
        await asyncio.wait_for(process.wait(), timeout=wait_timeout)
    except TimeoutError:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        await process.wait()
