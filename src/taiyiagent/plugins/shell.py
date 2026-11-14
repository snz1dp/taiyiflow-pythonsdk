from __future__ import annotations

import asyncio
import os
import shlex
from pathlib import Path
from typing import Any

from ..errors import redact
from ..models import ToolCall, ToolResult
from .definitions import tool_definition
from .manager import PluginManager, ToolExecutionContext, ToolHandler
from .processes import (
    BackgroundProcessRegistry,
    ManagedProcess,
    OutputBuffer,
    _read_stream,
    _terminate,
)


class ShellPlugin:
    def __init__(
        self,
        working_directory: str | Path,
        allowed_commands: list[str] | tuple[str, ...],
        *,
        timeout: float = 30.0,
        max_output_chars: int = 1_000_000,
        process_registry: BackgroundProcessRegistry | None = None,
    ) -> None:
        if timeout <= 0 or max_output_chars <= 0:
            raise ValueError("Shell timeout and output limit must be positive")
        self.working_directory = Path(working_directory).expanduser().resolve()
        self.allowed_commands = frozenset(allowed_commands)
        self.timeout = timeout
        self.max_output_chars = max_output_chars
        self.processes = process_registry or BackgroundProcessRegistry()

    async def execute(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        process: asyncio.subprocess.Process | None = None
        reader: asyncio.Task[None] | None = None
        try:
            args = call.arguments_dict
            command = args.get("command")
            if not isinstance(command, str):
                return ToolResult.fail(call.id, "Missing 'command' argument")
            arguments = shlex.split(command)
            if not arguments or arguments[0] not in self.allowed_commands:
                return ToolResult.fail(call.id, f"Command not in whitelist: {command}")
            cwd = self._resolve_cwd(args.get("cwd"), context.workspace)
            if args.get("background") is True:
                process = await self._start(arguments, cwd, background=True)
                entry = self.processes.add(
                    process,
                    session_id=context.session_id,
                    command=arguments,
                    cwd=cwd,
                    run_id=context.run_id,
                    task_id=context.task_id,
                )
                background_result = entry.describe()
                background_result["started"] = True
                return ToolResult.ok(call.id, background_result)

            process = await self._start(arguments, cwd, background=False)
            output = OutputBuffer(self.max_output_chars, drop_oldest=False)
            reader = asyncio.create_task(_read_stream(process.stdout, output))
            timed_out = False
            try:
                await asyncio.wait_for(process.wait(), self.timeout)
            except TimeoutError:
                timed_out = True
                _terminate(process)
                await process.wait()
            await reader
            result: dict[str, Any] = {
                "output": output.data,
                "outputTruncated": output.truncated,
                "exitCode": process.returncode,
            }
            if timed_out:
                result["timedOut"] = True
                failure = ToolResult.fail(call.id, f"Command timed out after {self.timeout:g}s")
                failure.result = result
                return failure
            return ToolResult.ok(call.id, result)
        except asyncio.CancelledError:
            if process is not None:
                _terminate(process)
            if reader is not None:
                reader.cancel()
            raise
        except Exception as exc:
            if process is not None:
                _terminate(process)
            return ToolResult.fail(call.id, redact(exc))

    async def process_list(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        return ToolResult.ok(
            call.id,
            {"processes": [entry.describe() for entry in self.processes.list(context.session_id)]},
        )

    async def process_output(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        entry, error = self._find(context, call)
        if error is not None:
            return error
        assert entry is not None
        args = call.arguments_dict
        result = entry.describe()
        result["stdout"] = entry.stdout.output(_offset(args.get("stdout_offset")))
        result["stderr"] = entry.stderr.output(_offset(args.get("stderr_offset")))
        return ToolResult.ok(call.id, result)

    async def process_stop(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        entry, error = self._find(context, call)
        if error is not None:
            return error
        assert entry is not None
        entry.stop()
        result = entry.describe()
        result["stop_requested"] = True
        return ToolResult.ok(call.id, result)

    async def process_cleanup(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        handle = _handle(call.arguments_dict)
        if handle is None:
            return ToolResult.ok(
                call.id, {"removed": self.processes.cleanup_finished(context.session_id)}
            )
        entry, error = self._find(context, call)
        if error is not None:
            return error
        assert entry is not None
        if entry.process.returncode is None:
            return ToolResult.fail(call.id, f"Process is still running: {handle}")
        return ToolResult.ok(
            call.id,
            {
                "process_handle": handle,
                "removed": self.processes.remove(handle, context.session_id),
            },
        )

    async def aclose(self) -> None:
        await self.processes.aclose()

    def _find(
        self, context: ToolExecutionContext, call: ToolCall
    ) -> tuple[ManagedProcess | None, ToolResult | None]:
        handle = _handle(call.arguments_dict)
        entry = self.processes.find(handle or "", context.session_id)
        if entry is not None:
            return entry, None
        failure = ToolResult.fail(
            call.id, f"Unknown or lost process handle: {handle or '<missing>'}"
        )
        failure.result = {
            "status": "lost",
            "error": "unknown_or_lost",
            "process_handle": handle,
        }
        return None, failure

    async def _start(
        self, arguments: list[str], cwd: Path, *, background: bool
    ) -> asyncio.subprocess.Process:
        options: dict[str, Any] = {
            "cwd": str(cwd),
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE if background else asyncio.subprocess.STDOUT,
        }
        if os.name == "posix":
            options["start_new_session"] = True
        return await asyncio.create_subprocess_exec(*arguments, **options)

    def _resolve_cwd(self, raw_cwd: Any, workspace: Path | None = None) -> Path:
        if raw_cwd is not None and not isinstance(raw_cwd, str):
            raise ValueError("cwd must be a string")
        root = (workspace or self.working_directory).resolve(strict=True)
        candidate = (root / (raw_cwd or ".")).resolve(strict=True)
        if not candidate.is_relative_to(root) or not candidate.is_dir():
            raise ValueError("Working directory escapes the shell sandbox")
        return candidate

    @staticmethod
    def register(
        manager: PluginManager,
        working_directory: str | Path,
        allowed_commands: list[str] | tuple[str, ...],
    ) -> ShellPlugin:
        plugin = ShellPlugin(working_directory, allowed_commands)
        command_schema = {
            "command": {"type": "string"},
            "cwd": {"type": "string"},
            "background": {"type": "boolean"},
        }
        output_schema = {
            "process_handle": {"type": "string"},
            "handle": {"type": "string"},
            "stdout_offset": {"type": "integer"},
            "stderr_offset": {"type": "integer"},
        }
        handle_schema = {
            "process_handle": {"type": "string"},
            "handle": {"type": "string"},
        }
        for name in ("shell", "bash", "execute_command", "shell_exec"):
            manager.register(
                name,
                plugin.execute,
                definition=tool_definition(
                    name,
                    "Execute a whitelisted command in the sandbox",
                    command_schema,
                    required=("command",),
                ),
            )
        entries: tuple[tuple[tuple[str, ...], ToolHandler, str, dict[str, Any]], ...] = (
            (
                ("process_list", "task_list"),
                plugin.process_list,
                "List local background processes for this session",
                {},
            ),
            (
                ("process_output", "task_output"),
                plugin.process_output,
                "Read local background process output",
                output_schema,
            ),
            (
                ("process_stop", "task_stop"),
                plugin.process_stop,
                "Stop a local background process",
                handle_schema,
            ),
            (
                ("process_cleanup", "task_delete", "task_cleanup"),
                plugin.process_cleanup,
                "Clean up local background process handles",
                handle_schema,
            ),
        )
        for names, handler, description, properties in entries:
            for name in names:
                manager.register(
                    name,
                    handler,
                    definition=tool_definition(name, description, properties),
                )
        manager.register_closeable(plugin)
        return plugin


def _handle(args: dict[str, Any]) -> str | None:
    value = args.get("process_handle") or args.get("handle")
    return value if isinstance(value, str) and value.strip() else None


def _offset(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0
