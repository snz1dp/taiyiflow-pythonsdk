from __future__ import annotations

import asyncio
import codecs
import os
import signal
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class OutputBuffer:
    def __init__(self, capacity: int, *, drop_oldest: bool = True) -> None:
        self.capacity = capacity
        self.drop_oldest = drop_oldest
        self.data = ""
        self.total_chars = 0
        self.dropped_chars = 0

    @property
    def truncated(self) -> bool:
        return self.dropped_chars > 0

    def append(self, value: str) -> None:
        self.total_chars += len(value)
        if not self.drop_oldest:
            remaining = max(0, self.capacity - len(self.data))
            self.data += value[:remaining]
            self.dropped_chars += max(0, len(value) - remaining)
            return
        self.data += value
        overflow = len(self.data) - self.capacity
        if overflow > 0:
            self.data = self.data[overflow:]
            self.dropped_chars += overflow

    def output(self, offset: int) -> dict[str, Any]:
        base = self.total_chars - len(self.data)
        reset = offset < base
        start = max(0, offset - base)
        return {
            "data": self.data[start:],
            "next_offset": base + len(self.data),
            "dropped_chars": self.dropped_chars,
            "offset_reset": reset,
        }


@dataclass(slots=True)
class ManagedProcess:
    handle: str
    session_id: str
    process: asyncio.subprocess.Process
    command: list[str]
    cwd: Path
    run_id: str | None
    task_id: str | None
    output_capacity: int
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ended_at: datetime | None = None
    stopped: bool = False
    timed_out: bool = False
    stdout: OutputBuffer = field(init=False)
    stderr: OutputBuffer = field(init=False)
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.stdout = OutputBuffer(self.output_capacity)
        self.stderr = OutputBuffer(self.output_capacity)

    @property
    def status(self) -> str:
        if self.process.returncode is None:
            return "running"
        if self.timed_out:
            return "timed_out"
        if self.stopped:
            return "stopped"
        return "finished"

    def start(self) -> None:
        self.tasks = [
            asyncio.create_task(_read_stream(self.process.stdout, self.stdout)),
            asyncio.create_task(_read_stream(self.process.stderr, self.stderr)),
            asyncio.create_task(self._wait()),
        ]

    async def _wait(self) -> None:
        await self.process.wait()
        if self.tasks:
            await asyncio.gather(*self.tasks[:2], return_exceptions=True)
        self.ended_at = datetime.now(UTC)

    def stop(self) -> None:
        if self.process.returncode is not None:
            return
        self.stopped = True
        _terminate(self.process)

    def describe(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "process_handle": self.handle,
            "status": self.status,
            "command": self.command,
            "cwd": str(self.cwd),
            "session_id": self.session_id,
            "created_at": self.created_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "exit_code": self.process.returncode,
        }
        if self.run_id:
            result["run_id"] = self.run_id
        if self.task_id:
            result["task_id"] = self.task_id
        return result


class BackgroundProcessRegistry:
    def __init__(self, *, max_processes: int = 64, output_capacity: int = 65_536) -> None:
        self.max_processes = max_processes
        self.output_capacity = output_capacity
        self._processes: dict[str, ManagedProcess] = {}

    def add(
        self,
        process: asyncio.subprocess.Process,
        *,
        session_id: str,
        command: list[str],
        cwd: Path,
        run_id: str | None,
        task_id: str | None,
    ) -> ManagedProcess:
        self.cleanup_finished()
        if len(self._processes) >= self.max_processes:
            _terminate(process)
            raise ValueError("Background process limit reached")
        entry = ManagedProcess(
            str(uuid.uuid4()),
            session_id,
            process,
            command,
            cwd,
            run_id,
            task_id,
            self.output_capacity,
        )
        self._processes[entry.handle] = entry
        entry.start()
        return entry

    def list(self, session_id: str) -> list[ManagedProcess]:
        return [entry for entry in self._processes.values() if entry.session_id == session_id]

    def find(self, handle: str, session_id: str) -> ManagedProcess | None:
        entry = self._processes.get(handle)
        return entry if entry and entry.session_id == session_id else None

    def cleanup_finished(self, session_id: str | None = None) -> int:
        removable = [
            handle
            for handle, entry in self._processes.items()
            if entry.process.returncode is not None
            and (session_id is None or entry.session_id == session_id)
        ]
        for handle in removable:
            del self._processes[handle]
        return len(removable)

    def remove(self, handle: str, session_id: str) -> bool:
        entry = self.find(handle, session_id)
        if entry is None or entry.process.returncode is None:
            return False
        del self._processes[handle]
        return True

    async def aclose(self) -> None:
        entries = list(self._processes.values())
        for entry in entries:
            entry.stop()
        tasks = [task for entry in entries for task in entry.tasks]
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=2)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        self._processes.clear()


async def _read_stream(stream: asyncio.StreamReader | None, output: OutputBuffer) -> None:
    if stream is None:
        return
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    while True:
        chunk = await stream.read(8192)
        if not chunk:
            output.append(decoder.decode(b"", final=True))
            return
        output.append(decoder.decode(chunk))


def _terminate(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except (ProcessLookupError, PermissionError):
        pass
