import asyncio
import os
import sys
from pathlib import Path

import pytest

from taiyiagent import ToolCall, ToolExecutionContext
from taiyiagent.plugins import FilePlugin, SearchPlugin, ShellPlugin


def call(call_id: str, name: str, **arguments: object) -> ToolCall:
    return ToolCall(id=call_id, name=name, arguments=arguments)


@pytest.mark.asyncio
async def test_file_read_write_edit_and_list(tmp_path: Path) -> None:
    plugin = FilePlugin(tmp_path)
    written = await plugin.write_file(call("1", "write_file", path="docs/a.txt", content="one"))
    assert written.success
    read = await plugin.read_file(call("2", "read_file", path="docs/a.txt"))
    assert read.success
    assert read.result["content"] == "one"
    edited = await plugin.edit_file(
        call(
            "3",
            "file_edit",
            path="docs/a.txt",
            old_string="one",
            new_string="two",
            expected_sha256_before=read.result["sha256"],
        )
    )
    assert edited.success
    assert edited.result["replacements"] == 1
    listed = await plugin.list_files(call("4", "list_files", path="docs"))
    assert listed.result["files"][0]["name"] == "a.txt"
    assert (tmp_path / "docs/a.txt").read_text() == "two"


@pytest.mark.asyncio
async def test_file_edit_conflicts_and_empty_write(tmp_path: Path) -> None:
    plugin = FilePlugin(tmp_path)
    assert (await plugin.write_file(call("1", "write_file", path="a.txt", content=""))).success
    await plugin.write_file(call("2", "write_file", path="a.txt", content="same same"))
    ambiguous = await plugin.edit_file(
        call("3", "file_edit", path="a.txt", old_string="same", new_string="new")
    )
    assert not ambiguous.success
    assert ambiguous.result["reason"] == "AMBIGUOUS_MATCH"
    stale = await plugin.edit_file(
        call(
            "4",
            "file_edit",
            path="a.txt",
            old_string="same",
            new_string="new",
            expected_sha256_before="0" * 64,
        )
    )
    assert not stale.success
    assert stale.result["reason"] == "STALE_CONTENT"


@pytest.mark.asyncio
async def test_file_and_search_reject_sandbox_escape(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret")
    files = FilePlugin(workspace)
    search = SearchPlugin(workspace)
    assert not (await files.read_file(call("1", "read_file", path="../outside.txt"))).success
    assert not (await files.write_file(call("2", "write_file", path="../new", content="x"))).success
    escaped = workspace / "link.txt"
    escaped.symlink_to(outside)
    assert not (await files.read_file(call("3", "read_file", path="link.txt"))).success
    assert not (
        await files.write_file(call("4", "write_file", path="link.txt", content="x"))
    ).success
    result = await search.search(call("5", "search", pattern="secret"))
    assert result.success
    assert result.result["matches"] == []


@pytest.mark.asyncio
async def test_search_finds_text_and_regex(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("hello world\nvalue=42\n")
    (tmp_path / "b.txt").write_text("nothing\n")
    plugin = SearchPlugin(tmp_path)
    text = await plugin.search(call("1", "search", pattern="hello"))
    assert text.result["matches"] == [{"file": "a.txt", "line": 1, "content": "hello world"}]
    regex = await plugin.search(call("2", "search", pattern=r"value=\d+", regex=True))
    assert regex.result["matches"][0]["line"] == 2


@pytest.mark.asyncio
async def test_shell_executes_whitelisted_argv_without_shell(tmp_path: Path) -> None:
    plugin = ShellPlugin(tmp_path, [sys.executable])
    context = ToolExecutionContext(session_id="session")
    result = await plugin.execute(
        context,
        call("1", "shell", command=f'"{sys.executable}" -c "print(\'hello\')"'),
    )
    assert result.success
    assert result.result["output"].strip() == "hello"
    assert result.result["exitCode"] == 0
    denied = await plugin.execute(context, call("2", "shell", command="echo unsafe"))
    assert not denied.success
    assert "whitelist" in (denied.error or "")
    escaped = await plugin.execute(
        context,
        call(
            "3",
            "shell",
            command=f'"{sys.executable}" -c "print(1)"',
            cwd="..",
        ),
    )
    assert not escaped.success


@pytest.mark.asyncio
async def test_shell_output_is_bounded(tmp_path: Path) -> None:
    plugin = ShellPlugin(tmp_path, [sys.executable], max_output_chars=5)
    result = await plugin.execute(
        ToolExecutionContext(session_id="session"),
        call("1", "shell", command=f'"{sys.executable}" -c "print(\'0123456789\')"'),
    )
    assert result.success
    assert result.result["output"] == "01234"
    assert result.result["outputTruncated"] is True


@pytest.mark.asyncio
async def test_shell_timeout_kills_process(tmp_path: Path) -> None:
    plugin = ShellPlugin(tmp_path, [sys.executable], timeout=0.01)
    result = await plugin.execute(
        ToolExecutionContext(session_id="session"),
        call("1", "shell", command=f'"{sys.executable}" -c "import time; time.sleep(10)"'),
    )
    assert not result.success
    assert result.result["timedOut"] is True


@pytest.mark.asyncio
async def test_background_process_output_isolation_stop_and_cleanup(tmp_path: Path) -> None:
    plugin = ShellPlugin(tmp_path, [sys.executable])
    first = ToolExecutionContext(session_id="first")
    second = ToolExecutionContext(session_id="second")
    started = await plugin.execute(
        first,
        call(
            "1",
            "shell",
            command=(
                f'"{sys.executable}" -c "import time; print(\'ready\', flush=True); time.sleep(10)"'
            ),
            background=True,
        ),
    )
    assert started.success
    handle = started.result["process_handle"]
    for _ in range(100):
        output = await plugin.process_output(
            first, call("2", "process_output", process_handle=handle)
        )
        if "ready" in output.result["stdout"]["data"]:
            break
        await asyncio.sleep(0.01)
    assert "ready" in output.result["stdout"]["data"]
    lost = await plugin.process_output(second, call("3", "process_output", process_handle=handle))
    assert not lost.success
    stopped = await plugin.process_stop(first, call("4", "process_stop", process_handle=handle))
    assert stopped.success
    entry = plugin.processes.find(handle, "first")
    assert entry is not None
    await asyncio.wait_for(entry.tasks[-1], 1)
    cleaned = await plugin.process_cleanup(
        first, call("5", "process_cleanup", process_handle=handle)
    )
    assert cleaned.result["removed"] is True
    await plugin.aclose()


@pytest.mark.asyncio
async def test_agent_close_cleans_background_processes(tmp_path: Path) -> None:
    if os.name != "posix":
        pytest.skip("process-group assertion is POSIX-specific")
    plugin = ShellPlugin(tmp_path, [sys.executable])
    started = await plugin.execute(
        ToolExecutionContext(session_id="session"),
        call(
            "1",
            "shell",
            command=f'"{sys.executable}" -c "import time; time.sleep(10)"',
            background=True,
        ),
    )
    entry = plugin.processes.find(started.result["process_handle"], "session")
    assert entry is not None
    process = entry.process
    await plugin.aclose()
    assert process.returncode is not None
