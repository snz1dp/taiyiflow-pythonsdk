import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from taiyiagent.models import ToolCall
from taiyiagent.plugins import PluginManager, TodoPlugin
from taiyiagent.plugins.manager import ToolExecutionContext
from taiyiagent.plugins.media import MediaUnderstandingPlugin
from taiyiagent.plugins.runtime import (
    AskUserPlugin,
    ScheduleTools,
    SessionContextPlugin,
    UserInteractionRequest,
    UserInteractionResponse,
)
from taiyiagent.plugins.search import SearchPlugin
from taiyiagent.plugins.shell import ShellPlugin
from taiyiagent.transport.config import FlowMetadata
from taiyiagent.transport.rest import RestClient

BASE = "https://example.test"


def context(tmp_path: Path, **kwargs: Any) -> ToolExecutionContext:
    values: dict[str, Any] = {
        "session_id": "session-1",
        "flow_id": "business-flow",
        "run_id": "run-1",
        "workspace": tmp_path,
    }
    values.update(kwargs)
    return ToolExecutionContext(**values)


def call(tool_name: str, **arguments: Any) -> ToolCall:
    return ToolCall(id="call-1", name=tool_name, arguments=arguments)


def rest_client() -> RestClient:
    return RestClient(BASE, "secret")


@pytest.mark.asyncio
@respx.mock
async def test_image_upload_uses_business_flow_and_understanding_uses_image_flow(
    tmp_path: Path,
) -> None:
    (tmp_path / "image.png").write_bytes(b"image")
    respx.get(BASE + "/api/v1/config").mock(
        return_value=httpx.Response(200, json={"image_flow_id": "image-flow"})
    )
    upload = respx.post(BASE + "/api/v1/agents/upload/business-flow").mock(
        return_value=httpx.Response(
            201, json={"flow_id": "business-flow", "file_path": "remote/image.png"}
        )
    )
    submitted = respx.post(BASE + "/api/v1/run/image-flow").mock(
        return_value=httpx.Response(202, json={"task_id": "flow-task-1", "status": "pending"})
    )
    respx.get(BASE + "/api/v1/run/tasks/flow-task-1").mock(
        return_value=httpx.Response(
            200,
            json={
                "task_id": "flow-task-1",
                "status": "success",
                "session_id": "media-session",
                "result": {"outputs": ["visible text"], "media_type": "text/plain"},
            },
        )
    )
    rest = rest_client()
    plugin = MediaUnderstandingPlugin(rest)
    try:
        result = await plugin.image(
            context(tmp_path), call("image_understanding", path="image.png")
        )
    finally:
        await rest.aclose()
    assert result.success
    assert result.result["text"] == "visible text"
    assert result.result["uploaded_files"][0]["remote_path"] == "remote/image.png"
    assert b'name="session_id"\r\n\r\nsession-1' in upload.calls[0].request.content
    request_body = json.loads(submitted.calls[0].request.content)
    assert request_body["async"] is True
    assert request_body["files"] == ["remote/image.png"]
    assert request_body["tweaks"]["TextOutput-fXIUJ"] == {"output_format": "text"}


@pytest.mark.asyncio
@respx.mock
async def test_document_run_is_synchronous_and_preserves_parsed_output(tmp_path: Path) -> None:
    (tmp_path / "document.pdf").write_bytes(b"pdf")
    respx.get(BASE + "/api/v1/config").mock(
        return_value=httpx.Response(
            200, json={"doc_unstructured_flow_id": "document-flow", "doc_exts": ["pdf"]}
        )
    )
    respx.post(BASE + "/api/v1/agents/upload/business-flow").mock(
        return_value=httpx.Response(
            201, json={"flow_id": "business-flow", "file_path": "remote/document.pdf"}
        )
    )
    run = respx.post(BASE + "/api/v1/run/document-flow").mock(
        return_value=httpx.Response(
            200,
            json={
                "outputs": ['{"items":[{"text":"first"}],"extra":1}'],
                "session_id": "document-session",
            },
        )
    )
    rest = rest_client()
    plugin = MediaUnderstandingPlugin(rest)
    try:
        result = await plugin.document(
            context(tmp_path), call("document_unstructured", path="document.pdf")
        )
    finally:
        await rest.aclose()
    assert result.success
    assert result.result["items"] == [{"text": "first"}]
    assert result.result["parsed_output"]["extra"] == 1
    assert json.loads(run.calls[0].request.content)["async"] is False


@pytest.mark.asyncio
@respx.mock
async def test_unknown_media_submission_is_not_retried_and_keeps_upload_receipt(
    tmp_path: Path,
) -> None:
    (tmp_path / "audio.mp3").write_bytes(b"audio")
    respx.get(BASE + "/api/v1/config").mock(
        return_value=httpx.Response(200, json={"audio_flow_id": "audio-flow"})
    )
    respx.post(BASE + "/api/v1/agents/upload/business-flow").mock(
        return_value=httpx.Response(
            201, json={"flow_id": "business-flow", "file_path": "remote/audio.mp3"}
        )
    )
    submissions = 0

    def fail_submission(request: httpx.Request) -> httpx.Response:
        nonlocal submissions
        submissions += 1
        raise httpx.ConnectError("lost", request=request)

    respx.post(BASE + "/api/v1/run/audio-flow").mock(side_effect=fail_submission)
    rest = rest_client()
    plugin = MediaUnderstandingPlugin(rest)
    try:
        result = await plugin.audio(
            context(tmp_path), call("audio_understanding", path="audio.mp3")
        )
    finally:
        await rest.aclose()
    assert not result.success
    assert result.result["failed_stage"] == "understanding"
    assert result.result["uploaded_files"][0]["remote_path"] == "remote/audio.mp3"
    assert submissions == 1


@pytest.mark.asyncio
@respx.mock
async def test_screenshot_understanding_adds_coordinate_prompt_and_text_output(
    tmp_path: Path,
) -> None:
    (tmp_path / "screen.png").write_bytes(b"screen")
    respx.get(BASE + "/api/v1/config").mock(
        return_value=httpx.Response(200, json={"image_flow_id": "image-flow"})
    )
    respx.post(BASE + "/api/v1/agents/upload/business-flow").mock(
        return_value=httpx.Response(
            201, json={"flow_id": "business-flow", "file_path": "remote/screen.png"}
        )
    )
    run = respx.post(BASE + "/api/v1/run/image-flow").mock(
        return_value=httpx.Response(202, json={"task_id": "task-1", "status": "pending"})
    )
    respx.get(BASE + "/api/v1/run/tasks/task-1").mock(
        return_value=httpx.Response(
            200,
            json={
                "task_id": "task-1",
                "status": "success",
                "result": {"outputs": ["button at 10,20"]},
            },
        )
    )
    rest = rest_client()
    plugin = MediaUnderstandingPlugin(rest)
    try:
        text = await plugin.understand_screenshot(
            context(tmp_path),
            {
                "path": "screen.png",
                "input": "Where is the button?",
                "screenshot_width_px": 100,
                "screenshot_height_px": 80,
            },
        )
    finally:
        await rest.aclose()
    assert text == "button at 10,20"
    body = json.loads(run.calls[0].request.content)
    assert "截图坐标上下文" in body["input_value"]
    assert "100 x 80" in body["input_value"]
    assert body["tweaks"]["image_compression_strategy"] == "passthrough"


@pytest.mark.asyncio
@respx.mock
async def test_schedule_tools_use_trusted_flow_and_validate_payload(tmp_path: Path) -> None:
    create = respx.post(BASE + "/api/v1/agents/client/schedules").mock(
        return_value=httpx.Response(200, json={"id": "schedule-1"})
    )
    rest = rest_client()
    tools = ScheduleTools(rest)
    try:
        invalid = await tools.create(
            context(tmp_path),
            call("schedule_create", name="bad", schedule_type="interval", prompt="x"),
        )
        result = await tools.create(
            context(tmp_path),
            call(
                "schedule_create",
                name="nightly",
                schedule_type="cron",
                cron_expr="0 0 * * *",
                prompt="run",
                flow_id="untrusted-flow",
            ),
        )
    finally:
        await rest.aclose()
    assert not invalid.success
    assert result.success
    payload = json.loads(create.calls[0].request.content)
    assert payload["flow_id"] == "business-flow"
    assert "untrusted-flow" not in payload.values()


@pytest.mark.asyncio
async def test_todo_is_bounded_transient_and_cleared_per_session(tmp_path: Path) -> None:
    plugin = TodoPlugin()
    result = await plugin.write(
        context(tmp_path),
        call("todo_write", items=[{"title": " first ", "status": "in_progress"}]),
    )
    assert result.success
    assert result.result["transient"] is True
    assert result.result["items"][0]["title"] == "first"
    plugin.close_session("session-1")
    assert plugin._sessions == {}
    invalid = await plugin.write(
        context(tmp_path), call("todo_write", items=[{"title": "x", "status": "done"}])
    )
    assert not invalid.success


@pytest.mark.asyncio
async def test_ask_user_validates_response_and_observes_cancellation(tmp_path: Path) -> None:
    class Handler:
        def __init__(self) -> None:
            self.started = asyncio.Event()

        async def ask(
            self, request: UserInteractionRequest, cancellation_requested: Callable[[], bool]
        ) -> UserInteractionResponse:
            self.started.set()
            if request.questions[0].id == "wait":
                await asyncio.Future()
            return UserInteractionResponse({"choice": ("a", "b")})

    handler = Handler()
    plugin = AskUserPlugin(handler)
    result = await plugin.ask(
        context(tmp_path),
        call("ask_user", questions=[{"id": "choice", "prompt": "Choose"}]),
    )
    assert result.success
    assert result.result["answers"] == {"choice": ["a", "b"]}

    cancelled = False
    waiting = asyncio.create_task(
        plugin.ask(
            context(tmp_path, cancellation_requested=lambda: cancelled),
            call("ask_user", questions=[{"id": "wait", "prompt": "Wait"}]),
        )
    )
    await handler.started.wait()
    cancelled = True
    cancelled_result = await waiting
    assert not cancelled_result.success
    assert "cancelled" in cancelled_result.error.lower()


@pytest.mark.asyncio
async def test_session_context_returns_metadata_and_tool_capabilities(tmp_path: Path) -> None:
    async def metadata() -> FlowMetadata:
        return FlowMetadata({"id": "business-flow", "features": ["future"], "unknown": 1})

    plugin = SessionContextPlugin(metadata)
    result = await plugin.read(
        context(
            tmp_path,
            task_id="collaboration-task",
            allowed_tools=frozenset({"read_file"}),
            capabilities={"inherited_skills": ["safe"]},
        ),
        call("session_context"),
    )
    assert result.success
    assert result.result["flow_metadata"]["unknown"] == 1
    assert result.result["allowed_tools"] == ["read_file"]
    assert result.result["capabilities"]["inherited_skills"] == ["safe"]


@pytest.mark.asyncio
async def test_builtin_aliases_and_glob_share_sandbox_handlers(tmp_path: Path) -> None:
    (tmp_path / "one.txt").write_text("needle", encoding="utf-8")
    (tmp_path / "two.bin").write_bytes(b"two")
    manager = PluginManager()
    SearchPlugin.register(manager, tmp_path)
    ShellPlugin.register(manager, tmp_path, ())
    try:
        assert manager.get_definition("glob_files") is not None
        assert manager.get_definition("shell_exec") is not None
        assert manager.get_definition("task_output") is not None
        result = await manager.execute(context(tmp_path), call("glob_files", pattern="*.txt"))
    finally:
        await manager.aclose()
    assert result.success
    assert result.result["files"] == ["one.txt"]
