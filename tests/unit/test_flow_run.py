import asyncio
import json
from pathlib import Path

import httpx
import pytest

from taiyiagent.errors import ProtocolError
from taiyiagent.models import ChatRequest
from taiyiagent.transport.capabilities import FileClient
from taiyiagent.transport.config import FlowMetadata, ServiceConfig
from taiyiagent.transport.flow_run import (
    FlowRunClient,
    FlowRunPollOptions,
    FlowRunTimeoutError,
    SubmissionUnknownError,
)
from taiyiagent.transport.rest import RestClient

BASE = "https://example.test"


def flow_client(handler: object) -> tuple[FlowRunClient, RestClient]:
    transport = httpx.MockTransport(handler)  # type: ignore[arg-type]
    rest = RestClient(BASE, "secret", client=httpx.AsyncClient(transport=transport))
    return FlowRunClient(rest), rest


def task(task_id: str = "task-1", status: str = "running") -> dict[str, object]:
    return {"task_id": task_id, "status": status}


@pytest.mark.asyncio
async def test_sync_and_async_flow_run_use_distinct_execution_modes() -> None:
    bodies: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={"outputs": ["document"], "session_id": "session-1"},
            headers={"x-session-id": "session-header"},
        )

    client, rest = flow_client(handler)
    try:
        result = await client.run("flow name", ChatRequest(input_value="hello"))
    finally:
        await rest.aclose()
    assert bodies == [{"input_value": "hello", "async": False}]
    assert result.outputs == ("document",)
    assert result.session_id == "session-1"
    assert result.response_session_header == "session-header"


@pytest.mark.asyncio
async def test_async_flow_run_polls_until_terminal() -> None:
    polls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal polls
        if request.method == "POST":
            assert json.loads(request.content)["async"] is True
            return httpx.Response(202, json=task())
        polls += 1
        if polls == 1:
            return httpx.Response(200, json=task())
        return httpx.Response(
            200,
            json=task(status="success")
            | {"result": {"outputs": ["done"], "media_type": "text/plain"}},
        )

    client, rest = flow_client(handler)
    try:
        submitted = await client.submit_async("flow", ChatRequest(input_value="hello"))
        completed = await client.await_task(
            submitted.task_id,
            options=FlowRunPollOptions(poll_interval=0.001, overall_timeout=1),
        )
    finally:
        await rest.aclose()
    assert completed.status == "success"
    assert completed.result is not None
    assert completed.result.outputs == ("done",)
    assert polls == 2


@pytest.mark.asyncio
async def test_submission_network_failure_is_unknown_and_not_retried() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("connection lost", request=request)

    client, rest = flow_client(handler)
    try:
        with pytest.raises(SubmissionUnknownError):
            await client.submit_async("flow", ChatRequest(input_value="hello"))
    finally:
        await rest.aclose()
    assert calls == 1


@pytest.mark.asyncio
async def test_async_submission_requires_202_and_task_id() -> None:
    client, rest = flow_client(lambda _request: httpx.Response(200, json=task()))
    try:
        with pytest.raises(SubmissionUnknownError):
            await client.submit_async("flow", ChatRequest(input_value="hello"))
    finally:
        await rest.aclose()

    client, rest = flow_client(lambda _request: httpx.Response(202, json={"status": "pending"}))
    try:
        with pytest.raises(SubmissionUnknownError):
            await client.submit_async("flow", ChatRequest(input_value="hello"))
    finally:
        await rest.aclose()


@pytest.mark.asyncio
async def test_flow_run_timeout_requests_cancellation() -> None:
    cancellations = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal cancellations
        if request.url.path.endswith("/cancel"):
            cancellations += 1
            return httpx.Response(202, json=task(status="cancelled"))
        return httpx.Response(200, json=task())

    client, rest = flow_client(handler)
    try:
        with pytest.raises(FlowRunTimeoutError) as caught:
            await client.await_task(
                "task-1",
                options=FlowRunPollOptions(poll_interval=0.001, overall_timeout=0.005),
            )
    finally:
        await rest.aclose()
    assert caught.value.task_id == "task-1"
    assert caught.value.cancellation_requested
    assert cancellations == 1


@pytest.mark.asyncio
async def test_async_wait_cancellation_requests_server_cancellation() -> None:
    cancellations = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal cancellations
        if request.url.path.endswith("/cancel"):
            cancellations += 1
            return httpx.Response(202, json=task(status="cancelled"))
        return httpx.Response(200, json=task())

    client, rest = flow_client(handler)
    waiting = asyncio.create_task(
        client.await_task(
            "task-1",
            options=FlowRunPollOptions(poll_interval=0.5, overall_timeout=10),
        )
    )
    await asyncio.sleep(0)
    waiting.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await waiting
    finally:
        await rest.aclose()
    assert cancellations == 1


@pytest.mark.asyncio
async def test_unknown_task_status_is_protocol_error() -> None:
    client, rest = flow_client(lambda _request: httpx.Response(200, json=task(status="mystery")))
    try:
        with pytest.raises(ProtocolError, match="Unknown flow task status"):
            await client.await_task("task-1")
    finally:
        await rest.aclose()


def test_service_config_normalizes_limits_and_preserves_raw() -> None:
    config = ServiceConfig(
        {
            "default_flow_id": " flow-default ",
            "doc_unstructured_flow_id": "doc-flow",
            "image_flow_id": "image-flow",
            "audio_flow_id": "audio-flow",
            "video_flow_id": "video-flow",
            "doc_exts": [".TXT", "txt", " Md ", 3],
            "max_file_size_upload": 2,
            "once_max_file_count": 3,
            "future_setting": {"enabled": True},
        }
    )
    assert config.default_flow_id() == "flow-default"
    assert config.document_extensions() == ("txt", "md")
    assert config.max_file_size_bytes() == 2 * 1024 * 1024
    assert config.once_max_file_count() == 3
    raw = config.raw
    raw["future_setting"] = {}
    assert config.raw["future_setting"] == {"enabled": True}

    with pytest.raises(ProtocolError, match="default_flow_id"):
        ServiceConfig({}).default_flow_id()
    with pytest.raises(ProtocolError, match="must be positive"):
        ServiceConfig({"once_max_file_count": 0}).once_max_file_count()


def test_flow_metadata_retains_unknown_features_and_fields() -> None:
    metadata = FlowMetadata(
        {
            "id": "flow",
            "features": ["reasoning", "future_feature"],
            "future_field": {"value": 1},
        }
    )
    assert metadata.supports("future_feature")
    assert metadata.additional_properties["future_field"] == {"value": 1}
    raw = metadata.raw
    raw["features"] = []
    assert metadata.features == ("reasoning", "future_feature")


@pytest.mark.asyncio
async def test_streaming_upload_reopens_content_after_token_refresh(tmp_path: Path) -> None:
    source = tmp_path / "hello.txt"
    source.write_text("hello", encoding="utf-8")
    token = "old-token"
    bodies: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        if request.headers["Authorization"] == "Bearer old-token":
            return httpx.Response(401, json={"detail": "expired"})
        return httpx.Response(201, json={"flow_id": "flow", "file_path": "remote.txt"})

    async def refresh() -> None:
        nonlocal token
        token = "new-token"

    rest = RestClient(
        BASE,
        access_token=lambda: token,
        on_unauthorized=refresh,
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    client = FileClient(rest)
    try:
        uploaded = await client.upload("flow", "session", source)
    finally:
        await rest.aclose()
    assert uploaded.file_path == "remote.txt"
    assert len(bodies) == 2
    assert all(b"hello" in body for body in bodies)
    assert all(b'filename="hello.txt"' in body for body in bodies)


@pytest.mark.asyncio
async def test_upload_preflight_rejects_invalid_files_and_limits(tmp_path: Path) -> None:
    source = tmp_path / "payload.bin"
    source.write_bytes(b"payload")
    empty = tmp_path / "empty.txt"
    empty.write_bytes(b"")
    link = tmp_path / "link.txt"
    try:
        link.symlink_to(source)
    except OSError:
        pytest.skip("Symbolic links are unavailable")
    rest = RestClient(
        BASE,
        "secret",
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: httpx.Response(500))
        ),
    )
    client = FileClient(rest)
    limits = ServiceConfig({"max_file_size_upload": 1, "once_max_file_count": 1})
    try:
        with pytest.raises(ValueError, match="must not be empty"):
            await client.upload("flow", "session", empty)
        with pytest.raises(ValueError, match="non-symbolic-link"):
            await client.upload("flow", "session", link)
        with pytest.raises(ValueError, match="extension"):
            await client.upload("flow", "session", source, allowed_extensions={"txt"})
        oversized = tmp_path / "oversized.bin"
        oversized.write_bytes(b"x" * (1024 * 1024 + 1))
        with pytest.raises(ValueError, match="exceeds max_file_size_upload"):
            await client.upload("flow", "session", oversized, limits=limits)
        with pytest.raises(ValueError, match="count"):
            await client.upload_many("flow", "session", [source, source], limits=limits)
    finally:
        await rest.aclose()
