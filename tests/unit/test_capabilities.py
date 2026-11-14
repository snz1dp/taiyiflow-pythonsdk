import json
from pathlib import Path

import httpx
import pytest
import respx

from taiyiagent.errors import ApiError
from taiyiagent.transport import RestClient
from taiyiagent.transport.capabilities import (
    CollaborationTaskClient,
    ConfigClient,
    FileClient,
    McpServerClient,
    MemoryClient,
    ScheduleClient,
    SessionClient,
    SkillClient,
)

BASE = "https://example.test"


@pytest.mark.asyncio
@respx.mock
async def test_session_client_routes_and_message_deletion() -> None:
    list_route = respx.get(BASE + "/api/v1/agents/flows/flow/sessions").mock(
        return_value=httpx.Response(200, json=[])
    )
    id_route = respx.get(BASE + "/api/v1/session_id").mock(
        return_value=httpx.Response(200, json={"session_id": "new-session"})
    )
    get_route = respx.get(BASE + "/api/v1/agents/flows/flow/sessions/session").mock(
        return_value=httpx.Response(200, json={"id": "session"})
    )
    rename_route = respx.post(BASE + "/api/v1/agents/flows/flow/sessions/session").mock(
        return_value=httpx.Response(200, json={})
    )
    delete_route = respx.delete(BASE + "/api/v1/agents/flows/flow/sessions/session").mock(
        return_value=httpx.Response(200, json={})
    )
    messages_route = respx.get(BASE + "/api/v1/agents/flows/flow/sessions/session/messages").mock(
        return_value=httpx.Response(200, json=[])
    )
    delete_messages_route = respx.delete(
        BASE + "/api/v1/agents/flows/flow/sessions/session/messages"
    ).mock(return_value=httpx.Response(200, json={}))
    client = SessionClient(RestClient(BASE, "secret"))
    try:
        assert await client.create_session_id() == "new-session"
        await client.list("flow", offset=2, limit=10, filters={"client_id": "client"})
        await client.get("flow", "session")
        await client.rename("flow", "session", "renamed")
        await client.delete("flow", "session")
        await client.delete_all("flow", ["session"])
        await client.messages("flow", "session", filters={"enabled": True})
        await client.delete_messages("flow", "session", ["one", "two"])
    finally:
        await client._rest.aclose()
    assert id_route.called
    assert dict(list_route.calls[0].request.url.params) == {
        "client_id": "client",
        "limit": "10",
        "offset": "2",
    }
    assert get_route.called
    assert json.loads(rename_route.calls[0].request.content) == {"session_name": "renamed"}
    assert delete_route.call_count == 2
    assert messages_route.calls[0].request.url.params["enabled"] == "true"
    assert delete_messages_route.calls[0].request.url.params["message_ids"] == "one,two"


@pytest.mark.asyncio
@respx.mock
async def test_skill_archive_uses_backend_multipart_field() -> None:
    upload_route = respx.post(BASE + "/api/v1/agents/skills/upload").mock(
        return_value=httpx.Response(201, json={"id": "skill"})
    )
    list_route = respx.get(BASE + "/api/v1/agents/skills").mock(
        return_value=httpx.Response(200, json=[])
    )
    content_route = respx.get(BASE + "/api/v1/agents/skills/skill").mock(
        return_value=httpx.Response(200, json={"content": "instructions"})
    )
    install_route = respx.post(BASE + "/api/v1/agents/skills/installations").mock(
        return_value=httpx.Response(201, json={})
    )
    installations_route = respx.get(BASE + "/api/v1/agents/skills/installations").mock(
        return_value=httpx.Response(200, json={})
    )
    uninstall_route = respx.post(
        BASE + "/api/v1/agents/skills/installations/record/uninstall"
    ).mock(return_value=httpx.Response(200, json={}))
    delete_route = respx.delete(BASE + "/api/v1/agents/skills/skill").mock(
        return_value=httpx.Response(200, json={})
    )
    client = SkillClient(RestClient(BASE, "secret"))
    try:
        await client.list(context={"flow_id": "flow"})
        await client.get_content("skill", context={"flow_id": "flow"})
        await client.upload({"name": "skill", "content": "instructions"})
        await client.upload_archive(
            name="skill",
            display_name="Skill",
            filename="skill.zip",
            zip_content=b"zip-content",
        )
        await client.report_installation({"skill_package_id": "skill"})
        await client.installations(filters={"client_id": "client"})
        await client.uninstall("record")
        await client.delete("skill")
    finally:
        await client._rest.aclose()
    multipart = upload_route.calls[-1].request.content
    assert b'name="package_file"' in multipart
    assert b'name="file";' not in multipart
    assert list_route.calls[0].request.url.params["flow_id"] == "flow"
    assert content_route.called
    assert install_route.called
    assert installations_route.called
    assert uninstall_route.called
    assert delete_route.called


@pytest.mark.asyncio
@respx.mock
async def test_memory_config_and_mcp_server_clients() -> None:
    memories_route = respx.route(path="/api/v1/agents/memories").mock(
        return_value=httpx.Response(200, json={})
    )
    memory_route = respx.get(BASE + "/api/v1/agents/memories/memory").mock(
        return_value=httpx.Response(200, json={})
    )
    forget_route = respx.post(BASE + "/api/v1/agents/memories/memory/forget").mock(
        return_value=httpx.Response(200, json={})
    )
    restore_route = respx.post(BASE + "/api/v1/agents/memories/memory/restore").mock(
        return_value=httpx.Response(200, json={})
    )
    config_route = respx.get(BASE + "/api/v1/config").mock(
        return_value=httpx.Response(200, json={"stream_protocol": "sse"})
    )
    servers_route = respx.route(path="/api/v1/agents/mcp/servers").mock(
        return_value=httpx.Response(200, json=[])
    )
    update_route = respx.post(BASE + "/api/v1/agents/mcp/servers/server").mock(
        return_value=httpx.Response(200, json={})
    )
    status_route = respx.post(BASE + "/api/v1/agents/mcp/servers/server/status").mock(
        return_value=httpx.Response(200, json={})
    )
    delete_route = respx.delete(BASE + "/api/v1/agents/mcp/servers/server").mock(
        return_value=httpx.Response(200, json={})
    )
    rest = RestClient(BASE, "secret")
    memories = MemoryClient(rest)
    config = ConfigClient(rest)
    servers = McpServerClient(rest)
    try:
        await memories.list(filters={"status": "active"})
        await memories.create({"summary_text": "remember"})
        await memories.get("memory")
        await memories.forget("memory")
        await memories.restore("memory")
        assert await config.get() == {"stream_protocol": "sse"}
        await servers.list("flow", client_type="cli")
        await servers.create({"server_url": "https://mcp.test"})
        await servers.update("server", {"name": "server"})
        await servers.set_enabled("server", True)
        await servers.delete("server")
    finally:
        await rest.aclose()
    assert memories_route.call_count == 2
    assert memory_route.called
    assert forget_route.called
    assert restore_route.called
    assert config_route.called
    assert servers_route.call_count == 2
    assert dict(servers_route.calls[0].request.url.params) == {
        "client_type": "cli",
        "flow_id": "flow",
    }
    assert update_route.called
    assert status_route.calls[0].request.content == b"enabled=true"
    assert delete_route.called


@pytest.mark.asyncio
@respx.mock
async def test_schedule_client_routes_and_route_queries() -> None:
    schedules_route = respx.route(path="/api/v1/agents/client/schedules").mock(
        return_value=httpx.Response(200, json={})
    )
    schedule_route = respx.route(path="/api/v1/agents/client/schedules/schedule").mock(
        return_value=httpx.Response(200, json={})
    )
    enable_route = respx.post(BASE + "/api/v1/agents/client/schedules/schedule/enable").mock(
        return_value=httpx.Response(200, json={})
    )
    disable_route = respx.post(BASE + "/api/v1/agents/client/schedules/schedule/disable").mock(
        return_value=httpx.Response(200, json={})
    )
    runs_route = respx.get(BASE + "/api/v1/agents/client/schedules/schedule/runs").mock(
        return_value=httpx.Response(200, json={})
    )
    report_route = respx.post(BASE + "/api/v1/agents/client/schedules/runs/run/report").mock(
        return_value=httpx.Response(200, json={})
    )
    client = ScheduleClient(RestClient(BASE, "secret"))
    try:
        await client.list(filters={"disabled": False})
        await client.get("schedule")
        await client.create({"name": "schedule"})
        await client.update("schedule", {"name": "updated"})
        await client.delete("schedule")
        await client.enable("schedule", route={"client_id": "client", "device_id": "device"})
        await client.disable("schedule", route={"client_id": "client"})
        await client.runs("schedule", offset=1, limit=5, filters={"status": "running"})
        await client.report("run", {"status": "success"})
    finally:
        await client._rest.aclose()
    assert schedules_route.call_count == 2
    assert schedule_route.call_count == 3
    assert dict(enable_route.calls[0].request.url.params) == {
        "client_id": "client",
        "device_id": "device",
    }
    assert disable_route.called
    assert dict(runs_route.calls[0].request.url.params) == {
        "limit": "5",
        "offset": "1",
        "status": "running",
    }
    assert report_route.called


@pytest.mark.asyncio
@respx.mock
async def test_collaboration_status_payloads_and_expected_versions() -> None:
    collection_route = respx.route(path="/api/v1/agents/client/collaborations").mock(
        return_value=httpx.Response(200, json={})
    )
    get_route = respx.get(BASE + "/api/v1/agents/client/collaborations/task").mock(
        return_value=httpx.Response(200, json={})
    )
    cancel_route = respx.post(BASE + "/api/v1/agents/client/collaborations/task/cancel").mock(
        return_value=httpx.Response(200, json={})
    )
    update_route = respx.patch(BASE + "/api/v1/agents/client/collaborations/task").mock(
        return_value=httpx.Response(200, json={})
    )
    client = CollaborationTaskClient(RestClient(BASE, "secret"))
    try:
        await client.create({"prompt": "work"})
        await client.get("task")
        await client.list(filters={"status": "pending"})
        await client.cancel("task", "request", expected_version=2)
        await client.running("task")
        await client.running("task", {"percent": 50}, expected_version=3)
        await client.success("task", "done", expected_version=4)
        await client.failure("task", " ", expected_version=5)
        await client.cancelled("task", "cancel-request", expected_version=6)
    finally:
        await client._rest.aclose()
    assert collection_route.call_count == 2
    assert get_route.called
    assert json.loads(cancel_route.calls[0].request.content) == {
        "request_id": "request",
        "expected_version": 2,
    }
    bodies = [json.loads(call.request.content) for call in update_route.calls]
    assert bodies[0] == {"status": "running"}
    assert bodies[1]["status_payload"] == {"percent": 50}
    assert bodies[2]["result_payload"] == {"result": "done"}
    assert bodies[3]["error_message"] == "Task execution failed"
    assert bodies[4]["status_payload"] == {"cancel_request_id": "cancel-request"}
    assert [body["expected_version"] for body in bodies[1:]] == [3, 4, 5, 6]


@pytest.mark.asyncio
async def test_file_upload_and_download(tmp_path: Path) -> None:
    source = tmp_path / "hello.txt"
    source.write_text("hello", encoding="utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            assert request.url.path == "/api/v1/agents/upload/flow"
            assert b'name="session_id"' in request.content
            assert b'name="file_path"' in request.content
            assert b'name="file"; filename="hello.txt"' in request.content
            return httpx.Response(201, json={"flow_id": "flow", "file_path": "flow/session/a.txt"})
        assert request.url.raw_path == b"/api/v1/agents/download/flow/session/folder/a%20b.txt"
        return httpx.Response(200, content=b"downloaded")

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FileClient(RestClient(BASE, "secret", client=http_client))
    uploaded = await client.upload("flow", "session", source, remote_path="folder/a b.txt")
    assert uploaded.flow_id == "flow"
    assert uploaded.file_path == "flow/session/a.txt"
    assert await client.download("flow", "session", "folder/a b.txt") == b"downloaded"
    await http_client.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_capability_multipart_refreshes_token_only_once() -> None:
    token = "old-token"
    refreshed = 0

    async def refresh() -> None:
        nonlocal token, refreshed
        refreshed += 1
        token = "new-token"

    route = respx.post(BASE + "/api/v1/agents/skills/upload").mock(
        side_effect=[
            httpx.Response(401, json={"detail": "expired"}),
            httpx.Response(200, json={"id": "skill"}),
        ]
    )
    rest = RestClient(BASE, access_token=lambda: token, on_unauthorized=refresh)
    client = SkillClient(rest)
    try:
        await client.upload_archive(
            name="skill", display_name="Skill", filename="skill.zip", zip_content=b"zip"
        )
    finally:
        await rest.aclose()
    assert refreshed == 1
    assert route.call_count == 2
    assert route.calls[0].request.headers["Authorization"] == "Bearer old-token"
    assert route.calls[1].request.headers["Authorization"] == "Bearer new-token"


@pytest.mark.asyncio
@respx.mock
async def test_capability_does_not_retry_second_unauthorized() -> None:
    refreshed = 0

    async def refresh() -> None:
        nonlocal refreshed
        refreshed += 1

    route = respx.get(BASE + "/api/v1/config").mock(
        return_value=httpx.Response(401, json={"detail": "expired"})
    )
    rest = RestClient(BASE, "secret", on_unauthorized=refresh)
    try:
        with pytest.raises(ApiError) as error:
            await ConfigClient(rest).get()
    finally:
        await rest.aclose()
    assert error.value.status_code == 401
    assert refreshed == 1
    assert route.call_count == 2
