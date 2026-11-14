from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
import respx

from taiyiagent.collaboration import TaskDispatch
from taiyiagent.models import ChatRequest, ToolCall
from taiyiagent.plugins import PluginManager, ToolExecutionContext
from taiyiagent.transport import RestClient
from taiyiagent.worker import (
    AgentSessionTaskHandler,
    SubtaskCapabilitySnapshot,
    SubtaskOrchestrator,
    SubtaskTask,
)

BASE = "https://taiyi.test"
TASKS = BASE + "/api/v1/agents/client/collaborations"


def context(request_mode: str = "agent") -> ToolExecutionContext:
    return ToolExecutionContext(
        session_id="parent-session",
        run_id="parent-run",
        task_id="parent-task",
        request_mode=request_mode,
        flow_id="flow-1",
        message_id="message-1",
        chat_request=ChatRequest(
            reasoning_enabled=True,
            internet_search=True,
            requested_skill_names=["skill-a"],
            available_tools=[{"name": "run_subtask"}, {"name": "read_file"}],
            client_mcp_servers={"allowed": {"command": "secret-command"}},
        ),
    )


def call(arguments: dict[str, Any]) -> ToolCall:
    return ToolCall(id="call-1", name="run_subtask", arguments=arguments)


def created_response(request: httpx.Request, task_id: str = "child-task") -> httpx.Response:
    payload = json.loads(request.content)
    return httpx.Response(
        200,
        json={
            "id": task_id,
            "version": 1,
            "status": "pending",
            "child_run_id": payload["child_run_id"],
            "child_session_id": payload["child_session_id"],
            "idempotency_key": payload["idempotency_key"],
        },
    )


async def test_subtask_payload_and_results_preserve_indices() -> None:
    requests: list[dict[str, Any]] = []
    dispatched: list[SubtaskTask] = []

    async def dispatch(task: SubtaskTask) -> dict[str, Any]:
        dispatched.append(task)
        if task.task_id == "child-0":
            await asyncio.sleep(0.01)
        return {"status": "success", "result": {"text": task.task_id}}

    def create(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        requests.append(payload)
        return created_response(request, f"child-{len(requests) - 1}")

    rest = RestClient(BASE, api_key="key")
    plugins = PluginManager()
    orchestrator = SubtaskOrchestrator(
        rest,
        plugins,
        dispatch,
        local_client_id="client",
        local_device_id="device",
    )
    assert plugins.has_handler("run_subtask")
    with respx.mock(base_url=BASE) as mocked:
        mocked.post("/api/v1/agents/client/collaborations").mock(side_effect=create)
        result = await orchestrator.execute(
            context(),
            call({"subtasks": [{"prompt": "first"}, {"prompt": "second"}]}),
        )
    await rest.aclose()

    assert [item["index"] for item in result["results"]] == [0, 1]
    assert result["successful"] == 2
    assert {task.task_id for task in dispatched} == {"child-0", "child-1"}
    command = requests[0]["command_payload"]
    assert command["input_value"] == "first"
    assert command["parent_id"] == "parent-run"
    assert command["request_mode"] == "agent"
    assert command["features"]["client_mcp_server_names"] == ["allowed"]
    assert "secret-command" not in json.dumps(requests)
    assert requests[0]["source_message_id"] == "message-1"
    assert requests[0]["parent_task_id"] == "parent-task"
    assert requests[0]["task_kind"] == "dialogue_subtask"


async def test_subtask_rejects_mode_upgrade_and_detached_execution() -> None:
    rest = RestClient(BASE, api_key="key")
    orchestrator = SubtaskOrchestrator(rest, PluginManager())
    upgraded = await orchestrator.execute(
        context("ask"), call({"prompt": "unsafe", "request_mode": "agent"})
    )
    detached = await orchestrator.execute(
        context(), call({"prompt": "x", "wait_for_completion": False})
    )
    await rest.aclose()
    assert "cannot exceed" in upgraded["error"]
    assert "Detached" in detached["error"]


async def test_subtask_creation_failure_only_reconciles() -> None:
    posts = 0
    dispatched: list[SubtaskTask] = []

    async def dispatch(task: SubtaskTask) -> dict[str, Any]:
        dispatched.append(task)
        return {"status": "success"}

    rest = RestClient(BASE, api_key="key")
    orchestrator = SubtaskOrchestrator(
        rest,
        PluginManager(),
        dispatch,
        local_client_id="client",
        local_device_id="device",
    )
    with respx.mock(base_url=BASE) as mocked:

        def create(request: httpx.Request) -> httpx.Response:
            nonlocal posts
            posts += 1
            payload = json.loads(request.content)
            mocked.get("/api/v1/agents/client/collaborations").respond(
                json={
                    "items": [
                        {
                            "id": "reconciled",
                            "version": 2,
                            "status": "pending",
                            "idempotency_key": payload["idempotency_key"],
                            "child_run_id": payload["child_run_id"],
                            "child_session_id": payload["child_session_id"],
                        }
                    ]
                }
            )
            return httpx.Response(500, json={"detail": "connection lost"})

        mocked.post("/api/v1/agents/client/collaborations").mock(side_effect=create)
        result = await orchestrator.execute(context(), call({"prompt": "reconcile"}))
    await rest.aclose()
    assert posts == 1
    assert result["results"][0]["status"] == "success"
    assert [task.task_id for task in dispatched] == ["reconciled"]


async def test_subtask_unknown_creation_is_not_executed() -> None:
    called = False

    async def dispatch(task: SubtaskTask) -> dict[str, Any]:
        nonlocal called
        called = True
        return {"status": "success"}

    rest = RestClient(BASE, api_key="key")
    orchestrator = SubtaskOrchestrator(
        rest, PluginManager(), dispatch, local_client_id="client", local_device_id="device"
    )
    with respx.mock(base_url=BASE) as mocked:
        mocked.post("/api/v1/agents/client/collaborations").respond(500)
        mocked.get("/api/v1/agents/client/collaborations").respond(json={"items": []})
        result = await orchestrator.execute(context(), call({"prompt": "unknown"}))
    await rest.aclose()
    assert result["results"][0]["status"] == "unknown"
    assert not called


async def test_remote_subtask_polls_terminal_result() -> None:
    statuses = iter(["running", "success"])

    async def dispatch(task: SubtaskTask) -> dict[str, Any]:
        raise AssertionError("remote subtask must not execute locally")

    rest = RestClient(BASE, api_key="key")
    orchestrator = SubtaskOrchestrator(
        rest,
        PluginManager(),
        dispatch,
        local_client_id="client",
        local_device_id="device",
        poll_interval=0.001,
    )
    with respx.mock(base_url=BASE) as mocked:
        mocked.post("/api/v1/agents/client/collaborations").mock(
            side_effect=lambda request: created_response(request, "remote-task")
        )
        mocked.get("/api/v1/agents/client/collaborations/remote-task").mock(
            side_effect=lambda request: httpx.Response(
                200,
                json={
                    "id": "remote-task",
                    "version": 2,
                    "status": next(statuses),
                    "result_payload": {"text": "done"},
                },
            )
        )
        result = await orchestrator.execute(
            context(),
            call(
                {
                    "prompt": "remote",
                    "target_client_id": "other-client",
                    "target_device_id": "other-device",
                }
            ),
        )
    await rest.aclose()
    assert result["results"][0]["status"] == "success"
    assert result["results"][0]["result"] == {"text": "done"}


async def test_subtask_timeout_cancels_without_reporting_success() -> None:
    cancelled: list[str] = []

    async def dispatch(task: SubtaskTask) -> dict[str, Any]:
        await asyncio.sleep(60)
        return {"status": "success"}

    async def cancel(task: SubtaskTask) -> None:
        cancelled.append(task.task_id)

    rest = RestClient(BASE, api_key="key")
    orchestrator = SubtaskOrchestrator(
        rest,
        PluginManager(),
        dispatch,
        canceller=cancel,
        local_client_id="client",
        local_device_id="device",
    )
    with respx.mock(base_url=BASE) as mocked:
        mocked.post("/api/v1/agents/client/collaborations").mock(
            side_effect=lambda request: created_response(request)
        )
        cancel_route = mocked.post(
            "/api/v1/agents/client/collaborations/child-task/cancel"
        ).respond(json={"id": "child-task", "version": 2, "status": "cancelled"})
        result = await orchestrator.execute(
            context(), call({"prompt": "slow", "timeout_seconds": 0.01})
        )
    await rest.aclose()
    assert result["successful"] == 0
    assert result["results"][0]["status"] == "timeout"
    assert cancelled == ["child-task"]
    assert cancel_route.called


async def test_parent_cancellation_cancels_created_subtask() -> None:
    started = asyncio.Event()
    cancelled: list[str] = []

    async def dispatch(task: SubtaskTask) -> dict[str, Any]:
        started.set()
        await asyncio.sleep(60)
        return {"status": "success"}

    async def cancel(task: SubtaskTask) -> None:
        cancelled.append(task.task_id)

    rest = RestClient(BASE, api_key="key")
    orchestrator = SubtaskOrchestrator(
        rest,
        PluginManager(),
        dispatch,
        canceller=cancel,
        local_client_id="client",
        local_device_id="device",
    )
    with respx.mock(base_url=BASE) as mocked:
        mocked.post("/api/v1/agents/client/collaborations").mock(
            side_effect=lambda request: created_response(request)
        )
        cancel_route = mocked.post(
            "/api/v1/agents/client/collaborations/child-task/cancel"
        ).respond(json={"id": "child-task", "version": 2, "status": "cancelled"})
        runner = asyncio.create_task(orchestrator.execute(context(), call({"prompt": "slow"})))
        await asyncio.wait_for(started.wait(), 1)
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner
    await rest.aclose()
    assert cancelled == ["child-task"]
    assert cancel_route.called


async def test_capability_snapshot_is_immutable_and_excludes_mcp_configuration() -> None:
    request = ChatRequest(
        knowledge_scopes=["scope-a"],
        available_tools=["read_file"],
        client_mcp_servers={"safe": {"token": "do-not-copy"}},
    )
    snapshot = SubtaskCapabilitySnapshot.from_request(request)
    request.knowledge_scopes.append("scope-b")
    request.client_mcp_servers["safe"]["token"] = "changed"
    assert snapshot.knowledge_scopes == ("scope-a",)
    assert snapshot.client_mcp_server_names == ("safe",)
    assert "do-not-copy" not in repr(snapshot)
    assert "changed" not in repr(snapshot)
    assert snapshot.available_tools() == ["read_file"]


async def test_dialogue_subtask_chat_request_applies_correlation_and_mcp_allowlist() -> None:
    class Resolver:
        async def resolve(self, flow_id: str, names: frozenset[str]) -> dict[str, Any]:
            assert flow_id == "flow-1"
            assert names == frozenset({"allowed"})
            return {
                "allowed": {"url": "https://mcp.test", "headers": {"Authorization": "secret"}},
                "rogue": {"url": "https://rogue.test"},
            }

    handler = AgentSessionTaskHandler(
        lambda flow_id: (_ for _ in ()).throw(AssertionError()),
        mcp_resolver=Resolver(),
    )
    task = TaskDispatch.from_payload(
        {
            "id": "child-task",
            "task_kind": "dialogue_subtask",
            "version": 1,
            "source_run_id": "parent-run",
            "child_run_id": "child-run",
            "child_session_id": "child-session",
            "command_payload": {
                "input_value": "child input",
                "flow_id": "flow-1",
                "request_mode": "plan",
                "features": {
                    "reasoning": True,
                    "requested_skill_names": ["skill-a"],
                    "client_mcp_server_names": ["allowed"],
                },
                "available_tools": ["read_file"],
            },
        },
        "task.dialogue_subtask.v1",
    )
    request = await handler._chat_request(task)
    assert request.run_id == "child-run"
    assert request.parent_id == "parent-run"
    assert request.request_type == "plan"
    assert request.available_tools == [{"name": "read_file"}]
    assert set(request.client_mcp_servers or {}) == {"allowed"}
