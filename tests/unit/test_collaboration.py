from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

from taiyiagent.collaboration import CollaborationPlugin, SchedulePlugin, TaskDispatch


class FakeWebSocket:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[str | bytes | None] = asyncio.Queue()
        self.sent: list[dict[str, Any]] = []
        self.closed = asyncio.Event()
        self.close_calls: list[tuple[int, str]] = []

    def feed(self, event: str, payload: dict[str, Any] | None = None) -> None:
        self.incoming.put_nowait(
            json.dumps({"type": "collaboration", "event": event, "payload": payload or {}})
        )

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_calls.append((code, reason))
        if not self.closed.is_set():
            self.closed.set()
            self.incoming.put_nowait(None)

    def __aiter__(self) -> FakeWebSocket:
        return self

    async def __anext__(self) -> str | bytes:
        message = await self.incoming.get()
        if message is None:
            raise StopAsyncIteration
        return message


async def wait_until(predicate: Callable[[], bool]) -> None:
    async with asyncio.timeout(1):
        while not predicate():  # noqa: ASYNC110
            await asyncio.sleep(0.005)


def _plugin(**overrides: Any) -> CollaborationPlugin:
    values: dict[str, Any] = {
        "base_url": "https://platform.example.test/root",
        "api_key": "token-1",
        "client_id": "client-1",
        "heartbeat_interval": 10.0,
        "inbound_silence_timeout": 30.0,
        "reconnect_delay": 0.01,
        "max_reconnect_delay": 0.02,
    }
    values.update(overrides)
    return CollaborationPlugin(**values)


@pytest.mark.asyncio
async def test_collaboration_is_passive_until_started(monkeypatch: pytest.MonkeyPatch) -> None:
    connections = 0

    async def connect(url: str, **kwargs: Any) -> FakeWebSocket:
        nonlocal connections
        connections += 1
        return FakeWebSocket()

    monkeypatch.setattr("taiyiagent.collaboration.client.connect", connect)
    plugin = _plugin()
    await asyncio.sleep(0)
    assert connections == 0
    assert not plugin.is_running
    await plugin.aclose()


@pytest.mark.asyncio
async def test_ready_session_recent_and_no_register_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = FakeWebSocket()
    captured: list[tuple[str, dict[str, Any]]] = []
    ready = asyncio.Event()

    async def connect(url: str, **kwargs: Any) -> FakeWebSocket:
        captured.append((url, kwargs))
        return websocket

    monkeypatch.setattr("taiyiagent.collaboration.client.connect", connect)
    plugin = _plugin(credential_headers={"X-Credential": "secret"})
    plugin.on_connection_ready(lambda payload: ready.set())
    assert not await plugin.send_session_update("session-1")
    await plugin.start()
    await wait_until(lambda: len(captured) == 1)
    assert not websocket.sent
    websocket.feed("connection.ready", {"client_id": "client-1"})
    await asyncio.wait_for(ready.wait(), timeout=1)
    await wait_until(lambda: len(websocket.sent) == 2)
    assert [item["event"] for item in websocket.sent] == [
        "session.update",
        "tasks.fetch_recent",
    ]
    query = parse_qs(urlsplit(captured[0][0]).query)
    assert query["__TOKEN__"] == ["token-1"]
    assert query["device-type"] == ["python"]
    assert query["connection-role"] == ["collaboration_worker"]
    assert "task.remote_task.v1" in query["capabilities"][0]
    assert captured[0][1]["additional_headers"] == {"X-Credential": "secret"}
    await plugin.aclose()


@pytest.mark.asyncio
async def test_dispatch_versions_ack_and_rejected_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = FakeWebSocket()
    accepted: list[TaskDispatch] = []
    allow = True

    async def connect(url: str, **kwargs: Any) -> FakeWebSocket:
        return websocket

    async def dispatch(task: TaskDispatch) -> bool:
        accepted.append(task)
        return allow

    monkeypatch.setattr("taiyiagent.collaboration.client.connect", connect)
    plugin = _plugin().on_task_dispatch(dispatch)
    await plugin.start()
    await wait_until(lambda: plugin._websocket is websocket)
    payload = {
        "task_id": "task-1",
        "task_kind": "remote_task",
        "version": 2,
        "source_principal_type": "system",
        "command_payload": {"flow_id": "flow-1", "input_value": "run"},
    }
    allow = False
    websocket.feed("task.dispatch", payload)
    await wait_until(lambda: len(accepted) == 1)
    assert not websocket.sent
    allow = True
    websocket.feed("task.dispatch", payload)
    websocket.feed("task.dispatch", payload)
    await wait_until(lambda: len(websocket.sent) == 1)
    assert len(accepted) == 2
    assert websocket.sent[0]["event"] == "task.ack"
    assert websocket.sent[0]["payload"]["expected_version"] == 2
    payload["version"] = 3
    payload["source_principal_type"] = "user"
    websocket.feed("task.dispatch", payload)
    await wait_until(lambda: len(websocket.sent) == 2)
    assert "expected_version" not in websocket.sent[1]["payload"]
    assert len(accepted) == 3
    await plugin.aclose()


@pytest.mark.asyncio
async def test_recent_tasks_only_execute_pending_and_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = FakeWebSocket()
    accepted: list[str] = []
    snapshots: list[str] = []

    async def connect(url: str, **kwargs: Any) -> FakeWebSocket:
        return websocket

    monkeypatch.setattr("taiyiagent.collaboration.client.connect", connect)
    plugin = _plugin().on_task_dispatch(lambda task: accepted.append(task.task_id) or True)
    plugin.on_task_snapshot(lambda task: snapshots.append(str(task["task_id"])))
    await plugin.start()
    await wait_until(lambda: plugin._websocket is websocket)
    websocket.feed(
        "tasks.recent",
        {
            "tasks": [
                {"task_id": "pending", "status": "pending", "version": 1},
                {"task_id": "running", "status": "running", "version": 1},
                {
                    "task_id": "cancelled",
                    "status": "pending",
                    "cancel_requested_at": "2026-11-14T00:00:00Z",
                },
            ],
            "next_cursor": {"offset": 20},
        },
    )
    await wait_until(lambda: len(websocket.sent) == 2)
    assert accepted == ["pending"]
    assert snapshots == ["pending", "running", "cancelled"]
    assert websocket.sent[0]["event"] == "task.ack"
    assert websocket.sent[1]["event"] == "tasks.fetch_recent"
    assert websocket.sent[1]["payload"]["cursor"] == {"offset": 20}
    await plugin.aclose()


@pytest.mark.asyncio
async def test_schedule_dispatch_release_cancel_ping_and_kicked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = FakeWebSocket()
    tasks: list[TaskDispatch] = []
    cancels: list[str] = []
    releases: list[str] = []
    kicked = asyncio.Event()

    async def connect(url: str, **kwargs: Any) -> FakeWebSocket:
        return websocket

    monkeypatch.setattr("taiyiagent.collaboration.client.connect", connect)
    plugin = _plugin().on_task_dispatch(lambda task: tasks.append(task) or True)
    plugin.on_task_cancel(lambda cancel: cancels.append(cancel.task_id))
    plugin.on_release(lambda release: releases.append(release.version))
    plugin.on_kicked(lambda reason: kicked.set())
    await plugin.start()
    await wait_until(lambda: plugin._websocket is websocket)
    schedule = {"schedule_id": "schedule-1", "run_record_id": "run-1"}
    websocket.feed("schedule_task.dispatch", schedule)
    websocket.feed("schedule_task.dispatch", schedule)
    websocket.feed("connection.ping")
    websocket.feed("release.push", {"version": "v2"})
    websocket.feed("task.cancel", {"task_id": "task-1"})
    websocket.feed("connection.kicked", {"reason": "replaced"})
    await asyncio.wait_for(kicked.wait(), timeout=1)
    await wait_until(lambda: len(websocket.sent) == 2)
    assert len(tasks) == 1
    assert tasks[0].task_type == "schedule"
    assert websocket.sent[0]["payload"] == {
        **schedule,
        "task_type": "schedule",
    }
    assert websocket.sent[1]["event"] == "connection.pong"
    assert releases == ["v2"]
    assert cancels == ["task-1"]
    assert not plugin.is_running
    await plugin.aclose()


@pytest.mark.asyncio
async def test_reconnect_refreshes_token_and_close_cancels_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websockets = [FakeWebSocket(), FakeWebSocket()]
    urls: list[str] = []
    tokens = iter(("token-1", "token-2"))
    connected_twice = asyncio.Event()
    task_started = asyncio.Event()
    task_cancelled = asyncio.Event()

    async def connect(url: str, **kwargs: Any) -> FakeWebSocket:
        urls.append(url)
        if len(urls) == 2:
            connected_twice.set()
        return websockets[len(urls) - 1]

    async def handler(task: TaskDispatch) -> None:
        task_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            task_cancelled.set()
            raise

    monkeypatch.setattr("taiyiagent.collaboration.client.connect", connect)
    plugin = _plugin(access_token=lambda: next(tokens), api_key=None)
    plugin.on_task(handler)
    await plugin.start()
    await wait_until(lambda: len(urls) == 1)
    websockets[0].feed("task.dispatch", {"task_id": "task-1", "version": 1})
    await asyncio.wait_for(task_started.wait(), timeout=1)
    await websockets[0].close()
    await asyncio.wait_for(connected_twice.wait(), timeout=1)
    assert parse_qs(urlsplit(urls[1]).query)["__TOKEN__"] == ["token-2"]
    await plugin.aclose()
    await asyncio.wait_for(task_cancelled.wait(), timeout=1)
    assert plugin.task_count == 0


@pytest.mark.asyncio
async def test_heartbeat_and_inbound_silence_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = FakeWebSocket()

    async def connect(url: str, **kwargs: Any) -> FakeWebSocket:
        return websocket

    monkeypatch.setattr("taiyiagent.collaboration.client.connect", connect)
    plugin = _plugin(heartbeat_interval=0.01, inbound_silence_timeout=0.03)
    await plugin.start()
    await wait_until(lambda: plugin._websocket is websocket)
    websocket.feed("connection.ready")
    await wait_until(lambda: any(item["event"] == "connection.ping" for item in websocket.sent))
    await asyncio.wait_for(websocket.closed.wait(), timeout=1)
    await wait_until(lambda: plugin._websocket is None)
    assert websocket.close_calls == [(1012, "inbound silence")]
    await plugin.aclose()


class RecordingCollaboration:
    def __init__(self) -> None:
        self.results: list[tuple[str, Any]] = []
        self.errors: list[tuple[str, str]] = []
        self.reported = asyncio.Event()

    async def send_task_result(self, task_id: str, result: Any) -> bool:
        self.results.append((task_id, result))
        self.reported.set()
        return True

    async def send_task_error(self, task_id: str, error: str) -> bool:
        self.errors.append((task_id, error))
        self.reported.set()
        return True


@pytest.mark.asyncio
async def test_schedule_once_success_failure_and_close() -> None:
    collaboration = RecordingCollaboration()
    plugin = SchedulePlugin(collaboration)  # type: ignore[arg-type]
    plugin.schedule_once("once", lambda: None, delay=0)
    await asyncio.wait_for(collaboration.reported.wait(), timeout=1)
    assert collaboration.results == [("once", "completed")]
    assert plugin.task_count == 0

    collaboration.reported.clear()

    def fail() -> None:
        raise ValueError("bad schedule")

    plugin.schedule_once("failure", fail, delay=0)
    await asyncio.wait_for(collaboration.reported.wait(), timeout=1)
    assert collaboration.errors == [("failure", "bad schedule")]

    started = asyncio.Event()

    async def recurring() -> None:
        started.set()

    plugin.schedule("periodic", recurring, initial_delay=0, period=0.01)
    await asyncio.wait_for(started.wait(), timeout=1)
    assert plugin.cancel("periodic")
    assert not plugin.cancel("periodic")
    await plugin.aclose()
    assert plugin.task_count == 0
