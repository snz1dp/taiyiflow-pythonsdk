from __future__ import annotations

import asyncio
import json
import logging
import random
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from typing import TypeAlias, cast
from urllib.parse import urlencode, urlsplit, urlunsplit

from websockets.asyncio.client import ClientConnection, connect

from ..errors import ConfigurationError, redact
from ..hooks import maybe_await
from ..transport.capabilities import JsonObject, JsonValue
from .models import ReleasePush, TaskCancel, TaskDispatch

log = logging.getLogger(__name__)

ROLE_COLLABORATION_WORKER = "collaboration_worker"
ROLE_PRIMARY_CLI = "primary_cli"
WORKER_CAPABILITIES = "notification.v1,task.remote_task.v1,task.dialogue_subtask.v1"

TaskHandler: TypeAlias = Callable[[TaskDispatch], Awaitable[None] | None]
TaskDispatchHandler: TypeAlias = Callable[[TaskDispatch], Awaitable[bool] | bool]
TaskCancelHandler: TypeAlias = Callable[[TaskCancel], Awaitable[None] | None]
TaskSnapshotHandler: TypeAlias = Callable[[JsonObject], Awaitable[None] | None]
ReleaseHandler: TypeAlias = Callable[[ReleasePush], Awaitable[None] | None]
ConnectionReadyHandler: TypeAlias = Callable[[JsonObject], Awaitable[None] | None]
KickedHandler: TypeAlias = Callable[[str], Awaitable[None] | None]


class CollaborationPlugin:
    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        *,
        access_token: Callable[[], str] | None = None,
        role: str = ROLE_COLLABORATION_WORKER,
        client_id: str = "",
        device_id: str = "",
        instance_id: str = "",
        credential_headers: Mapping[str, str] | None = None,
        heartbeat_interval: float = 30.0,
        inbound_silence_timeout: float = 90.0,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
        connect_timeout: float = 10.0,
        max_dispatch_versions: int = 4096,
    ) -> None:
        if access_token is None and api_key is None:
            raise ConfigurationError("api_key or access_token is required")
        if role not in {ROLE_COLLABORATION_WORKER, ROLE_PRIMARY_CLI}:
            raise ConfigurationError("Unsupported collaboration role")
        if not client_id.strip() and not device_id.strip():
            raise ConfigurationError("client_id or device_id is required")
        if role == ROLE_PRIMARY_CLI and not instance_id.strip():
            raise ConfigurationError("instance_id is required for primary_cli")
        if not 0 < heartbeat_interval < inbound_silence_timeout:
            raise ConfigurationError(
                "Inbound silence timeout must be greater than heartbeat interval"
            )
        if min(reconnect_delay, max_reconnect_delay, connect_timeout) <= 0:
            raise ConfigurationError("Collaboration timeouts must be positive")
        if max_dispatch_versions <= 0:
            raise ConfigurationError("max_dispatch_versions must be positive")
        self._base_url = base_url.rstrip("/")
        self._access_token = access_token or (lambda: api_key or "")
        self._role = role
        self._client_id = client_id
        self._device_id = device_id
        self._instance_id = instance_id
        self._credential_headers = dict(credential_headers or {})
        self._heartbeat_interval = heartbeat_interval
        self._inbound_silence_timeout = inbound_silence_timeout
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        self._connect_timeout = connect_timeout
        self._max_dispatch_versions = max_dispatch_versions
        self._running = False
        self._closed = False
        self._connected = False
        self._websocket: ClientConnection | None = None
        self._runner: asyncio.Task[None] | None = None
        self._heartbeat: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._write_lock = asyncio.Lock()
        self._dispatch_versions: OrderedDict[str, int] = OrderedDict()
        self._session_id = ""
        self._last_inbound = 0.0
        self._reconnect_attempts = 0
        self._task_handler: TaskHandler | None = None
        self._task_dispatch_handler: TaskDispatchHandler | None = None
        self._task_cancel_handler: TaskCancelHandler | None = None
        self._task_snapshot_handler: TaskSnapshotHandler | None = None
        self._release_handler: ReleaseHandler | None = None
        self._connection_ready_handler: ConnectionReadyHandler | None = None
        self._kicked_handler: KickedHandler | None = None

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def task_count(self) -> int:
        return len(self._tasks)

    def on_task(self, handler: TaskHandler) -> CollaborationPlugin:
        self._task_handler = handler
        self._task_dispatch_handler = None
        return self

    def on_task_dispatch(self, handler: TaskDispatchHandler) -> CollaborationPlugin:
        self._task_dispatch_handler = handler
        self._task_handler = None
        return self

    def on_task_cancel(self, handler: TaskCancelHandler) -> CollaborationPlugin:
        self._task_cancel_handler = handler
        return self

    def on_task_snapshot(self, handler: TaskSnapshotHandler) -> CollaborationPlugin:
        self._task_snapshot_handler = handler
        return self

    def on_release(self, handler: ReleaseHandler) -> CollaborationPlugin:
        self._release_handler = handler
        return self

    def on_connection_ready(self, handler: ConnectionReadyHandler) -> CollaborationPlugin:
        self._connection_ready_handler = handler
        return self

    def on_kicked(self, handler: KickedHandler) -> CollaborationPlugin:
        self._kicked_handler = handler
        return self

    async def start(self) -> None:
        if self._closed:
            raise ConfigurationError("Collaboration plugin is closed")
        if self._running:
            return
        self._running = True
        self._last_inbound = asyncio.get_running_loop().time()
        self._runner = asyncio.create_task(self._run())
        self._heartbeat = asyncio.create_task(self._heartbeat_loop())

    async def aclose(self) -> None:
        if self._closed and not self._running:
            return
        self._closed = True
        self._running = False
        self._connected = False
        websocket = self._websocket
        self._websocket = None
        if websocket is not None:
            try:
                await websocket.close()
            except Exception:
                log.warning("Collaboration WebSocket close failed", exc_info=True)
        current = asyncio.current_task()
        tasks = [
            task
            for task in (self._runner, self._heartbeat, *self._tasks)
            if task is not None and task is not current
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._runner = None
        self._heartbeat = None
        self._tasks.clear()

    async def send_session_update(self, session_id: str | None) -> bool:
        self._session_id = session_id or ""
        if not self._connected:
            return False
        return await self._send_envelope("session.update", {"session_id": self._session_id})

    async def send_task_ack(self, task_id: str, expected_version: int | None = None) -> bool:
        return await self._send_task_envelope("task.ack", task_id, expected_version)

    async def send_task_result(
        self,
        task_id: str,
        result: JsonValue,
        expected_version: int | None = None,
    ) -> bool:
        return await self._send_task_envelope(
            "task.result", task_id, expected_version, result=result
        )

    async def send_task_error(
        self, task_id: str, error: str, expected_version: int | None = None
    ) -> bool:
        return await self._send_task_envelope(
            "task.error", task_id, expected_version, error=redact(error)
        )

    async def send_task_status(
        self, task_id: str, status: str, expected_version: int | None = None
    ) -> bool:
        return await self._send_task_envelope(
            "task.status", task_id, expected_version, status=status
        )

    async def send_schedule_ack(self, schedule_id: str, run_record_id: str) -> bool:
        return await self._send_schedule_envelope("task.ack", schedule_id, run_record_id)

    async def send_schedule_status(self, schedule_id: str, run_record_id: str, status: str) -> bool:
        return await self._send_schedule_envelope(
            "task.status", schedule_id, run_record_id, status=status
        )

    async def send_schedule_result(
        self, schedule_id: str, run_record_id: str, result: JsonValue
    ) -> bool:
        return await self._send_schedule_envelope(
            "task.result", schedule_id, run_record_id, result=result
        )

    async def send_schedule_error(self, schedule_id: str, run_record_id: str, error: str) -> bool:
        return await self._send_schedule_envelope(
            "task.error", schedule_id, run_record_id, error=redact(error)
        )

    async def _send_task_envelope(
        self,
        event: str,
        task_id: str,
        expected_version: int | None,
        **values: JsonValue,
    ) -> bool:
        payload: JsonObject = {"task_id": task_id, **values}
        if expected_version is not None:
            payload["expected_version"] = expected_version
        return await self._send_envelope(event, payload)

    async def _send_schedule_envelope(
        self,
        event: str,
        schedule_id: str,
        run_record_id: str,
        **values: JsonValue,
    ) -> bool:
        return await self._send_envelope(
            event,
            {
                "schedule_id": schedule_id,
                "run_record_id": run_record_id,
                "task_type": "schedule",
                **values,
            },
        )

    async def _send_envelope(self, event: str, payload: JsonObject) -> bool:
        async with self._write_lock:
            websocket = self._websocket
            if websocket is None or not self._running:
                return False
            envelope = {
                "type": ROLE_PRIMARY_CLI if self._role == ROLE_PRIMARY_CLI else "collaboration",
                "event": event,
                "payload": payload,
            }
            try:
                await websocket.send(json.dumps(envelope, separators=(",", ":")))
                return True
            except Exception:
                log.warning("Failed to send collaboration event %s", event, exc_info=True)
                return False

    def _build_websocket_url(self) -> str:
        parsed = urlsplit(self._base_url if "://" in self._base_url else f"wss://{self._base_url}")
        scheme = {"http": "ws", "https": "wss"}.get(parsed.scheme, parsed.scheme)
        if scheme not in {"ws", "wss"}:
            raise ConfigurationError("base_url must use HTTP(S) or WS(S)")
        path = parsed.path.rstrip("/") + "/api/v1/agents/client/websocket"
        query = {
            "__TOKEN__": self._access_token(),
            "client-id": self._client_id,
            "device-id": self._device_id,
            "connection-role": self._role,
            "device-type": "python",
            "client-type": "text",
            "instance-id": self._instance_id,
        }
        if self._role == ROLE_COLLABORATION_WORKER:
            query["capabilities"] = WORKER_CAPABILITIES
        return urlunsplit(
            (scheme, parsed.netloc, path, urlencode({k: v for k, v in query.items() if v}), "")
        )

    async def _run(self) -> None:
        while self._running:
            websocket: ClientConnection | None = None
            try:
                websocket = await connect(
                    self._build_websocket_url(),
                    additional_headers=self._credential_headers,
                    open_timeout=self._connect_timeout,
                    close_timeout=self._connect_timeout,
                )
                if not self._running:
                    await websocket.close()
                    break
                self._websocket = websocket
                self._last_inbound = asyncio.get_running_loop().time()
                async for message in websocket:
                    self._last_inbound = asyncio.get_running_loop().time()
                    if isinstance(message, str):
                        await self._handle_message(message)
                    if not self._running:
                        break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._running:
                    log.warning("Collaboration connection failed: %s", redact(exc))
            finally:
                self._connected = False
                if self._websocket is websocket:
                    self._websocket = None
                    if websocket is not None:
                        try:
                            await websocket.close()
                        except Exception:
                            log.debug("Collaboration socket cleanup failed", exc_info=True)
            if self._running:
                delay = self.reconnect_delay_seconds(self._reconnect_attempts)
                self._reconnect_attempts += 1
                await asyncio.sleep(delay)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._heartbeat_interval)
            if not self._connected:
                continue
            now = asyncio.get_running_loop().time()
            if now - self._last_inbound > self._inbound_silence_timeout:
                self._connected = False
                websocket = self._websocket
                if websocket is not None:
                    self._websocket = None
                    await websocket.close(code=1012, reason="inbound silence")
                continue
            await self._send_envelope("connection.ping", {})

    def reconnect_delay_seconds(self, attempt: int) -> float:
        ceiling = min(
            self._max_reconnect_delay,
            self._reconnect_delay * (1 << min(max(attempt, 0), 4)),
        )
        return random.SystemRandom().uniform(ceiling / 2, ceiling)

    async def _handle_message(self, message: str) -> None:
        try:
            envelope = json.loads(message)
            if not isinstance(envelope, dict):
                raise ValueError("envelope must be an object")
            event = envelope.get("event")
            payload = envelope.get("payload", {})
            if not isinstance(event, str) or not isinstance(payload, dict):
                raise ValueError("invalid collaboration envelope")
            body = cast(JsonObject, payload)
            if event == "connection.ready":
                await self._connection_ready(body)
            elif event == "task.dispatch":
                await self._task_dispatch(body)
            elif event == "task.cancel":
                if self._task_cancel_handler is not None:
                    await maybe_await(self._task_cancel_handler(TaskCancel.from_payload(body)))
            elif event == "primary_task.dispatch":
                await self._submit(TaskDispatch.from_payload(body, "primary"))
            elif event == "schedule_task.dispatch":
                await self._schedule_dispatch(body)
            elif event == "tasks.recent":
                await self._recent_tasks(body)
            elif event == "connection.kicked":
                reason = body.get("reason", "Kicked by platform")
                self._closed = True
                self._running = False
                self._connected = False
                if self._kicked_handler is not None:
                    await maybe_await(self._kicked_handler(str(reason)))
            elif event == "release.push":
                if self._release_handler is not None:
                    version = body.get("version", "")
                    await maybe_await(self._release_handler(ReleasePush(str(version), body)))
            elif event == "connection.ping":
                await self._send_envelope("connection.pong", {})
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            log.warning("Invalid collaboration message: %s", redact(exc))
        except Exception:
            log.warning("Failed to handle collaboration event", exc_info=True)

    async def _connection_ready(self, payload: JsonObject) -> None:
        if self._closed:
            return
        self._connected = True
        self._reconnect_attempts = 0
        self._last_inbound = asyncio.get_running_loop().time()
        if self._session_id:
            await self.send_session_update(self._session_id)
        if self._role == ROLE_COLLABORATION_WORKER:
            await self._send_envelope("tasks.fetch_recent", {"limit": 20})
        if self._connection_ready_handler is not None:
            await maybe_await(self._connection_ready_handler(payload))

    async def _task_dispatch(self, payload: JsonObject) -> None:
        if self._task_handler is None and self._task_dispatch_handler is None:
            return
        if (
            not _text(payload, "task_id")
            and _text(payload, "schedule_id")
            and _text(payload, "run_record_id")
        ):
            await self._schedule_dispatch(payload)
            return
        task_id = _text(payload, "task_id")
        if not task_id:
            return
        version = payload.get("version", 0)
        numeric_version = (
            version if isinstance(version, int) and not isinstance(version, bool) else 0
        )
        previous = self._dispatch_versions.get(task_id)
        if not self._accept_dispatch(task_id, numeric_version):
            return
        task = TaskDispatch.from_payload(payload, "task")
        accepted = await self._submit(task)
        if accepted:
            source = _text(payload, "source_principal_type") or "user"
            await self.send_task_ack(task_id, None if source == "user" else numeric_version)
        else:
            self._restore_dispatch(task_id, previous)

    async def _schedule_dispatch(self, payload: JsonObject) -> None:
        if self._task_handler is None and self._task_dispatch_handler is None:
            return
        schedule_id = _text(payload, "schedule_id")
        run_record_id = _text(payload, "run_record_id")
        if not schedule_id or not run_record_id:
            return
        key = f"schedule:{run_record_id}"
        previous = self._dispatch_versions.get(key)
        if not self._accept_dispatch(key, 0):
            return
        accepted = await self._submit(TaskDispatch.from_payload(payload, "schedule"))
        if accepted:
            await self.send_schedule_ack(schedule_id, run_record_id)
        else:
            self._restore_dispatch(key, previous)

    async def _recent_tasks(self, payload: JsonObject) -> None:
        tasks = payload.get("tasks")
        if isinstance(tasks, list):
            for item in tasks:
                if not isinstance(item, dict):
                    continue
                task = item
                if self._task_snapshot_handler is not None:
                    await maybe_await(self._task_snapshot_handler(task))
                if task.get("status") == "pending" and task.get("cancel_requested_at") is None:
                    await self._task_dispatch(task)
        cursor = payload.get("next_cursor")
        if isinstance(cursor, dict):
            await self._send_envelope("tasks.fetch_recent", {"limit": 20, "cursor": cursor})

    async def _submit(self, task: TaskDispatch) -> bool:
        dispatch_handler = self._task_dispatch_handler
        if dispatch_handler is not None:
            try:
                return bool(await maybe_await(dispatch_handler(task)))
            except Exception:
                log.warning("Task dispatch handler failed: %s", task.task_id, exc_info=True)
                return False
        handler = self._task_handler
        if handler is None:
            return False

        async def run() -> None:
            try:
                await maybe_await(handler(task))
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning("Task handler failed: %s", task.task_id, exc_info=True)

        running = asyncio.create_task(run())
        self._tasks.add(running)
        running.add_done_callback(self._tasks.discard)
        return True

    def _accept_dispatch(self, key: str, version: int) -> bool:
        previous = self._dispatch_versions.get(key)
        if previous is not None and (version == 0 or previous >= version):
            return False
        self._dispatch_versions[key] = version
        self._dispatch_versions.move_to_end(key)
        while len(self._dispatch_versions) > self._max_dispatch_versions:
            self._dispatch_versions.popitem(last=False)
        return True

    def _restore_dispatch(self, key: str, previous: int | None) -> None:
        if previous is None:
            self._dispatch_versions.pop(key, None)
        else:
            self._dispatch_versions[key] = previous


def _text(payload: JsonObject, field: str) -> str:
    value = payload.get(field)
    return value if isinstance(value, str) else ""
