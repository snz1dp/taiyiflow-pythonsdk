from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..errors import ConfigurationError, ProtocolError, TransportError
from ..models import ChatRequest
from .rest import RestClient, path_segment

_TERMINAL_STATUSES = frozenset({"success", "failed", "cancelled"})
_ACTIVE_STATUSES = frozenset({"pending", "running"})
log = logging.getLogger(__name__)


class FlowRunError(TransportError):
    pass


class SubmissionUnknownError(FlowRunError):
    pass


class FlowRunTimeoutError(FlowRunError):
    def __init__(self, task_id: str, cancellation_requested: bool) -> None:
        self.task_id = task_id
        self.cancellation_requested = cancellation_requested
        suffix = " after cancellation was requested" if cancellation_requested else ""
        FlowRunError.__init__(
            self, f"Flow task did not reach a terminal status before timeout{suffix}"
        )


@dataclass(frozen=True, slots=True)
class FlowRunResult:
    http_status: int
    media_type: str | None
    session_id: str | None
    response_session_header: str | None
    outputs: tuple[str, ...]
    raw_body: str
    json: Any = None

    @classmethod
    def from_response(cls, response: Any) -> FlowRunResult:
        raw_body = response.content.decode("utf-8", errors="replace")
        parsed: Any = None
        media_type = response.headers.get("content-type", "").split(";", 1)[0] or None
        if media_type and "json" in media_type.lower() and response.content:
            try:
                parsed = response.json()
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ProtocolError("Flow run returned invalid JSON") from exc
            if isinstance(parsed, dict) and isinstance(parsed.get("media_type"), str):
                media_type = parsed["media_type"]
        return cls(
            http_status=response.status_code,
            media_type=media_type,
            session_id=_text(parsed, "session_id"),
            response_session_header=response.headers.get("x-session-id"),
            outputs=_outputs(parsed.get("outputs") if isinstance(parsed, dict) else None),
            raw_body=raw_body,
            json=parsed,
        )

    @classmethod
    def from_task_result(cls, result: dict[str, Any]) -> FlowRunResult:
        return cls(
            http_status=200,
            media_type=_text(result, "media_type"),
            session_id=_text(result, "session_id"),
            response_session_header=None,
            outputs=_outputs(result.get("outputs")),
            raw_body=json.dumps(result, ensure_ascii=False, separators=(",", ":")),
            json=result,
        )


@dataclass(frozen=True, slots=True)
class FlowRunTask:
    task_id: str
    status: str
    flow_id: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    result: FlowRunResult | None = None
    error: Any = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_json(cls, value: Any) -> FlowRunTask:
        if not isinstance(value, dict):
            raise ProtocolError("Flow task response must be an object")
        task_id = value.get("task_id")
        status = value.get("status")
        if not isinstance(task_id, str) or not task_id.strip():
            raise ProtocolError("Flow task response requires task_id")
        if not isinstance(status, str) or not status.strip():
            raise ProtocolError("Flow task response requires status")
        raw_result = value.get("result")
        result = (
            FlowRunResult.from_task_result(raw_result) if isinstance(raw_result, dict) else None
        )
        return cls(
            task_id=task_id,
            status=status,
            flow_id=_text(value, "flow_id"),
            session_id=_text(value, "session_id"),
            run_id=_text(value, "run_id"),
            result=result,
            error=value.get("error"),
            raw=value,
        )

    @property
    def terminal(self) -> bool:
        return self.status in _TERMINAL_STATUSES


@dataclass(frozen=True, slots=True)
class FlowRunPollOptions:
    request_timeout: float = 30.0
    poll_interval: float = 2.0
    overall_timeout: float = 300.0

    def __post_init__(self) -> None:
        if min(self.request_timeout, self.poll_interval, self.overall_timeout) <= 0:
            raise ConfigurationError("Flow run timeouts and poll interval must be positive")


class FlowRunClient:
    def __init__(self, rest: RestClient) -> None:
        self._rest = rest
        self._closed = False

    async def run(
        self,
        flow_id: str,
        request: ChatRequest,
        *,
        request_timeout: float | None = None,
    ) -> FlowRunResult:
        response = await self._rest.post_raw(
            _run_path(flow_id), _body(request, asynchronous=False), request_timeout=request_timeout
        )
        if response.status_code == 202:
            raise ProtocolError("Synchronous run unexpectedly returned an asynchronous task")
        return FlowRunResult.from_response(response)

    async def submit_async(
        self,
        flow_id: str,
        request: ChatRequest,
        *,
        request_timeout: float | None = None,
    ) -> FlowRunTask:
        try:
            response = await self._rest.post_raw(
                _run_path(flow_id),
                _body(request, asynchronous=True),
                request_timeout=request_timeout,
                raise_for_status=False,
            )
        except TransportError as exc:
            raise SubmissionUnknownError("Flow task submission outcome is unknown") from exc
        if not 200 <= response.status_code < 300:
            RestClient._raise_for_status(response, "POST", _run_path(flow_id))
        if response.status_code != 202:
            raise SubmissionUnknownError("Flow task submission did not return HTTP 202")
        try:
            payload = response.json()
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise SubmissionUnknownError("Flow task submission returned invalid JSON") from exc
        task_id = payload.get("task_id") if isinstance(payload, dict) else None
        if not isinstance(task_id, str) or not task_id.strip():
            raise SubmissionUnknownError("Flow task submission returned no task_id")
        return FlowRunTask.from_json(payload)

    async def poll(self, task_id: str, *, request_timeout: float | None = None) -> FlowRunTask:
        response = await self._rest.get_raw(_task_path(task_id), request_timeout=request_timeout)
        try:
            payload = response.json()
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ProtocolError("Flow task poll returned invalid JSON") from exc
        return FlowRunTask.from_json(payload)

    async def cancel(self, task_id: str, *, request_timeout: float | None = None) -> FlowRunTask:
        response = await self._rest.post_raw(
            _task_path(task_id) + "/cancel", {}, request_timeout=request_timeout
        )
        if response.status_code != 202:
            raise ProtocolError("Task cancellation did not return HTTP 202")
        try:
            payload = response.json()
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ProtocolError("Task cancellation returned invalid JSON") from exc
        return FlowRunTask.from_json(payload)

    async def await_task(
        self,
        task_id: str,
        *,
        options: FlowRunPollOptions | None = None,
        cancellation_requested: Callable[[], bool] | None = None,
    ) -> FlowRunTask:
        if self._closed:
            raise FlowRunError("Flow run client is closed")
        poll_options = options or FlowRunPollOptions()
        deadline = time.monotonic() + poll_options.overall_timeout
        cancel_requested = False
        try:
            while not self._closed:
                if not cancel_requested and cancellation_requested and cancellation_requested():
                    await self._best_effort_cancel(task_id, poll_options.request_timeout)
                    cancel_requested = True
                task = await self.poll(task_id, request_timeout=poll_options.request_timeout)
                if task.terminal:
                    return task
                if task.status not in _ACTIVE_STATUSES:
                    raise ProtocolError(f"Unknown flow task status: {task.status}")
                if time.monotonic() >= deadline:
                    if not cancel_requested:
                        await self._best_effort_cancel(task_id, poll_options.request_timeout)
                        cancel_requested = True
                    raise FlowRunTimeoutError(task_id, cancel_requested)
                await asyncio.sleep(
                    min(poll_options.poll_interval, max(0.001, deadline - time.monotonic()))
                )
        except asyncio.CancelledError:
            if not cancel_requested:
                await self._best_effort_cancel(task_id, poll_options.request_timeout)
            raise
        raise FlowRunError("Flow run client is closed")

    async def aclose(self) -> None:
        self._closed = True

    async def _best_effort_cancel(self, task_id: str, request_timeout: float) -> None:
        try:
            await self.cancel(task_id, request_timeout=request_timeout)
        except Exception:
            log.debug("Flow task cancellation request failed", exc_info=True)


def _body(request: ChatRequest, *, asynchronous: bool) -> dict[str, Any]:
    body = request.model_dump(by_alias=True, exclude_none=True)
    body["async"] = asynchronous
    return body


def _run_path(flow_id: str) -> str:
    if not flow_id.strip():
        raise ConfigurationError("flow_id is required")
    return f"/api/v1/run/{path_segment(flow_id)}"


def _task_path(task_id: str) -> str:
    if not task_id.strip():
        raise ConfigurationError("task_id is required")
    return f"/api/v1/run/tasks/{path_segment(task_id)}"


def _text(value: Any, key: str) -> str | None:
    if not isinstance(value, dict):
        return None
    item = value.get(key)
    return item if isinstance(item, str) else None


def _outputs(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str))
