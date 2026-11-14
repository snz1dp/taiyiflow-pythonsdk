from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from typing import Any

from ..errors import ProtocolError
from ..models import ChatRequest, StreamEvent
from .rest import RestClient, path_segment


class SseStreamClient:
    def __init__(self, rest: RestClient, flow_id: str) -> None:
        self._rest = rest
        self._flow_id = flow_id

    async def stream(
        self,
        request: ChatRequest,
        *,
        on_session_id: Callable[[str], None] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        path = f"/api/v1/run/{path_segment(self._flow_id)}/stream"
        headers = {"Accept": "text/event-stream"}
        async with self._rest.stream(
            "POST", path, json=request.to_wire(), headers=headers
        ) as response:
            session_id = response.headers.get("X-TaiyiFlow-Session-id")
            if session_id and on_session_id:
                on_session_id(session_id)
            event_type: str | None = None
            data_lines: list[str] = []
            async for line in response.aiter_lines():
                if not line:
                    if data_lines:
                        yield parse_sse_event(event_type, "\n".join(data_lines))
                    event_type = None
                    data_lines = []
                elif line.startswith(":"):
                    continue
                elif line.startswith("event:"):
                    event_type = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].removeprefix(" "))
            if data_lines:
                yield parse_sse_event(event_type, "\n".join(data_lines))


def parse_sse_event(event_type: str | None, data: str) -> StreamEvent:
    try:
        parsed: Any = json.loads(data)
        if event_type is not None:
            return StreamEvent(event=event_type, data=parsed)
        if not isinstance(parsed, dict):
            raise ValueError("event envelope must be an object")
        return StreamEvent.model_validate(parsed)
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        raise ProtocolError("Failed to parse SSE stream event") from exc
