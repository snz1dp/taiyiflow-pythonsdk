from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError

from ..errors import ProtocolError, TransportError, redact
from ..models import ChatRequest, StreamEvent, describe_close
from .rest import path_segment


class StreamConnectionError(TransportError):
    pass


class WebSocketStreamClient:
    def __init__(
        self,
        base_url: str,
        access_token: Callable[[], str],
        flow_id: str,
        *,
        credential_headers: Mapping[str, str] | None = None,
        connect_timeout: float = 10.0,
        max_queue: int = 1024,
    ) -> None:
        self._url = self._build_url(base_url, access_token, flow_id)
        self._headers = dict(credential_headers or {})
        self._connect_timeout = connect_timeout
        self._max_queue = max_queue

    async def stream(self, request: ChatRequest) -> AsyncIterator[StreamEvent]:
        try:
            websocket = await connect(
                self._url(),
                additional_headers=self._headers,
                open_timeout=self._connect_timeout,
                close_timeout=self._connect_timeout,
                max_queue=self._max_queue,
            )
        except Exception as exc:
            raise StreamConnectionError(redact(exc)) from exc

        try:
            await websocket.send(json.dumps(request.to_wire(), separators=(",", ":")))
            async for message in websocket:
                if isinstance(message, bytes):
                    continue
                yield _parse_event(message)
        except ConnectionClosedError as exc:
            reason = describe_close(exc.rcvd.code if exc.rcvd else exc.code, exc.reason)
            raise TransportError(reason) from exc
        finally:
            await websocket.close()

    @staticmethod
    def _build_url(
        base_url: str,
        access_token: Callable[[], str],
        flow_id: str,
    ) -> Callable[[], str]:
        parsed = urlsplit(base_url if "://" in base_url else f"wss://{base_url}")
        scheme = {"http": "ws", "https": "wss"}.get(parsed.scheme, parsed.scheme)
        if scheme not in {"ws", "wss"}:
            raise ValueError("base_url must use http(s) or ws(s)")
        path = parsed.path.rstrip("/") + f"/api/v1/run/{path_segment(flow_id)}/websocket"

        def build() -> str:
            query = urlencode({"__TOKEN__": access_token()})
            return urlunsplit((scheme, parsed.netloc, path, query, ""))

        return build


def _parse_event(message: str | dict[str, Any]) -> StreamEvent:
    try:
        value = json.loads(message) if isinstance(message, str) else message
        if not isinstance(value, dict):
            raise ValueError("event envelope must be an object")
        return StreamEvent.model_validate(value)
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        raise ProtocolError("Failed to parse WebSocket stream event") from exc
