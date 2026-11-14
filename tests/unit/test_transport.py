import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest
import respx

from taiyiagent.models import ChatRequest, StreamProtocol
from taiyiagent.transport import RestClient, SseStreamClient, StreamProtocolResolver
from taiyiagent.transport.websocket import WebSocketStreamClient


@pytest.mark.asyncio
@respx.mock
async def test_rest_refreshes_dynamic_token_once() -> None:
    tokens = iter(["old-token", "new-token"])
    refreshed = 0

    async def refresh() -> None:
        nonlocal refreshed
        refreshed += 1

    route = respx.get("https://example.test/api/v1/config").mock(
        side_effect=[
            httpx.Response(401, json={"detail": "expired"}),
            httpx.Response(200, json={"stream_protocol": "sse"}),
        ]
    )
    client = RestClient(
        "https://example.test",
        access_token=lambda: next(tokens),
        on_unauthorized=refresh,
    )
    try:
        assert await client.get("/api/v1/config") == {"stream_protocol": "sse"}
    finally:
        await client.aclose()
    assert refreshed == 1
    assert route.calls[0].request.headers["Authorization"] == "Bearer old-token"
    assert route.calls[1].request.headers["Authorization"] == "Bearer new-token"


@pytest.mark.asyncio
async def test_sse_parses_envelopes_multiline_data_and_session_header() -> None:
    payload = (
        b": keepalive\n\n"
        b'data: {"event":"message",\n'
        b'data: "data":{"chunk":"hello"}}\n\n'
        b"event: close\n"
        b'data: {"reason":"done"}'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["files"] == ["file.txt"]
        assert "input_files" not in body
        return httpx.Response(
            200,
            headers={"X-TaiyiFlow-Session-id": "server-session"},
            content=payload,
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    rest = RestClient("https://example.test", "secret", client=http_client)
    sse = SseStreamClient(rest, "flow/a")
    resolved: list[str] = []
    events = [
        event
        async for event in sse.stream(
            ChatRequest(input_value="hello", files=["file.txt"]),
            on_session_id=resolved.append,
        )
    ]
    assert [event.event for event in events] == ["message", "close"]
    assert events[0].message_text == "hello"
    assert resolved == ["server-session"]
    await http_client.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_protocol_resolution_is_cached_and_can_refresh() -> None:
    route = respx.get("https://example.test/api/v1/config").mock(
        side_effect=[
            httpx.Response(200, json={"stream_protocol": "sse"}),
            httpx.Response(200, json={"stream_protocol": "websocket"}),
        ]
    )
    rest = RestClient("https://example.test", "secret")
    resolver = StreamProtocolResolver(rest)
    assert await resolver.resolve() is StreamProtocol.SSE
    assert await resolver.resolve() is StreamProtocol.SSE
    assert route.call_count == 1
    assert resolver.automatic
    assert await resolver.refresh() is StreamProtocol.WEBSOCKET
    assert route.call_count == 2
    await rest.aclose()


class FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed = False

    def __aiter__(self) -> AsyncIterator[str]:
        return self._events()

    async def _events(self) -> AsyncIterator[str]:
        yield json.dumps({"event": "message", "data": {"chunk": "hello"}})
        yield json.dumps({"event": "close", "data": {}})

    async def send(self, value: str) -> None:
        self.sent.append(value)

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_websocket_sends_one_request_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    websocket = FakeWebSocket()
    captured_url: list[str] = []

    async def connect(url: str, **kwargs: object) -> FakeWebSocket:
        captured_url.append(url)
        assert kwargs["additional_headers"] == {"X-Credential": "credential"}
        return websocket

    monkeypatch.setattr("taiyiagent.transport.websocket.connect", connect)
    client = WebSocketStreamClient(
        "https://example.test/root",
        lambda: "token with space",
        "flow/a",
        credential_headers={"X-Credential": "credential"},
    )
    events = [event async for event in client.stream(ChatRequest(input_value="hello"))]
    assert [event.message_text for event in events] == ["hello", None]
    assert len(websocket.sent) == 1
    assert json.loads(websocket.sent[0])["input_value"] == "hello"
    assert websocket.closed
    assert "/root/api/v1/run/flow%2Fa/websocket" in captured_url[0]
    assert "__TOKEN__=token+with+space" in captured_url[0]


class BlockingWebSocket(FakeWebSocket):
    async def _events(self) -> AsyncIterator[str]:
        yield json.dumps({"event": "message", "data": {"chunk": "first"}})
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_websocket_cancellation_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = BlockingWebSocket()

    async def connect(url: str, **kwargs: object) -> BlockingWebSocket:
        return websocket

    monkeypatch.setattr("taiyiagent.transport.websocket.connect", connect)
    client = WebSocketStreamClient("https://example.test", lambda: "secret", "flow")
    stream = client.stream(ChatRequest(input_value="hello"))
    assert (await stream.__anext__()).message_text == "first"
    pending = asyncio.create_task(stream.__anext__())
    await asyncio.sleep(0)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert websocket.closed
