from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from typing import cast
from urllib.parse import urlsplit

import httpx

from ..errors import ConfigurationError
from ..plugins.manager import PluginManager
from ..transport.capabilities import JsonObject, JsonValue
from .base import PROTOCOL_VERSION, McpError, McpTool, parse_result, parse_tools, register_tools

log = logging.getLogger(__name__)


class StreamableHttpMcpClient:
    def __init__(
        self,
        server_url: str,
        *,
        headers: Mapping[str, str] | None = None,
        timeout: float = 30.0,
        max_response_bytes: int = 1024 * 1024,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if urlsplit(server_url).scheme not in {"http", "https"}:
            raise ConfigurationError("MCP server_url must use HTTP or HTTPS")
        if timeout <= 0 or max_response_bytes <= 0:
            raise ConfigurationError("MCP timeout and response limit must be positive")
        self._server_url = server_url
        self._headers = dict(headers or {})
        self._timeout = timeout
        self._max_response_bytes = max_response_bytes
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=timeout),
            follow_redirects=False,
        )
        self._request_id = 0
        self._session_id: str | None = None
        self._initialized = False
        self._closed = False
        self._initialize_lock = asyncio.Lock()

    @property
    def session_id(self) -> str | None:
        return self._session_id

    async def list_tools(self) -> list[McpTool]:
        await self._ensure_initialized()
        return parse_tools(await self._request("tools/list", {}))

    async def call_tool(self, tool_name: str, arguments: JsonObject) -> JsonObject:
        await self._ensure_initialized()
        return await self._request("tools/call", {"name": tool_name, "arguments": arguments})

    async def register_tools(self, server_name: str, plugin_manager: PluginManager) -> list[str]:
        registered = await register_tools(self, server_name, plugin_manager)
        plugin_manager.register_closeable(self)
        return registered

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        async with self._initialize_lock:
            if self._initialized:
                return
            if self._closed:
                raise McpError("MCP HTTP client is closed")
            await self._request(
                "initialize",
                {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "taiyiflow-pythonsdk", "version": "1.0.0"},
                },
            )
            await self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})
            self._initialized = True

    async def _request(self, method: str, params: JsonObject) -> JsonObject:
        self._request_id += 1
        request_id = self._request_id
        body = await self._post(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
        )
        if body is None:
            raise McpError(f"MCP {method} response is empty")
        content_type, content = body
        if "text/event-stream" in content_type.lower():
            payload = _matching_sse_payload(content, request_id)
        else:
            payload = _decode_json(content)
        return parse_result(payload, request_id, method)

    async def _post(self, payload: JsonObject) -> tuple[str, bytes] | None:
        if self._closed:
            raise McpError("MCP HTTP client is closed")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            **self._headers,
        }
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id
        try:
            async with self._client.stream(
                "POST", self._server_url, json=payload, headers=headers
            ) as response:
                session_id = response.headers.get("Mcp-Session-Id")
                if session_id and session_id.strip():
                    self._session_id = session_id.strip()
                content = await _read_limited(response, self._max_response_bytes)
                if not 200 <= response.status_code < 300:
                    raise McpError(f"MCP HTTP error {response.status_code}")
                if response.status_code == 202 or not content.strip():
                    return None
                return response.headers.get("Content-Type", ""), content
        except (httpx.HTTPError, OSError) as exc:
            raise McpError("MCP HTTP request failed") from exc

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._initialized and self._session_id:
            try:
                headers = {**self._headers, "Mcp-Session-Id": self._session_id}
                response = await self._client.delete(self._server_url, headers=headers)
                if response.status_code >= 400:
                    log.warning("MCP session termination returned HTTP %s", response.status_code)
            except (httpx.HTTPError, OSError):
                log.warning("MCP session termination failed", exc_info=True)
        if self._owns_client:
            await self._client.aclose()


async def _read_limited(response: httpx.Response, limit: int) -> bytes:
    content = bytearray()
    async for chunk in response.aiter_bytes():
        content.extend(chunk)
        if len(content) > limit:
            raise McpError("MCP HTTP response size limit exceeded")
    return bytes(content)


def _decode_json(content: bytes) -> JsonValue:
    try:
        return cast(JsonValue, json.loads(content))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise McpError("Invalid MCP JSON response") from exc


def _matching_sse_payload(content: bytes, request_id: int) -> JsonValue:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise McpError("Invalid MCP SSE encoding") from exc
    matched: JsonValue | None = None
    for event in text.replace("\r\n", "\n").replace("\r", "\n").split("\n\n"):
        data = [
            line[5:].removeprefix(" ") for line in event.split("\n") if line.startswith("data:")
        ]
        if not data:
            continue
        payload = _decode_json("\n".join(data).encode())
        if isinstance(payload, dict) and payload.get("id") == request_id:
            matched = payload
    if matched is None:
        raise McpError("MCP SSE response has no matching JSON-RPC id")
    return matched
