from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import quote, urlencode
from uuid import uuid4

import httpx

from ..errors import ApiError, ConfigurationError, TransportError
from ..models import AgentLoopControlResult, ResumableCheckpoint

TokenProvider = Callable[[], str]
UnauthorizedHandler = Callable[[], Awaitable[None]]


def path_segment(value: str) -> str:
    return quote(value, safe="")


def _multipart_text(value: str) -> str:
    return value.replace("\\", "_").replace('"', "_").replace("\r", "_").replace("\n", "_")


async def _multipart_body(
    boundary: str,
    fields: Mapping[str, Any],
    field_name: str,
    filename: str,
    content_type: str,
    content: AsyncIterator[bytes],
) -> AsyncIterator[bytes]:
    delimiter = f"--{boundary}\r\n".encode()
    for name, value in fields.items():
        yield delimiter
        disposition = f'Content-Disposition: form-data; name="{_multipart_text(str(name))}"'
        yield f"{disposition}\r\n\r\n".encode()
        yield value if isinstance(value, bytes) else str(value).encode()
        yield b"\r\n"
    yield delimiter
    yield (
        f'Content-Disposition: form-data; name="{_multipart_text(field_name)}"; '
        f'filename="{filename}"\r\nContent-Type: {content_type}\r\n\r\n'
    ).encode()
    async for chunk in content:
        yield chunk
    yield f"\r\n--{boundary}--\r\n".encode()


def append_query(path: str, parameters: Mapping[str, Any]) -> str:
    items: list[tuple[str, Any]] = []
    for key in sorted(parameters):
        value = parameters[key]
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            items.extend((key, item) for item in value if item is not None)
        else:
            items.append((key, value))
    if not items:
        return path
    return path + ("&" if "?" in path else "?") + urlencode(items)


class RestClient:
    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        *,
        access_token: TokenProvider | None = None,
        timeout: float = 30.0,
        credential_headers: Mapping[str, str] | None = None,
        client: httpx.AsyncClient | None = None,
        on_unauthorized: UnauthorizedHandler | None = None,
    ) -> None:
        if not base_url.strip():
            raise ConfigurationError("base_url is required")
        if access_token is None and api_key is None:
            raise ConfigurationError("api_key or access_token is required")
        if timeout <= 0:
            raise ConfigurationError("timeout must be positive")
        self.base_url = base_url.rstrip("/")
        self._access_token = access_token or (lambda: api_key or "")
        self._credential_headers = dict(credential_headers or {})
        self._on_unauthorized = on_unauthorized
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=timeout),
            follow_redirects=False,
        )

    @property
    def headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        token = self._access_token()
        if token.strip():
            headers["Authorization"] = f"Bearer {token}"
        headers.update(self._credential_headers)
        return headers

    async def get(self, path: str, *, params: Mapping[str, Any] | None = None) -> Any:
        return await self.request("GET", path, params=params)

    async def get_bytes(self, path: str, *, params: Mapping[str, Any] | None = None) -> bytes:
        response = await self._send("GET", path, params=params)
        return response.content

    async def post(
        self,
        path: str,
        body: Any = None,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        return await self.request("POST", path, json=body, params=params)

    async def post_form(self, path: str, form: Mapping[str, Any]) -> Any:
        return await self.request("POST", path, data=dict(form))

    async def patch(self, path: str, body: Any = None) -> Any:
        return await self.request("PATCH", path, json=body)

    async def delete(self, path: str) -> Any:
        return await self.request("DELETE", path)

    async def post_multipart(
        self,
        path: str,
        *,
        fields: Mapping[str, Any],
        field_name: str,
        filename: str,
        content: bytes | Callable[[], AsyncIterator[bytes]],
        content_type: str = "application/octet-stream",
    ) -> Any:
        safe_filename = _multipart_text(filename)
        safe_content_type = content_type.replace("\r", "").replace("\n", "")
        if isinstance(content, bytes):
            return await self.request(
                "POST",
                path,
                data=dict(fields),
                files={field_name: (safe_filename, content, safe_content_type)},
            )
        boundary = f"taiyiagent-{uuid4().hex}"
        return await self.request(
            "POST",
            path,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            _content_factory=lambda: _multipart_body(
                boundary, fields, field_name, safe_filename, safe_content_type, content()
            ),
        )

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self._send(method, path, **kwargs)
        if not response.content.strip():
            return {}
        try:
            return response.json()
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise TransportError(f"Expected JSON response for {method} {path}") from exc

    async def request_raw(
        self,
        method: str,
        path: str,
        *,
        raise_for_status: bool = True,
        **kwargs: Any,
    ) -> httpx.Response:
        return await self._send(method, path, _raise_for_status=raise_for_status, **kwargs)

    async def get_raw(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        request_timeout: float | None = None,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        return await self.request_raw(
            "GET",
            path,
            params=params,
            timeout=request_timeout,
            raise_for_status=raise_for_status,
        )

    async def post_raw(
        self,
        path: str,
        body: Any = None,
        *,
        request_timeout: float | None = None,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        return await self.request_raw(
            "POST",
            path,
            json=body,
            timeout=request_timeout,
            raise_for_status=raise_for_status,
        )

    async def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        extra_headers = dict(kwargs.pop("headers", {}))
        raise_for_status = bool(kwargs.pop("_raise_for_status", True))
        content_factory = kwargs.pop("_content_factory", None)
        if kwargs.get("timeout") is None:
            kwargs.pop("timeout", None)
        for attempt in range(2):
            attempt_kwargs = dict(kwargs)
            if content_factory is not None:
                attempt_kwargs["content"] = content_factory()
            try:
                response = await self._client.request(
                    method,
                    self._url(path),
                    headers=self.headers | extra_headers,
                    **attempt_kwargs,
                )
            except (httpx.HTTPError, OSError) as exc:
                raise TransportError(f"REST request failed: {method} {path}") from exc
            if response.status_code == 401 and self._on_unauthorized and attempt == 0:
                await self._on_unauthorized()
                continue
            if raise_for_status:
                self._raise_for_status(response, method, path)
            return response
        raise AssertionError("unreachable")

    @asynccontextmanager
    async def stream(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> AsyncIterator[httpx.Response]:
        extra_headers = dict(kwargs.pop("headers", {}))
        for attempt in range(2):
            context = self._client.stream(
                method,
                self._url(path),
                headers=self.headers | extra_headers,
                **kwargs,
            )
            try:
                response = await context.__aenter__()
            except (httpx.HTTPError, OSError) as exc:
                await context.__aexit__(None, None, None)
                raise TransportError(f"Stream request failed: {method} {path}") from exc
            try:
                if response.status_code == 401 and self._on_unauthorized and attempt == 0:
                    await self._on_unauthorized()
                    continue
                if not 200 <= response.status_code < 300:
                    body = await _read_limited(response)
                    raise ApiError(
                        response.status_code,
                        _parse_detail(body),
                        method=method,
                        path=path,
                    )
                yield response
                return
            finally:
                await context.__aexit__(None, None, None)
        raise AssertionError("unreachable")

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _url(self, path: str) -> str:
        return path if path.startswith(("http://", "https://")) else self.base_url + path

    @staticmethod
    def _raise_for_status(response: httpx.Response, method: str, path: str) -> None:
        if 200 <= response.status_code < 300:
            return
        raise ApiError(
            response.status_code,
            _parse_detail(response.content[: 64 * 1024]),
            method=method,
            path=path,
        )


async def _read_limited(response: httpx.Response) -> bytes:
    chunks = bytearray()
    async for chunk in response.aiter_bytes():
        chunks.extend(chunk[: 64 * 1024 - len(chunks)])
        if len(chunks) >= 64 * 1024:
            break
    return bytes(chunks)


def _parse_detail(body: bytes) -> Any:
    if not body.strip():
        return None
    try:
        parsed = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return body.decode("utf-8", errors="replace")[:4096]
    return parsed.get("detail") if isinstance(parsed, dict) and "detail" in parsed else parsed


class ExecutionController:
    def __init__(self, rest: RestClient, client_id: str, device_id: str | None = None) -> None:
        if not client_id.strip():
            raise ConfigurationError("client_id is required")
        self._rest = rest
        self._client_id = client_id
        self._device_id = device_id

    async def pause(self, session_id: str, run_id: str | None) -> AgentLoopControlResult:
        body = self._control_body(session_id, run_id)
        body["client_id"] = self._client_id
        if self._device_id:
            body["device_id"] = self._device_id
        response = await self._rest.post(self._control_path(session_id, "pause"), body)
        return AgentLoopControlResult.model_validate(response)

    async def cancel(self, session_id: str, run_id: str | None) -> AgentLoopControlResult:
        response = await self._rest.post(
            self._control_path(session_id, "cancel"),
            self._control_body(session_id, run_id),
        )
        return AgentLoopControlResult.model_validate(response)

    async def list_resumable(
        self,
        flow_id: str | None = None,
        session_id: str | None = None,
    ) -> list[ResumableCheckpoint]:
        response = await self._rest.get(
            "/api/v1/run/checkpoints/resumable",
            params={
                "client_id": self._client_id,
                "device_id": self._device_id,
                "flow_id": flow_id,
                "session_id": session_id,
            },
        )
        if not isinstance(response, list):
            raise TransportError("Resumable checkpoint response must be an array")
        return [ResumableCheckpoint.model_validate(item) for item in response]

    @staticmethod
    def _control_body(session_id: str, run_id: str | None) -> dict[str, Any]:
        if not session_id.strip():
            raise ConfigurationError("session_id is required")
        return {"run_id": run_id or None, "request_id": str(uuid4())}

    @staticmethod
    def _control_path(session_id: str, operation: str) -> str:
        return f"/api/v1/run/sessions/{path_segment(session_id)}/{operation}"
