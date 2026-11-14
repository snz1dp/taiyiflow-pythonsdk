from __future__ import annotations

import asyncio
import mimetypes
import stat
from collections.abc import AsyncIterator, Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias, cast

from ..errors import ProtocolError
from .config import FlowMetadata, ServiceConfig
from .rest import RestClient, path_segment

JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]
JsonMapping: TypeAlias = Mapping[str, JsonValue]
QueryMapping: TypeAlias = Mapping[str, object]


def _json(value: object) -> JsonValue:
    return cast(JsonValue, value)


def _object(value: object, operation: str) -> JsonObject:
    if not isinstance(value, dict):
        raise ProtocolError(f"{operation} response must be an object")
    return cast(JsonObject, value)


class SessionClient:
    def __init__(self, rest: RestClient) -> None:
        self._rest = rest

    async def create_session_id(self) -> str:
        response = _object(await self._rest.get("/api/v1/session_id"), "Create session ID")
        session_id = response.get("session_id")
        if not isinstance(session_id, str) or not session_id.strip():
            raise ProtocolError("Create session ID response is missing session_id")
        return session_id

    async def list(
        self,
        flow_id: str,
        *,
        offset: int = 0,
        limit: int = 100,
        filters: QueryMapping | None = None,
    ) -> JsonValue:
        return _json(
            await self._rest.get(
                _flow_sessions(flow_id),
                params={**(filters or {}), "offset": offset, "limit": limit},
            )
        )

    async def get(self, flow_id: str, session_id: str) -> JsonValue:
        return _json(await self._rest.get(_session_path(flow_id, session_id)))

    async def rename(self, flow_id: str, session_id: str, session_name: str) -> JsonValue:
        return _json(
            await self._rest.post(
                _session_path(flow_id, session_id), {"session_name": session_name}
            )
        )

    async def delete(self, flow_id: str, session_id: str) -> None:
        await self._rest.delete(_session_path(flow_id, session_id))

    async def delete_all(self, flow_id: str, session_ids: Collection[str]) -> None:
        for session_id in session_ids:
            await self.delete(flow_id, session_id)

    async def messages(
        self, flow_id: str, session_id: str, *, filters: QueryMapping | None = None
    ) -> JsonValue:
        return _json(
            await self._rest.get(_session_path(flow_id, session_id) + "/messages", params=filters)
        )

    async def delete_messages(
        self, flow_id: str, session_id: str, message_ids: Collection[str]
    ) -> None:
        await self._rest.request(
            "DELETE",
            _session_path(flow_id, session_id) + "/messages",
            params={"message_ids": ",".join(message_ids)},
        )


class SkillClient:
    _BASE = "/api/v1/agents/skills"

    def __init__(self, rest: RestClient) -> None:
        self._rest = rest

    async def list(self, *, context: QueryMapping | None = None) -> JsonValue:
        return _json(await self._rest.get(self._BASE, params=context))

    async def get_content(self, skill_id: str, *, context: QueryMapping | None = None) -> JsonValue:
        return _json(await self._rest.get(f"{self._BASE}/{path_segment(skill_id)}", params=context))

    async def upload(self, skill: JsonMapping) -> JsonValue:
        return _json(await self._rest.post(f"{self._BASE}/upload", dict(skill)))

    async def upload_archive(
        self,
        *,
        name: str,
        display_name: str,
        filename: str,
        zip_content: bytes,
    ) -> JsonValue:
        return _json(
            await self._rest.post_multipart(
                f"{self._BASE}/upload",
                fields={"name": name, "display_name": display_name},
                field_name="package_file",
                filename=filename,
                content=zip_content,
                content_type="application/zip",
            )
        )

    async def delete(self, skill_id: str) -> None:
        await self._rest.delete(f"{self._BASE}/{path_segment(skill_id)}")

    async def report_installation(self, installation: JsonMapping) -> JsonValue:
        return _json(await self._rest.post(f"{self._BASE}/installations", dict(installation)))

    async def installations(self, *, filters: QueryMapping | None = None) -> JsonValue:
        return _json(await self._rest.get(f"{self._BASE}/installations", params=filters))

    async def uninstall(self, record_id: str) -> JsonValue:
        return _json(
            await self._rest.post(f"{self._BASE}/installations/{path_segment(record_id)}/uninstall")
        )


class MemoryClient:
    _BASE = "/api/v1/agents/memories"

    def __init__(self, rest: RestClient) -> None:
        self._rest = rest

    async def list(self, *, filters: QueryMapping | None = None) -> JsonValue:
        return _json(await self._rest.get(self._BASE, params=filters))

    async def create(self, memory: JsonMapping) -> JsonValue:
        return _json(await self._rest.post(self._BASE, dict(memory)))

    async def get(self, memory_id: str) -> JsonValue:
        return _json(await self._rest.get(f"{self._BASE}/{path_segment(memory_id)}"))

    async def forget(self, memory_id: str) -> JsonValue:
        return _json(await self._rest.post(f"{self._BASE}/{path_segment(memory_id)}/forget"))

    async def restore(self, memory_id: str) -> JsonValue:
        return _json(await self._rest.post(f"{self._BASE}/{path_segment(memory_id)}/restore"))


class ConfigClient:
    def __init__(self, rest: RestClient) -> None:
        self._rest = rest

    async def get(self) -> JsonObject:
        return _object(await self._rest.get("/api/v1/config"), "Get config")

    async def get_service_config(self) -> ServiceConfig:
        return ServiceConfig(await self.get())


class FlowMetadataClient:
    def __init__(self, rest: RestClient) -> None:
        self._rest = rest

    async def get(self, flow_id_or_name: str) -> FlowMetadata:
        response = await self._rest.get(f"/api/v1/run/{path_segment(flow_id_or_name)}/metadata")
        return FlowMetadata(_object(response, "Get flow metadata"))


@dataclass(frozen=True, slots=True)
class UploadedFile:
    flow_id: str
    file_path: str


class FileClient:
    def __init__(self, rest: RestClient) -> None:
        self._rest = rest

    async def upload(
        self,
        flow_id: str,
        session_id: str,
        file: str | Path,
        *,
        remote_path: str | None = None,
        limits: ServiceConfig | None = None,
        allowed_extensions: Collection[str] | None = None,
    ) -> UploadedFile:
        source = Path(file)
        await _validate_upload(source, limits, allowed_extensions)
        fields: dict[str, object] = {"session_id": session_id}
        if remote_path and remote_path.strip():
            fields["file_path"] = remote_path
        content_type = mimetypes.guess_type(source.name)[0] or "application/octet-stream"
        response = _object(
            await self._rest.post_multipart(
                f"/api/v1/agents/upload/{path_segment(flow_id)}",
                fields=fields,
                field_name="file",
                filename=source.name,
                content=lambda: _read_file_chunks(source),
                content_type=content_type,
            ),
            "Upload file",
        )
        uploaded_flow_id = response.get("flow_id")
        file_path = response.get("file_path")
        if not isinstance(uploaded_flow_id, str) or not isinstance(file_path, str):
            raise ProtocolError("Upload file response is missing flow_id or file_path")
        return UploadedFile(uploaded_flow_id, file_path)

    async def upload_many(
        self,
        flow_id: str,
        session_id: str,
        files: Collection[str | Path],
        *,
        limits: ServiceConfig,
        allowed_extensions: Collection[str] | None = None,
    ) -> list[UploadedFile]:
        if not files:
            raise ValueError("At least one upload file is required")
        maximum = limits.once_max_file_count()
        if maximum is not None and len(files) > maximum:
            raise ValueError("Upload file count exceeds once_max_file_count")
        return [
            await self.upload(
                flow_id,
                session_id,
                file,
                limits=limits,
                allowed_extensions=allowed_extensions,
            )
            for file in files
        ]

    async def download(self, flow_id: str, session_id: str, file_path: str) -> bytes:
        encoded_path = "/".join(path_segment(part) for part in file_path.split("/"))
        return await self._rest.get_bytes(
            f"/api/v1/agents/download/{path_segment(flow_id)}/{path_segment(session_id)}"
            f"/{encoded_path}"
        )


class ScheduleClient:
    _BASE = "/api/v1/agents/client/schedules"

    def __init__(self, rest: RestClient) -> None:
        self._rest = rest

    async def list(self, *, filters: QueryMapping | None = None) -> JsonValue:
        return _json(await self._rest.get(self._BASE, params=filters))

    async def get(self, schedule_id: str) -> JsonValue:
        return _json(await self._rest.get(self._path(schedule_id)))

    async def create(self, schedule: JsonMapping) -> JsonValue:
        return _json(await self._rest.post(self._BASE, dict(schedule)))

    async def update(self, schedule_id: str, schedule: JsonMapping) -> JsonValue:
        return _json(await self._rest.patch(self._path(schedule_id), dict(schedule)))

    async def delete(self, schedule_id: str) -> None:
        await self._rest.delete(self._path(schedule_id))

    async def enable(self, schedule_id: str, *, route: QueryMapping | None = None) -> JsonValue:
        return _json(await self._rest.post(self._path(schedule_id) + "/enable", params=route))

    async def disable(self, schedule_id: str, *, route: QueryMapping | None = None) -> JsonValue:
        return _json(await self._rest.post(self._path(schedule_id) + "/disable", params=route))

    async def runs(
        self,
        schedule_id: str,
        *,
        offset: int = 0,
        limit: int = 20,
        filters: QueryMapping | None = None,
    ) -> JsonValue:
        return _json(
            await self._rest.get(
                self._path(schedule_id) + "/runs",
                params={**(filters or {}), "offset": offset, "limit": limit},
            )
        )

    async def report(self, run_record_id: str, report: JsonMapping) -> JsonValue:
        return _json(
            await self._rest.post(
                f"{self._BASE}/runs/{path_segment(run_record_id)}/report", dict(report)
            )
        )

    def _path(self, schedule_id: str) -> str:
        return f"{self._BASE}/{path_segment(schedule_id)}"


class CollaborationTaskClient:
    _BASE = "/api/v1/agents/client/collaborations"

    def __init__(self, rest: RestClient) -> None:
        self._rest = rest

    async def create(self, payload: JsonMapping) -> JsonValue:
        return _json(await self._rest.post(self._BASE, dict(payload)))

    async def get(self, task_id: str) -> JsonValue:
        return _json(await self._rest.get(self._path(task_id)))

    async def list(self, *, filters: QueryMapping | None = None) -> JsonValue:
        return _json(await self._rest.get(self._BASE, params=filters))

    async def cancel(
        self, task_id: str, request_id: str, *, expected_version: int | None = None
    ) -> JsonValue:
        payload: JsonObject = {"request_id": request_id}
        _put_version(payload, expected_version)
        return _json(await self._rest.post(self._path(task_id) + "/cancel", payload))

    async def running(
        self,
        task_id: str,
        progress: JsonValue = None,
        *,
        expected_version: int | None = None,
    ) -> JsonValue:
        payload: JsonObject = {"status": "running"}
        if progress is not None:
            payload["status_payload"] = _object_payload(progress, "progress")
        _put_version(payload, expected_version)
        return _json(await self._rest.patch(self._path(task_id), payload))

    async def success(
        self, task_id: str, result: JsonValue = None, *, expected_version: int | None = None
    ) -> JsonValue:
        payload: JsonObject = {
            "status": "success",
            "result_payload": _object_payload(result, "result"),
        }
        _put_version(payload, expected_version)
        return _json(await self._rest.patch(self._path(task_id), payload))

    async def failure(
        self, task_id: str, error: str | None = None, *, expected_version: int | None = None
    ) -> JsonValue:
        payload: JsonObject = {
            "status": "failed",
            "error_message": error.strip() if error and error.strip() else "Task execution failed",
        }
        _put_version(payload, expected_version)
        return _json(await self._rest.patch(self._path(task_id), payload))

    async def cancelled(
        self,
        task_id: str,
        cancel_request_id: str,
        *,
        expected_version: int | None = None,
    ) -> JsonValue:
        payload: JsonObject = {
            "status": "cancelled",
            "status_payload": {"cancel_request_id": cancel_request_id},
        }
        _put_version(payload, expected_version)
        return _json(await self._rest.patch(self._path(task_id), payload))

    def _path(self, task_id: str) -> str:
        return f"{self._BASE}/{path_segment(task_id)}"


class McpServerClient:
    _BASE = "/api/v1/agents/mcp/servers"

    def __init__(self, rest: RestClient) -> None:
        self._rest = rest

    async def list(self, flow_id: str, *, client_type: str = "web") -> JsonValue:
        return _json(
            await self._rest.get(
                self._BASE, params={"flow_id": flow_id, "client_type": client_type}
            )
        )

    async def create(self, server: JsonMapping) -> JsonValue:
        return _json(await self._rest.post(self._BASE, dict(server)))

    async def update(self, server_id: str, server: JsonMapping) -> JsonValue:
        return _json(await self._rest.post(self._path(server_id), dict(server)))

    async def delete(self, server_id: str) -> None:
        await self._rest.delete(self._path(server_id))

    async def set_enabled(self, server_id: str, enabled: bool) -> JsonValue:
        return _json(
            await self._rest.post_form(
                self._path(server_id) + "/status", {"enabled": str(enabled).lower()}
            )
        )

    def _path(self, server_id: str) -> str:
        return f"{self._BASE}/{path_segment(server_id)}"


async def _validate_upload(
    source: Path,
    limits: ServiceConfig | None,
    allowed_extensions: Collection[str] | None,
) -> None:
    try:
        details = await asyncio.to_thread(source.lstat)
    except OSError as exc:
        message = f"Upload must be a regular, non-symbolic-link file: {source.name}"
        raise ValueError(message) from exc
    if not stat.S_ISREG(details.st_mode):
        raise ValueError(f"Upload must be a regular, non-symbolic-link file: {source.name}")
    if details.st_size == 0:
        raise ValueError(f"Upload file must not be empty: {source.name}")
    if limits is not None:
        maximum = limits.max_file_size_bytes()
        if maximum is not None and details.st_size > maximum:
            raise ValueError(f"Upload file exceeds max_file_size_upload: {source.name}")
    if allowed_extensions is not None:
        normalized = {item.strip().lower().removeprefix(".") for item in allowed_extensions}
        if source.suffix.lower().removeprefix(".") not in normalized:
            raise ValueError(f"Upload file extension is not allowed: {source.name}")


async def _read_file_chunks(source: Path, chunk_size: int = 1024 * 1024) -> AsyncIterator[bytes]:
    handle = await asyncio.to_thread(source.open, "rb")
    try:
        while True:
            chunk = await asyncio.to_thread(handle.read, chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        await asyncio.to_thread(handle.close)


def _flow_sessions(flow_id: str) -> str:
    return f"/api/v1/agents/flows/{path_segment(flow_id)}/sessions"


def _session_path(flow_id: str, session_id: str) -> str:
    return f"{_flow_sessions(flow_id)}/{path_segment(session_id)}"


def _put_version(payload: JsonObject, expected_version: int | None) -> None:
    if expected_version is not None:
        payload["expected_version"] = expected_version


def _object_payload(value: JsonValue, field: str) -> JsonObject:
    if isinstance(value, dict):
        return value
    return {field: value}
