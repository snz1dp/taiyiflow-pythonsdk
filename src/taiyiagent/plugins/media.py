from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from ..errors import redact
from ..models import ChatRequest, ToolCall, ToolResult
from ..transport.capabilities import ConfigClient, FileClient, UploadedFile
from ..transport.config import ServiceConfig
from ..transport.flow_run import (
    FlowRunClient,
    FlowRunPollOptions,
    FlowRunResult,
    FlowRunTask,
    FlowRunTimeoutError,
)
from ..transport.rest import RestClient
from .definitions import tool_definition
from .manager import PluginManager, ToolExecutionContext

_IMAGE_EXTENSIONS = frozenset({"jpg", "jpeg", "png", "gif", "svg", "webp", "bmp", "tif", "tiff"})
_AUDIO_EXTENSIONS = frozenset({"mp3", "wav", "ogg", "flac", "aif", "m4a", "aac"})
_VIDEO_EXTENSIONS = frozenset({"mp4", "webm", "avi", "mov", "flv", "wmv", "rmvb", "mkv"})
_DOCUMENT_EXTENSIONS = (
    _AUDIO_EXTENSIONS
    | _VIDEO_EXTENSIONS
    | frozenset({"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx"})
)
_SCREENSHOT_SYSTEM = (
    "你是截图理解助手。当前输入图像仅来自桌面 ComputerUse 或浏览器自动化截图，"
    "不是文档结构化解析任务。"
    "请只依据截图中可见的像素内容，优先识别界面结构、可交互元素、状态变化、"
    "错误提示、可见文本和与当前问题直接相关的线索；"
    "不要臆测不可见内容，不要补全被遮挡区域，也不要把截图按文档理解流程处理。"
    "若问题涉及按钮、输入框、链接或其他目标的位置，"
    "请优先使用截图左上角为原点的截图本地像素坐标(x,y)描述，"
    "不要自行换算成全局桌面坐标；只有在问题明确要求且已提供显示器 origin 等元数据时，"
    "才说明如何换算。"
)
_SCREENSHOT_USER = (
    "请仅依据截图中可见内容回答问题，并且只返回纯文本结果；若截图中没有足够证据，"
    '请直接说明"截图中未见相关信息"。\n问题：{question}'
)


class MediaUnderstandingPlugin:
    def __init__(
        self,
        rest: RestClient,
        *,
        synchronous_timeout: float = 29 * 60,
        image_poll_options: FlowRunPollOptions | None = None,
        audio_video_poll_options: FlowRunPollOptions | None = None,
    ) -> None:
        self.config = ConfigClient(rest)
        self.files = FileClient(rest)
        self.flow_runs = FlowRunClient(rest)
        self.synchronous_timeout = synchronous_timeout
        self.image_poll_options = image_poll_options or FlowRunPollOptions(overall_timeout=29 * 60)
        self.audio_video_poll_options = audio_video_poll_options or FlowRunPollOptions(
            overall_timeout=59 * 60
        )

    async def document(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        paths: list[Path] = []
        uploaded: list[UploadedFile] = []
        try:
            config = await self.config.get_service_config()
            paths = _resolve_paths(context, call.arguments_dict)
            for path in paths:
                extension = _extension(path)
                if extension in _IMAGE_EXTENSIONS:
                    raise ValueError(f"Image input must use image_understanding: {path.name}")
                if extension not in _DOCUMENT_EXTENSIONS:
                    raise ValueError(f"Document extension is not enabled: {extension}")
                supported = config.document_extensions()
                if supported and extension not in supported:
                    raise ValueError(f"Server does not support document extension: {extension}")
            uploaded = await self._upload(context, paths, config)
            request = _request(context, call.arguments_dict, uploaded)
            request.output_type = _text(call.arguments_dict, "output_type", "any")
            try:
                result = await self.flow_runs.run(
                    config.document_flow_id(), request, request_timeout=self.synchronous_timeout
                )
            except Exception as exc:
                raise _MediaOperationError("understanding", paths, uploaded, exc) from exc
            return ToolResult.ok(call.id, _document_result(context, result, paths, uploaded))
        except _MediaOperationError as exc:
            return exc.result(call.id)
        except Exception as exc:
            return ToolResult.fail(call.id, redact(exc))

    async def image(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        return await self._media_tool(context, call, "image")

    async def audio(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        return await self._media_tool(context, call, "audio")

    async def video(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        return await self._media_tool(context, call, "video")

    async def understand_screenshot(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> str:
        result = await self._media_result(context, arguments, "image", screenshot=True)
        return str(result["text"])

    async def aclose(self) -> None:
        await self.flow_runs.aclose()

    async def _media_tool(
        self,
        context: ToolExecutionContext,
        call: ToolCall,
        kind: Literal["image", "audio", "video"],
    ) -> ToolResult:
        try:
            return ToolResult.ok(
                call.id, await self._media_result(context, call.arguments_dict, kind)
            )
        except _MediaOperationError as exc:
            return exc.result(call.id)
        except Exception as exc:
            return ToolResult.fail(call.id, redact(exc))

    async def _media_result(
        self,
        context: ToolExecutionContext,
        arguments: Mapping[str, Any],
        kind: Literal["image", "audio", "video"],
        *,
        screenshot: bool = False,
    ) -> dict[str, Any]:
        config = await self.config.get_service_config()
        flow_id = {
            "image": config.image_flow_id,
            "audio": config.audio_flow_id,
            "video": config.video_flow_id,
        }[kind]()
        paths = _resolve_paths(context, arguments)
        supported = {
            "image": _IMAGE_EXTENSIONS,
            "audio": _AUDIO_EXTENSIONS,
            "video": _VIDEO_EXTENSIONS,
        }[kind]
        for path in paths:
            extension = _extension(path)
            if extension not in supported:
                raise ValueError(f"Unsupported {kind} extension: {extension}")
        uploaded: list[UploadedFile] = []
        try:
            uploaded = await self._upload(context, paths, config)
            request = _request(context, arguments, uploaded)
            request.output_type = "any"
            if kind == "image":
                tweaks: dict[str, Any] = {"TextOutput-fXIUJ": {"output_format": "text"}}
                if screenshot:
                    request.input_value = _screenshot_input(arguments)
                    tweaks["Prompt-d2zsV"] = {"template": _SCREENSHOT_SYSTEM}
                    tweaks["Prompt-GRXIq"] = {"template": _SCREENSHOT_USER}
                compression = (
                    "passthrough"
                    if screenshot
                    else _text(arguments, "image_compression_strategy", "")
                )
                if compression:
                    tweaks["image_compression_strategy"] = compression
                request.tweaks = tweaks
            try:
                submitted = await self.flow_runs.submit_async(flow_id, request)
                options = (
                    self.image_poll_options if kind == "image" else self.audio_video_poll_options
                )
                task = await self.flow_runs.await_task(
                    submitted.task_id,
                    options=options,
                    cancellation_requested=context.cancellation_requested,
                )
                if task.status != "success":
                    raise RuntimeError(_task_error(task))
            except Exception as exc:
                raise _MediaOperationError("understanding", paths, uploaded, exc) from exc
            if task.result is None:
                raise _MediaOperationError(
                    "understanding",
                    paths,
                    uploaded,
                    RuntimeError("Successful media understanding task returned no result"),
                )
            return {
                "text": _limit(_output_text(task.result)),
                "media_type": task.result.media_type or "text/plain",
                "session_id": _first(task.result.session_id, task.session_id, context.session_id),
                "uploaded_files": _uploaded_files(paths, uploaded),
            }
        except _MediaOperationError:
            raise
        except Exception as exc:
            raise _MediaOperationError("understanding", paths, uploaded, exc) from exc

    async def _upload(
        self, context: ToolExecutionContext, paths: list[Path], config: ServiceConfig
    ) -> list[UploadedFile]:
        if context.flow_id is None or not context.flow_id.strip():
            raise ValueError("Media understanding requires the current business flow")
        count = config.once_max_file_count()
        if count is not None and len(paths) > count:
            raise ValueError("File count exceeds once_max_file_count")
        uploaded: list[UploadedFile] = []
        for path in paths:
            try:
                uploaded.append(
                    await self.files.upload(
                        context.flow_id, context.session_id, path, limits=config
                    )
                )
            except Exception as exc:
                raise _MediaOperationError("upload", paths, uploaded, exc) from exc
        return uploaded

    @staticmethod
    def register(manager: PluginManager, rest: RestClient) -> MediaUnderstandingPlugin:
        plugin = MediaUnderstandingPlugin(rest)
        common = {
            "path": {"type": "string"},
            "paths": {"type": "array", "items": {"type": "string"}},
            "input": {"type": "string"},
            "input_type": {"type": "string"},
            "working_directory": {"type": "string"},
        }
        definitions = (
            (
                "document_unstructured",
                "Upload documents to the server document flow.",
                "document_understanding",
                plugin.document,
            ),
            (
                "image_understanding",
                "Upload images to the server image understanding flow.",
                "image_understanding",
                plugin.image,
            ),
            (
                "audio_understanding",
                "Upload audio to the server audio understanding flow.",
                "audio_understanding",
                plugin.audio,
            ),
            (
                "video_understanding",
                "Upload video to the server video understanding flow.",
                "video_understanding",
                plugin.video,
            ),
        )
        for name, description, feature, handler in definitions:
            properties = dict(common)
            if name == "document_unstructured":
                properties["output_type"] = {"type": "string"}
            manager.register(
                name,
                handler,
                definition=tool_definition(name, description, properties, feature=feature),
            )
        manager.register_closeable(plugin)
        return plugin


class ImageUnderstandingService:
    def __init__(self, rest: RestClient) -> None:
        self.media = MediaUnderstandingPlugin(rest)

    async def understand_screenshot(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> str:
        return await self.media.understand_screenshot(context, arguments)

    async def aclose(self) -> None:
        await self.media.aclose()


class _MediaOperationError(Exception):
    def __init__(
        self,
        stage: str,
        paths: list[Path],
        uploaded: list[UploadedFile],
        cause: Exception,
    ) -> None:
        self.stage = stage
        self.paths = paths
        self.uploaded = uploaded
        self.cause = cause
        super().__init__(f"{stage} failed: {_limit(redact(cause), 8192)}")

    def result(self, call_id: str) -> ToolResult:
        result = ToolResult.fail(call_id, str(self))
        details: dict[str, Any] = {
            "failed_stage": self.stage,
            "uploaded_files": _uploaded_files(self.paths, self.uploaded),
        }
        if isinstance(self.cause, FlowRunTimeoutError):
            details["task_id"] = self.cause.task_id
            details["cancellation_requested"] = self.cause.cancellation_requested
        result.result = details
        return result


def _resolve_paths(context: ToolExecutionContext, arguments: Mapping[str, Any]) -> list[Path]:
    if context.workspace is None:
        raise ValueError("Media understanding requires a session workspace")
    raw_paths: list[str] = []
    for name in ("path", "file_path", "filePath"):
        value = _text(arguments, name, "")
        if value:
            raw_paths.append(value)
    for name in ("paths", "file_paths", "filePaths"):
        values = arguments.get(name)
        if isinstance(values, list):
            raw_paths.extend(
                value.strip() for value in values if isinstance(value, str) and value.strip()
            )
    if not raw_paths:
        raise ValueError("At least one path is required")
    workspace = context.workspace.expanduser().resolve(strict=True)
    base = workspace
    working_directory = _text(arguments, "working_directory", "")
    if working_directory:
        candidate = (workspace / working_directory).resolve(strict=True)
        if not candidate.is_relative_to(workspace) or not candidate.is_dir():
            raise ValueError("Working directory is outside the workspace or is not a directory")
        base = candidate
    paths: list[Path] = []
    for raw_path in raw_paths:
        path = (base / raw_path).resolve(strict=True)
        if not path.is_relative_to(workspace):
            raise ValueError("Path escapes the configured workspace")
        paths.append(path)
    return paths


def _request(
    context: ToolExecutionContext, arguments: Mapping[str, Any], uploaded: list[UploadedFile]
) -> ChatRequest:
    return ChatRequest(
        input_value=_first(
            _text(arguments, "input", ""),
            _text(arguments, "input_value", ""),
            _text(arguments, "prompt", ""),
        ),
        input_type=_text(arguments, "input_type", "any"),
        session_id=context.session_id,
        files=[item.file_path for item in uploaded],
    )


def _document_result(
    context: ToolExecutionContext,
    result: FlowRunResult,
    paths: list[Path],
    uploaded: list[UploadedFile],
) -> dict[str, Any]:
    output = _limit(_output_text(result)).strip()
    parsed: Any = None
    if output:
        try:
            parsed = json.loads(output)
        except json.JSONDecodeError:
            parsed = None
    if isinstance(parsed, list):
        items = parsed
    elif isinstance(parsed, dict) and isinstance(parsed.get("items"), list):
        items = parsed["items"]
    elif isinstance(parsed, str):
        items = [{"text": parsed.strip()}] if parsed.strip() else []
    elif parsed is not None:
        items = [parsed]
    else:
        items = [{"text": output}] if output else []
    response: dict[str, Any] = {
        "count": len(items),
        "items": items,
        "media_type": result.media_type or "text/plain",
        "session_id": _first(result.session_id, result.response_session_header, context.session_id),
        "uploaded_files": _uploaded_files(paths, uploaded),
    }
    if isinstance(parsed, dict) and not (len(parsed) == 1 and "items" in parsed):
        response["parsed_output"] = parsed
    elif parsed is None and output:
        response["raw_output"] = output
    return response


def _output_text(result: FlowRunResult) -> str:
    if isinstance(result.json, dict) and "outputs" in result.json:
        return "\n".join(result.outputs)
    return "\n".join(result.outputs) if result.outputs else result.raw_body


def _task_error(task: FlowRunTask) -> str:
    error = task.error if isinstance(task.error, Mapping) else {}
    message = error.get("message") if isinstance(error.get("message"), str) else ""
    if not message:
        message = (
            "Media understanding task was cancelled"
            if task.status == "cancelled"
            else "Media understanding task failed"
        )
    code = error.get("code") if isinstance(error.get("code"), str) else ""
    return f"{message} ({code})" if code else message


def _screenshot_input(arguments: Mapping[str, Any]) -> str:
    question = _first(
        _text(arguments, "input_value", ""),
        _text(arguments, "input", ""),
        _text(arguments, "prompt", ""),
        "请概述截图中的界面结构、可见文本、可交互元素和关键状态信息。",
    )
    prefix = "截图坐标上下文："
    if prefix in question:
        return question
    details = [
        prefix
        + "若需要描述目标位置，请使用截图本地像素坐标(x,y)，原点在截图左上角；"
        + "除非问题明确要求，否则不要直接输出全局桌面坐标。"
    ]
    width = _number(arguments, "screenshot_width_px")
    height = _number(arguments, "screenshot_height_px")
    if width > 0 and height > 0:
        details.append(f"当前截图尺寸为 {width} x {height} 像素。")
    display_id = _text(arguments, "display_id", "")
    if display_id:
        details.append(f"来源显示器 display_id={display_id}。")
    display_index = _number(arguments, "display_index")
    if display_index > 0:
        details.append(f"来源显示器 display_index={display_index}。")
    if "display_origin_x" in arguments or "display_origin_y" in arguments:
        origin_x = _number(arguments, "display_origin_x")
        origin_y = _number(arguments, "display_origin_y")
        details.append(
            f"全局坐标换算参数 display_origin_x={origin_x}、display_origin_y={origin_y}。"
        )
    scale = _float(arguments, "display_scale_factor")
    if scale > 0:
        details.append(f"显示器 scale_factor={scale}。")
    return question + "\n\n" + " ".join(details)


def _uploaded_files(paths: list[Path], uploaded: list[UploadedFile]) -> list[dict[str, str]]:
    return [
        {
            "local_path": str(paths[index]),
            "extension": _extension(paths[index]),
            "remote_path": item.file_path,
        }
        for index, item in enumerate(uploaded)
    ]


def _text(arguments: Mapping[str, Any], name: str, fallback: str) -> str:
    value = arguments.get(name)
    return value.strip() if isinstance(value, str) and value.strip() else fallback


def _number(arguments: Mapping[str, Any], name: str) -> int:
    value = arguments.get(name)
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _float(arguments: Mapping[str, Any], name: str) -> float:
    value = arguments.get(name)
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else 0.0


def _extension(path: Path) -> str:
    return path.suffix.removeprefix(".").lower()


def _first(*values: str | None) -> str:
    return next((value for value in values if value and value.strip()), "")


def _limit(value: str, maximum: int = 4 * 1024 * 1024) -> str:
    return value if len(value) <= maximum else value[:maximum] + "\n...[truncated]"
