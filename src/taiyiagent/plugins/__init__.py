from collections.abc import Awaitable, Callable
from pathlib import Path

from ..transport.config import FlowMetadata
from ..transport.rest import RestClient
from .files import FilePlugin
from .manager import PluginManager, ToolDefinition, ToolExecutionContext, ToolHandler
from .media import ImageUnderstandingService, MediaUnderstandingPlugin
from .runtime import (
    AskUserPlugin,
    InteractionImage,
    InteractionQuestion,
    ScheduleTools,
    SessionContextPlugin,
    TodoPlugin,
    UserInteractionHandler,
    UserInteractionRequest,
    UserInteractionResponse,
)
from .search import SearchPlugin
from .shell import ShellPlugin


def register_builtin_plugins(
    manager: PluginManager,
    sandbox_root: str | Path,
    allowed_shell_commands: list[str] | tuple[str, ...] = (),
    *,
    rest: RestClient | None = None,
    metadata_provider: Callable[[], Awaitable[FlowMetadata]] | None = None,
    user_interaction_handler: UserInteractionHandler | None = None,
    enable_media: bool = True,
) -> None:
    ShellPlugin.register(manager, sandbox_root, allowed_shell_commands)
    FilePlugin.register(manager, sandbox_root)
    SearchPlugin.register(manager, sandbox_root)
    if rest is not None:
        if enable_media:
            MediaUnderstandingPlugin.register(manager, rest)
        ScheduleTools.register(manager, rest)
    if metadata_provider is not None:
        SessionContextPlugin.register(manager, metadata_provider)
    TodoPlugin.register(manager)
    AskUserPlugin.register(manager, user_interaction_handler)


__all__ = [
    "AskUserPlugin",
    "FilePlugin",
    "ImageUnderstandingService",
    "InteractionImage",
    "InteractionQuestion",
    "MediaUnderstandingPlugin",
    "PluginManager",
    "ScheduleTools",
    "SearchPlugin",
    "SessionContextPlugin",
    "ShellPlugin",
    "TodoPlugin",
    "UserInteractionHandler",
    "UserInteractionRequest",
    "UserInteractionResponse",
    "ToolDefinition",
    "ToolExecutionContext",
    "ToolHandler",
    "register_builtin_plugins",
]
