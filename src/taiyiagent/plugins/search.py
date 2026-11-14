from __future__ import annotations

import asyncio
import os
import re
from pathlib import Path
from typing import Any

from ..errors import redact
from ..models import ToolCall, ToolResult
from .definitions import tool_definition
from .manager import PluginManager


class SearchPlugin:
    def __init__(
        self,
        sandbox_root: str | Path,
        *,
        max_results: int = 100,
        max_file_bytes: int = 1_000_000,
    ) -> None:
        if max_results <= 0 or max_file_bytes <= 0:
            raise ValueError("Search limits must be positive")
        self.root = Path(sandbox_root).expanduser().resolve()
        self.max_results = max_results
        self.max_file_bytes = max_file_bytes

    async def search(self, call: ToolCall, *, root: Path | None = None) -> ToolResult:
        workspace = (root or self.root).expanduser().resolve()
        return await asyncio.to_thread(self._search, call, workspace)

    def _search(self, call: ToolCall, workspace: Path) -> ToolResult:
        try:
            args = call.arguments_dict
            pattern = args.get("pattern")
            if not isinstance(pattern, str):
                raise ValueError("Missing 'pattern' argument")
            raw_path = args.get("path", ".")
            if not isinstance(raw_path, str):
                raise ValueError("path must be a string")
            root = (workspace / raw_path).resolve(strict=True)
            if not root.is_relative_to(workspace):
                raise ValueError("Path escapes sandbox")
            expression = re.compile(pattern if args.get("regex") is True else re.escape(pattern))
            matches: list[dict[str, Any]] = []
            truncated = False
            for directory, names, files in os.walk(root, followlinks=False):
                names[:] = [
                    name
                    for name in names
                    if not (Path(directory) / name).is_symlink()
                    and (Path(directory) / name).resolve().is_relative_to(workspace)
                ]
                for name in sorted(files):
                    path = Path(directory) / name
                    if path.is_symlink() or not path.is_file():
                        continue
                    resolved = path.resolve()
                    if not resolved.is_relative_to(workspace):
                        continue
                    try:
                        if resolved.stat().st_size > self.max_file_bytes:
                            continue
                        content = resolved.read_text(encoding="utf-8")
                    except (OSError, UnicodeError):
                        continue
                    for line_number, line in enumerate(content.splitlines(), 1):
                        if expression.search(line):
                            matches.append(
                                {
                                    "file": str(resolved.relative_to(workspace)),
                                    "line": line_number,
                                    "content": line.strip(),
                                }
                            )
                            if len(matches) >= self.max_results:
                                truncated = True
                                break
                    if truncated:
                        break
                if truncated:
                    break
            return ToolResult.ok(
                call.id,
                {
                    "matches": matches,
                    "totalMatches": len(matches),
                    "truncated": truncated,
                },
            )
        except Exception as exc:
            return ToolResult.fail(call.id, f"Search failed: {redact(exc)}")

    async def glob(self, call: ToolCall, *, root: Path | None = None) -> ToolResult:
        workspace = (root or self.root).expanduser().resolve()
        return await asyncio.to_thread(self._glob, call, workspace)

    def _glob(self, call: ToolCall, workspace: Path) -> ToolResult:
        try:
            args = call.arguments_dict
            pattern = args.get("pattern")
            raw_path = args.get("path", ".")
            if (
                not isinstance(pattern, str)
                or not pattern
                or Path(pattern).is_absolute()
                or ".." in Path(pattern).parts
            ):
                raise ValueError("Missing or unsafe 'pattern' argument")
            if not isinstance(raw_path, str):
                raise ValueError("path must be a string")
            root = (workspace / raw_path).resolve(strict=True)
            if not root.is_relative_to(workspace) or not root.is_dir():
                raise ValueError("Path escapes sandbox or is not a directory")
            matches: list[str] = []
            truncated = False
            for path in sorted(root.glob(pattern)):
                if path.is_symlink() or not path.is_file():
                    continue
                resolved = path.resolve()
                if not resolved.is_relative_to(workspace):
                    continue
                matches.append(str(resolved.relative_to(workspace)))
                if len(matches) >= self.max_results:
                    truncated = True
                    break
            return ToolResult.ok(
                call.id,
                {"files": matches, "count": len(matches), "truncated": truncated},
            )
        except Exception as exc:
            return ToolResult.fail(call.id, f"Glob failed: {redact(exc)}")

    @staticmethod
    def register(manager: PluginManager, sandbox_root: str | Path) -> SearchPlugin:
        plugin = SearchPlugin(sandbox_root)
        schema = {
            "pattern": {"type": "string"},
            "path": {"type": "string"},
            "regex": {"type": "boolean"},
        }
        for name in ("search", "grep"):
            manager.register(
                name,
                lambda context, call: plugin.search(call, root=context.workspace),
                definition=tool_definition(
                    name, "Search text in workspace files", schema, required=("pattern",)
                ),
            )
        manager.register(
            "glob_files",
            lambda context, call: plugin.glob(call, root=context.workspace),
            definition=tool_definition(
                "glob_files",
                "List sandbox files matching a glob pattern",
                {"pattern": {"type": "string"}, "path": {"type": "string"}},
                required=("pattern",),
            ),
        )
        return plugin
