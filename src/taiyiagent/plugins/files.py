from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from pathlib import Path
from typing import Any

from ..errors import redact
from ..models import ToolCall, ToolResult
from .definitions import tool_definition
from .manager import PluginManager


class FilePlugin:
    def __init__(self, sandbox_root: str | Path, *, max_file_bytes: int = 1_000_000) -> None:
        if max_file_bytes <= 0:
            raise ValueError("max_file_bytes must be positive")
        self.root = Path(sandbox_root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_file_bytes = max_file_bytes

    async def read_file(self, call: ToolCall, *, root: Path | None = None) -> ToolResult:
        return await asyncio.to_thread(self._read_file, call, self._workspace(root))

    async def write_file(self, call: ToolCall, *, root: Path | None = None) -> ToolResult:
        return await asyncio.to_thread(self._write_file, call, self._workspace(root))

    async def edit_file(self, call: ToolCall, *, root: Path | None = None) -> ToolResult:
        return await asyncio.to_thread(self._edit_file, call, self._workspace(root))

    async def list_files(self, call: ToolCall, *, root: Path | None = None) -> ToolResult:
        return await asyncio.to_thread(self._list_files, call, self._workspace(root))

    def _workspace(self, root: Path | None) -> Path:
        if root is None:
            return self.root
        workspace = root.expanduser().resolve()
        workspace.mkdir(parents=True, exist_ok=True)
        return workspace

    def _read_file(self, call: ToolCall, root: Path) -> ToolResult:
        try:
            args = call.arguments_dict
            path = self._required_string(args, "path")
            target = self._resolve_existing(path, root)
            if not target.is_file():
                raise ValueError(f"Not a file: {path}")
            content = self._read_bytes(target)
            return ToolResult.ok(
                call.id,
                {
                    "path": str(target),
                    "content": content.decode("utf-8"),
                    "size": len(content),
                    "sha256": _sha256(content),
                },
            )
        except Exception as exc:
            return ToolResult.fail(call.id, f"Read failed: {redact(exc)}")

    def _write_file(self, call: ToolCall, root: Path) -> ToolResult:
        temporary: Path | None = None
        try:
            args = call.arguments_dict
            path = self._required_string(args, "path")
            content = self._required_string(args, "content", allow_empty=True).encode("utf-8")
            if len(content) > self.max_file_bytes:
                raise ValueError("File exceeds the configured size limit")
            target = self._resolve_writable(path, root)
            temporary = self._atomic_write(target, content)
            return ToolResult.ok(
                call.id,
                {
                    "path": str(target),
                    "written": True,
                    "size": len(content),
                    "sha256_after": _sha256(content),
                },
            )
        except Exception as exc:
            return ToolResult.fail(call.id, f"Write failed: {redact(exc)}")
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _edit_file(self, call: ToolCall, root: Path) -> ToolResult:
        temporary: Path | None = None
        try:
            args = call.arguments_dict
            path = self._required_string(args, "path")
            old_string = self._required_string(args, "old_string")
            new_string = self._required_string(args, "new_string", allow_empty=True)
            if not old_string:
                raise ValueError("old_string must not be empty")
            target = self._resolve_writable(path, root)
            before_bytes = self._read_bytes(target)
            before_hash = _sha256(before_bytes)
            expected = args.get("expected_sha256_before")
            if isinstance(expected, str) and expected and expected.lower() != before_hash:
                return _edit_conflict(
                    call,
                    target,
                    "STALE_CONTENT",
                    before_hash,
                    "Expected content SHA-256 does not match current content",
                )
            before = before_bytes.decode("utf-8")
            matched_old, matched_new, normalized = _adapt_line_endings(
                before, old_string, new_string
            )
            occurrences = before.count(matched_old)
            replace_all = args.get("replace_all") is True
            dry_run = args.get("dry_run") is True
            if occurrences == 0:
                return _edit_conflict(
                    call, target, "MATCH_NOT_FOUND", before_hash, "old_string was not found"
                )
            if occurrences > 1 and not replace_all:
                return _edit_conflict(
                    call,
                    target,
                    "AMBIGUOUS_MATCH",
                    before_hash,
                    "old_string is ambiguous; add context or set replace_all=true",
                )
            after = before.replace(matched_old, matched_new, -1 if replace_all else 1)
            after_bytes = after.encode("utf-8")
            if len(after_bytes) > self.max_file_bytes:
                raise ValueError("File exceeds the configured size limit")
            if not dry_run and after != before:
                temporary = self._atomic_write(target, after_bytes, before_hash)
            result: dict[str, Any] = {
                "status": "applied",
                "path": str(target),
                "dry_run": dry_run,
                "replacements": occurrences if replace_all else 1,
                "occurrences": occurrences,
                "replace_all": replace_all,
                "bytes_before": len(before_bytes),
                "bytes_after": len(after_bytes),
                "sha256_before": before_hash,
                "sha256_after": _sha256(after_bytes),
                "changed": after != before,
            }
            if normalized:
                result["normalized_old_string_match"] = True
            return ToolResult.ok(call.id, result)
        except Exception as exc:
            return ToolResult.fail(call.id, f"Edit failed: {redact(exc)}")
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _list_files(self, call: ToolCall, root: Path) -> ToolResult:
        try:
            args = call.arguments_dict
            raw_path = args.get("path", ".")
            if not isinstance(raw_path, str):
                raise ValueError("path must be a string")
            target = self._resolve_existing(raw_path, root)
            if not target.is_dir():
                raise ValueError(f"Not a directory: {raw_path}")
            files = [
                {
                    "name": item.name,
                    "directory": item.is_dir(),
                    "size": item.stat().st_size if item.is_file() else 0,
                }
                for item in sorted(target.iterdir(), key=lambda value: value.name)
            ]
            return ToolResult.ok(call.id, {"path": str(target), "files": files})
        except Exception as exc:
            return ToolResult.fail(call.id, f"List failed: {redact(exc)}")

    def _resolve_existing(self, raw_path: str, root: Path) -> Path:
        candidate = (root / raw_path).resolve(strict=True)
        self._require_inside(candidate, raw_path, root)
        return candidate

    def _resolve_writable(self, raw_path: str, root: Path) -> Path:
        candidate = Path(os.path.normpath(root / raw_path))
        self._require_inside(candidate, raw_path, root)
        if candidate == root:
            raise ValueError("Path does not identify a file")
        ancestor = candidate.parent
        while not ancestor.exists():
            if ancestor == ancestor.parent:
                raise ValueError("Path has no existing sandbox ancestor")
            ancestor = ancestor.parent
        self._require_inside(ancestor.resolve(strict=True), raw_path, root)
        candidate.parent.mkdir(parents=True, exist_ok=True)
        real_parent = candidate.parent.resolve(strict=True)
        self._require_inside(real_parent, raw_path, root)
        target = real_parent / candidate.name
        if target.is_symlink():
            raise ValueError("Symbolic link writes are not allowed")
        if target.exists() and not target.is_file():
            raise ValueError("Write target is not a regular file")
        return target

    def _require_inside(self, candidate: Path, raw_path: str, root: Path) -> None:
        if not candidate.is_relative_to(root):
            raise ValueError(f"Path escapes sandbox: {raw_path}")

    def _read_bytes(self, path: Path) -> bytes:
        if path.stat().st_size > self.max_file_bytes:
            raise ValueError("File exceeds the configured size limit")
        return path.read_bytes()

    def _atomic_write(
        self, target: Path, content: bytes, expected_before_hash: str | None = None
    ) -> Path:
        descriptor, temporary_name = tempfile.mkstemp(prefix=".taiyiagent-", dir=target.parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
            os.chmod(temporary, target.stat().st_mode if target.exists() else 0o600)
            if expected_before_hash is not None:
                current_hash = _sha256(target.read_bytes()) if target.exists() else None
                if current_hash != expected_before_hash:
                    raise ValueError(
                        "File content changed before the atomic edit could be committed"
                    )
            if target.is_symlink() or not temporary.parent.samefile(target.parent):
                raise ValueError("Atomic write target failed sandbox validation")
            os.replace(temporary, target)
            return temporary
        except Exception:
            temporary.unlink(missing_ok=True)
            raise

    @staticmethod
    def _required_string(args: dict[str, Any], name: str, *, allow_empty: bool = False) -> str:
        value = args.get(name)
        if not isinstance(value, str) or (not allow_empty and not value):
            raise ValueError(f"Missing or invalid '{name}' argument")
        return value

    @staticmethod
    def register(manager: PluginManager, sandbox_root: str | Path) -> FilePlugin:
        plugin = FilePlugin(sandbox_root)
        path_schema = {"path": {"type": "string"}}
        entries = (
            (
                "read_file",
                "Read a UTF-8 file inside the workspace",
                path_schema,
                ("path",),
                lambda context, call: plugin.read_file(call, root=context.workspace),
            ),
            (
                "write_file",
                "Write a UTF-8 file inside the workspace",
                {"path": {"type": "string"}, "content": {"type": "string"}},
                ("path", "content"),
                lambda context, call: plugin.write_file(call, root=context.workspace),
            ),
            (
                "file_edit",
                "Edit a UTF-8 file with exact string replacement",
                {
                    "path": {"type": "string"},
                    "old_string": {"type": "string"},
                    "new_string": {"type": "string"},
                    "expected_sha256_before": {"type": "string"},
                    "dry_run": {"type": "boolean"},
                    "replace_all": {"type": "boolean"},
                },
                ("path", "old_string", "new_string"),
                lambda context, call: plugin.edit_file(call, root=context.workspace),
            ),
            (
                "list_files",
                "List files inside the workspace",
                path_schema,
                (),
                lambda context, call: plugin.list_files(call, root=context.workspace),
            ),
        )
        for name, description, properties, required, handler in entries:
            manager.register(
                name,
                handler,
                definition=tool_definition(name, description, properties, required=required),
            )
        return plugin


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _edit_conflict(
    call: ToolCall, path: Path, reason: str, current_hash: str, message: str
) -> ToolResult:
    result = ToolResult.fail(call.id, message)
    result.result = {
        "status": "conflict",
        "reason": reason,
        "path": str(path),
        "sha256_before": current_hash,
    }
    return result


def _adapt_line_endings(content: str, old: str, new: str) -> tuple[str, str, bool]:
    if old in content:
        return old, new, False
    normalized_old = old.replace("\r\n", "\n")
    normalized_new = new.replace("\r\n", "\n")
    if normalized_old != old and normalized_old in content:
        return normalized_old, normalized_new, True
    crlf_old = normalized_old.replace("\n", "\r\n")
    if crlf_old != old and crlf_old in content:
        return crlf_old, normalized_new.replace("\n", "\r\n"), True
    return old, new, False
