from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..models import ToolCall, ToolResult
from ..plugins.definitions import tool_definition
from ..plugins.manager import PluginManager, ToolExecutionContext
from .archives import read_declared_skill_file
from .manager import SkillManager
from .models import SkillQuery, SkillSelection, SkillSource
from .supplier import SkillSupplier

_READ_ONLY = {SkillSource.COMPAT, SkillSource.BUNDLED, SkillSource.PLATFORM}


class SkillTools:
    def __init__(
        self,
        supplier: SkillSupplier,
        manager: SkillManager,
        default_names: tuple[str, ...] = (),
    ) -> None:
        self.supplier = supplier
        self.manager = manager
        self.default_names = default_names

    def register(self, plugins: PluginManager) -> None:
        plugins.register(
            "skill_list",
            self.list,
            definition=tool_definition(
                "skill_list", "List skills visible to the current session", {}
            ),
        )
        plugins.register(
            "skill_fetch",
            self.fetch,
            definition=tool_definition(
                "skill_fetch",
                "Fetch visible skill content or a declared attachment",
                {"name": {"type": "string"}, "file": {"type": "string"}},
                required=("name",),
            ),
        )
        plugins.register(
            "skill_create",
            self.create,
            definition=tool_definition(
                "skill_create",
                "Create a local skill in an allowed scope",
                {
                    "slug": {"type": "string"},
                    "content": {"type": "string"},
                    "scope": {"type": "string", "enum": ["workspace", "user"]},
                },
                required=("slug", "content"),
            ),
        )
        plugins.register(
            "skill_upload",
            self.upload,
            definition=tool_definition(
                "skill_upload",
                "Upload a validated local skill archive to the platform",
                {"name": {"type": "string"}},
                required=("name",),
            ),
        )
        plugins.register(
            "skill_delete",
            self.delete,
            definition=tool_definition(
                "skill_delete",
                "Delete an allowed local or platform skill",
                {"name": {"type": "string"}},
                required=("name",),
            ),
        )

    async def list(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            skills = await self.supplier.visible_skills(
                self._query(context), self._selection(context)
            )
            values = [
                {
                    "name": skill.name,
                    "slug": skill.slug,
                    "description": skill.description,
                    "summary": skill.summary,
                    "source": skill.source.value,
                    "version": skill.version,
                    "read_only": skill.source in _READ_ONLY,
                    "missing_env": list(skill.missing_environment()),
                }
                for skill in skills
            ]
            return ToolResult.ok(call.id, {"skills": values})
        except Exception as exc:
            return ToolResult.fail(call.id, _safe_message(exc))

    async def fetch(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            name = _required(call.arguments_dict, "name")
            skill = await self.supplier.fetch(self._query(context), self._selection(context), name)
            if skill is None:
                return ToolResult.fail(call.id, "Unknown or unavailable skill")
            result: dict[str, Any] = {
                "name": skill.name,
                "slug": skill.slug,
                "source": skill.source.value,
            }
            requested_file = call.arguments_dict.get("file", "")
            if not isinstance(requested_file, str):
                raise ValueError("Invalid skill file")
            if not requested_file.strip():
                result.update({"content": skill.body, "is_summary": False})
            else:
                result.update(
                    {
                        "file": requested_file,
                        "content": read_declared_skill_file(skill, requested_file),
                    }
                )
            return ToolResult.ok(call.id, result)
        except Exception as exc:
            return ToolResult.fail(call.id, _safe_message(exc))

    async def create(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            scope_name = call.arguments_dict.get("scope", "workspace")
            if scope_name not in {"workspace", "user"}:
                raise ValueError("Invalid skill scope")
            scope = SkillSource.USER if scope_name == "user" else SkillSource.WORKSPACE
            created = await self.manager.create(
                scope,
                _required(call.arguments_dict, "slug"),
                _required(call.arguments_dict, "content"),
            )
            return ToolResult.ok(
                call.id, {"created": True, "name": created.slug, "scope": scope.value}
            )
        except Exception as exc:
            return ToolResult.fail(call.id, _safe_message(exc))

    async def upload(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            name = _required(call.arguments_dict, "name")
            skill = await self.supplier.fetch(self._query(context), self._selection(context), name)
            if skill is None or skill.source in _READ_ONLY or skill.skill_dir is None:
                raise ValueError("Only mutable local skills can be uploaded")
            response = await self.manager.upload(skill)
            result: dict[str, Any] = {"uploaded": True, "name": skill.slug}
            if isinstance(response, dict) and isinstance(response.get("id"), str):
                result["id"] = response["id"]
            return ToolResult.ok(call.id, result)
        except Exception as exc:
            return ToolResult.fail(call.id, _safe_message(exc))

    async def delete(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        try:
            name = _required(call.arguments_dict, "name")
            skill = await self.supplier.find(self._query(context), self._selection(context), name)
            if skill is None:
                return ToolResult.fail(call.id, "Unknown or unavailable skill")
            await self.manager.delete(skill)
            return ToolResult.ok(
                call.id, {"deleted": True, "name": skill.slug, "source": skill.source.value}
            )
        except Exception as exc:
            return ToolResult.fail(call.id, _safe_message(exc))

    @staticmethod
    def _query(context: ToolExecutionContext) -> SkillQuery:
        if not context.flow_id:
            raise ValueError("Skill flow context is unavailable")
        return SkillQuery(
            flow_id=context.flow_id,
            session_id=context.session_id,
            request_mode=context.request_mode,
            principal=f"{context.flow_id}\0{context.session_id}",
            available_tools=context.allowed_tools,
        )

    def _selection(self, context: ToolExecutionContext) -> SkillSelection:
        requested: tuple[str, ...] = ()
        if isinstance(context.capabilities, Mapping):
            values = context.capabilities.get("requested_skill_names", ())
            if isinstance(values, list | tuple):
                requested = tuple(item for item in values if isinstance(item, str))
        return SkillSelection(requested_names=requested, default_names=self.default_names)


def _required(arguments: Mapping[str, Any], field: str) -> str:
    value = arguments.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing '{field}'")
    return value.strip()


def _safe_message(exception: Exception) -> str:
    if not isinstance(exception, ValueError):
        return "Skill operation failed"
    return str(exception) or "Invalid skill operation"
