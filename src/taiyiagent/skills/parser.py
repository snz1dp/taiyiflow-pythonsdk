from __future__ import annotations

import re
from pathlib import Path, PurePosixPath
from typing import Any, cast

import yaml

from .models import ClientSkillFile, JsonValue, SkillDefinition, SkillLimits, SkillSource

_FRONTMATTER = re.compile(r"\A---\r?\n(.*?)\r?\n---(?:\r?\n|$)", re.DOTALL)
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:")


class SkillDocumentParser:
    def parse(
        self,
        markdown: str,
        *,
        skill_file: Path | None = None,
        skill_dir: Path | None = None,
        source: SkillSource,
        relative_dir: str | None = None,
        limits: SkillLimits | None = None,
    ) -> SkillDefinition:
        limits = limits or SkillLimits()
        match = _FRONTMATTER.match(markdown)
        if match is None:
            raise ValueError("SKILL.md requires valid YAML frontmatter")
        try:
            frontmatter = yaml.safe_load(match.group(1))
        except yaml.YAMLError as exc:
            raise ValueError("Unable to parse SKILL.md frontmatter") from exc
        _validate_yaml(frontmatter)
        if not isinstance(frontmatter, dict):
            raise ValueError("SKILL.md frontmatter must be an object")
        name = _text(frontmatter, "name")
        if not name or not name.strip() or len(name) > 200:
            raise ValueError("SKILL.md frontmatter requires name")
        body = markdown[match.end() :]
        if not body.strip():
            raise ValueError("Skill body is empty")
        if len(body) > limits.max_body_chars:
            raise ValueError("Skill body is too large")
        metadata = _json_value(frontmatter.get("metadata"))
        return SkillDefinition(
            name=name,
            slug=_text(frontmatter, "slug"),
            description=_limited(_text(frontmatter, "description"), 4000),
            homepage=_text(frontmatter, "homepage"),
            category=_text(frontmatter, "category"),
            prerequisites=_strings(frontmatter.get("prerequisites"), "prerequisites"),
            files=_files(frontmatter.get("files"), limits.max_files_per_skill),
            metadata=metadata,
            body=body,
            skill_file=skill_file,
            skill_dir=skill_dir,
            source=source,
            relative_dir=relative_dir,
            enabled=_boolean(frontmatter.get("enabled", True)),
            version=_text(frontmatter, "version"),
        )


def safe_skill_relative_path(value: str) -> str:
    if (
        not value
        or not value.strip()
        or "\0" in value
        or value.startswith(("/", "\\"))
        or _WINDOWS_ABSOLUTE.match(value)
        or len(value) > 500
    ):
        raise ValueError("Invalid skill file path")
    normalized = value.replace("\\", "/")
    path = PurePosixPath(normalized)
    parts = path.parts
    if path.is_absolute() or len(parts) > 8 or ".." in parts:
        raise ValueError("Invalid skill file path")
    if any(part.startswith(".") for part in parts):
        raise ValueError("Invalid skill file path")
    return str(path)


def _files(value: object, maximum: int) -> tuple[ClientSkillFile, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or len(value) > maximum:
        raise ValueError("Invalid skill files")
    result: list[ClientSkillFile] = []
    for item in value:
        if isinstance(item, str):
            result.append(ClientSkillFile(safe_skill_relative_path(item)))
        elif isinstance(item, dict):
            path = item.get("path", item.get("source", ""))
            source = item.get("source")
            description = item.get("description")
            if not isinstance(path, str) or (source is not None and not isinstance(source, str)):
                raise ValueError("Invalid skill file declaration")
            if description is not None and not isinstance(description, str):
                raise ValueError("Invalid skill file declaration")
            result.append(
                ClientSkillFile(
                    safe_skill_relative_path(path),
                    source,
                    _limited(description, 1000),
                )
            )
        else:
            raise ValueError("Invalid skill file declaration")
    return tuple(result)


def _strings(value: object, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or len(value) > 32:
        raise ValueError(f"Invalid {field}")
    if any(not isinstance(item, str) or len(item) > 1000 for item in value):
        raise ValueError(f"Invalid {field}")
    return tuple(value)


def _validate_yaml(value: object, depth: int = 0, count: list[int] | None = None) -> None:
    count = [0] if count is None else count
    count[0] += 1
    if count[0] > 1000 or depth > 16:
        raise ValueError("YAML structure is too complex")
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("YAML keys must be strings")
            _validate_yaml(item, depth + 1, count)
    elif isinstance(value, list):
        for item in value:
            _validate_yaml(item, depth + 1, count)
    elif value is not None and not isinstance(value, str | int | float | bool):
        raise ValueError("YAML contains unsupported values")


def _json_value(value: object) -> JsonValue:
    _validate_yaml(value)
    return cast(JsonValue, value)


def _text(value: dict[str, Any], field: str) -> str | None:
    item = value.get(field)
    if item is None or isinstance(item, dict | list):
        return None
    if isinstance(item, bool):
        return str(item).lower()
    return str(item)


def _boolean(value: object) -> bool:
    return value if isinstance(value, bool) else True


def _limited(value: str | None, maximum: int) -> str | None:
    if value is not None and len(value) > maximum:
        raise ValueError("Skill text is too long")
    return value
