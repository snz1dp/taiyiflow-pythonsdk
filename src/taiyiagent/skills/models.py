from __future__ import annotations

import os
import re
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import TypeAlias

JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]


def skill_key(value: str | None) -> str:
    return "" if value is None else value.strip().lower()


class SkillSource(StrEnum):
    WORKSPACE = "workspace"
    USER = "user"
    COMPAT = "compat"
    BUNDLED = "bundled"
    PLATFORM = "platform"


@dataclass(frozen=True, slots=True)
class SkillLimits:
    max_skills: int = 100
    max_depth: int = 8
    max_entries: int = 2000
    max_file_bytes: int = 1_000_000
    max_total_bytes: int = 4_000_000
    max_body_chars: int = 200_000
    max_files_per_skill: int = 32


@dataclass(frozen=True, slots=True)
class ClientSkillFile:
    path: str
    source: str | None = None
    description: str | None = None

    def as_wire(self) -> JsonObject:
        return _without_none(
            {"path": self.path, "source": self.source, "description": self.description}
        )


@dataclass(frozen=True, slots=True)
class SkillDefinition:
    name: str
    body: str
    source: SkillSource
    id: str | None = None
    slug: str | None = None
    description: str | None = None
    homepage: str | None = None
    category: str | None = None
    prerequisites: tuple[str, ...] = ()
    files: tuple[ClientSkillFile, ...] = ()
    metadata: JsonValue = None
    summary: str | None = None
    skill_file: Path | None = None
    skill_dir: Path | None = None
    relative_dir: str | None = None
    enabled: bool = True
    version: str | None = None
    elevated_risk: bool = False

    def __post_init__(self) -> None:
        if not self.slug:
            object.__setattr__(self, "slug", self.name)
        if not self.summary:
            object.__setattr__(self, "summary", summarize_skill(self.body))
        object.__setattr__(self, "metadata", deepcopy(self.metadata))

    @property
    def key(self) -> str:
        return skill_key(self.slug)

    def matches(self, candidate: str) -> bool:
        key = skill_key(candidate)
        return key in {self.key, skill_key(self.name), skill_key(self.relative_dir)}

    def as_client(self, *, summary_only: bool) -> ClientSkillDefinition:
        return ClientSkillDefinition(
            name=self.name,
            slug=self.slug,
            description=self.description,
            homepage=self.homepage,
            category=self.category,
            prerequisites=self.prerequisites,
            files=self.files,
            metadata=deepcopy(self.metadata),
            summary=self.summary,
            content=self.summary if summary_only else self.body,
            summary_only=summary_only,
            skill_dir=self.relative_dir,
            root_dir=None,
            source=self.source.value,
            relative_dir=self.relative_dir,
            enabled=self.enabled,
            version=self.version,
        )

    def missing_environment(self, environ: Mapping[str, str] | None = None) -> tuple[str, ...]:
        environment = os.environ if environ is None else environ
        if not isinstance(self.metadata, dict):
            return ()
        required = self.metadata.get("requires_env")
        if not isinstance(required, list):
            return ()
        return tuple(
            item
            for item in required
            if isinstance(item, str)
            and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", item)
            and item not in environment
        )


@dataclass(frozen=True, slots=True)
class ClientSkillDefinition:
    name: str
    slug: str | None = None
    description: str | None = None
    homepage: str | None = None
    category: str | None = None
    prerequisites: tuple[str, ...] = ()
    files: tuple[ClientSkillFile, ...] = ()
    metadata: JsonValue = None
    summary: str | None = None
    content: str | None = None
    summary_only: bool = False
    skill_dir: str | None = None
    root_dir: str | None = None
    source: str | None = None
    relative_dir: str | None = None
    enabled: bool = True
    version: str | None = None

    def as_wire(self) -> JsonObject:
        return _without_none(
            {
                "name": self.name,
                "slug": self.slug,
                "description": self.description,
                "homepage": self.homepage,
                "category": self.category,
                "prerequisites": list(self.prerequisites),
                "files": [item.as_wire() for item in self.files],
                "metadata": deepcopy(self.metadata),
                "summary": self.summary,
                "content": self.content,
                "is_summary": self.summary_only,
                "skill_dir": self.skill_dir,
                "root_dir": self.root_dir,
                "source": self.source,
                "relative_dir": self.relative_dir,
                "enabled": self.enabled,
                "version": self.version,
            }
        )


@dataclass(frozen=True, slots=True)
class ClientSkillPackage:
    package_name: str
    skills: tuple[ClientSkillDefinition, ...]
    root_dir: str | None = None
    bundled: bool = False

    def as_wire(self) -> JsonObject:
        return _without_none(
            {
                "package_name": self.package_name,
                "root_dir": self.root_dir,
                "is_bundled": self.bundled,
                "skills": [item.as_wire() for item in self.skills],
            }
        )


@dataclass(frozen=True, slots=True)
class SkillQuery:
    flow_id: str
    session_id: str
    request_mode: str | None = None
    principal: str | None = None
    available_tools: frozenset[str] | None = None

    def __post_init__(self) -> None:
        mode = self.request_mode if self.request_mode in {"ask", "plan"} else "agent"
        object.__setattr__(self, "request_mode", mode)
        if self.principal is None:
            object.__setattr__(self, "principal", f"{self.flow_id}\0{self.session_id}")


@dataclass(frozen=True, slots=True)
class SkillSelection:
    requested_names: tuple[str, ...] = ()
    default_names: tuple[str, ...] = ()
    local_supply_enabled: bool = True
    allowed_skills: frozenset[str] | None = None

    def __post_init__(self) -> None:
        requested = tuple(
            dict.fromkeys(item for item in self.requested_names if item and item.strip())
        )
        defaults = tuple(
            dict.fromkeys(item for item in self.default_names if item and item.strip())
        )
        object.__setattr__(self, "requested_names", requested)
        object.__setattr__(self, "default_names", defaults)
        if self.allowed_skills is not None:
            object.__setattr__(
                self, "allowed_skills", frozenset(skill_key(item) for item in self.allowed_skills)
            )


@dataclass(frozen=True, slots=True)
class SkillSupplyPolicy:
    local_enabled: bool = True
    platform_enabled: bool = True
    tolerate_platform_failure: bool = True

    @classmethod
    def local_only(cls) -> SkillSupplyPolicy:
        return cls(platform_enabled=False)


@dataclass(frozen=True, slots=True)
class SkillSupplySnapshot:
    packages: tuple[ClientSkillPackage, ...] = ()
    requested_skill_names: tuple[str, ...] = ()
    diagnostics: tuple[str, ...] = ()

    def as_wire(self) -> list[JsonObject]:
        return [package.as_wire() for package in self.packages]

    def subset(self, allowed_names: Iterable[str]) -> SkillSupplySnapshot:
        allowed = {skill_key(item) for item in allowed_names}
        packages: list[ClientSkillPackage] = []
        for package in self.packages:
            skills = tuple(
                skill
                for skill in package.skills
                if skill_key(skill.slug) in allowed or skill_key(skill.name) in allowed
            )
            if skills:
                packages.append(replace(package, skills=skills))
        requested = tuple(name for name in self.requested_skill_names if skill_key(name) in allowed)
        return SkillSupplySnapshot(tuple(packages), requested, self.diagnostics)


EMPTY_SKILL_SUPPLY = SkillSupplySnapshot()


def summarize_skill(body: str) -> str:
    for raw_line in body.splitlines():
        line = raw_line.strip().removeprefix("#").strip()
        if line:
            return line[:500]
    return ""


def validate_skill_packages(packages: tuple[ClientSkillPackage, ...]) -> None:
    if len(packages) > 50:
        raise ValueError("Too many client skill packages")
    skill_count = 0
    characters = 0
    for package in packages:
        if not package.package_name.strip() or package.root_dir is not None or not package.skills:
            raise ValueError("Invalid client skill package")
        for skill in package.skills:
            skill_count += 1
            if skill_count > 100 or not skill.name.strip() or skill.root_dir is not None:
                raise ValueError("Invalid or excessive client skill content")
            _validate_relative(skill.skill_dir)
            _validate_relative(skill.relative_dir)
            characters += sum(
                len(value or "") for value in (skill.content, skill.summary, skill.description)
            )
            if characters > 4_000_000 or len(skill.files) > 32:
                raise ValueError("Client skill content exceeds limits")
            for file in skill.files:
                _validate_relative(file.path)


def validate_skill_package_payloads(packages: list[dict[str, object]]) -> None:
    if len(packages) > 50:
        raise ValueError("Too many client skill packages")
    skill_count = 0
    characters = 0
    for package in packages:
        package_name = package.get("package_name")
        if (
            not isinstance(package_name, str)
            or not package_name.strip()
            or package.get("root_dir") is not None
        ):
            raise ValueError("Invalid client skill package")
        skills = package.get("skills")
        if not isinstance(skills, list) or not skills:
            raise ValueError("Invalid client skill package")
        for value in skills:
            if not isinstance(value, dict):
                raise ValueError("Invalid or excessive client skill content")
            skill_count += 1
            name = value.get("name")
            if (
                skill_count > 100
                or not isinstance(name, str)
                or not name.strip()
                or value.get("path") is not None
                or value.get("root_dir") is not None
            ):
                raise ValueError("Invalid or excessive client skill content")
            for field_name in ("skill_dir", "relative_dir"):
                field_value = value.get(field_name)
                if field_value is not None and not isinstance(field_value, str):
                    raise ValueError("Invalid skill path")
                _validate_relative(field_value)
            characters += sum(
                len(item)
                for item in (value.get("content"), value.get("summary"), value.get("description"))
                if isinstance(item, str)
            )
            files = value.get("files", [])
            if not isinstance(files, list) or characters > 4_000_000 or len(files) > 32:
                raise ValueError("Client skill content exceeds limits")
            for file in files:
                if not isinstance(file, dict) or not isinstance(file.get("path"), str):
                    raise ValueError("Invalid skill path")
                _validate_relative(file["path"])


def _validate_relative(value: str | None) -> None:
    if not value:
        return
    if "\0" in value or value.startswith(("/", "\\")) or re.match(r"^[A-Za-z]:", value):
        raise ValueError("Absolute skill paths are forbidden")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or len(path.parts) > 8:
        raise ValueError("Invalid skill path")


def _without_none(value: Mapping[str, JsonValue]) -> JsonObject:
    return {key: deepcopy(item) for key, item in value.items() if item is not None}
