from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from importlib import resources
from pathlib import Path
from typing import Protocol, cast

from ..transport.capabilities import SkillClient
from .models import (
    JsonObject,
    JsonValue,
    SkillDefinition,
    SkillLimits,
    SkillQuery,
    SkillSource,
)
from .parser import SkillDocumentParser

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DiscoveredSkills:
    skills: tuple[SkillDefinition, ...]
    diagnostics: tuple[str, ...] = ()


class SkillProvider(Protocol):
    @property
    def source(self) -> SkillSource: ...

    async def discover(self, query: SkillQuery) -> DiscoveredSkills: ...

    async def load(self, skill: SkillDefinition, query: SkillQuery) -> SkillDefinition: ...


class FileSystemSkillProvider:
    def __init__(
        self, root: str | Path, source: SkillSource, limits: SkillLimits | None = None
    ) -> None:
        if source in {SkillSource.BUNDLED, SkillSource.PLATFORM}:
            raise ValueError("Filesystem skill source must be workspace, user, or compat")
        self._root = Path(root)
        self._source = source
        self._limits = limits or SkillLimits()
        self._parser = SkillDocumentParser()

    @property
    def source(self) -> SkillSource:
        return self._source

    async def discover(self, query: SkillQuery) -> DiscoveredSkills:
        return await asyncio.to_thread(self._discover_sync)

    async def load(self, skill: SkillDefinition, query: SkillQuery) -> SkillDefinition:
        if skill.skill_file is None:
            return skill
        return await asyncio.to_thread(self._load_sync, skill)

    def _discover_sync(self) -> DiscoveredSkills:
        result: list[SkillDefinition] = []
        diagnostics: list[str] = []
        if not self._root.is_dir() or self._root.is_symlink():
            return DiscoveredSkills(())
        real_root = self._root.resolve()
        count = 0
        total_bytes = 0
        for root, directories, files in os.walk(real_root, followlinks=False):
            root_path = Path(root)
            relative = root_path.relative_to(real_root)
            depth = len(relative.parts)
            directories[:] = sorted(
                name
                for name in directories
                if not name.startswith(".")
                and name != "node_modules"
                and not (root_path / name).is_symlink()
            )
            if depth >= self._limits.max_depth:
                directories[:] = []
            for filename in sorted(files):
                count += 1
                if count > self._limits.max_entries:
                    diagnostics.append(f"{self._source.value}: entry limit reached")
                    return DiscoveredSkills(
                        tuple(sorted(result[: self._limits.max_skills], key=lambda item: item.key)),
                        tuple(diagnostics),
                    )
                if filename.lower() != "skill.md":
                    continue
                path = root_path / filename
                if path.is_symlink():
                    continue
                details = path.lstat()
                total_bytes += details.st_size
                if total_bytes > self._limits.max_total_bytes:
                    continue
                relative_dir = relative.as_posix() if relative.parts else ""
                try:
                    definition = self._read(path, relative_dir)
                except (OSError, UnicodeError, ValueError):
                    logger.warning("Ignoring invalid skill document at %s", relative_dir)
                    diagnostics.append(f"{self._source.value}: invalid skill {relative_dir}")
                    continue
                result.append(definition)
                if len(result) >= self._limits.max_skills:
                    return DiscoveredSkills(
                        tuple(sorted(result, key=lambda item: item.key)), tuple(diagnostics)
                    )
        return DiscoveredSkills(
            tuple(sorted(result, key=lambda item: item.key)), tuple(diagnostics)
        )

    def _load_sync(self, skill: SkillDefinition) -> SkillDefinition:
        if skill.skill_file is None:
            return skill
        return self._read(skill.skill_file, skill.relative_dir or skill.key)

    def _read(self, path: Path, relative_dir: str) -> SkillDefinition:
        details = path.lstat()
        if not path.is_file() or path.is_symlink() or details.st_size > self._limits.max_file_bytes:
            raise ValueError("Invalid skill document")
        real_path = path.resolve(strict=True)
        if not real_path.is_relative_to(self._root.resolve(strict=True)):
            raise ValueError("Skill document escapes its source root")
        markdown = real_path.read_bytes().decode("utf-8")
        return self._parser.parse(
            markdown,
            skill_file=real_path,
            skill_dir=real_path.parent,
            source=self._source,
            relative_dir=relative_dir,
            limits=self._limits,
        )


class BundledSkillProvider:
    def __init__(
        self,
        package: str,
        resource_roots: tuple[str, ...],
        limits: SkillLimits | None = None,
    ) -> None:
        self._package = package
        self._resource_roots = resource_roots
        self._limits = limits or SkillLimits()
        self._parser = SkillDocumentParser()

    @property
    def source(self) -> SkillSource:
        return SkillSource.BUNDLED

    async def discover(self, query: SkillQuery) -> DiscoveredSkills:
        return await asyncio.to_thread(self._discover_sync)

    async def load(self, skill: SkillDefinition, query: SkillQuery) -> SkillDefinition:
        return skill

    def _discover_sync(self) -> DiscoveredSkills:
        result: list[SkillDefinition] = []
        diagnostics: list[str] = []
        for resource_root in self._resource_roots:
            root = resources.files(self._package).joinpath(resource_root)
            candidates = [root] if root.joinpath("SKILL.md").is_file() else list(root.iterdir())
            for directory in sorted(candidates, key=lambda item: item.name):
                document = directory.joinpath("SKILL.md")
                if not document.is_file():
                    continue
                try:
                    data = document.read_bytes()
                    if len(data) > self._limits.max_file_bytes:
                        raise ValueError("Bundled skill is too large")
                    result.append(
                        self._parser.parse(
                            data.decode("utf-8"),
                            source=SkillSource.BUNDLED,
                            relative_dir=directory.name,
                            limits=self._limits,
                        )
                    )
                except (OSError, UnicodeError, ValueError):
                    diagnostics.append(f"bundled: invalid skill {directory.name}")
                if len(result) >= self._limits.max_skills:
                    return DiscoveredSkills(tuple(result), tuple(diagnostics))
        return DiscoveredSkills(tuple(result), tuple(diagnostics))


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    skill: SkillDefinition
    expires_at: float


class PlatformSkillProvider:
    def __init__(
        self,
        client: SkillClient,
        limits: SkillLimits | None = None,
        *,
        ttl_seconds: float = 300,
        cache_size: int = 64,
    ) -> None:
        self._client = client
        self._limits = limits or SkillLimits()
        self._ttl_seconds = ttl_seconds
        self._cache_size = cache_size
        self._parser = SkillDocumentParser()
        self._cache: OrderedDict[tuple[str, str, str, str, str | None], _CacheEntry] = OrderedDict()
        self._loading: dict[
            tuple[str, str, str, str, str | None], asyncio.Task[SkillDefinition]
        ] = {}
        self._lock = asyncio.Lock()

    @property
    def source(self) -> SkillSource:
        return SkillSource.PLATFORM

    async def discover(self, query: SkillQuery) -> DiscoveredSkills:
        response = await self._client.list(context=self._context(query))
        values: object
        if isinstance(response, list):
            values = response
        elif isinstance(response, dict):
            values = (
                response.get("data")
                if isinstance(response.get("data"), list)
                else response.get("skills")
            )
        else:
            values = None
        if not isinstance(values, list):
            return DiscoveredSkills(())
        result: list[SkillDefinition] = []
        for item in values[: self._limits.max_skills]:
            if not isinstance(item, dict):
                continue
            skill_id = _text(item, "id")
            slug = _text(item, "name")
            if skill_id is None or slug is None:
                continue
            display_name = _text(item, "display_name")
            result.append(
                SkillDefinition(
                    id=skill_id,
                    name=display_name or slug,
                    slug=slug,
                    description=_text(item, "description"),
                    category=_text(item, "category"),
                    metadata=cast(JsonValue, item.get("metadata")),
                    summary=_text(item, "summary"),
                    body="",
                    source=SkillSource.PLATFORM,
                    relative_dir=slug,
                    enabled=item.get("enabled", True) is not False,
                    version=_text(item, "version"),
                    elevated_risk=(_text(item, "risk_level") or "").lower() == "elevated",
                )
            )
        return DiscoveredSkills(tuple(result))

    async def load(self, skill: SkillDefinition, query: SkillQuery) -> SkillDefinition:
        if skill.source != SkillSource.PLATFORM or skill.id is None:
            return skill
        key = (query.principal or "", query.flow_id, query.session_id, skill.id, skill.version)
        async with self._lock:
            hit = self._cache.get(key)
            now = time.monotonic()
            if hit is not None and hit.expires_at > now:
                self._cache.move_to_end(key)
                return hit.skill
            self._cache.pop(key, None)
            pending = self._loading.get(key)
            if pending is None:
                pending = asyncio.create_task(self._load_and_cache(key, skill, query))
                self._loading[key] = pending
                pending.add_done_callback(self._loading_done(key))
        return await asyncio.shield(pending)

    def _loading_done(
        self, key: tuple[str, str, str, str, str | None]
    ) -> Callable[[asyncio.Task[SkillDefinition]], None]:
        def done(task: asyncio.Task[SkillDefinition]) -> None:
            self._loading.pop(key, None)
            if not task.cancelled():
                task.exception()

        return done

    async def _load_and_cache(
        self,
        key: tuple[str, str, str, str, str | None],
        skill: SkillDefinition,
        query: SkillQuery,
    ) -> SkillDefinition:
        context = {**self._context(query), "format": "content"}
        response = await self._client.get_content(skill.id or "", context=context)
        markdown = _text(response, "content") if isinstance(response, dict) else None
        if (
            markdown is None
            and isinstance(response, dict)
            and isinstance(response.get("data"), dict)
        ):
            markdown = _text(cast(JsonObject, response["data"]), "content")
        if markdown is None or not markdown.strip():
            raise ValueError("Platform skill content is empty")
        loaded = self._parser.parse(
            markdown,
            source=SkillSource.PLATFORM,
            relative_dir=skill.relative_dir,
            limits=self._limits,
        )
        merged = replace(
            loaded,
            id=skill.id,
            slug=skill.slug,
            summary=skill.summary,
            enabled=skill.enabled,
            version=skill.version,
            elevated_risk=skill.elevated_risk,
        )
        async with self._lock:
            while len(self._cache) >= self._cache_size:
                self._cache.popitem(last=False)
            self._cache[key] = _CacheEntry(merged, time.monotonic() + self._ttl_seconds)
        return merged

    @staticmethod
    def _context(query: SkillQuery) -> dict[str, object]:
        context: dict[str, object] = {
            "flow_id": query.flow_id,
            "session_id": query.session_id,
            "request_type": query.request_mode,
            "allow_elevated_risk": False,
        }
        if query.available_tools is not None:
            context["available_tools"] = ",".join(sorted(query.available_tools))
        return context


def _text(value: Mapping[str, JsonValue], field: str) -> str | None:
    item = value.get(field)
    if item is None or isinstance(item, dict | list):
        return None
    text = str(item)
    return text if text.strip() else None
