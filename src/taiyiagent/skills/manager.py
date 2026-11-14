from __future__ import annotations

import asyncio
import inspect
import shutil
import uuid
from collections.abc import Awaitable, Callable
from enum import StrEnum
from pathlib import Path

from ..transport.capabilities import SkillClient
from .archives import create_skill_archive, inspect_skill_archive, validate_skill_slug
from .models import JsonValue, SkillDefinition, SkillLimits, SkillSource
from .parser import SkillDocumentParser

_READ_ONLY = {SkillSource.COMPAT, SkillSource.BUNDLED}


class SkillManager:
    def __init__(
        self,
        workspace_skills: str | Path | None,
        user_skills: str | Path | None,
        client: SkillClient | None = None,
    ) -> None:
        self.workspace_skills = (
            None if workspace_skills is None else Path(workspace_skills).absolute()
        )
        self.user_skills = None if user_skills is None else Path(user_skills).absolute()
        self.client = client
        self._parser = SkillDocumentParser()

    async def create(self, scope: SkillSource, slug: str, markdown: str) -> SkillDefinition:
        if scope not in {SkillSource.WORKSPACE, SkillSource.USER}:
            raise ValueError("Skills can only be created in workspace or user scope")
        validate_skill_slug(slug)
        root = self.workspace_skills if scope == SkillSource.WORKSPACE else self.user_skills
        if root is None:
            raise ValueError("Requested skill scope is not configured")
        parsed = self._parser.parse(markdown, source=scope, relative_dir=slug)
        if parsed.name != slug:
            raise ValueError("Frontmatter name must match skill slug")
        return await asyncio.to_thread(self._create_sync, root, scope, slug, markdown)

    def _create_sync(
        self, root: Path, scope: SkillSource, slug: str, markdown: str
    ) -> SkillDefinition:
        skill_dir = root / slug
        if skill_dir.exists() or skill_dir.is_symlink():
            raise ValueError("Skill directory already exists or is invalid")
        root.mkdir(parents=True, exist_ok=True)
        temporary = root / f".install-{uuid.uuid4().hex}"
        staging = temporary / slug
        staging.mkdir(parents=True)
        try:
            (staging / "SKILL.md").write_text(markdown, encoding="utf-8")
            if skill_dir.exists() or skill_dir.is_symlink():
                raise ValueError("Skill directory already exists or is invalid")
            staging.rename(skill_dir)
            return self._parser.parse(
                markdown,
                skill_file=skill_dir / "SKILL.md",
                skill_dir=skill_dir,
                source=scope,
                relative_dir=slug,
                limits=SkillLimits(),
            )
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    async def upload(self, skill: SkillDefinition) -> JsonValue:
        if self.client is None:
            raise ValueError("Platform skill client is not configured")
        archive = await asyncio.to_thread(create_skill_archive, skill)
        return await self.client.upload_archive(
            name=skill.slug or skill.name,
            display_name=skill.name,
            filename=f"{skill.slug}.zip",
            zip_content=archive,
        )

    async def delete(self, skill: SkillDefinition) -> JsonValue | None:
        if skill.source == SkillSource.PLATFORM:
            if self.client is None or skill.id is None:
                raise ValueError("Platform delete is unavailable")
            await self.client.delete(skill.id)
            return None
        if skill.source in _READ_ONLY or skill.skill_dir is None:
            raise ValueError("This skill source is read-only")
        root = self.workspace_skills if skill.source == SkillSource.WORKSPACE else self.user_skills
        if root is None:
            raise ValueError("Skill scope is not configured")
        await asyncio.to_thread(self._delete_sync, root, skill)
        return None

    @staticmethod
    def _delete_sync(root: Path, skill: SkillDefinition) -> None:
        if skill.skill_dir is None:
            raise ValueError("This skill source is read-only")
        real_root = root.resolve(strict=True)
        if skill.skill_dir.is_symlink():
            raise ValueError("Skill directory is outside its configured scope")
        real_skill = skill.skill_dir.resolve(strict=True)
        if (
            not real_skill.is_relative_to(real_root)
            or real_skill == real_root
            or real_skill.name != skill.slug
        ):
            raise ValueError("Skill directory is outside its configured scope")
        shutil.rmtree(real_skill)

    async def install_archive(
        self, user_skills: str | Path | None, slug: str, archive: bytes
    ) -> None:
        validate_skill_slug(slug)
        if user_skills is None:
            raise ValueError("User skill scope is not configured")
        entries = inspect_skill_archive(archive)
        await asyncio.to_thread(self._install_sync, Path(user_skills), slug, entries)

    @staticmethod
    def _install_sync(root: Path, slug: str, entries: dict[str, bytes]) -> None:
        root = root.absolute()
        root.mkdir(parents=True, exist_ok=True)
        destination = root / slug
        if destination.exists() or destination.is_symlink():
            raise ValueError("Skill is already installed")
        temporary = root / f".install-{uuid.uuid4().hex}"
        temporary.mkdir()
        try:
            for relative, content in entries.items():
                prefix = f"{slug}/"
                if not relative.startswith(prefix):
                    raise ValueError("Archive contains another skill root")
                output = temporary / relative.removeprefix(prefix)
                if not output.is_relative_to(temporary):
                    raise ValueError("Archive path escapes staging")
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(content)
            if not (temporary / "SKILL.md").is_file():
                raise ValueError("Archive misses SKILL.md")
            if destination.exists() or destination.is_symlink():
                raise ValueError("Skill is already installed")
            temporary.rename(destination)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)


class SkillSyncMode(StrEnum):
    DISABLED = "DISABLED"
    CONFIRM = "CONFIRM"
    AUTO = "AUTO"


class SkillSyncStatus(StrEnum):
    DISABLED = "DISABLED"
    CONFIRMATION_REQUIRED = "CONFIRMATION_REQUIRED"
    INSTALLED = "INSTALLED"
    REJECTED = "REJECTED"


class SkillSynchronizer:
    def __init__(
        self,
        client: SkillClient | None,
        user_skills: str | Path | None,
        mode: SkillSyncMode = SkillSyncMode.DISABLED,
        confirmation: Callable[[str], bool | Awaitable[bool]] | None = None,
    ) -> None:
        self.client = client
        self.user_skills = user_skills
        self.mode = mode
        self.confirmation = confirmation
        self.manager = SkillManager(None, user_skills, client)

    async def install(self, skill: SkillDefinition, archive: bytes) -> SkillSyncStatus:
        if self.mode == SkillSyncMode.DISABLED:
            return SkillSyncStatus.DISABLED
        if skill.elevated_risk:
            return SkillSyncStatus.REJECTED
        if self.mode == SkillSyncMode.CONFIRM:
            approved = (
                False if self.confirmation is None else self.confirmation(skill.slug or skill.name)
            )
            if inspect.isawaitable(approved):
                approved = await approved
            if not approved:
                return SkillSyncStatus.CONFIRMATION_REQUIRED
        await self.manager.install_archive(self.user_skills, skill.slug or skill.name, archive)
        if self.client is not None and skill.id is not None:
            await self.client.report_installation(
                {
                    "skill_package_id": skill.id,
                    "install_source": "python-sdk",
                    "client_platform": "python",
                }
            )
        return SkillSyncStatus.INSTALLED
