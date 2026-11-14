from __future__ import annotations

import io
import re
import stat
import zipfile
from pathlib import PurePosixPath

from .models import ClientSkillFile, SkillDefinition

MAX_ENTRIES = 64
MAX_ENTRY_BYTES = 1_000_000
MAX_TOTAL_BYTES = 4_000_000
_EXTENSIONS = frozenset(
    {
        "md",
        "txt",
        "json",
        "yaml",
        "yml",
        "csv",
        "xml",
        "html",
        "css",
        "js",
        "ts",
        "py",
        "sh",
        "png",
        "jpg",
        "jpeg",
        "webp",
    }
)
_SLUG = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")


def validate_skill_slug(slug: str) -> None:
    if not _SLUG.fullmatch(slug) or len(slug) > 64:
        raise ValueError("Skill slug must use lowercase letters, digits and hyphens")


def safe_archive_path(value: str) -> str:
    if (
        not value
        or not value.strip()
        or "\0" in value
        or value.startswith(("/", "\\"))
        or "\\" in value
        or re.match(r"^[A-Za-z]:", value)
    ):
        raise ValueError("Unsafe skill archive path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or len(path.parts) > 8:
        raise ValueError("Unsafe skill archive path")
    if any(part.startswith(".") for part in path.parts):
        raise ValueError("Hidden skill archive entries are forbidden")
    suffix = path.suffix.removeprefix(".").lower()
    if not suffix or suffix not in _EXTENSIONS:
        raise ValueError("Unsupported skill archive file type")
    return str(path)


def create_skill_archive(skill: SkillDefinition) -> bytes:
    if (
        skill.skill_file is None
        or skill.skill_dir is None
        or skill.slug is None
        or not _SLUG.fullmatch(skill.slug)
    ):
        raise ValueError("Only a validated local skill can be uploaded")
    root = skill.skill_dir.resolve(strict=True)
    declared = (ClientSkillFile("SKILL.md"), *skill.files)
    if len(declared) > MAX_ENTRIES:
        raise ValueError("Too many skill archive entries")
    written: set[str] = set()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for declaration in declared:
            relative = safe_archive_path(declaration.path)
            if relative in written:
                continue
            written.add(relative)
            source = root.joinpath(relative)
            if source.is_symlink() or not source.is_file():
                raise ValueError("Declared skill file is not a regular file")
            real = source.resolve(strict=True)
            if not real.is_relative_to(root) or real.stat().st_size > MAX_ENTRY_BYTES:
                raise ValueError("Declared skill file exceeds safety limits")
            archive.writestr(f"{skill.slug}/{relative}", real.read_bytes())
    content = output.getvalue()
    if len(content) > MAX_TOTAL_BYTES:
        raise ValueError("Skill archive is too large")
    return content


def inspect_skill_archive(archive: bytes) -> dict[str, bytes]:
    if not archive or len(archive) > MAX_TOTAL_BYTES * 2:
        raise ValueError("Invalid skill archive size")
    entries: dict[str, bytes] = {}
    total = 0
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as package:
            infos = package.infolist()
            if len(infos) > MAX_ENTRIES:
                raise ValueError("Too many skill archive entries")
            for info in infos:
                mode = info.external_attr >> 16
                kind = stat.S_IFMT(mode)
                if kind == stat.S_IFLNK or kind == 0o010000:
                    raise ValueError("Archive links are forbidden")
                if info.is_dir():
                    continue
                name = safe_archive_path(info.filename)
                if name in entries:
                    raise ValueError("Duplicate skill archive entry")
                if info.file_size > MAX_ENTRY_BYTES:
                    raise ValueError("Skill archive entry is too large")
                content = package.read(info)
                if len(content) > MAX_ENTRY_BYTES:
                    raise ValueError("Skill archive entry is too large")
                total += len(content)
                if total > MAX_TOTAL_BYTES or (
                    info.compress_size > 0 and len(content) > info.compress_size * 200
                ):
                    raise ValueError("Skill archive exceeds expansion limits")
                entries[name] = content
    except (zipfile.BadZipFile, NotImplementedError, RuntimeError) as exc:
        raise ValueError("Invalid skill archive") from exc
    if total > len(archive) * 200 + 1024:
        raise ValueError("Suspicious archive compression ratio")
    documents = 0
    for name, content in entries.items():
        if name == "SKILL.md" or name.endswith("/SKILL.md"):
            try:
                starts_frontmatter = content.decode("utf-8").startswith("---")
            except UnicodeDecodeError:
                starts_frontmatter = False
            documents += int(starts_frontmatter)
    if documents != 1:
        raise ValueError("Skill archive must contain one SKILL.md")
    return entries


def read_declared_skill_file(skill: SkillDefinition, requested: str) -> str:
    relative = safe_archive_path(requested)
    if skill.skill_dir is None or relative not in {item.path for item in skill.files}:
        raise ValueError("File is not declared by the skill")
    root = skill.skill_dir.resolve(strict=True)
    source = root.joinpath(relative)
    if source.is_symlink() or not source.is_file() or source.stat().st_size > MAX_ENTRY_BYTES:
        raise ValueError("Declared skill file is unavailable")
    real = source.resolve(strict=True)
    if not real.is_relative_to(root):
        raise ValueError("Declared skill file is unavailable")
    try:
        return real.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise ValueError("Declared skill file is unavailable") from exc
