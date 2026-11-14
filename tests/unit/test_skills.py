from __future__ import annotations

import asyncio
import io
import stat
import zipfile
from pathlib import Path
from typing import Any, cast

import pytest

from taiyiagent import (
    ChatRequest,
    ClientSkillPackage,
    FileSystemSkillProvider,
    PlatformSkillProvider,
    SkillDefinition,
    SkillDocumentParser,
    SkillManager,
    SkillQuery,
    SkillRegistry,
    SkillSelection,
    SkillSource,
    SkillSupplier,
    SkillSupplyPolicy,
    SkillSupplySnapshot,
    SkillSynchronizer,
    SkillSyncMode,
    SkillSyncStatus,
    SkillTools,
    TaiyiAgent,
    ToolCall,
    ToolExecutionContext,
)
from taiyiagent.skills import inspect_skill_archive, validate_skill_package_payloads
from taiyiagent.worker import SubtaskCapabilitySnapshot


def markdown(name: str, body: str, *, extra: str = "") -> str:
    return f"---\nname: {name}\ndescription: {name} description\n{extra}---\n{body}\n"


def write_skill(root: Path, name: str, body: str, *, extra: str = "") -> Path:
    directory = root / name
    directory.mkdir(parents=True)
    document = directory / "SKILL.md"
    document.write_text(markdown(name, body, extra=extra), encoding="utf-8")
    return document


def query(*, tools: frozenset[str] | None = None) -> SkillQuery:
    return SkillQuery("flow-1", "session-1", available_tools=tools)


def packages_by_source(snapshot: SkillSupplySnapshot, source: str) -> ClientSkillPackage:
    return next(item for item in snapshot.packages if item.package_name == f"taiyiflow-{source}")


def test_parser_reads_frontmatter_and_checks_only_environment_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SKILL_TEST_SECRET", "do-not-export-this-value")
    parsed = SkillDocumentParser().parse(
        markdown(
            "demo",
            "# Heading\n\nUseful instructions",
            extra=(
                "prerequisites: [python]\n"
                "files:\n  - path: assets/example.txt\n    description: example\n"
                "metadata:\n  requires_env: [SKILL_TEST_SECRET, SKILL_TEST_MISSING]\n"
            ),
        ),
        source=SkillSource.WORKSPACE,
    )
    assert parsed.summary == "Heading"
    assert parsed.files[0].path == "assets/example.txt"
    assert parsed.missing_environment() == ("SKILL_TEST_MISSING",)
    assert "do-not-export-this-value" not in repr(parsed.missing_environment())


@pytest.mark.parametrize(
    "document",
    [
        "no frontmatter",
        "---\nname: demo\n---\n",
        markdown("demo", "body", extra="files: [../secret.txt]\n"),
        markdown("demo", "body", extra="files: [.hidden/secret.txt]\n"),
        markdown("demo", "body", extra="files: [C:/secret.txt]\n"),
    ],
)
def test_parser_rejects_invalid_documents(document: str) -> None:
    with pytest.raises(ValueError):
        SkillDocumentParser().parse(document, source=SkillSource.WORKSPACE)


async def test_local_source_precedence_full_text_and_summaries(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    user = tmp_path / "user"
    write_skill(workspace, "shared", "workspace full body")
    write_skill(user, "shared", "user shadowed body")
    write_skill(user, "other", "other full body")
    supplier = SkillSupplier(
        SkillRegistry(
            [
                FileSystemSkillProvider(user, SkillSource.USER),
                FileSystemSkillProvider(workspace, SkillSource.WORKSPACE),
            ]
        ),
        SkillSupplyPolicy.local_only(),
    )
    snapshot = await supplier.supply(
        query(), SkillSelection(requested_names=("shared",), default_names=("missing-default",))
    )
    shared = packages_by_source(snapshot, "workspace").skills[0]
    other = packages_by_source(snapshot, "user").skills[0]
    assert shared.content == "workspace full body\n"
    assert shared.summary_only is False
    assert other.slug == "other"
    assert other.content == "other full body"
    assert other.summary_only is True
    assert snapshot.requested_skill_names[:2] == ("shared", "other")
    assert "missing-default" in snapshot.requested_skill_names
    subset = snapshot.subset({"shared"})
    assert len(subset.packages) == 1
    assert subset.requested_skill_names == ("shared",)


async def test_visibility_filters_request_type_browser_and_allowed_skills(tmp_path: Path) -> None:
    root = tmp_path / "skills"
    write_skill(root, "ask-only", "ask body", extra="metadata:\n  request_type: ask\n")
    write_skill(root, "browser", "browser body", extra="category: browser\n")
    write_skill(root, "ordinary", "ordinary body")
    supplier = SkillSupplier(
        SkillRegistry([FileSystemSkillProvider(root, SkillSource.WORKSPACE)]),
        SkillSupplyPolicy.local_only(),
    )
    visible = await supplier.visible_skills(query(tools=frozenset()), SkillSelection())
    assert {item.slug for item in visible} == {"ordinary"}
    browser_query = query(tools=frozenset({"taiyiflow-in-chrome__read_text"}))
    visible = await supplier.visible_skills(browser_query, SkillSelection())
    assert {item.slug for item in visible} == {"browser", "ordinary"}
    selected = await supplier.visible_skills(
        browser_query, SkillSelection(allowed_skills=frozenset({"ordinary"}))
    )
    assert [item.slug for item in selected] == ["ordinary"]


class _PlatformClient:
    def __init__(self) -> None:
        self.content_calls = 0
        self.contexts: list[dict[str, object]] = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def list(self, *, context: dict[str, object]) -> list[dict[str, Any]]:
        self.contexts.append(dict(context))
        return [
            {
                "id": "platform-id",
                "name": "platform-skill",
                "display_name": "Platform Skill",
                "summary": "platform summary",
                "version": "v1",
            }
        ]

    async def get_content(self, skill_id: str, *, context: dict[str, object]) -> dict[str, str]:
        self.content_calls += 1
        self.contexts.append(dict(context))
        self.started.set()
        await self.release.wait()
        return {"content": markdown("Platform Skill", "platform full body")}


async def test_platform_content_cache_is_scoped_and_single_flight() -> None:
    client = _PlatformClient()
    provider = PlatformSkillProvider(cast(Any, client))
    first = (await provider.discover(query())).skills[0]
    pending = asyncio.create_task(provider.load(first, query()))
    await client.started.wait()
    duplicate = asyncio.create_task(provider.load(first, query()))
    await asyncio.sleep(0)
    client.release.set()
    loaded, same = await asyncio.gather(pending, duplicate)
    assert loaded.body == same.body == "platform full body\n"
    assert client.content_calls == 1
    await provider.load(first, query())
    assert client.content_calls == 1
    other_session = SkillQuery("flow-1", "session-2")
    client.release.set()
    await provider.load(first, other_session)
    assert client.content_calls == 2
    assert client.contexts[0]["allow_elevated_risk"] is False
    assert client.contexts[1]["format"] == "content"


async def test_agent_first_frame_and_recovery_reuse_skill_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "skills"
    document = write_skill(root, "demo", "original body")
    supplier = SkillSupplier(
        SkillRegistry([FileSystemSkillProvider(root, SkillSource.WORKSPACE)]),
        SkillSupplyPolicy.local_only(),
    )
    agent = TaiyiAgent(
        base_url="https://example.invalid",
        flow_id="flow-1",
        api_key="test-key",
        enable_builtin_tools=False,
        identity_path=tmp_path / "client-id",
        skill_supplier=supplier,
    )
    try:
        session = agent.session("session-1")
        request = ChatRequest(
            input_value="hello",
            session_id="session-1",
            execution_mode="start",
            requested_skill_names=["demo"],
        )
        session.run_context.begin(request)
        await agent._prepare_tool_supply(session, request)
        assert request.client_skill_packages is not None
        original = request.client_skill_packages[0]["skills"][0]["content"]
        assert original == "original body\n"
        document.write_text(markdown("demo", "changed body"), encoding="utf-8")
        recovery = ChatRequest(session_id="session-1", execution_mode="attach")
        await agent._prepare_tool_supply(session, recovery)
        assert recovery.client_skill_packages == request.client_skill_packages
        assert recovery.requested_skill_names == request.requested_skill_names
    finally:
        await agent.aclose()


async def test_skill_tools_manage_local_skills_without_exposing_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SKILL_TOOL_SECRET", "secret-value-that-must-not-leak")
    workspace = tmp_path / "workspace"
    manager = SkillManager(workspace, tmp_path / "user")
    supplier = SkillSupplier(
        SkillRegistry([FileSystemSkillProvider(workspace, SkillSource.WORKSPACE)]),
        SkillSupplyPolicy.local_only(),
    )
    tools = SkillTools(supplier, manager)
    context = ToolExecutionContext(session_id="session-1", flow_id="flow-1")
    created = await tools.create(
        context,
        ToolCall(
            id="create",
            name="skill_create",
            arguments={
                "slug": "demo",
                "content": markdown(
                    "demo",
                    "tool body",
                    extra=(
                        "files: [notes.txt]\n"
                        "metadata:\n  requires_env: [SKILL_TOOL_SECRET, MISSING_SKILL_TOKEN]\n"
                    ),
                ),
            },
        ),
    )
    assert created.success is True
    (workspace / "demo" / "notes.txt").write_text("declared notes", encoding="utf-8")
    listed = await tools.list(context, ToolCall(id="list", name="skill_list"))
    assert listed.result["skills"][0]["missing_env"] == ["MISSING_SKILL_TOKEN"]
    assert "secret-value-that-must-not-leak" not in repr(listed.result)
    fetched = await tools.fetch(
        context,
        ToolCall(id="fetch", name="skill_fetch", arguments={"name": "demo", "file": "notes.txt"}),
    )
    assert fetched.result["content"] == "declared notes"
    deleted = await tools.delete(
        context, ToolCall(id="delete", name="skill_delete", arguments={"name": "demo"})
    )
    assert deleted.success is True
    assert not (workspace / "demo").exists()


def _archive(entries: list[tuple[str, bytes, int | None]]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as package:
        for name, content, mode in entries:
            info = zipfile.ZipInfo(name)
            info.compress_type = zipfile.ZIP_DEFLATED
            if mode is not None:
                info.create_system = 3
                info.external_attr = mode << 16
            package.writestr(info, content)
    return output.getvalue()


def test_archive_install_and_path_safety(tmp_path: Path) -> None:
    archive = _archive(
        [
            ("demo/SKILL.md", markdown("demo", "archived body").encode(), None),
            ("demo/notes.txt", b"notes", None),
        ]
    )
    assert inspect_skill_archive(archive)["demo/notes.txt"] == b"notes"
    manager = SkillManager(None, tmp_path / "user")
    asyncio.run(manager.install_archive(tmp_path / "user", "demo", archive))
    assert (tmp_path / "user" / "demo" / "notes.txt").read_bytes() == b"notes"
    malicious = [
        "../SKILL.md",
        "demo/../escape.txt",
        "demo\\escape.txt",
        "demo/.hidden.txt",
        "demo/executable.exe",
    ]
    for path in malicious:
        with pytest.raises(ValueError):
            inspect_skill_archive(
                _archive(
                    [
                        ("demo/SKILL.md", markdown("demo", "body").encode(), None),
                        (path, b"bad", None),
                    ]
                )
            )
    symlink = _archive(
        [
            ("demo/SKILL.md", markdown("demo", "body").encode(), None),
            ("demo/link.txt", b"target", stat.S_IFLNK | 0o777),
        ]
    )
    with pytest.raises(ValueError, match="links"):
        inspect_skill_archive(symlink)
    bomb = _archive(
        [
            ("demo/SKILL.md", markdown("demo", "body").encode(), None),
            ("demo/zeros.txt", b"0" * 1_000_000, None),
        ]
    )
    with pytest.raises(ValueError, match="expansion"):
        inspect_skill_archive(bomb)


async def test_sync_is_disabled_by_default_and_rejects_elevated_risk(tmp_path: Path) -> None:
    skill = SkillDefinition(name="demo", body="body", source=SkillSource.PLATFORM)
    synchronizer = SkillSynchronizer(None, tmp_path / "user")
    assert await synchronizer.install(skill, b"archive") == SkillSyncStatus.DISABLED
    elevated = SkillDefinition(
        name="demo", body="body", source=SkillSource.PLATFORM, elevated_risk=True
    )
    synchronizer.mode = SkillSyncMode.AUTO
    assert await synchronizer.install(elevated, b"archive") == SkillSyncStatus.REJECTED


def test_subtask_inherits_only_snapshot_skill_packages() -> None:
    parent = ChatRequest(
        requested_skill_names=["demo"],
        client_skill_packages=[
            {
                "package_name": "taiyiflow-workspace",
                "skills": [{"name": "demo", "slug": "demo", "content": "body"}],
            }
        ],
        available_tools=["skill_fetch"],
    )
    snapshot = SubtaskCapabilitySnapshot.from_request(parent)
    assert parent.client_skill_packages is not None
    parent.client_skill_packages[0]["skills"][0]["content"] = "changed"
    child = ChatRequest()
    snapshot.apply(child)
    assert child.client_skill_packages[0]["skills"][0]["content"] == "body"
    assert child.requested_skill_names == ["demo"]
    validate_skill_package_payloads(cast(list[dict[str, object]], child.client_skill_packages))


def test_outgoing_skill_package_rejects_host_paths() -> None:
    with pytest.raises(ValueError):
        validate_skill_package_payloads(
            [
                {
                    "package_name": "taiyiflow-workspace",
                    "root_dir": "/private/host",
                    "skills": [{"name": "demo"}],
                }
            ]
        )
