from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from taiyiagent.browser import BrowserMode
from taiyiagent.errors import ConfigurationError
from taiyiagent.worker.cli import build_worker, main
from taiyiagent.worker.config import (
    AuthType,
    WorkerAuthConfig,
    WorkerBrowserConfig,
    WorkerConfig,
    WorkerExtensionsConfig,
    WorkerIdentityConfig,
    WorkerSkillsConfig,
    WorkerToolsConfig,
)


def config(workspace: Path) -> WorkerConfig:
    return WorkerConfig(
        base_url="https://taiyi.test",
        auth=WorkerAuthConfig(type=AuthType.API_KEY, token_secret="TEST_TOKEN"),
        identity=WorkerIdentityConfig(worker_id="worker-1", display_name="Worker"),
        workspace=workspace,
    )


async def test_build_worker_loads_extension_without_exposing_secret(tmp_path: Path) -> None:
    module = ModuleType("worker_test_extensions")
    module.validator = SimpleNamespace(validate=lambda *args: None)
    config_value = replace(
        config(tmp_path),
        extensions=WorkerExtensionsConfig(identity_validator="worker_test_extensions:validator"),
    )
    sys.modules[module.__name__] = module
    try:
        worker = build_worker(config_value, {"TEST_TOKEN": "secret-token"})
        assert worker.identity is not None
        assert worker.identity.worker_id == "worker-1"
        assert "secret-token" not in repr(config_value)
        agent = worker._new_agent("flow-a")
        assert "read_file" not in agent.plugins.tool_names
        assert "image_understanding" not in agent.plugins.tool_names
        assert "skill_list" not in agent.plugins.tool_names
        assert agent.browser_runtime is None
        await agent.aclose()
        await worker.aclose()
    finally:
        sys.modules.pop(module.__name__, None)


def test_cli_requires_identity_validator(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="identity-validator"):
        build_worker(config(tmp_path), {"TEST_TOKEN": "secret-token"})


async def test_worker_tool_config_is_explicit_and_uses_agent_skill_client(tmp_path: Path) -> None:
    module = ModuleType("worker_test_tool_extensions")
    module.validator = SimpleNamespace(validate=lambda *args: None)
    sys.modules[module.__name__] = module
    config_value = replace(
        config(tmp_path),
        extensions=WorkerExtensionsConfig(
            identity_validator="worker_test_tool_extensions:validator"
        ),
        tools=WorkerToolsConfig(
            media=True,
            skills=WorkerSkillsConfig(enabled=True, platform_enabled=True, default_names=("demo",)),
            browser=WorkerBrowserConfig(
                mode=BrowserMode.REMOTE_CDP,
                remote_endpoint_secret="CDP_ENDPOINT",
                remote_authorization_secret="CDP_AUTHORIZATION",
            ),
        ),
    )
    try:
        worker = build_worker(
            config_value,
            {
                "TEST_TOKEN": "secret-token",
                "CDP_ENDPOINT": "wss://browser.example/cdp?token=private",
                "CDP_AUTHORIZATION": "Bearer private",
            },
        )
        agent = worker._new_agent("flow-a")
        try:
            assert "image_understanding" in agent.plugins.tool_names
            assert "read_file" not in agent.plugins.tool_names
            assert "skill_list" in agent.plugins.tool_names
            assert "taiyiflow-in-chrome__tabs_create" in agent.plugins.tool_names
            assert agent.skill_supplier is not None
            assert agent.default_skill_names == ("demo",)
            assert agent.browser_runtime is not None
            assert "private" not in repr(agent.browser_runtime.settings)
            assert (
                agent.browser_runtime.settings.remote_headers["Authorization"] == "Bearer private"
            )
        finally:
            await agent.aclose()
        await worker.aclose()
    finally:
        sys.modules.pop(module.__name__, None)


def test_cli_help_does_not_start_worker(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exited:
        main(["--help"])
    assert exited.value.code == 0
    assert "--config" in capsys.readouterr().out
