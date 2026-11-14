from __future__ import annotations

import asyncio
import socket
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from taiyiagent.browser import (
    BrowserMode,
    BrowserNetworkPolicy,
    BrowserRuntime,
    BrowserSettings,
    register_browser_tools,
)
from taiyiagent.browser.runtime import DOM_GENERATION_SCRIPT
from taiyiagent.models import ToolCall
from taiyiagent.plugins import PluginManager, ToolExecutionContext
from taiyiagent.transport import RestClient

ACTIONS = (
    "tabs_context",
    "tabs_create",
    "navigate",
    "set_view_size",
    "wait_for",
    "click",
    "type",
    "hover",
    "scroll",
    "select_option",
    "press_key",
    "get_element_info",
    "get_element_ref",
    "screenshot",
    "read_text",
    "read_console",
    "execute_js",
)


class FakeLocator:
    def __init__(self, page: FakePage) -> None:
        self.page = page

    async def click(self, **_: Any) -> None:
        self.page.clicked = True

    async def fill(self, value: str, **_: Any) -> None:
        self.page.value = value

    async def press_sequentially(self, value: str, **_: Any) -> None:
        self.page.value += value

    async def press(self, _: str) -> None:
        return

    async def hover(self, **_: Any) -> None:
        return

    async def select_option(self, **_: Any) -> list[str]:
        return ["selected"]

    async def is_visible(self) -> bool:
        return True

    async def text_content(self) -> str:
        return "element text"

    async def inner_text(self) -> str:
        return "page text"

    async def evaluate(self, script: str, _: Any = None) -> Any:
        if "outerHTML" in script:
            return "<button>element text</button>"
        if "attributes" in script:
            return {"id": "action"}
        if "scrollBy" in script:
            return None
        return self.page.value

    async def bounding_box(self) -> dict[str, int]:
        return {"x": 1, "y": 2, "width": 3, "height": 4}


class FakeKeyboard:
    def __init__(self) -> None:
        self.pressed = ""

    async def press(self, value: str) -> None:
        self.pressed = value


class FakePage:
    def __init__(self) -> None:
        self.url = "about:blank"
        self.viewport_size = {"width": 1280, "height": 720}
        self.keyboard = FakeKeyboard()
        self.handlers: dict[str, Any] = {}
        self.generation = 1
        self.value = ""
        self.clicked = False
        self.closed = False
        self.block_wait = False

    def set_default_timeout(self, _: float) -> None:
        return

    def on(self, event: str, handler: Any) -> None:
        self.handlers[event] = handler

    def locator(self, _: str) -> FakeLocator:
        return FakeLocator(self)

    async def title(self) -> str:
        return "Fake page"

    def is_closed(self) -> bool:
        return self.closed

    async def goto(self, url: str, **_: Any) -> None:
        self.url = url

    async def set_viewport_size(self, size: dict[str, int]) -> None:
        self.viewport_size = size

    async def wait_for_selector(self, _: str, **__: Any) -> None:
        if self.block_wait:
            await asyncio.Event().wait()

    async def wait_for_function(self, *_: Any, **__: Any) -> None:
        return

    async def wait_for_load_state(self, *_: Any, **__: Any) -> None:
        return

    async def evaluate(self, script: str, _: Any = None) -> Any:
        if script == DOM_GENERATION_SCRIPT:
            return self.generation
        if "scrollWidth" in script:
            return 1280 * 720
        return {"value": self.value}

    async def screenshot(self, **_: Any) -> bytes:
        return b"\x89PNG\r\n\x1a\n"


class FakeContext:
    def __init__(self) -> None:
        self.pages: list[FakePage] = []
        self.closed = False
        self.routes: list[str] = []

    def set_default_timeout(self, _: float) -> None:
        return

    async def new_page(self) -> FakePage:
        page = FakePage()
        self.pages.append(page)
        return page

    async def route(self, pattern: str, _: Any) -> None:
        self.routes.append(pattern)

    async def close(self) -> None:
        self.closed = True


class FakeBrowser:
    def __init__(self) -> None:
        self.contexts: list[FakeContext] = []
        self.options: list[dict[str, Any]] = []
        self.closed = False
        self.connected = True

    def is_connected(self) -> bool:
        return self.connected

    def on(self, _: str, __: Any) -> None:
        return

    async def new_context(self, **options: Any) -> FakeContext:
        self.options.append(options)
        context = FakeContext()
        self.contexts.append(context)
        return context

    async def close(self) -> None:
        self.closed = True


def context(session: str = "session-a", *, workspace: Path | None = None) -> ToolExecutionContext:
    return ToolExecutionContext(session_id=session, flow_id="flow-a", workspace=workspace)


def runtime(settings: BrowserSettings | None = None) -> BrowserRuntime:
    return BrowserRuntime(
        settings or BrowserSettings(mode=BrowserMode.HEADLESS),
        RestClient("http://127.0.0.1:1", "not-used"),
    )


async def execute(
    plugins: PluginManager,
    action: str,
    arguments: dict[str, Any] | None = None,
    *,
    ctx: ToolExecutionContext | None = None,
) -> Any:
    return await plugins.execute(
        ctx or context(), ToolCall(name="taiyiflow-in-chrome__" + action, arguments=arguments or {})
    )


async def test_disabled_registration_and_inventory_do_not_start_browser() -> None:
    disabled_plugins = PluginManager()
    rest = RestClient("http://127.0.0.1:1", "not-used")
    assert register_browser_tools(disabled_plugins, rest, BrowserSettings.disabled()) is None
    assert disabled_plugins.definitions == ()

    plugins = PluginManager()
    browser = runtime()
    browser.register(plugins)
    try:
        assert {definition.name for definition in plugins.definitions} == {
            "taiyiflow-in-chrome__" + action for action in ACTIONS
        }
        result = await execute(plugins, "tabs_context")
        assert result.success
        assert result.result == {"tabs": [], "browser_type": "chromium", "mode": "HEADLESS"}
        assert browser._browser is None
    finally:
        await plugins.aclose()


async def test_browser_identity_comes_only_from_trusted_context() -> None:
    plugins = PluginManager()
    browser = runtime()
    browser.register(plugins)
    try:
        result = await execute(
            plugins,
            "tabs_create",
            {"flow_id": "forged", "session_id": "forged"},
            ctx=ToolExecutionContext(session_id="model-session"),
        )
        assert not result.success
        assert "trusted flow_id and session_id" in result.error
    finally:
        await plugins.aclose()


async def test_remote_settings_redact_secrets_and_reject_injection() -> None:
    settings = BrowserSettings(
        mode=BrowserMode.REMOTE_CDP,
        remote_endpoint="wss://browser.example/cdp?token=top-secret",
        remote_headers={"Authorization": "Bearer private"},
    )
    assert settings.redacted_remote_host() == "wss://browser.example"
    assert "top-secret" not in repr(settings)
    assert "Bearer private" not in repr(settings)
    with pytest.raises(ValueError):
        BrowserSettings(
            mode=BrowserMode.REMOTE_CDP,
            remote_endpoint="wss://user:pass@example.test/cdp",
        )
    with pytest.raises(ValueError):
        BrowserSettings(remote_headers={"X-Test": "safe\r\nX-Injected: true"})
    assert not BrowserSettings(mode=BrowserMode.HEADLESS).execute_js_enabled


async def test_network_policy_blocks_local_targets_and_supports_allowlists() -> None:
    policy = BrowserNetworkPolicy(BrowserSettings(mode=BrowserMode.HEADLESS))
    for url in (
        "file:///etc/passwd",
        "javascript:alert(1)",
        "data:text/html,hello",
        "http://user:pass@example.com/",
        "http://127.0.0.1/",
        "http://localhost/",
        "http://10.0.0.1/",
        "http://169.254.169.254/",
        "http://[::1]/",
        "http://[fc00::1]/",
    ):
        assert not await policy.is_allowed(url), url
    assert await policy.validate("about:blank") == "about:blank"
    local = BrowserNetworkPolicy(
        BrowserSettings(mode=BrowserMode.HEADLESS, allowed_hosts=frozenset({"localhost"}))
    )
    assert await local.is_allowed("http://localhost:8080/")
    assert not await local.is_allowed("http://localhost.evil.example/")
    cidr = BrowserNetworkPolicy(
        BrowserSettings(mode=BrowserMode.HEADLESS, allowed_cidrs=frozenset({"127.0.0.0/8"}))
    )
    assert await cidr.is_allowed("http://127.0.0.1:8080/")
    assert not await cidr.is_allowed("http://10.0.0.1/")


async def test_public_navigation_uses_dns_results(monkeypatch: pytest.MonkeyPatch) -> None:
    def resolve(_: str, __: Any, **___: Any) -> list[Any]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    policy = BrowserNetworkPolicy(BrowserSettings(mode=BrowserMode.HEADLESS))
    assert await policy.is_allowed("https://example.test/")


async def test_remote_runtime_owns_only_new_contexts_and_does_not_close_browser() -> None:
    plugins = PluginManager()
    browser = runtime(
        BrowserSettings(mode=BrowserMode.REMOTE_CDP, remote_endpoint="wss://browser.example/cdp")
    )
    fake = FakeBrowser()
    browser._browser = fake
    browser.register(plugins)
    try:
        created = await execute(plugins, "tabs_create")
        assert created.success
        assert len(fake.contexts) == 1
        assert fake.options[0]["accept_downloads"] is False
        assert fake.contexts[0].routes == ["**/*"]
        other = await execute(plugins, "tabs_context", ctx=context("session-b"))
        assert other.result["tabs"] == []
    finally:
        await plugins.aclose()
    assert fake.contexts[0].closed
    assert not fake.closed


async def test_session_cleanup_element_refs_and_console_bounds(tmp_path: Path) -> None:
    plugins = PluginManager()
    browser = runtime(BrowserSettings(mode=BrowserMode.HEADLESS, max_console_messages=1))
    fake = FakeBrowser()
    browser._browser = fake
    browser.register(plugins)
    try:
        await execute(plugins, "tabs_create")
        reference = await execute(plugins, "get_element_ref", {"selector": "#action"})
        assert reference.success
        element_ref = reference.result["element_ref_id"]
        assert (await execute(plugins, "click", {"element_ref_id": element_ref})).success
        fake.contexts[0].pages[0].generation += 1
        stale = await execute(plugins, "click", {"element_ref_id": element_ref})
        assert not stale.success
        assert "stale" in stale.error

        page = fake.contexts[0].pages[0]
        page.handlers["console"](SimpleNamespace(type="log", text="first"))
        page.handlers["console"](SimpleNamespace(type="error", text="second"))
        console = await execute(plugins, "read_console")
        assert console.result == {
            "messages": [{"type": "error", "text": "second"}],
            "truncated": True,
        }

        screenshot = await execute(plugins, "screenshot", ctx=context(workspace=tmp_path))
        assert screenshot.success
        assert (tmp_path / screenshot.result["path"]).read_bytes().startswith(b"\x89PNG")
        await plugins.close_session("session-a")
        assert fake.contexts[0].closed
        inventory = await execute(plugins, "tabs_context")
        assert inventory.result["tabs"] == []
    finally:
        await plugins.aclose()


async def test_execute_js_switch_and_timeout_invalidate_owned_session() -> None:
    plugins = PluginManager()
    browser = runtime(BrowserSettings(mode=BrowserMode.HEADLESS, action_timeout=0.02))
    fake = FakeBrowser()
    browser._browser = fake
    browser.register(plugins)
    try:
        await execute(plugins, "tabs_create")
        denied = await execute(plugins, "execute_js", {"code": "() => 1"})
        assert not denied.success
        assert "execute_js_enabled" in denied.error

        fake.contexts[0].pages[0].block_wait = True
        timed_out = await execute(plugins, "wait_for", {"selector": "#never"})
        assert not timed_out.success
        assert "timed out" in timed_out.error
        recreated = await execute(plugins, "tabs_create")
        assert recreated.success
        assert len(fake.contexts) == 2
    finally:
        await plugins.aclose()


async def test_enabled_execute_js_and_agent_disabled_default() -> None:
    plugins = PluginManager()
    browser = runtime(BrowserSettings(mode=BrowserMode.HEADLESS, execute_js_enabled=True))
    fake = FakeBrowser()
    browser._browser = fake
    browser.register(plugins)
    try:
        await execute(plugins, "tabs_create")
        result = await execute(plugins, "execute_js", {"code": "() => 1"})
        assert result.success
        assert result.result == {"value": ""}
    finally:
        await plugins.aclose()

    from taiyiagent import TaiyiAgent

    agent = TaiyiAgent(
        base_url="https://example.test", api_key="secret", enable_builtin_tools=False
    )
    try:
        assert agent.browser_runtime is None
        assert not any(
            name.startswith("taiyiflow-in-chrome__") for name in agent.plugins.tool_names
        )
    finally:
        await agent.aclose()
