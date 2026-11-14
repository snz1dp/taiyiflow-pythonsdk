from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from importlib import import_module
from pathlib import Path
from typing import Any, cast

from ..errors import redact
from ..models import ToolCall, ToolResult
from ..plugins.definitions import tool_definition
from ..plugins.manager import PluginManager, ToolExecutionContext
from ..plugins.media import ImageUnderstandingService
from ..transport import RestClient
from .network import BrowserNetworkPolicy
from .settings import BrowserMode, BrowserSettings

PREFIX = "taiyiflow-in-chrome__"
DOM_GENERATION_SCRIPT = """(()=>{if(!window.__taiyiflowDomGeneration){
window.__taiyiflowDomGeneration=1;const root=document.documentElement||document;
new MutationObserver(()=>window.__taiyiflowDomGeneration++).observe(root,
{subtree:true,childList:true,attributes:true,characterData:true});}
return window.__taiyiflowDomGeneration;})()"""

BrowserHandler = Callable[
    [ToolExecutionContext, Mapping[str, Any]], Awaitable[dict[str, Any] | list[Any] | Any]
]


@dataclass(slots=True)
class _ElementRef:
    selector: str
    generation: int


@dataclass(slots=True)
class _ConsoleEntry:
    type: str
    text: str


@dataclass(slots=True)
class _Tab:
    session: _SessionPages
    id: str
    page: Any
    refs: dict[str, _ElementRef] = field(default_factory=dict)
    console: deque[_ConsoleEntry] = field(default_factory=deque)
    console_overflow: bool = False


@dataclass(slots=True)
class _SessionPages:
    context: Any
    tabs: OrderedDict[str, _Tab] = field(default_factory=OrderedDict)
    active_tab: str | None = None
    valid: bool = True


class BrowserRuntime:
    def __init__(self, settings: BrowserSettings, rest_client: RestClient) -> None:
        if settings.mode is BrowserMode.DISABLED:
            raise ValueError("Disabled browser settings cannot create a runtime")
        self.settings = settings
        self.network_policy = BrowserNetworkPolicy(settings)
        self.image_understanding = ImageUnderstandingService(rest_client)
        self._sessions: dict[str, _SessionPages] = {}
        self._playwright: Any = None
        self._browser: Any = None
        self._remote_disconnected = False
        self._next_connection_attempt = 0.0
        self._closed = False
        self._lock = asyncio.Lock()

    def register(self, plugins: PluginManager) -> None:
        specifications: tuple[
            tuple[str, str, Mapping[str, Any], tuple[str, ...], BrowserHandler], ...
        ] = (
            (
                "tabs_context",
                "List tabs owned by the current SDK session.",
                {},
                (),
                self._tabs_context,
            ),
            (
                "tabs_create",
                "Create a tab owned by the current SDK session.",
                {"url": _string(), "activate": _boolean()},
                (),
                self._tabs_create,
            ),
            (
                "navigate",
                "Navigate an SDK-owned tab to an allowed HTTP(S) URL.",
                {"url": _string(), "tab_id": _string()},
                ("url",),
                self._navigate,
            ),
            (
                "set_view_size",
                "Set the current tab viewport size.",
                {
                    "mode": _string(),
                    "width": _integer(),
                    "height": _integer(),
                    "tab_id": _string(),
                },
                (),
                self._set_view_size,
            ),
            (
                "wait_for",
                "Wait for an element, page text, or network idle.",
                {
                    "selector": _string(),
                    "text": _string(),
                    "network_idle": _boolean(),
                    "timeout_ms": _integer(),
                    "tab_id": _string(),
                },
                (),
                self._wait_for,
            ),
            (
                "click",
                "Click an element in an SDK-owned tab.",
                _selector_properties(),
                (),
                self._click,
            ),
            (
                "type",
                "Type text into an element in an SDK-owned tab.",
                {
                    **_selector_properties(),
                    "text": _string(),
                    "clear": _boolean(),
                    "submit": _boolean(),
                },
                ("text",),
                self._type,
            ),
            ("hover", "Hover over an element.", _selector_properties(), (), self._hover),
            (
                "scroll",
                "Scroll the page or an element.",
                {
                    "selector": _string(),
                    "tab_id": _string(),
                    "delta_x": _integer(),
                    "delta_y": _integer(),
                },
                (),
                self._scroll,
            ),
            (
                "select_option",
                "Select an option by value or label.",
                {**_selector_properties(), "value": _string(), "label": _string()},
                (),
                self._select_option,
            ),
            (
                "press_key",
                "Press a key or supported modifier combination.",
                {
                    "key": _string(),
                    "modifiers": {"type": "array", "items": _string()},
                    "tab_id": _string(),
                },
                ("key",),
                self._press_key,
            ),
            (
                "get_element_info",
                "Read structured element information.",
                _selector_properties(),
                (),
                self._element_info,
            ),
            (
                "get_element_ref",
                "Create a navigation and DOM-generation-bound element reference.",
                {"selector": _string(), "tab_id": _string()},
                ("selector",),
                self._element_ref,
            ),
            (
                "read_text",
                "Read bounded page or element text.",
                {
                    "selector": _string(),
                    "tab_id": _string(),
                    "max_characters": _integer(),
                },
                (),
                self._read_text,
            ),
            (
                "read_console",
                "Read bounded console messages from an SDK-owned tab.",
                {"limit": _integer(), "tab_id": _string()},
                (),
                self._read_console,
            ),
            (
                "execute_js",
                "Execute JavaScript only when the independent script switch is enabled.",
                {"code": _string(), "tab_id": _string()},
                ("code",),
                self._execute_js,
            ),
        )
        for action, description, properties, required, handler in specifications:
            name = PREFIX + action
            plugins.register(
                name,
                self._tool_handler(handler),
                definition=tool_definition(name, description, properties, required=required),
            )
        plugins.register(
            PREFIX + "screenshot",
            self._screenshot_tool,
            definition=tool_definition(
                PREFIX + "screenshot",
                "Save a bounded PNG screenshot in the session workspace.",
                {
                    "full_page": _boolean(),
                    "understand": _boolean(),
                    "input": _string(),
                    "tab_id": _string(),
                },
            ),
        )
        plugins.register_closeable(self)
        plugins.register_session_cleanup(self.close_session)

    def _tool_handler(
        self, handler: BrowserHandler
    ) -> Callable[[ToolExecutionContext, ToolCall], Awaitable[ToolResult]]:
        async def invoke(context: ToolExecutionContext, call: ToolCall) -> ToolResult:
            try:
                result = await self._run(context, lambda: handler(context, call.arguments_dict))
                return ToolResult.ok(call.id, result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                return ToolResult.fail(call.id, self._safe_error(exc))

        return invoke

    async def _screenshot_tool(self, context: ToolExecutionContext, call: ToolCall) -> ToolResult:
        arguments = call.arguments_dict
        try:
            result = await self._run(context, lambda: self._screenshot(context, arguments))
            if not isinstance(result, dict):
                raise TypeError("Screenshot result must be an object")
            if _boolean_value(arguments, "understand", False):
                await self._add_image_understanding(context, result, arguments)
            return ToolResult.ok(call.id, result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return ToolResult.fail(call.id, self._safe_error(exc))

    async def _run(
        self, context: ToolExecutionContext, operation: Callable[[], Awaitable[Any]]
    ) -> Any:
        if self._closed:
            raise RuntimeError("Browser runtime is closed")
        key = self._key(context)
        async with self._lock:
            if _cancelled(context):
                raise RuntimeError("Browser operation cancelled before execution")
            task = asyncio.ensure_future(operation())
            cancellation = asyncio.create_task(_wait_for_cancellation(context))
            try:
                done, _ = await asyncio.wait(
                    {task, cancellation},
                    timeout=self.settings.action_timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if task in done:
                    return task.result()
                task.cancel()
                await _suppress_cancelled(task)
                self._invalidate(key)
                if cancellation in done:
                    raise RuntimeError("Browser operation cancelled")
                raise TimeoutError("Browser operation timed out")
            except asyncio.CancelledError:
                task.cancel()
                await _suppress_cancelled(task)
                self._invalidate(key)
                raise
            finally:
                cancellation.cancel()
                await _suppress_cancelled(cancellation)

    async def _tabs_context(
        self, context: ToolExecutionContext, _: Mapping[str, Any]
    ) -> dict[str, Any]:
        return await self._tabs(context)

    async def _tabs_create(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        session = await self._session(context, create=True)
        assert session is not None
        if len(session.tabs) >= self.settings.max_pages_per_session:
            raise RuntimeError("Maximum pages per session reached")
        tab = await self._create_tab(session)
        await self._navigate_tab(tab, _text(arguments, "url", "about:blank"))
        if _boolean_value(arguments, "activate", True):
            session.active_tab = tab.id
        return await self._tab_json(tab, session.active_tab)

    async def _navigate(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=True)
        await self._navigate_tab(tab, _text(arguments, "url", ""))
        return await self._tab_json(tab, tab.session.active_tab)

    async def _set_view_size(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=True)
        mode = _text(arguments, "mode", "desktop")
        if mode == "desktop":
            width, height = 1280, 720
        elif mode == "mobile":
            width, height = 375, 812
        elif mode == "custom":
            width, height = _int(arguments, "width", 0), _int(arguments, "height", 0)
        else:
            raise ValueError("Unknown viewport mode")
        if not 200 <= width <= 4096 or not 200 <= height <= 4096:
            raise ValueError("Viewport dimensions must be between 200 and 4096")
        await tab.page.set_viewport_size({"width": width, "height": height})
        self._invalidate_refs(tab)
        result = await self._tab_json(tab, tab.session.active_tab)
        result.update(width=width, height=height)
        return result

    async def _wait_for(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=False)
        timeout = min(30_000, max(1, _int(arguments, "timeout_ms", 15_000)))
        selector = _text(arguments, "selector", "")
        text = _text(arguments, "text", "")
        network_idle = _boolean_value(arguments, "network_idle", False)
        if selector:
            await tab.page.wait_for_selector(selector, timeout=timeout)
        elif text:
            await tab.page.wait_for_function(
                "text => document.body && document.body.innerText.includes(text)",
                arg=text,
                timeout=timeout,
            )
        if network_idle:
            await tab.page.wait_for_load_state("networkidle", timeout=timeout)
        if not selector and not text and not network_idle:
            raise ValueError("wait_for requires selector, text, or network_idle")
        return {"waited": True}

    async def _click(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=False)
        locator = await self._locator(tab, arguments)
        await locator.click(timeout=self._timeout_ms())
        return {"clicked": True}

    async def _type(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=False)
        locator = await self._locator(tab, arguments)
        value = arguments.get("text")
        if not isinstance(value, str):
            raise ValueError("type requires text")
        if _boolean_value(arguments, "clear", True):
            await locator.fill(value, timeout=self._timeout_ms())
        else:
            await locator.press_sequentially(value, timeout=self._timeout_ms())
        if _boolean_value(arguments, "submit", False):
            await locator.press("Enter")
        return {"typed": True}

    async def _hover(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=False)
        locator = await self._locator(tab, arguments)
        await locator.hover(timeout=self._timeout_ms())
        return {"hovered": True}

    async def _scroll(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=False)
        delta_x = _bounded(_int(arguments, "delta_x", 0), -100_000, 100_000)
        delta_y = _bounded(_int(arguments, "delta_y", 300), -100_000, 100_000)
        selector = await self._selector(tab, arguments, required=False)
        if selector:
            await tab.page.locator(selector).evaluate(
                "(el, delta) => el.scrollBy(delta[0], delta[1])", [delta_x, delta_y]
            )
        else:
            await tab.page.evaluate(
                "delta => window.scrollBy(delta[0], delta[1])", [delta_x, delta_y]
            )
        return {"delta_x": delta_x, "delta_y": delta_y}

    async def _select_option(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> list[str]:
        tab = await self._tab(context, arguments, create=False)
        locator = await self._locator(tab, arguments)
        value, label = _text(arguments, "value", ""), _text(arguments, "label", "")
        if bool(value) == bool(label):
            raise ValueError("select_option requires exactly one of value or label")
        if value:
            selected = await locator.select_option(value=value, timeout=self._timeout_ms())
        else:
            selected = await locator.select_option(label=label, timeout=self._timeout_ms())
        return cast(list[str], selected)

    async def _press_key(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=False)
        key = _text(arguments, "key", "")
        if not key or len(key) > 64:
            raise ValueError("A bounded key is required")
        modifiers = {"alt": "Alt", "ctrl": "Control", "shift": "Shift", "meta": "Meta"}
        raw_modifiers = arguments.get("modifiers")
        parts: list[str] = []
        if isinstance(raw_modifiers, list):
            for modifier in raw_modifiers:
                if not isinstance(modifier, str) or modifier.lower() not in modifiers:
                    raise ValueError("Unsupported key modifier")
                parts.append(modifiers[modifier.lower()])
        parts.append(key)
        await tab.page.keyboard.press("+".join(parts))
        return {"pressed": True}

    async def _element_info(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=False)
        selector = await self._selector(tab, arguments, required=True)
        locator = tab.page.locator(selector)
        result: dict[str, Any] = {
            "selector": selector,
            "visible": await locator.is_visible(),
            "text": _limit(await locator.text_content(), self.settings.max_read_text_characters),
            "value": _limit(
                str(await locator.evaluate("el => el.value == null ? '' : el.value")), 4000
            ),
            "outer_html": _limit(str(await locator.evaluate("el => el.outerHTML")), 8000),
            "attributes": await locator.evaluate(
                "el => Object.fromEntries(Array.from(el.attributes).map(a => [a.name, a.value]))"
            ),
        }
        box = await locator.bounding_box()
        if box is not None:
            result["bounds"] = box
        return result

    async def _element_ref(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=False)
        selector = _text(arguments, "selector", "")
        if not selector:
            raise ValueError("get_element_ref requires selector")
        locator = tab.page.locator(selector)
        generation = await self._dom_generation(tab)
        reference = "el-" + str(uuid.uuid4())
        tab.refs[reference] = _ElementRef(selector, generation)
        return {
            "element_ref_id": reference,
            "selector": selector,
            "tab_id": tab.id,
            "url": tab.page.url,
            "text": _limit(await locator.text_content(), self.settings.max_read_text_characters),
            "outer_html": _limit(str(await locator.evaluate("el => el.outerHTML")), 8000),
        }

    async def _screenshot(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=False)
        if context.workspace is None:
            raise RuntimeError("Screenshot requires a session workspace")
        full_page = _boolean_value(arguments, "full_page", False)
        if full_page:
            pixels = await tab.page.evaluate(
                "() => Math.max(document.documentElement.scrollWidth, innerWidth)"
                " * Math.max(document.documentElement.scrollHeight, innerHeight)"
            )
            if int(pixels) > 16_777_216:
                raise ValueError("Full-page screenshot exceeds the pixel limit")
        content = await tab.page.screenshot(full_page=full_page, timeout=self._timeout_ms())
        if len(content) > self.settings.max_screenshot_bytes:
            raise ValueError("Screenshot exceeds the configured byte limit")
        relative = f".snz1dp/browser/{uuid.uuid4()}.png"
        try:
            await asyncio.to_thread(self._persist_screenshot, context.workspace, relative, content)
        except Exception as exc:
            raise RuntimeError("Unable to persist screenshot in the session workspace") from exc
        viewport = tab.page.viewport_size
        result = await self._tab_json(tab, tab.session.active_tab)
        result.update(
            path=relative,
            bytes=len(content),
            format="png",
            viewport_width=viewport["width"],
            viewport_height=viewport["height"],
        )
        return result

    async def _add_image_understanding(
        self, context: ToolExecutionContext, result: dict[str, Any], arguments: Mapping[str, Any]
    ) -> None:
        understanding_arguments = {
            "path": result["path"],
            "input": _text(arguments, "input", ""),
            "screenshot_width_px": result["viewport_width"],
            "screenshot_height_px": result["viewport_height"],
        }
        try:
            result["image_understanding"] = await self.image_understanding.understand_screenshot(
                context, understanding_arguments
            )
        except Exception as exc:
            result["image_understanding_error"] = self._safe_error(exc)

    async def _read_text(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=False)
        maximum = _bounded(
            _int(arguments, "max_characters", self.settings.max_read_text_characters),
            1,
            self.settings.max_read_text_characters,
        )
        selector = await self._selector(tab, arguments, required=False)
        text = await tab.page.locator(selector or "body").inner_text()
        return {
            "text": _limit(text, maximum),
            "truncated": text is not None and len(text) > maximum,
        }

    async def _read_console(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        tab = await self._tab(context, arguments, create=False)
        limit = _bounded(
            _int(arguments, "limit", self.settings.max_console_messages),
            1,
            self.settings.max_console_messages,
        )
        entries = list(tab.console)
        messages: list[dict[str, str]] = []
        characters = 0
        truncated = False
        for entry in entries[-limit:]:
            if characters + len(entry.text) > 64_000:
                truncated = True
                break
            messages.append({"type": entry.type, "text": entry.text})
            characters += len(entry.text)
        return {"messages": messages, "truncated": truncated or tab.console_overflow}

    async def _execute_js(self, context: ToolExecutionContext, arguments: Mapping[str, Any]) -> Any:
        if not self.settings.execute_js_enabled:
            raise PermissionError("execute_js requires BrowserSettings.execute_js_enabled=True")
        tab = await self._tab(context, arguments, create=False)
        code = _text(arguments, "code", "")
        if not code or len(code) > 100_000:
            raise ValueError("A non-empty, bounded JavaScript expression is required")
        value = await tab.page.evaluate(code)
        converted = json.loads(json.dumps(value))
        if len(json.dumps(converted, separators=(",", ":"))) > 100_000:
            raise ValueError("JavaScript result exceeds the size limit")
        return converted

    async def _tabs(self, context: ToolExecutionContext) -> dict[str, Any]:
        session = await self._session(context, create=False)
        result: dict[str, Any] = {"tabs": []}
        if session is not None:
            result["tabs"] = [
                await self._tab_json(tab, session.active_tab) for tab in session.tabs.values()
            ]
            if session.active_tab is not None:
                result["active_tab_id"] = session.active_tab
        result.update(browser_type="chromium", mode=self.settings.mode.name)
        return result

    async def _session(
        self, context: ToolExecutionContext, *, create: bool
    ) -> _SessionPages | None:
        key = self._key(context)
        existing = self._sessions.get(key)
        if existing is not None:
            if not existing.valid or self._browser is None or not self._browser.is_connected():
                self._sessions.pop(key, None)
                await self._close_quietly(existing.context)
                if self.settings.mode is BrowserMode.REMOTE_CDP:
                    self._remote_disconnected = True
            else:
                return existing
        if not create:
            return None
        await self._ensure_browser()
        self._remote_disconnected = False
        browser_context = await self._browser.new_context(
            viewport={"width": 1280, "height": 720}, accept_downloads=False
        )
        browser_context.set_default_timeout(self._timeout_ms())

        async def route_request(route: Any) -> None:
            if await self.network_policy.is_allowed(route.request.url):
                await route.continue_()
            else:
                await route.abort("blockedbyclient")

        await browser_context.route("**/*", route_request)
        created = _SessionPages(browser_context)
        self._sessions[key] = created
        return created

    async def _ensure_browser(self) -> None:
        if self._browser is not None and self._browser.is_connected():
            return
        now = time.monotonic()
        if now < self._next_connection_attempt:
            raise RuntimeError("Browser connection backoff is active")
        try:
            if self._playwright is None:
                async_playwright = import_module("playwright.async_api").async_playwright
                self._playwright = await async_playwright().start()
            chromium = self._playwright.chromium
            if self.settings.mode is BrowserMode.HEADLESS:
                options: dict[str, Any] = {
                    "headless": True,
                    "args": list(self.settings.launch_arguments),
                }
                if self.settings.executable_path is not None:
                    options["executable_path"] = str(self.settings.executable_path)
                self._browser = await chromium.launch(**options)
            else:
                self._browser = await chromium.connect_over_cdp(
                    self.settings.remote_endpoint,
                    headers=dict(self.settings.remote_headers),
                    timeout=self._timeout_ms(),
                )
            connected_browser = self._browser

            def disconnected(_: Any = None) -> None:
                if self._browser is not connected_browser:
                    return
                self._remote_disconnected = self.settings.mode is BrowserMode.REMOTE_CDP
                self._next_connection_attempt = time.monotonic() + 30
                for session in self._sessions.values():
                    session.valid = False

            self._browser.on("disconnected", disconnected)
            self._next_connection_attempt = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._next_connection_attempt = now + 30
            message = (
                "Unable to connect to remote Chromium at " + self.settings.redacted_remote_host()
                if self.settings.mode is BrowserMode.REMOTE_CDP
                else "Unable to start fixed local Chromium"
            )
            raise RuntimeError(message) from exc

    async def _create_tab(self, session: _SessionPages) -> _Tab:
        page = await session.context.new_page()
        page.set_default_timeout(self._timeout_ms())
        tab = _Tab(session, str(uuid.uuid4()), page)
        session.tabs[tab.id] = tab
        session.active_tab = tab.id

        def console(message: Any) -> None:
            if len(tab.console) >= self.settings.max_console_messages:
                tab.console.popleft()
                tab.console_overflow = True
            tab.console.append(_ConsoleEntry(message.type, _limit(message.text, 4000)))

        page.on("console", console)
        page.on("close", lambda _: self._invalidate_tab(tab))
        page.on("crash", lambda _: self._invalidate_tab(tab))
        page.on("domcontentloaded", lambda _: self._invalidate_refs(tab))
        await self._navigate_tab(tab, "about:blank")
        return tab

    async def _tab(
        self, context: ToolExecutionContext, arguments: Mapping[str, Any], *, create: bool
    ) -> _Tab:
        session = await self._session(context, create=create)
        if session is None:
            raise RuntimeError("No browser tab exists; call tabs_create first")
        requested = _text(arguments, "tab_id", "")
        identifier = requested or session.active_tab
        tab = session.tabs.get(identifier or "")
        if tab is None or not session.valid or tab.page.is_closed():
            raise ValueError("Tab is not owned by this session or is no longer valid")
        return tab

    async def _locator(self, tab: _Tab, arguments: Mapping[str, Any]) -> Any:
        return tab.page.locator(await self._selector(tab, arguments, required=True))

    async def _selector(self, tab: _Tab, arguments: Mapping[str, Any], *, required: bool) -> str:
        reference = _text(arguments, "element_ref_id", "")
        if reference:
            element = tab.refs.get(reference)
            if element is None or element.generation != await self._dom_generation(tab):
                raise ValueError("Element reference is stale or belongs to another tab/session")
            return element.selector
        selector = _text(arguments, "selector", "")
        if len(selector) > 2000 or (required and not selector):
            raise ValueError("A valid selector or element_ref_id is required")
        return selector

    async def _dom_generation(self, tab: _Tab) -> int:
        return int(await tab.page.evaluate(DOM_GENERATION_SCRIPT))

    async def _navigate_tab(self, tab: _Tab, url: str) -> None:
        validated = await self.network_policy.validate(url)
        if url != "about:blank" or tab.page.url != "about:blank":
            await tab.page.goto(validated, timeout=self._timeout_ms())
        await self.network_policy.validate(tab.page.url)
        self._invalidate_refs(tab)

    def _invalidate_tab(self, tab: _Tab) -> None:
        self._invalidate_refs(tab)
        tab.session.tabs.pop(tab.id, None)
        if tab.session.active_tab == tab.id:
            tab.session.active_tab = next(iter(tab.session.tabs), None)

    @staticmethod
    def _invalidate_refs(tab: _Tab) -> None:
        tab.refs.clear()

    def _invalidate(self, key: str) -> None:
        session = self._sessions.get(key)
        if session is not None:
            session.valid = False

    async def _tab_json(self, tab: _Tab, active: str | None) -> dict[str, Any]:
        return {
            "tab_id": tab.id,
            "title": _limit(await tab.page.title(), 1000),
            "url": tab.page.url,
            "active": tab.id == active,
        }

    async def close_session(self, session_id: str) -> None:
        if self._closed:
            return
        async with self._lock:
            for key in list(self._sessions):
                if not key.endswith("\0" + session_id):
                    continue
                removed = self._sessions.pop(key)
                await self._close_quietly(removed.context)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        async with self._lock:
            for session in self._sessions.values():
                await self._close_quietly(session.context)
            self._sessions.clear()
            if self._browser is not None and self.settings.mode is BrowserMode.HEADLESS:
                await self._close_quietly(self._browser)
            self._browser = None
            if self._playwright is not None:
                await self._close_quietly(self._playwright)
            self._playwright = None
        await self.image_understanding.aclose()

    def _key(self, context: ToolExecutionContext) -> str:
        if context.flow_id is None or not context.flow_id.strip() or not context.session_id.strip():
            raise ValueError("Browser tools require trusted flow_id and session_id")
        return context.flow_id + "\0" + context.session_id

    def _safe_error(self, error: BaseException) -> str:
        message = str(error) or type(error).__name__
        if self.settings.remote_endpoint is not None:
            message = message.replace(self.settings.remote_endpoint, "[redacted-endpoint]")
        for secret in self.settings.remote_headers.values():
            if secret:
                message = message.replace(secret, "[redacted-header]")
        return _limit(redact(message), 8192)

    def _timeout_ms(self) -> float:
        return self.settings.action_timeout * 1000

    @staticmethod
    def _persist_screenshot(workspace: Path, relative: str, content: bytes) -> Path:
        root = workspace.expanduser().resolve(strict=True)
        if not root.is_dir():
            raise ValueError("Workspace is not a directory")
        output = (root / relative).resolve()
        if not output.is_relative_to(root):
            raise ValueError("Screenshot path escapes the workspace")
        output.parent.mkdir(parents=True, exist_ok=True)
        if not output.parent.resolve().is_relative_to(root):
            raise ValueError("Screenshot directory escapes the workspace")
        with output.open("xb") as stream:
            stream.write(content)
        return output

    @staticmethod
    async def _close_quietly(resource: Any) -> None:
        try:
            close = getattr(resource, "stop", None) or getattr(resource, "close", None)
            if close is not None:
                result = close()
                if hasattr(result, "__await__"):
                    await result
        except Exception:
            return


def register_browser_tools(
    plugins: PluginManager, rest_client: RestClient, settings: BrowserSettings
) -> BrowserRuntime | None:
    if settings.mode is BrowserMode.DISABLED:
        return None
    runtime = BrowserRuntime(settings, rest_client)
    runtime.register(plugins)
    return runtime


async def _wait_for_cancellation(context: ToolExecutionContext) -> None:
    event = asyncio.Event()
    while not _cancelled(context):
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(event.wait(), timeout=0.05)


def _cancelled(context: ToolExecutionContext) -> bool:
    return context.cancellation_requested is not None and context.cancellation_requested()


async def _suppress_cancelled(task: asyncio.Future[Any]) -> None:
    with contextlib.suppress(asyncio.CancelledError):
        await task


def _selector_properties() -> dict[str, Any]:
    return {"selector": _string(), "element_ref_id": _string(), "tab_id": _string()}


def _string() -> dict[str, str]:
    return {"type": "string"}


def _integer() -> dict[str, str]:
    return {"type": "integer"}


def _boolean() -> dict[str, str]:
    return {"type": "boolean"}


def _text(arguments: Mapping[str, Any], name: str, fallback: str) -> str:
    value = arguments.get(name)
    return value if isinstance(value, str) else fallback


def _int(arguments: Mapping[str, Any], name: str, fallback: int) -> int:
    value = arguments.get(name)
    return value if isinstance(value, int) and not isinstance(value, bool) else fallback


def _boolean_value(arguments: Mapping[str, Any], name: str, fallback: bool) -> bool:
    value = arguments.get(name)
    return value if isinstance(value, bool) else fallback


def _bounded(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, value))


def _limit(value: str | None, maximum: int) -> str:
    if value is None:
        return ""
    return value if len(value) <= maximum else value[:maximum] + "..."
