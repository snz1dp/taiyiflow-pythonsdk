# 浏览器工具

浏览器是可选能力，默认 `DISABLED`。启用后注册 17 个 `taiyiflow-in-chrome__*` 工具，覆盖页签、导航、等待、交互、元素引用、截图、文本/控制台读取和受开关控制的 JavaScript 执行。

## 安装

```bash
python -m pip install -e '.[browser]'
```

HEADLESS 还需调用方安装 Playwright Chromium。

## HEADLESS

```python
from taiyiagent import BrowserMode, BrowserSettings, TaiyiAgent

settings = BrowserSettings(
    mode=BrowserMode.HEADLESS,
    allowed_hosts=frozenset({"example.com"}),
)

agent = TaiyiAgent(..., browser_settings=settings)
```

HEADLESS 关闭时只关闭 SDK 自己创建的 Browser。

## REMOTE_CDP

```python
import os

settings = BrowserSettings(
    mode=BrowserMode.REMOTE_CDP,
    remote_endpoint=os.environ["TAIYIFLOW_CDP_ENDPOINT"],
    remote_headers={"Authorization": os.environ["TAIYIFLOW_CDP_AUTHORIZATION"]},
)
```

- endpoint 只接受 HTTP(S)/WS(S)，不允许 URL userinfo。
- SDK 只创建并管理自己的 BrowserContext 和页面，不接管既有页签，不关闭外部 Browser。
- endpoint 与 Authorization 不出现在普通配置快照或诊断输出中。
- Worker YAML 通过环境变量名称引用 endpoint 和 Authorization，不直接保存值。

## 网络与资源策略

导航只接受 `about:blank` 和无 userinfo 的 HTTP(S) URL。默认阻断本地、私网、链路本地、组播和未指定地址，可通过以下配置收敛放行：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `allowed_hosts` | 空 | 精确主机 allowlist |
| `allowed_cidrs` | 空 | IP/CIDR allowlist |
| `action_timeout` | 15 秒 | 浏览器操作超时，最大 120 秒 |
| `max_pages_per_session` | 10 | 单会话页面上限 |
| `max_read_text_characters` | 8000 | 文本读取上限 |
| `max_console_messages` | 200 | 控制台消息上限 |
| `max_screenshot_bytes` | 10 MiB | 截图大小上限 |
| `execute_js_enabled` | `False` | JavaScript 执行开关 |

每个 flow + session 使用独立 BrowserContext、页签、console 与 element ref 状态。操作超时或取消会使对应浏览器会话失效。

截图只能写入 workspace 的 `.snz1dp/browser` 下。SDK 不提供浏览器下载与上传工具；截图理解复用媒体理解服务，不占用浏览器 action timeout。

完整示例见 `examples/browser.py`。
