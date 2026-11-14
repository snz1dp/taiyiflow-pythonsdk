# TaiyiFlow Python SDK

> 面向 Python 后台、CLI 与服务的 TaiyiFlow SDK：通过 `TaiyiAgent` 主动驱动平台智能体，通过 `DigitalWorker` 构建可接活、请示、回报和恢复的数字员工。

**当前状态：Phase 0–10 离线实现完成，真实平台与目标部署环境仍待使用方验收。** 自动化测试、契约 fixtures 和本地假服务不能替代真实对话、媒体 Flow、平台技能、Chromium/CDP、MCP 服务端、协同 Worker、第三方身份与 SSO 验收。

## 特性

- Python 3.11+、`asyncio`、异步迭代器与异步上下文管理器。
- 一个 `TaiyiAgent` 绑定一个 Flow，多会话并行且独立取消、审批和恢复。
- WebSocket/SSE 流式对话、REST 能力面与四类 Hook。
- File/Search/Shell、媒体理解、SKILL、可选 Chromium、Streamable HTTP/stdio MCP。
- `pause`/`cancel`/`attach`/`resume`/`retry`/`repeat` 与有界结果 Outbox。
- 可选协同连接、DigitalWorker、身份 SPI、平台审批、加密凭证与受控子任务。

分发名为 `taiyiflow-sdk`，导入名为 `taiyiagent`。

## 安装

```bash
python -m pip install -e .
```

可选依赖：

```bash
python -m pip install -e '.[browser]'  # Playwright 浏览器
python -m pip install -e '.[worker]'   # Worker 加密凭证
python -m pip install -e '.[dev]'      # 开发与测试
```

## 快速开始

```python
import asyncio
import os

from taiyiagent import TaiyiAgent


async def main() -> None:
    async with TaiyiAgent(
        base_url=os.environ["TAIYIFLOW_BASE_URL"],
        api_key=os.environ["TAIYIFLOW_API_KEY"],
        flow_id=os.environ.get("TAIYIFLOW_FLOW_ID"),
    ) as agent:
        session = agent.session()
        async for event in session.chat("你好，请介绍一下你自己"):
            if event.message_text is not None:
                print(event.message_text, end="", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
```

`flow_id` 可省略，首次执行从 `/api/v1/config` 异步解析服务端默认 Flow。不要把真实密钥写入源码、示例、配置仓库或日志。

## 文档

完整中文开发手册位于 [`docs/`](docs/index.md)：

- [快速开始](docs/getting-started.md)
- [智能体与会话](docs/agent-and-session.md)
- [Hook 与权限审批](docs/hooks.md)
- [内置工具](docs/plugins.md)、[技能](docs/skills.md)、[浏览器](docs/browser.md)、[MCP](docs/mcp.md)
- [执行控制与恢复](docs/execution-control.md)
- [DigitalWorker](docs/digital-worker.md) 与 [协同插件](docs/collaboration.md)
- [部署](docs/deployment.md)、[质量验证与验收](docs/verification.md)、[协议参考](docs/protocol.md)

本地构建文档站点：

```bash
npm --prefix docs install
npm --prefix docs run docs:dev
npm --prefix docs run docs:build
```

## 安全与可靠性边界

- 默认权限为 `CONFIRM`；Shell 白名单为空，File/Search 限定 workspace。
- Shell 工作目录与 MCP cwd 不是操作系统沙盒，部署者仍需最小权限隔离。
- 工具结果只通过 `POST /api/v1/agents/client/call/result` 回报。
- `attach`/`resume` 不创建新 run；`retry`/`repeat` 才明确创建。
- 状态未知或副作用不确定时禁止自动重跑；Outbox 只补传结果，不重新执行工具。
- 协同 WS 默认关闭；同一 workerId 只运行一个 Worker 实例。
- API Key、JWT、persistentToken、Authorization 与敏感 env 不写入普通 state、workspace 或日志。

## 开发

```bash
make install-dev
make check
make build
make docs-dev
```

也可直接运行 `pytest`、`ruff check .`、`ruff format --check .`、`mypy src` 和 `python -m build`。使用已有虚拟环境时可执行 `make PYTHON=.venv/bin/python check`。完整目标通过 `make help` 查看。

开发约束见 [AGENTS.md](AGENTS.md)。完整范围、阶段状态、协议差异与验收条件以 [plans/001-python-sdk-implementation.md](plans/001-python-sdk-implementation.md) 为唯一权威计划。

当前版本为 `0.1.0`，处于发布候选验收阶段，不表示已经发布。未经明确要求，不创建 Git 提交、不推送、不发布包、不部署 Worker。

## License

TBD
