# 快速开始

本文帮助你运行第一个 TaiyiFlow Python SDK 程序。

## 环境要求

| 项 | 要求 |
|----|------|
| Python | 3.11+ |
| 包管理 | 支持 PEP 517/518 的 pip 环境 |
| 后端 | 可访问的 TaiyiFlow 平台服务，以及有效 API Key |
| Flow | 可显式指定；省略时使用 `/api/v1/config` 的服务端默认 Flow |

分发名为 `taiyiflow-sdk`，导入名为 `taiyiagent`。

## 安装

在源码目录安装开发版本：

```bash
python -m pip install -e '.[dev]'
```

仅安装核心运行时：

```bash
python -m pip install -e .
```

可选能力：

```bash
python -m pip install -e '.[browser]'  # Playwright Chromium
python -m pip install -e '.[worker]'   # Worker 加密凭证
```

HEADLESS 浏览器还需要调用方自行安装 Playwright Chromium；SDK 的离线测试不会下载或启动浏览器。

## 配置环境变量

```bash
export TAIYIFLOW_BASE_URL=https://your-platform.example.com
export TAIYIFLOW_API_KEY=your-api-key
export TAIYIFLOW_FLOW_ID=your-flow-id  # 可选
```

不要把真实密钥写入源码、示例、配置仓库或日志。

## 最简示例

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
        print()


if __name__ == "__main__":
    asyncio.run(main())
```

`session.chat()` 返回单一异步事件流，文本只从文本型 `data.chunk` 提取。`async with` 会在退出时关闭会话、插件和传输资源。

## 本地工具安全默认

`TaiyiAgent` 默认注册 File、Search、Shell 与媒体工具，但 Shell 命令白名单默认为空，未显式允许的命令会被拒绝。默认权限策略为 `CONFIRM`：只读工具放行，写操作进入审批。

```python
from pathlib import Path
from taiyiagent import TaiyiAgent

agent = TaiyiAgent(
    base_url="https://your-platform.example.com",
    api_key="...",
    workspace=Path("./workspace"),
    allowed_shell_commands=("ls", "cat"),
    permission_policy="confirm",
)
```

生产代码应从安全配置或环境变量读取密钥，不要照抄上述占位密钥。File/Search 的 workspace 限制和 Shell 工作目录都不是操作系统沙盒，部署时仍需最小系统权限与进程隔离。

## 运行仓库示例

```bash
python examples/minimal.py
python examples/multi_session.py
```

全部示例及额外环境变量见 [示例程序](examples.md)。

## 下一步

- 多会话与完整配置：[智能体与会话](agent-and-session.md)
- 工具接管与审批：[钩子体系与权限审批](hooks.md)
- 断线恢复与结果补传：[执行控制与恢复](execution-control.md)
- 常驻任务执行：[数字员工](digital-worker.md)
