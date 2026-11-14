# 智能体与会话

`TaiyiAgent` 是 SDK 主入口，一个实例绑定一个业务 Flow；`AgentSession` 表示独立会话，提供流式对话、执行控制与审批裁决。SDK 只提供 `asyncio` 原生 API，不维护隐式创建事件循环的同步镜像。

## 创建 TaiyiAgent

```python
from taiyiagent import TaiyiAgent

agent = TaiyiAgent(
    base_url="https://your-platform.example.com",
    api_key="your-api-key",
    flow_id="my-agent-flow",
)
```

推荐使用 `async with TaiyiAgent(...)` 管理生命周期；手动创建时必须在最后调用 `await agent.aclose()`。

### 连接与身份

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `base_url` | 必填 | 平台地址 |
| `api_key` | 与 `access_token` 二选一 | 静态 API Key |
| `access_token` | 无 | 动态令牌函数，每次请求读取最新值 |
| `flow_id` | 服务端默认 Flow | 首次执行异步解析 `/api/v1/config`；显式值优先 |
| `client_id` | 本地持久化标识 | 客户端标识 |
| `device_id` | 回退 `client_id` | 设备标识 |
| `identity_path` | 默认身份文件位置 | 自定义客户端标识文件 |
| `timeout` | 30 秒 | REST 请求超时 |
| `connect_timeout` | 10 秒 | 流连接超时 |
| `credential_headers` | 空 | 附加凭证头 |

### 会话与流

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_sessions` | 100 | 本地会话上限 |
| `event_queue_size` | 256 | 单会话有界事件队列容量 |
| `stream_protocol` | `AUTO` | `AUTO`、`SSE` 或 `WEBSOCKET` |
| `stream_hooks` | 空 | 全局流 Hook 列表 |
| `session_hooks` | 空 | 全局会话生命周期 Hook 列表 |

AUTO 读取服务端 `stream_protocol`。WebSocket 建连失败可以回退 SSE；已建立执行不会在活跃流中盲目切换协议并重复启动。

### 权限与工具

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `permission_policy` | `CONFIRM` | `AUTO`、`CONFIRM` 或 `READONLY` |
| `permission_hook` | 无 | 替换默认策略入口的策略级 Hook |
| `permission_hooks` | 空 | 默认策略之后追加的权限 Hook 列表 |
| `permission_timeout` | 300 秒 | 本地 ASK 等待超时 |
| `workspace` | 当前目录 | File/Search/Shell 与受控子进程工作区 |
| `allowed_shell_commands` | 空 | Shell 可执行命令白名单 |
| `enable_builtin_tools` | `True` | 注册内置 File/Search/Shell 等工具 |
| `enable_media_tools` | 跟随内置工具 | 独立控制媒体理解工具 |
| `tool_hooks` | 空 | 全局工具 Hook 列表 |
| `skill_supplier` | 无 | 技能供应器 |
| `default_skill_names` | 空 | 默认发送全文的技能 |
| `browser_settings` | `DISABLED` | 可选 Chromium 配置 |

`permission_hook` 与 `permission_hooks` 用途不同：前者替换默认本地策略等待，适合 Worker 平台审批；后者是在默认策略后追加观察者或裁决者。详见 [Hook 与权限审批](hooks.md)。

### 断线恢复与 Outbox

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `recovery_policy` | `MANUAL` | `MANUAL` 或 `AUTO` |
| `recovery_max_attempts` | 2 | 自动恢复最大尝试次数 |
| `recovery_min_backoff` | 0.5 秒 | 恢复退避下限 |
| `recovery_max_backoff` | 5 秒 | 恢复退避上限 |
| `auto_restart_on_execution_loss` | `True` | 仅在确认执行丢失且无副作用不确定时允许安全 start |
| `state_dir` | 无 | 启用文件 Outbox；与 `outbox` 互斥 |
| `outbox` | 内存 Outbox | 注入自定义有界结果 Outbox |

恢复细节见 [执行控制与恢复](execution-control.md)。

## AgentSession API

### 创建会话与对话

```python
session = agent.session()  # 随机 session_id
session = agent.session("session-1")  # 指定 session_id

stream = session.chat("生成摘要")
stream = session.chat(
    "总结附件",
    tweaks={"temperature": 0.2},
    input_files=["/server/upload/report.pdf"],
    request_type="agent",
)
async for event in stream:
    ...
```

高级调用可传入 `ChatRequest`。传入完整请求时不能再组合 `tweaks`、`input_files` 或 `request_type` 覆盖参数。

### 执行控制

| 方法 | 语义 |
|------|------|
| `pause()` | 请求服务端暂停当前 run |
| `cancel()` | 请求服务端取消并清理本地流资源 |
| `attach()` | 重接原 run，不创建新执行 |
| `resume(checkpoint=None)` | 恢复可续跑 checkpoint，不创建新执行 |
| `retry()` | 明确创建新 run，按持久化状态续做或重发输入 |
| `repeat()` | 明确创建新 run，复用上一轮输入模板 |
| `interrupt()` | 只取消本地执行和流资源 |
| `recover_report(entry)` | 仅重交已保存的检查点结果，不重新执行工具 |

### 审批、状态与等待

```python
accepted = session.approve(call_id)
rejected = session.deny(call_id, "not allowed")

finished = await session.wait(timeout=30)
print(session.session_id, session.state, session.run_context.run_id)
```

未知或重复审批回执返回 `False`。`wait()` 超时返回 `False`；调用方任务被取消时按 asyncio 语义传播 `CancelledError`。

### 会话级 Hook

```python
session = agent.session(
    stream_hooks=[stream_hook],
    tool_hooks=[tool_hook],
    permission_hooks=[permission_hook],
    session_hooks=[session_hook],
)
```

也可通过 `agent.on_stream()`、`agent.on_tool()`、`agent.on_permission()`、`agent.on_session()` 注册全局 Hook。

## 多会话并行

```python
import asyncio


async def consume(session, message: str) -> None:
    async for event in session.chat(message):
        if event.message_text is not None:
            print(f"[{session.session_id}] {event.message_text}", end="")


async with TaiyiAgent(...) as agent:
    sessions = [agent.session(f"session-{index}") for index in range(3)]
    await asyncio.gather(
        *(consume(session, f"任务 {index}") for index, session in enumerate(sessions))
    )
```

每个会话拥有独立连接、有界队列、RunContext、审批和取消状态。慢消费者产生背压，不会切换成无界缓冲。

## REST 能力面

| 属性 | 用途 |
|------|------|
| `session_api` | 平台会话与消息查询；`sessions` 是本地会话管理器 |
| `skills` / `memories` / `config` | 技能、记忆与服务端配置 |
| `files` | 文件上传与下载 |
| `flow_metadata_api` | Flow Metadata；也可使用 `get_flow_metadata()` 缓存接口 |
| `mcp_servers` | 平台 MCP server 配置 |
| `collaboration_tasks` / `schedules` | 协同任务与定时任务 |
| `execution` | run 执行控制与可恢复 checkpoint |
| `rest` | 底层异步 REST 客户端 |

后端没有 Java `ScheduleClient.getRun()` 对应路由，Python SDK 不提供该伪能力。
