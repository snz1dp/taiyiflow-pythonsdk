# 钩子（Hook）体系与权限审批

Hook 是 SDK 的扩展点。流 Hook 与会话 Hook 观察同一事件流和生命周期，不创建竞争消费通道；工具与权限 Hook 位于统一执行链中。

## 四类 Hook

| Hook | 主要方法 | 用途 |
|------|----------|------|
| `StreamHook` | `on_chunk`、`on_event`、`on_error`、`on_close` | 观察流事件 |
| `ToolExecutionHook` | `before`、`takeover`、`after` | 修改、接管或观察工具调用 |
| `PermissionHook` | `on_permission` | 返回 `ALLOW`、`DENY` 或 `ASK` |
| `SessionHook` | `on_start`、`on_finish`、`on_interrupt`、`on_close` | 观察会话生命周期 |

所有 Hook 方法均为异步方法。Hook 异常不会另建无界后台任务；资源清理由所属 Agent 或 Session 负责。

## 流观察

```python
from taiyiagent import StreamHook


class LoggingStreamHook(StreamHook):
    async def on_chunk(self, session_id: str, chunk: str) -> None:
        print(chunk, end="", flush=True)

    async def on_error(self, session_id: str, error: BaseException) -> None:
        print(f"stream failed: {type(error).__name__}")


agent.on_stream(LoggingStreamHook())
```

即使注册 StreamHook，调用方仍通过 `async for event in session.chat(...)` 消费同一事件流。

## 工具执行链

```text
默认权限策略或 permission_hook
    -> 追加 permission_hooks
    -> ToolExecutionHook.before
    -> takeover 或内置/自定义工具
    -> ToolExecutionHook.after
    -> REST 工具结果回报
```

`before` 返回修改后的 `ToolCall`；`takeover` 只有返回非 `None` 才接管；`after` 可观察或修改 `ToolResult`。

```python
from taiyiagent import ToolExecutionHook, ToolResult


class ToolHook(ToolExecutionHook):
    async def takeover(self, session_id: str, tool_call):
        if tool_call.name != "business_lookup":
            return None
        return ToolResult(
            tool_call_id=tool_call.id,
            success=True,
            result={"source": "business-system"},
        )


agent.on_tool(ToolHook())
```

工具结果只通过 `POST /api/v1/agents/client/call/result` 回报，不在对话 WebSocket 首帧之后增加回传旁路。

## 权限策略

| 策略 | 只读工具 | 写操作 |
|------|----------|--------|
| `READONLY` | 放行 | 拒绝 |
| `CONFIRM` | 放行 | ASK，等待裁决 |
| `AUTO` | 放行 | 放行 |

默认是 `CONFIRM`。写工具包括 Shell、写文件、文件编辑、技能管理与浏览器变更操作等；浏览器读取工具按只读处理。

```python
from taiyiagent import PermissionDecision, PermissionHook


class Permission(PermissionHook):
    async def on_permission(self, session_id: str, tool_call):
        if tool_call.name == "shell":
            return PermissionDecision.DENY
        return PermissionDecision.ALLOW


agent.on_permission(Permission())
```

### 本地 ASK 审批

默认策略返回 ASK 后，调用进入会话级等待：

```python
session.approve(call_id)
session.deny(call_id, "not allowed")
```

批准后才执行；拒绝或超时形成失败工具结果并回报平台。未知或重复回执返回 `False`。

### 策略级 permission_hook

构造 `TaiyiAgent(permission_hook=...)` 会替换默认本地策略入口，适合把审批委托给平台：

```python
agent = TaiyiAgent(
    ...,
    permission_policy="confirm",
    permission_hook=platform_permission_hook,
)
```

这与 `on_permission()` / `permission_hooks` 不同，后者是在默认策略之后追加 Hook。DigitalWorker 使用策略级入口接入平台审批，避免平台裁决之前被本地 ASK 等待阻断。

## 注册自定义工具

```python
from taiyiagent import ToolExecutionContext, ToolResult


async def business_lookup(context: ToolExecutionContext, call) -> ToolResult:
    return ToolResult(
        tool_call_id=call.id,
        success=True,
        result={"session": context.session_id},
    )


agent.register_tool("business_lookup", business_lookup)
```

自定义工具与内置工具、HTTP MCP、stdio MCP 使用同一权限、Hook、并发、取消和回报链。自定义工具若产生外部副作用，必须自行保证幂等键与平台对账，不能依赖结果回报失败后重跑来修复。

## Worker 审批

Worker 默认使用平台审批通道。平台通道需要 `approval-handler.on_permission()`；本地通道还需要 `approval-audit.record()` 完成平台审计。缺少必要适配器时写工具失败关闭，不会静默降级为无审计的本地批准。
