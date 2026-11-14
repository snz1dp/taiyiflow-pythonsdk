# 协同插件

`CollaborationPlugin` 提供底层协同 WebSocket。它默认不启动，只有显式调用 `await collaboration.start()` 后才建立被动连接。

## 生命周期

```python
from taiyiagent import CollaborationPlugin

collaboration = CollaborationPlugin(
    base_url="https://your-platform.example.com",
    api_key="your-api-key",
    client_id="client-1",
    device_id="device-1",
)

collaboration.on_task_dispatch(handle_task)
collaboration.on_task_cancel(handle_cancel)
collaboration.on_task_snapshot(handle_snapshot)

await collaboration.start()
try:
    ...
finally:
    await collaboration.aclose()
```

生产代码应从环境变量或安全配置读取密钥。

## 回调

| 方法 | 用途 |
|------|------|
| `on_task` / `on_task_dispatch` | 任务下发；两种形式二选一 |
| `on_task_cancel` | 取消任务 |
| `on_task_snapshot` | 重连任务快照对账 |
| `on_release` | release push |
| `on_connection_ready` | 连接 ready |
| `on_kicked` | 被平台踢出 |

协同封套为 `{type, event, payload}`，握手传 client/device/instance 信息并等待 `connection.ready`，没有额外 register 首帧。

## 重连与任务对账

插件实现心跳、入站静默检测、有界重连、任务版本去重、cancel/release 和 SchedulePlugin。重连快照中：

- 平台明确 pending 且未取消的任务才可由 Worker 自动进入 handler。
- dispatched、acknowledged、running 等状态只对账，不重放。
- 已取消或重复版本不得再次执行。
- 平台不可达或状态未知时保持不确定，不猜测成功或失败。

大多数常驻数字员工应直接使用 [DigitalWorker](digital-worker.md)，而不是自行重组任务状态机。

## 定时任务

`SchedulePlugin` 与 `ScheduleTools` 基于协同连接和平台 Schedule API。后端没有 Java `ScheduleClient.getRun()` 对应路由，Python SDK 仅提供已有的 runs 列表与 report 路由，不增加伪能力。
