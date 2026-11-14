# 内置工具插件

内置工具通过 `PluginManager` 注册，实际调用统一经过 ToolDispatcher。默认启用内置工具，但 Shell 白名单为空，写操作仍受权限策略约束。

## 能力概览

| 能力 | 约束 |
|------|------|
| File | workspace 内读写与细粒度编辑，阻断路径穿越和符号链接逃逸 |
| Search | workspace 内文本/正则搜索，限制结果规模 |
| Shell | 显式命令白名单，不使用 `shell=True`，支持超时与取消 |
| 后台进程 | SDK 私有句柄、增量输出、有界缓冲和进程树清理 |
| 媒体理解 | 文档、图像、音频、视频经当前 Flow 上传后调用专用理解 Flow |
| 用户交互与辅助工具 | 按注册配置供应，不自动扩大请求权限 |

```python
from pathlib import Path
from taiyiagent import TaiyiAgent

agent = TaiyiAgent(
    ...,
    workspace=Path("./workspace"),
    allowed_shell_commands=("ls", "cat", "echo"),
    enable_builtin_tools=True,
)
```

只关闭 File/Search/Shell、保留媒体理解：

```python
agent = TaiyiAgent(
    ...,
    enable_builtin_tools=False,
    enable_media_tools=True,
)
```

## 文件与搜索边界

- 相对路径以配置的 workspace 为根，不接受逃逸 workspace 的路径。
- 符号链接解析后的真实路径仍必须位于 workspace 内。
- 工具输出与搜索结果有规模限制，不能作为无界文件传输通道。
- workspace 是应用层路径约束，不是操作系统级安全沙盒。

## Shell 与后台进程

Shell 不使用系统 shell 字符串解析，白名单按可执行命令收敛。空白名单拒绝所有命令。工作目录限制不能阻止一个已允许的程序访问其操作系统权限范围内的其他资源。

后台进程句柄是 SDK 私有句柄，不是平台 taskId，也不应被当作可长期信任的 PID。停止和清理只针对当前注册表内匹配的进程树；会话取消与 Agent/Worker 关闭会清理所属资源。

生产部署建议：

1. 仅放行业务必需命令，避免放行可再启动任意程序的通用解释器。
2. 使用独立低权限系统用户和容器/进程隔离。
3. 限制 CPU、内存、进程数、文件系统和网络权限。
4. 不把 workspace 当作凭证或长期权威业务结果的存储位置。

## 媒体理解

文档使用同步 FlowRun；图像、音频和视频使用异步任务轮询。媒体先通过当前业务 Flow 上传，再调用服务端配置的专用理解 Flow。

- FlowRun 提交结果未知时抛出不确定状态，不自动重新 POST。
- 轮询超时保留 task ID 与取消状态，不伪报成功。
- 媒体工具不会隐式要求开启 Shell，也不会提升权限策略。
- 真实媒体 Flow 与平台配置仍需使用方联调验收。

## 工具结果可靠性

工具最终结果先写入有界 Outbox，再通过 REST 回报。网络失败可补传已保存结果，补传不会重新执行工具。结果冲突或重试预算耗尽进入死信；不得通过重跑副作用操作修复回报失败。

`state_dir` 用于瞬态结果与 checkpoint 记录，不是长期任务历史或权威业务数据库。
