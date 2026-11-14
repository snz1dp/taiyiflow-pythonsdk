# MCP 接入

SDK 支持 Streamable HTTP 与 stdio 两种 MCP 形态。注册后的工具以 `c_{server}_{tool}` 命名，与内置和自定义工具共用权限、Hook、并发、取消和 REST 结果回报链。

## Streamable HTTP MCP

```python
from taiyiagent import StreamableHttpMcpClient

client = StreamableHttpMcpClient(
    "https://mcp.example.com/mcp",
    headers={"Authorization": "Bearer ..."},
)
names = await client.register_tools("business", agent.plugins)
```

- `server_url` 只接受 HTTP/HTTPS。
- 支持 JSON 与 SSE 响应、MCP session header 和响应大小上限。
- 注册到 `agent.plugins` 后，客户端由插件管理器统一关闭。
- 取消和超时不会隐式重放 `tools/call`。

真实服务端兼容性仍需使用方联调。

## stdio MCP

```python
from pathlib import Path
from taiyiagent import StdioMcpManager, StdioMcpPolicy, StdioMcpServerConfig

policy = StdioMcpPolicy(
    workspace=Path("./workspace"),
    allowed_commands=frozenset({"my-mcp-server"}),
    allowed_environment=frozenset({"MCP_MODE"}),
)
manager = StdioMcpManager(policy)
config = StdioMcpServerConfig(
    command="my-mcp-server",
    args=("--readonly",),
    env={"MCP_MODE": "safe"},
    cwd=Path("./workspace"),
)
names = await manager.register_tools("local", config, agent.plugins)
```

### 默认拒绝项

- 命令默认全部拒绝，必须加入 `allowed_commands`。
- 显式环境变量默认全部拒绝，必须加入 `allowed_environment`。
- 配置解析不经过 shell，不解释 shell 引号、转义、管道或重定向。
- cwd 必须位于 workspace；该限制仍不是操作系统沙盒。
- server 数、参数、帧、输出和进程寿命均有上限。

敏感 env 不持久化、不写日志。stdio 子进程由 SDK 管理，关闭与取消按资源所有权清理。

## 平台 MCP 配置

`agent.mcp_servers` 提供平台 MCP server 配置管理。平台配置只决定可用来源，不会绕过本地权限链或自动扩大请求工具集合。

Worker 子任务的 MCP resolver 只能解析能力快照允许的 server 名称；敏感配置只用于本次请求，不进入能力快照。
