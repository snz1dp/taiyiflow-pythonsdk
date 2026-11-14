# 传输协议参考

协议事实以 `../taiyiflow-dev` 后端代码为准；Java SDK 是功能与可靠性参照，不代替后端协议。Python 属性使用 `snake_case`，线上 JSON 字段和协议字符串保持原样。

## 通道与端点

| 通道 | 端点/格式 |
|------|-----------|
| 对话 WebSocket | `/api/v1/run/{flow_id}/websocket` |
| SSE | `POST /api/v1/run/{flow_id}/stream` |
| 协议配置 | `GET /api/v1/config` 的 `stream_protocol` |
| Flow Metadata | `GET /api/v1/run/{flow_id}/metadata` |
| 非流式 FlowRun | `POST /api/v1/run/{flow_id}` |
| 工具结果 | `POST /api/v1/agents/client/call/result` |
| 协同 WebSocket | `/api/v1/agents/client/websocket` |
| 流事件封套 | `{"event": "...", "data": {...}}` |
| 消息文本 | 文本型 `data.chunk` |
| 协同封套 | `{"type": "...", "event": "...", "payload": {...}}` |

## 对话 WebSocket

- 鉴权 query 使用 `__TOKEN__`；路径段和 query 正确编码，日志必须脱敏。
- 建连后 10 秒内发送首帧。
- 首帧之后服务端不再读取对话 WS。
- 工具结果禁止通过 WS 回传，只能走 REST 结果端点。
- AUTO 协议读取 `/api/v1/config`；显式配置优先。
- 建连失败的 SSE 回退不得导致已建立执行被意外重复启动。

## 流事件

事件字符串完整包括：

| 事件 | 说明 |
|------|------|
| `message` | 文本或消息数据 |
| `heartbeat` | 心跳 |
| `error` | 错误 |
| `close` | 关闭 |
| `custom` | 自定义事件 |
| `bytes` | 字节数据 |
| `tool_call` | 工具调用 |
| `file_written` | 文件写入 |
| `client_tool` | 客户端工具 |
| `mcp_tool` | MCP 工具 |
| `built_in` | 内置工具 |

`StreamEvent.message_text` 只读取文本型 `data.chunk`，错误读取 `data.error`。不依赖本地化错误文本分支。

## SSE

SSE 解析支持 `event:`、多行 `data:`、封套 JSON、EOF 和错误响应，并读取 `X-TaiyiFlow-Session-id`。SSE 与 WS 使用同一 `ChatRequest` 线上模型和事件语义。

## REST 与错误

- 使用 `Authorization: Bearer` 和配置的 credential headers。
- 动态 access token 每次请求读取；401 换牌后每个操作最多重试一次。
- `ApiError` 保留 HTTP 状态码与结构化 `detail/code`。
- 出站模型使用线上 alias 并排除 `None`，入站模型保留合理前向兼容性。
- 空响应、上传下载、超时和连接取消按传输语义处理。

## 协同协议

协同 WS 默认关闭，启用后等待 `connection.ready`，没有 register 首帧。实现心跳、入站静默检测、有界重连、任务版本去重、pending-only 对账、cancel/release、session update 和踢出处理。

重连时只有平台明确 pending 且未取消的任务可自动执行；状态未知不得重放副作用。

## 已知后端差异

- 技能 ZIP 上传 multipart 字段为 `package_file`。
- 后端没有 Java `ScheduleClient.getRun()` 对应路由，Python 不提供该接口。
- persistentToken 的 `/xeai/sso` 属于外部身份服务，不在 `taiyiflow-dev` 后端仓库内。
- `SimplifiedAPIRequest` 的长期/跨会话记忆、关联会话、AgentApp、客户端技能包/指令等字段按后端模型保留。

修改协议模型、事件常量或端点时，必须同步更新契约 fixtures、对照测试和本页。
