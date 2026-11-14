# TaiyiFlow Python SDK 实施计划

> 状态：待联调（Phase 0–10 离线实现与验证完成；真实环境验收继续待使用方执行）  
> 制定日期：2026-11-13  
> 参考实现：`../taiyiflow-javasdk`，Phase 10 对齐目标 `1a3fcfa5423649fe22e1f1210ddd321a5f3914ba`；此前基线 `cb9b303a6fadaa4d8869d773931b5242831c37f8`  
> 协议权威：`../taiyiflow-dev` 后端实现；Java SDK 用于功能与行为对照  
> 本文档是 Python SDK 当前唯一权威实施计划，范围或进度变化应同步更新本文档。

## 1. 目标与原则

实现一个面向 Python 后台、CLI 与服务的 TaiyiFlow SDK，覆盖 Java SDK 的两类使用形态：

1. `TaiyiAgent` 主动驱动平台智能体：多会话并行、WS/SSE 流式对话、REST 辅助能力、工具执行与 Hook。
2. `DigitalWorker` 常驻数字员工：协同连接、任务执行与回报、审批、恢复对账、子任务、凭证与运行状态管理。

实施遵循以下原则：

- **协议一致，API Python 化**：线上字段、事件、端点和错误语义与后端一致；公开 Python API 使用 `snake_case`、类型标注、上下文管理器和异步迭代器，不机械复制 Java Builder。
- **异步优先**：基于 `asyncio`，同一事件循环内支持多会话并行、独立取消与背压；首期不额外维护同步镜像 API。
- **安全默认**：写工具默认需要审批；文件与搜索限定 workspace；Shell 与 stdio MCP 默认拒绝未显式允许的命令；日志与异常不得泄露令牌。
- **副作用谨慎**：`attach`/`resume` 不创建新执行；状态未知、结果回报未知或工具副作用不确定时禁止盲目重试。
- **可验证对齐**：每个阶段用 Java 对照测试和后端协议核查闭环；单元测试不冒充真实平台联调。

## 2. 技术选型与包结构

### 2.1 基线

| 项目 | 决策 |
|------|------|
| Python | 3.11+ |
| 分发名 / 导入名 | `taiyiflow-sdk` / `taiyiagent` |
| 构建 | PEP 517/518，`pyproject.toml`，`src` 布局 |
| 并发 | `asyncio`、`TaskGroup`、有界异步队列 |
| HTTP / SSE | `httpx`，自行严格解析 SSE 帧 |
| WebSocket | `websockets` |
| 数据模型 | Pydantic v2；序列化使用 wire alias 并排除 `None` |
| 测试 | `pytest`、`pytest-asyncio`、`respx`，本地假 WS/SSE 服务 |
| 质量 | Ruff、Mypy、Coverage、`python -m build` |
| 可选依赖 | YAML 为核心依赖；加密凭证放入 `worker` extra，Playwright 放入 `browser` extra |

依赖版本在 Phase 0 根据当前兼容版本锁定到构建配置；核心依赖保持最小，不为简单封装引入大型框架。

### 2.2 目标目录

```text
taiyiflow-pythonsdk/
├── pyproject.toml
├── README.md
├── AGENTS.md
├── plans/
│   └── 001-python-sdk-implementation.md
├── src/taiyiagent/
│   ├── __init__.py                 # 稳定公共出口
│   ├── agent.py                    # TaiyiAgent、配置与资源所有权
│   ├── session.py                  # AgentSession、会话状态与执行控制
│   ├── hooks.py                    # Stream/Tool/Permission/Session Hook
│   ├── models.py                   # 请求、事件、工具、检查点等协议模型
│   ├── errors.py                   # 公共异常层级
│   ├── transport/
│   │   ├── rest.py                 # REST 基础客户端与能力客户端
│   │   ├── websocket.py            # 对话 WS
│   │   ├── sse.py                  # SSE 与协议回退
│   │   └── protocol.py             # /api/v1/config 协议解析
│   ├── tools/
│   │   ├── dispatcher.py           # 权限与 Hook 链、工具结果回报
│   │   ├── plugins.py              # 插件注册与调用
│   │   ├── runtime.py              # 并发、取消、共享资源与 Outbox
│   │   └── processes.py            # 后台进程句柄与清理
│   ├── plugins/
│   │   ├── files.py
│   │   ├── search.py
│   │   └── shell.py
│   ├── skills/                         # SKILL 来源、供应、管理与同步
│   ├── browser/                        # 可选 Chromium 与网络/文件安全
│   ├── mcp/
│   │   ├── http.py                 # Streamable HTTP MCP
│   │   └── stdio.py                # 受控 stdio MCP 子进程
│   ├── collaboration/
│   │   ├── client.py               # 协同 WS、心跳、重连、任务事件
│   │   └── schedule.py             # 定时任务回调
│   └── worker/
│       ├── worker.py               # DigitalWorker 生命周期与任务路由
│       ├── tasks.py                # 幂等执行、状态回报与恢复对账
│       ├── subtasks.py             # 受控子任务
│       ├── auth.py                 # API Key/JWT/persistentToken
│       ├── credentials.py          # 独立加密凭证存储
│       ├── config.py               # YAML 配置与密钥引用
│       └── cli.py                  # Worker CLI
├── examples/
│   ├── minimal.py
│   ├── multi_session.py
│   ├── tool_takeover.py
│   ├── execution_control.py
│   ├── skills.py
│   ├── browser.py
│   └── worker.py
└── tests/
    ├── unit/
    ├── contract/                   # Java 用例与后端协议对照
    ├── integration/                # 默认跳过，显式配置后运行
    └── fixtures/
```

允许在实现时合并小模块，但不得把传输、会话、工具副作用和 Worker 状态机重新耦合为单体文件。

## 3. 公共 API 草案

Phase 1 冻结签名后再实现后续模块；如后端协议要求调整，应记录原因并同步 README 示例。

### 3.1 最小流式对话

```python
import os

from taiyiagent import TaiyiAgent


async def main() -> None:
    async with TaiyiAgent(
        base_url=os.environ["TAIYIFLOW_BASE_URL"],
        api_key=os.environ["TAIYIFLOW_API_KEY"],
        flow_id=os.environ["TAIYIFLOW_FLOW_ID"],
    ) as agent:
        session = agent.new_session()
        async for event in session.chat("你好，请介绍一下你自己"):
            if event.message_text is not None:
                print(event.message_text, end="", flush=True)
```

`chat()` 返回异步事件流；Hook 是同一事件流上的观察/扩展机制，不另建一条竞争消费通道。调用方可创建任务并发迭代多个会话，也可通过 `await session.wait()` 等待由其他消费者驱动的会话结束。

### 3.2 Java 到 Python 命名映射

| Java | Python 目标 API | 语义 |
|------|-----------------|------|
| `TaiyiAgent.builder().build()` | `TaiyiAgent(...)` | 构造时校验必填配置 |
| `newSession()` | `new_session()` | 新建隔离会话，可指定 `session_id` |
| `chat(...)` | `chat(...)` | 返回 `AsyncIterator[StreamEvent]` |
| `onStream()` | `on_stream()` | 注册流 Hook |
| `onToolExecution()` | `on_tool_execution()` | 注册工具 Hook |
| `onPermission()` | `on_permission()` | 注册权限 Hook |
| `onLifecycle()` | `on_lifecycle()` | 注册会话 Hook |
| `pause()` / `cancel()` | 同名异步方法 | 服务端执行控制 |
| `attach()` / `resume()` | 同名方法，返回事件流 | 重接原执行或检查点，不主动新建 run |
| `retry()` / `repeat()` | 同名方法，返回事件流 | 明确创建新 run |
| `approve()` / `deny()` | 同名异步方法 | 裁决本地待审批工具 |
| `interrupt()` | 同名异步方法 | 仅取消本地执行与流资源 |
| `await()` | `wait(timeout=None)` | 避免 Python 关键字冲突 |
| `shutdown()` | `aclose()` / `async with` | 幂等释放会话、插件、MCP 与传输资源 |
| `worker.start()/stop()` | `start()/aclose()` | 明确生命周期与资源所有权 |

### 3.3 模型与异常

- 模型至少覆盖 `ChatRequest`、`StreamEvent`、`ToolCall`、`ToolResult`、`RunContext`、`ResumableCheckpoint`、`AgentLoopControlResult`、Worker 任务与身份模型。
- Python 属性使用 `snake_case`，JSON 严格使用 `input_value`、`session_id`、`run_id` 等线上名称；未知入站字段保留前向兼容性，出站请求不得静默拼错字段。
- `StreamEvent.message_text` 只读取文本型 `data.chunk`；错误信息读取 `data.error`。
- 异常层级规划为 `TaiyiFlowError` 下的 `ConfigurationError`、`TransportError`、`ProtocolError`、`AuthenticationError`、`ApiError`、`SessionClosedError`、`ToolExecutionError`、`ApprovalTimeoutError` 和 `RecoveryError`。
- `ApiError` 保留 HTTP 状态码及结构化 `detail/code`；异常字符串默认不包含 Authorization、WS `__TOKEN__` 或敏感环境变量。

## 4. 协议与行为对齐清单

### 4.1 对话与 REST

- WS：`/api/v1/run/{flow_id}/websocket`，鉴权 query 为 `__TOKEN__`；路径段和 query 必须正确编码。
- 建连后 10 秒内发送首帧；首帧覆盖 Java `ChatRequest` 的全部线上字段。首帧之后服务端不再读取对话 WS，工具结果禁止经 WS 回传。
- SSE：`POST /api/v1/run/{flow_id}/stream`，支持 `event:`、多行 `data:`、封套 JSON、EOF 与错误响应，读取 `X-TaiyiFlow-Session-id`。
- `/api/v1/config` 的 `stream_protocol` 决定 AUTO 协议；显式配置优先，WS 连接失败按配置回退 SSE，不得在活跃执行中盲目切换并重复产生副作用。
- 事件常量完整覆盖 `message`、`heartbeat`、`error`、`close`、`custom`、`bytes`、`tool_call`、`file_written`、`client_tool`、`mcp_tool`、`built_in`。
- 工具结果走 `POST /api/v1/agents/client/call/result`。
- REST 覆盖会话、技能、记忆、配置、文件上传下载、MCP server、协同任务、定时任务与执行控制；统一处理 Bearer、动态 access token、credential headers、超时、空响应和结构化错误。

### 4.2 会话与 Hook

- 一个 `TaiyiAgent` 绑定一个 flow；会话拥有独立 `session_id`、连接、事件队列、RunContext、审批与取消状态。
- 有界队列提供背压；慢消费者、Hook 异常、取消和 agent 关闭均不得造成无界内存增长或孤儿任务。
- 工具调用顺序固定为：`PermissionHook -> before -> takeover 或内置插件 -> after -> REST 回报`。
- `takeover` 返回非 `None` 才接管；`before` 可修改或拒绝调用；`after` 可观察或修改结果。
- 不挂 Hook 时仍可消费事件并执行默认工具链；权限默认只读放行、写操作拦截。
- `ASK` 形成会话级等待；批准后才执行，拒绝/超时形成失败结果并回报；未知或重复回执返回 `False`。超时支持 0（永不超时）或 10–6000 秒。

### 4.3 执行控制与恢复

- `pause`/`cancel` 调用服务端控制接口；`interrupt` 只做本地取消。
- `attach` 重接活跃 run；`resume` 发现并恢复可续跑检查点，均不得主动创建新执行。
- `retry` 和 `repeat` 明确创建新 run，分别保留 Java 的续做和原输入模板语义。
- 默认恢复策略为 MANUAL；AUTO 按有界指数退避执行对账，顺序为 attach、checkpoint resume、确认执行丢失后的安全 start。
- 存在 waiting/pending/uncertain 工具副作用、平台不可达或状态不明时禁止自动 start。
- 未确认工具结果只进入专用瞬态 Outbox；补传不重新执行工具。Outbox 有条目、单条字节、总字节和重试上限，损坏记录隔离并告警。

### 4.4 插件、MCP 与进程

- 对齐 File、Search、Shell 的工具名称、参数和结果结构；路径穿越、符号链接逃逸、绝对路径、命令白名单和输出上限必须有测试。
- Shell 不使用 `shell=True`；工作目录不等于操作系统沙盒，文档必须明确部署者仍需最小权限隔离。
- 后台进程使用 SDK 私有句柄，支持 list/output/stop/cleanup、stdout/stderr 独立 offset、有界尾部缓冲和进程树清理；不得把句柄当作平台 taskId 或信任旧 PID。
- 会话取消与 agent/worker 关闭清理后台进程；pause/resume 不终止进程。
- Streamable HTTP MCP 与 stdio MCP 共用权限、Hook、并发、取消和结果回报链。
- stdio 配置解析不经过 shell，不解释引号/转义；命令与环境变量默认全拒绝，cwd 必须位于 workspace，限制 server 数、参数、帧、输出和进程寿命，敏感 env 不持久化、不记录日志。

### 4.5 协同与 DigitalWorker

- 协同 WS 默认关闭；启用后连接 `/api/v1/agents/client/websocket`，使用 `{type,event,payload}` 封套，握手传 client/device/instance 信息并等待 `connection.ready`，无 register 首帧。
- 实现心跳、入站静默检测、有界重连、任务下发/取消/快照、release push、session update 和踢出处理。
- Worker 默认把 remote/dialogue/schedule 任务路由到 flow 会话，自定义 task handler 优先；同 flow + session 串行执行。
- Worker 身份、角色、能力与认证主体绑定由使用方 SPI 校验/登记，SDK 不绕过第三方身份治理。
- 任务先成功回报 running 才执行；终态回报失败进入 `REPORT_UNKNOWN`，不得通过重跑任务修复。
- 重连只自动执行平台明确为 pending 且未取消的任务；dispatched/acknowledged/running 只对账，不重放。
- 审批默认平台通道；本地回调必须带平台审计。未配置必要审批适配器时写工具失败关闭。
- 持久令牌只放独立加密凭证存储；workspace、普通 state 目录、日志和异常不得保存长期凭证。
- 同一 workerId 仅单实例运行；SDK 不引入主备、选举或 fencing。跨进程恢复以平台状态为权威。
- 受控子任务保持 request mode 只能持平或降级、能力快照冻结、MCP 敏感配置不入快照、批量结果按索引聚合、创建响应丢失先查询再决定是否重试。

## 5. 分阶段实施

状态取值：待实施 / 进行中 / 待联调 / 已完成。只有代码、测试和文档均达到退出条件才可标记已完成。

### Phase 0 — 协议核查与工程骨架

- 状态：已完成（2026-11-14，离线工程与协议核查）。
- 核查后端路由、请求/响应 schema、关闭码、工具事件和协同封套，形成与 Java 基线差异清单；不假设 Java 中所有注释都是最新协议。
- 核查结果：`SimplifiedAPIRequest` 相比 Java 基线新增长期/跨会话记忆、关联会话、AgentApp、客户端技能包/指令等字段，Python `ChatRequest` 已按后端补齐；工具结果确认使用 `call_id`、`error_message`，关闭码新增 4000 并与 Java 当前代码一致。
- Phase 6 补充差异：技能 ZIP 上传以后端 `agent_skill.py` 的 multipart 字段 `package_file` 为准，不复制 Java 当前的 `file`；后端不存在 Java `ScheduleClient.getRun()` 对应的 `GET /api/v1/agents/client/schedules/runs/{run_record_id}` 路由，Python 不提供该伪能力，仅实现已有的 runs 列表与 report 路由。
- 建立 `pyproject.toml`、src 布局、依赖 extras、Ruff/Mypy/Pytest 配置、包版本和 `__init__.py` 公共出口。
- 准备测试 fixtures、环境变量约定和默认跳过的 integration marker。
- 退出条件：包可在 3.11+ 安装；空实现不伪装成功；质量命令可执行；协议差异已记录并消除歧义。

### Phase 1 — 模型、异常、配置与 Hook 契约

- 状态：已完成（2026-11-14，16 个模型/Hook 测试与 Ruff 通过）；依赖 Phase 0。
- 实现协议模型、alias 序列化、事件便捷属性、异常层级、Agent 配置校验和四类 Hook。
- 用 Java `ChatRequestTest`、`StreamEventTest`、`ToolCallTest`、`ToolResultTest`、`EventConstantsTest`、`CloseCodesTest`、`TaiyiAgentBuilderTest` 做对照。
- 冻结 `chat()` 异步迭代、Hook 调用线程/事件循环约束及会话关闭行为。
- 退出条件：协议 golden fixtures 往返序列化一致；未知入站字段兼容；敏感数据不会出现在模型/异常默认字符串中。

### Phase 2 — REST、WS、SSE 与协议解析

- 状态：已完成（2026-11-14，离线传输实现与模拟网络测试）；依赖 Phase 1。
- 实现异步 REST 基础客户端、动态 token、错误映射、上传下载、对话 WS、SSE、AUTO 协议解析及有界超时。
- 覆盖分片 WS 文本、错误 JSON、SSE 多行 data、连接失败回退、取消时连接关闭、base URL/路径编码和令牌脱敏。
- 退出条件：传输单测无真实网络依赖；所有连接可取消且资源释放；WS 不提供工具结果回传旁路。

### Phase 3 — Agent、多会话与最小可用版本

- 状态：待联调（2026-11-14，M1 离线实现、示例与核心测试通过，真实平台最小对话待使用方确认）；依赖 Phase 2。
- 实现 `TaiyiAgent`、`AgentSession`、SessionManager、RunContext、状态机、事件队列、Hook 分发、等待与幂等关闭。
- 完成 `examples/minimal.py` 和 `multi_session.py`，验证多会话并行、会话隔离、独立取消、慢消费者与 agent 关闭。
- 形成 **M1：核心对话可用**，仅声明此里程碑已覆盖的能力，不提前宣称 Worker 对齐。
- 退出条件：核心单测通过；最小示例只依赖三个环境变量；真实平台最小对话由使用方联调确认后标记完成。

### Phase 4 — 工具调度、审批与内置插件

- 状态：离线完成（2026-11-14，54 个测试、Ruff 和 Mypy 通过；真实平台工具调度与回报待使用方联调）；依赖 Phase 3。
- 实现 PluginManager、ToolDispatcher、执行上下文、权限策略、ApprovalBroker 和工具结果 REST 回报。
- 移植 File/Search/Shell 及后台进程管理，完成 sandbox、白名单、输出限制、取消和清理测试。
- 完成工具接管、拒绝、ASK 超时、Hook 修改、重复审批、回报失败等测试及示例。
- 退出条件：Java ToolDispatcher、ToolExecution、Approval、Plugins、Outbox 相关关键用例均有 Python 对照；危险默认配置失败关闭。

### Phase 5 — 执行控制、恢复与结果 Outbox

- 状态：离线完成（2026-11-14，71 个测试、Ruff 和 Mypy 通过；真实平台执行控制、检查点恢复与结果补偿待使用方联调）；依赖 Phase 4。
- 实现 pause/cancel/attach/resume/retry/repeat、检查点发现、断线对账、MANUAL/AUTO 恢复与有界内存/文件 Outbox。
- 覆盖多轮会话、断线、重连、执行丢失、不确定工具副作用、补偿重报、损坏记录隔离和重启补传。
- 完成 `execution_control.py` 示例。
- 退出条件：任何恢复路径都不会把“状态未知”当成“可安全重放”；恢复测试可证明不重复执行工具。

### Phase 6 — REST 能力全集与 MCP

- 状态：待联调（2026-11-14，离线实现与 89 个测试、Ruff、Mypy 验证通过；真实平台 REST 能力与真实 MCP 服务端待使用方联调）；依赖 Phase 2、4。
- 完成 Session/Skill/Memory/Config/File/Schedule/CollaborationTask/McpServer 客户端及统一 401 换牌重试接口；REST 会话入口为 `session_api`，保留 `sessions` 作为本地会话管理器。
- 实现 Streamable HTTP MCP 与 stdio MCP 生命周期、JSON/SSE 与换行/Content-Length 分帧、JSON-RPC 关联、`c_{server}_{tool}` 工具注册、安全策略、容量限制和取消清理；注册后统一进入 PluginManager/ToolDispatcher 链。
- 验证 initialize 单次执行、session header、响应 ID、取消/超时不重放 `tools/call`、pending 清理、默认命令/env 拒绝、符号链接逃逸、帧上限、manager 容量和进程关闭。
- 退出条件：能力端点与后端核查一致；每次换牌每个操作最多重试一次；MCP 取消不隐式重放 `tools/call`，敏感配置不持久化。离线条件已满足，真实端点和服务端行为待联调。

### Phase 7 — 协同插件与定时任务

- 状态：待联调（2026-11-14，离线实现与 97 个测试、Ruff、Mypy 验证通过；真实协同平台连接、任务和 release 待使用方联调）；依赖 Phase 4、6。
- 实现显式 `start()` 的可选协同连接、角色 query 握手、ready 后 recent 拉取、心跳/静默检测、有界退避重连、任务/cancel/release 事件和 SchedulePlugin。
- 验证默认无被动连接、动态 token 重连、重复任务版本过滤、pending-only 对账执行、分页、schedule ack、连接踢出和关闭时任务清理。
- 退出条件：Java CollaborationProtocol、CollaborationPlugin、SchedulePlugin 关键用例有 Python 对照；默认构造 Agent 不建立协同 WS。离线条件已满足，真实连接行为待联调。

### Phase 8 — DigitalWorker、认证与受控子任务

- 状态：待联调（2026-11-14，离线实现与 124 个测试、Ruff、Mypy 验证通过；真实 Worker 任务、审批、恢复、SSO/persistentToken、第三方身份系统与 MCP resolver 待使用方联调）；依赖 Phase 5–7。
- 实现 Worker SPI、身份校验/登记、任务路由与串行调度、状态回报、审批审计、恢复对账、观测 Hook。
- 实现 API Key/JWT/persistentToken、401 换牌、独立加密凭证存储、instance/state/workspace 隔离。
- 实现受控子任务、能力快照、MCP resolver、本地/远程投递、取消/超时和创建不确定性只读对账；创建异常不直接重试，本地执行复用 running 门控和终态回报链。
- 提供 YAML 配置、扩展动态加载、`taiyiagent-worker` CLI、Worker 示例与最小部署说明；配置只引用环境变量名称，不写入真实密钥。
- 形成 **M3：数字员工能力对齐候选版**，离线条件已满足，真实平台行为不由 mock 结果替代验收。
- 退出条件：Java WorkerRuntime、TaskExecutor、DigitalWorker、Auth、Subtask 关键用例有 Python 对照；真实平台任务、审批、恢复由使用方联调确认。

### Phase 9 — 发布质量与验收

- 状态：待联调（2026-11-14，离线发布质量门禁及 Python 3.11.18/3.13.7 实机验证完成；目标部署平台和真实环境验收待使用方执行）；依赖 Phase 3–8。
- [x] README 按实际实现更新，移除“规划中”状态；所有示例执行静态检查，API 文档与签名一致。
- [x] 在 Python 3.11.18 与 3.13.7 运行完整单测、契约测试、Ruff、Mypy、构建和 wheel/sdist 安装冒烟。
- [x] 进行并发/取消/资源泄漏检查、依赖与凭证泄露审查；真实环境联调单独记录，不用 mock 结果替代。
- [x] 明确版本号 `0.1.0`、当前变更和发布流程；未获明确授权不发布到包仓库。
- [x] 通过 Conda 独立环境完成 Python 3.11 实机安装运行，并修复协同静默超时与连接清理竞争导致的重复关闭。
- [ ] 由使用方在目标部署平台安装运行，并完成真实后端和第三方系统验收。
- 验证记录：两个 Python 版本各 124 个离线测试通过，分支覆盖率 78%；Ruff、67 个文件格式检查、43 个源码文件 Mypy strict、5 个示例 Mypy、`pip check` 和危险/凭证模式扫描通过；心跳关闭回归在 Python 3.11 连续通过 20 次。
- 发布物记录：`taiyiflow_sdk-0.1.0-py3-none-any.whl` 与 `taiyiflow_sdk-0.1.0.tar.gz` 均构建成功，并分别在 Python 3.11 独立虚拟环境完成安装、包/Worker 导入和 `taiyiagent-worker --help` 冒烟。
- 退出条件：发布物可全新安装；公开 API、文档、测试和实际行为一致；已知限制明确列出。离线条件已满足，完整发布验收仍受上述实机与真实联调项约束。

### Phase 10 — Java SDK 能力变更新增复刻（AA–AG 对齐）

- 状态：待联调（2026-11-14，已按 Java `cb9b303..1a3fcfa` 实际代码与 `taiyiflow-dev` 协议完成离线复刻，不迁移文档站、Spring Boot 或 Electron UI；真实环境验收待使用方执行）；依赖 Phase 0–9。
- [x] 协议底座：类型化 `/config`、默认 Flow 缺省解析、Flow Metadata 缓存与 features、工具 schema/首帧过滤、同步/异步 FlowRun、原始响应、流式上传和 workspace 传播。
  - 验证记录（2026-11-14）：Python 3.13.7 全量 141 个离线测试通过，Python 3.11.18 协议底座及相邻回归 59 项通过；Ruff、71 个文件格式检查和 45 个源码文件 Mypy strict 通过。覆盖提交未知不重交、超时/协程取消通知服务端、元数据 single-flight、恢复快照及 401 后流式上传完整重放。
- [x] 媒体与高价值工具：文档/图片/音频/视频理解、截图理解公共服务、Schedule/session_context/瞬态 todo、常用别名和可选 ask_user SPI；全部复用统一权限、Hook、取消与回报链。
  - 验证记录（2026-11-14）：Python 3.13.7 全量 150 个离线测试通过，Python 3.11.18 新增能力与相邻回归 39 项通过；Ruff、75 个文件格式检查和 48 个源码文件 Mypy strict 通过。验证业务 Flow 上传与专用 Flow 理解、文档同步/媒体异步、提交未知不重交、截图提示与坐标上下文、Todo 会话清理和交互取消；真实媒体 Flow 待使用方联调。
- [x] SKILL：本地/平台来源、过滤、摘要/全文、首帧包与名称、会话与恢复快照、只读/变更工具、ZIP 安全、显式同步和子任务最小继承。
  - 验证记录（2026-11-14）：Python 3.13.7 与 3.11.18 各 165 个离线测试通过；Ruff、84 个文件格式检查和 56 个源码文件 Mypy strict 通过。覆盖 workspace/user 来源覆盖、请求模式与工具类别过滤、平台内容分会话缓存与 single-flight、首帧/恢复快照不漂移、子任务包深拷贝继承、环境变量仅返回缺失名称、ZIP 路径/链接/扩展名/压缩炸弹阻断及同步默认 DISABLED；真实平台技能待使用方联调。
- [x] 浏览器：可选 Playwright 依赖，Chromium HEADLESS/REMOTE_CDP、17 个工具、会话/tab/ref 隔离、网络与文件安全、有界资源及关闭清理；默认 DISABLED，execute_js/下载/上传默认关闭。
  - 验证记录（2026-11-14）：Python 3.13.7 与 3.11.18 各 174 个离线测试通过；Ruff 和 60 个源码文件 Mypy strict 通过。覆盖默认禁用、17 工具注册、可信会话身份、HTTP(S)/私有地址与 host/CIDR 策略、CDP 新建上下文且不关闭外部 Browser、会话清理、DOM generation 引用失效、控制台边界、workspace 截图和超时失效；未下载或启动真实 Chromium，真实 HEADLESS/CDP 待使用方联调。
- [x] 装配与验收：Worker 配置、示例、README/AGENTS、契约与负向测试、Python 3.11/3.13 全量质量门禁和构建安装。
  - 验证记录（2026-11-14）：Python 3.13.7 与 3.11.18 各 175 个离线测试通过；Ruff、91 个文件格式检查、60 个源码文件与 7 个示例 Mypy strict 通过。Worker 默认关闭内置/媒体/SKILL/浏览器，媒体可独立启用，平台技能复用 Agent 客户端，远程 CDP secret 仅从环境变量解析且 repr 脱敏。
  - 发布物记录：`python -m build` 成功生成 wheel 与 sdist；两者分别在 Python 3.11.18 隔离虚拟环境完成安装、包/BrowserSettings/Worker 导入、`taiyiagent-worker --help` 与 `pip check` 冒烟，未安装 Playwright 时核心包可正常导入。sdist 首次安装命中 pip 旧构建缓存，禁用缓存并强制从当前 sdist 重建后通过；未下载或启动真实 Chromium。
- [x] 实现复核与最小修复（2026-11-14）：对照 Java ToolDispatcher、AgentSession、DigitalWorker 与 WorkerRuntime，修复 Worker 并发上限未限制已接收操作、排队任务取消后仍可能启动，以及平台审批 Hook 被默认本地 ASK 等待阻断；修复 `SessionStream.wait()` 吞掉调用方取消。新增权限覆盖、Worker 审批装配、并发/排队取消与调用方取消回归，不改变工具结果回报和恢复语义。
- [x] 文档规范整理（2026-11-14）：参照 Java SDK `docs` 的 VitePress 中文导航、本地搜索、Mermaid 与可配置 base，拆分快速开始、Agent/Session、Hook、插件、SKILL、浏览器、MCP、恢复、Worker、协同、部署、验收和协议页面；README 改为入口，不移植 Spring Boot 或虚构容器能力，继续标明真实平台待验收。
  - 验证记录（2026-11-14）：Python 3.13.7 全量 179 个离线测试通过；Ruff、106 个文件格式检查、60 个源码文件 Mypy strict、`python -m build` 和 VitePress 死链/站点构建通过。浏览器实测首页、中文导航、本地搜索、Mermaid 与深色主题，控制台无错误；真实平台与第三方系统仍待使用方验收。
- 安全边界：默认 Flow 不用于 Worker 派单缺省路由；媒体上传始终使用当前业务 Flow，理解使用服务端配置 Flow；提交结果未知不重交；技能文本不提权；远程浏览器不接管既有页面或关闭外部 Browser。
- 退出条件：上述离线代码、测试、示例与文档完成，真实媒体 Flow、Chromium/CDP、平台技能和部署环境仍单独标记待联调。

## 6. 里程碑与优先级

| 里程碑 | 包含阶段 | 可交付能力 |
|--------|----------|------------|
| M1 核心对话 | 0–3 | 安装包、WS/SSE 流式对话、多会话、Hook 观察、资源管理 |
| M2 工具化 Agent | 4–7 | 工具/审批/插件、执行恢复、MCP、REST 全集、协同 |
| M3 数字员工 | 8 | Worker、认证、任务对账、受控子任务、CLI |
| M4 发布候选 | 9 | 全量质量门禁、真实联调记录、可安装发布物 |

实施顺序默认 M1 → M2 → M3 → M4。每个里程碑先交付可验证闭环，再扩展下一层；不在核心会话尚未稳定时并行堆叠 Worker 功能。

## 7. 测试与验收策略

### 7.1 自动化测试

- **模型契约**：使用与 Java 相同 JSON fixture，逐字段检查 alias、排除 `None`、事件字符串和关闭码。
- **传输**：模拟 REST/WS/SSE，覆盖分片、乱序关闭、超时、取消、401、非 2xx、空响应与错误详情。
- **并发**：并发多个会话，验证事件不串流、独立取消、队列有界、关闭后无 pending task。
- **Hook**：默认路径、执行顺序、takeover、before/after 修改、deny/ask、Hook 异常与超时。
- **安全**：workspace 逃逸、shell 白名单、MCP 命令/env 默认拒绝、日志与异常脱敏、进程树清理。
- **可靠性**：断线恢复、幂等回报、Outbox 上限与损坏隔离、任务状态对账，不允许重放副作用。
- **发布**：构建 wheel/sdist，在干净虚拟环境安装并导入，运行最小离线冒烟。

### 7.2 真实环境联调

真实后端和第三方系统联调由使用方执行，SDK 仓库不额外搭建持久测试服务。提供可复现示例和检查项：

1. 最小 WS 对话与 SSE 强制模式。
2. 多会话并行和单会话取消。
3. 工具接管、写操作审批、拒绝和超时。
4. pause/attach/resume/retry/repeat，确认无意外新 run 或重复工具。
5. 协同任务接收、running/终态回报、断线重连与平台状态对账。
6. Worker 身份、认证换牌、审批审计、产物上传与重启恢复。

联调通过前，对应阶段标记“待联调”，不得仅凭单元测试宣称完整兼容。

## 8. 风险与控制

| 风险 | 控制措施 |
|------|----------|
| Java 实现与后端协议漂移 | Phase 0 核查后端；端点/schema 改动同步契约 fixtures 与计划 |
| 异步 API 同时支持回调和迭代导致竞争消费 | 单一事件分发源，Hook 为观察者；Phase 1 冻结所有权与背压语义 |
| 断线重试重复执行工具或任务 | 平台状态为权威；不确定状态失败关闭；补偿只重传结果 |
| 文件、Shell、MCP 形成安全旁路 | 统一权限链；默认拒绝；路径/命令/env 测试；不依赖工作目录充当沙盒 |
| 凭证写入 URL、日志或普通状态目录 | 统一脱敏；独立加密凭证存储；测试扫描异常和日志 |
| 一次移植范围过大 | 按 M1–M4 闭环；各阶段独立验收，不以文件数量判断完成度 |
| 文档宣称超过实际能力 | README 标注当前状态；阶段完成后按实证更新，不预先勾选 |

## 9. 当前完成项与下一步

当前完成：

- 核查 Java SDK 的模块、公开接口、协议关键点、插件、Worker 与测试分布。
- 建立 Python 工程、协议模型、异常与四类 Hook。
- 完成 REST/WS/SSE、AUTO 协议解析、多会话事件流、背压、独立取消和资源管理。
- 提供最小、多会话、工具接管与执行控制示例；实现工具审批、Hook 链、REST 回报、File/Search/Shell 和后台进程管理。
- 实现执行控制、MANUAL/AUTO 对账恢复、幂等结果补偿、有界内存/文件 Outbox、损坏记录隔离及重启后的检查点结果补传。
- 完成 Session/Skill/Memory/Config/File/Schedule/CollaborationTask/McpServer REST 客户端，统一鉴权、结构化错误与单操作最多一次 401 换牌重试。
- 完成 Streamable HTTP/stdio MCP、JSON-RPC 关联、工具注册、默认拒绝的 stdio 安全策略、帧与容量限制、取消和统一关闭。
- 完成可选协同连接、角色握手、ready/recent、心跳与退避重连、任务版本去重、pending-only 对账、cancel/release 和 SchedulePlugin。
- 完成 DigitalWorker、第三方身份 SPI、API Key/JWT/persistentToken 认证、AES-256-GCM 独立凭证存储、任务 running 门控与版本对账、平台/本地审批审计、受控子任务、共享 Outbox、Worker YAML 扩展加载、CLI 与嵌入示例。
- 按 Java `cb9b303..1a3fcfa` 完成默认 Flow、Flow Metadata/FlowRun、媒体理解、高价值工具、完整 SKILL 供应链路和可选 Chromium 浏览器；Worker tools 默认关闭并按 YAML 显式装配，新增 SKILL/浏览器示例。
- 此前 Python 3.11.18 与 3.13.7 各 175 个离线测试通过；本次缺陷修复后 Python 3.13.7 全量 179 个离线测试通过，Ruff、106 个文件格式检查、60 个源码文件 Mypy strict 通过。
- wheel 与 sdist 均构建成功；此前已在 Python 3.11.18 独立虚拟环境完成安装、包/BrowserSettings/Worker 导入和 Worker CLI 冒烟。README 已改为中文文档入口，VitePress 构建与浏览器实测通过，真实平台验收限制保持不变。

下一步：由使用方在目标部署平台、真实 TaiyiFlow 后端、媒体 Flow、平台技能、Chromium/CDP 及第三方身份/SSO/MCP 环境执行第 7.2 节联调并记录结果；联调通过前保持“待联调”，不宣称完整发布验收完成。
