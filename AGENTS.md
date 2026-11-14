# AGENTS.md — TaiyiFlow Python SDK

## 当前状态

本仓库已完成 Phase 0–10 的离线代码、自动化测试与 Python 3.11.18/3.13.7 实机验证，并按 Java SDK `cb9b303..1a3fcfa` 新变更复刻默认 Flow、媒体理解、SKILL 与可选浏览器能力；目标部署平台、真实平台、媒体 Flow、平台技能、Chromium/CDP、MCP 服务端、协同 Worker、第三方身份与 SSO 行为仍待使用方验收。实施以 [plans/001-python-sdk-implementation.md](plans/001-python-sdk-implementation.md) 为唯一权威总计划，应随实际进度更新计划状态，不得仅修改 README 宣称功能已完成或通过真实验收。

## 项目定位

面向 Python 后台、CLI 与服务的 TaiyiFlow SDK：

- `TaiyiAgent` 主动驱动平台智能体，支持多会话并行、WS/SSE 流式对话、REST 能力、工具执行和 Hook。
- `DigitalWorker` 作为常驻数字员工，支持协同任务、身份与认证、审批、状态回报、恢复对账和受控子任务。
- 平台负责 LLM 编排；SDK 不实现前端 UI、语音 dialogue 链路或另一套 Agent 大脑。

参考 `../taiyiflow-javasdk` 的功能与行为，但不要逐行翻译 Java。Python 公开 API 使用 `snake_case`、类型标注、`asyncio`、异步迭代器与异步上下文管理器；线上 JSON 字段和协议字符串保持原样。

## 权威来源与参考顺序

1. **协议事实以 `../taiyiflow-dev` 后端代码为准**，包括路由、schema、事件、关闭码、执行控制与协同封套。
2. **功能与可靠性语义参考 `../taiyiflow-javasdk`**；Phase 10 当前对齐目标为提交 `1a3fcfa5423649fe22e1f1210ddd321a5f3914ba`，此前基线为 `cb9b303a6fadaa4d8869d773931b5242831c37f8`。
3. 桌面端执行控制与工具运行时可参考 `../taiyiflow-electron`，但不能越过后端协议。
4. 文档或注释与代码冲突时，先核查并记录差异，不得选择更方便实现的一种静默通过。

修改协议模型、事件常量或端点时，必须同步更新契约 fixtures、对照测试和相关文档。

## 目标技术栈

- Python 3.11+；分发名 `taiyiflow-sdk`，导入名 `taiyiagent`。
- PEP 517/518 + `pyproject.toml` + `src/taiyiagent` 布局。
- `asyncio`；HTTP/SSE 使用 `httpx`；WS 使用 `websockets`；模型使用 Pydantic v2。
- Pytest、pytest-asyncio、respx、本地假 WS/SSE 服务；Ruff、Mypy、Coverage、build。
- Worker 的 YAML 与加密凭证依赖放可选 extra，核心依赖保持最小。

## 常用命令

以下命令是工程标准入口：

```bash
python -m pip install -e '.[dev]'
pytest
pytest -m 'not integration'
pytest -m integration             # 仅在真实环境变量与后端就绪后使用
ruff check .
ruff format --check .
mypy src
python -m build
```

新增或修改代码时，至少运行受影响测试及静态检查；发布前运行完整质量门禁并在干净虚拟环境安装构建产物。真实后端与第三方系统联调由使用方执行，不擅自启动、部署或修改外部服务。

## 目标模块边界

```text
src/taiyiagent/
├── agent.py                 # TaiyiAgent 门面、配置与资源所有权
├── session.py               # AgentSession、状态机、执行控制
├── hooks.py                 # 四类 Hook
├── models.py                # 协议模型与 wire alias
├── errors.py                # 公共异常层级
├── transport/               # REST、WS、SSE、协议解析
├── tools/                   # 插件管理、工具调度、运行时、进程管理
├── plugins/                 # File、Search、Shell、媒体与高价值工具
├── skills/                  # SKILL 来源、供应、管理与同步
├── browser/                 # 可选 Chromium 运行时与网络策略
├── mcp/                     # Streamable HTTP 与 stdio MCP
├── collaboration/           # 可选协同连接与定时任务
└── worker/                  # 数字员工、任务、认证、凭证、配置与 CLI
```

模块可按实现规模合并，但传输、会话状态、工具副作用和 Worker 对账不得耦合成难以独立测试的单体。稳定公共 API 从包根显式导出，内部模块不承诺兼容。

## 核心设计约束

### 异步与资源管理

1. 首期只提供 asyncio 原生 API，不用隐式创建事件循环的方式维护同步镜像。
2. `session.chat()` 返回单一异步事件流；Hook 是观察者，不创建竞争消费通道。
3. 一个 Agent 绑定一个 flow；每个会话有独立连接、队列、RunContext、审批和取消状态。
4. 事件队列必须有界并实施背压；慢消费者、Hook 异常和关闭不得导致无界缓冲。
5. 所有任务、连接和子进程必须有明确所有者；取消和 `aclose()` 幂等清理，不留后台孤儿任务。
6. 不在库代码中配置全局 logging；使用命名 logger，调用方控制输出。

### 协议与模型

1. Python 属性用 `snake_case`，序列化使用线上 alias 并排除 `None`；出站字段拼写应被校验，入站模型保留合理前向兼容性。
2. WS 首帧在连接后 10 秒内发送；首帧之后服务端不再读取对话 WS。
3. 工具结果只走 `POST /api/v1/agents/client/call/result`，不得增加 WS 回传旁路。
4. 消息文本只从文本型 `data.chunk` 提取；11 类事件字符串必须与后端一致。
5. AUTO 协议读取 `/api/v1/config`；显式配置优先，SSE 回退不得导致执行被意外重复启动。
6. REST 保留状态码和结构化错误 detail/code，不依赖本地化错误文本分支。

### Hook 与工具安全

1. 工具链顺序固定为 `Permission -> before -> takeover 或插件 -> after -> REST 回报`。
2. `takeover` 只有返回非 `None` 才接管；权限默认只读放行、写操作拦截。
3. ASK 必须等待显式裁决；拒绝或超时形成失败结果并回报；未知/重复回执返回 `False`。
4. File/Search 必须限制在 workspace 内并测试路径穿越与符号链接逃逸。
5. Shell 不使用 `shell=True`，命令白名单默认收敛；工作目录不是操作系统沙盒。
6. stdio MCP 不经过 shell，命令与显式 env 默认全拒绝，cwd 限定 workspace，敏感 env 不持久化、不写日志。
7. 后台进程句柄是 SDK 私有句柄，不是 taskId/PID；停止操作只针对当前注册表内匹配进程树。
8. HTTP MCP、stdio MCP、内置工具和自定义工具必须经过同一权限、Hook、并发、取消与回报链。

### 执行控制与恢复

1. `pause`/`cancel` 调用服务端；`interrupt` 只取消本地资源。
2. `attach` 和 `resume` 不主动创建新执行；`retry` 和 `repeat` 才明确创建新 run。
3. 默认 MANUAL 恢复；AUTO 按 attach → checkpoint resume → 明确执行丢失后的安全 start。
4. 平台不可达、状态未知或存在不确定工具副作用时禁止自动 start，禁止以重跑任务修复结果回报失败。
5. Outbox 只保存有界瞬态结果/checkpoint 记录；补传不得重新执行工具，容量与重试必须受限，损坏记录隔离并告警。

### Worker、身份与凭证

1. 协同 WS 默认关闭，仅显式启用后建立被动连接。
2. 第三方身份 SPI 负责 workerId、角色、能力与认证主体的登记和绑定；SDK 不自动绕过身份治理。
3. 自定义 task handler 优先；默认任务路由到 flow 会话，同 flow + session 串行。
4. 任务必须先成功回报 running 才执行；只有平台明确 pending 且未取消的重连任务可自动执行。
5. 平台审批是默认通道；本地回调必须完成平台审计，缺少必要适配器时写工具失败关闭。
6. 记忆、任务、结果、产物和检查点以平台为权威；workspace 不保存权威业务结果或长期任务历史。
7. API Key、JWT、accessToken、persistentToken 不写入普通 workspace/state；持久令牌只放独立加密凭证存储。
8. URL query 的 `__TOKEN__`、Authorization、credential headers 和敏感 env 必须在日志、异常与诊断输出中脱敏。
9. 同一 workerId 仅单实例；不新增主备、选举或 fencing 抽象。

## 编码与测试约定

- 公开函数、方法和模型完整类型标注；避免无约束 `Any` 扩散到公共 API。
- 不使用 `except Exception: pass` 隐藏协议、取消或状态错误；`asyncio.CancelledError` 必须按取消语义传播。
- 仅在系统边界做校验，内部信任已建立的不变量；不重写网络库或 async 运行时已有能力。
- 测试默认不访问真实网络、不执行任意本机命令、不写用户目录；文件系统使用临时目录。
- 集成测试统一 marker 并默认跳过，通过 `TAIYIFLOW_BASE_URL`、`TAIYIFLOW_API_KEY`、`TAIYIFLOW_FLOW_ID` 等显式启用，不提交真实凭证。
- 对 Java 测试按行为移植，不要求类名一一对应；协议、Hook、恢复、安全和 Worker 关键用例必须能追溯到对照场景。
- 不用 mock 测试通过替代真实平台联调；未联调功能在计划中标记“待联调”。
- 示例只从环境变量读取配置，不硬编码密钥，不打印令牌。

## 文档与变更约束

- 小范围修复直接修改对应模块和测试，不顺带重构无关代码。
- 新需求若改变范围、API 或可靠性语义，先更新权威计划再实施。
- README 只描述已经实现或明确标为目标的能力；示例签名必须与实际 API 一致。
- 每阶段完成需同时满足代码、自动化测试、文档和必要联调条件；不能只按文件数量判断完成。
- 未经用户明确要求，不创建 Git 提交、不推送、不发布包、不部署 Worker。

## 计划入口

- [plans/001-python-sdk-implementation.md](plans/001-python-sdk-implementation.md)：范围、API 草案、协议清单、Phase 0–9、里程碑与验收标准。
