# 质量验证与验收

项目严格区分离线代码验证与真实平台验收。Mock、契约 fixtures、本地假 WS/SSE 服务和单元测试只能证明对应离线行为，不能宣称真实平台已联调。

## 离线质量门禁

```bash
python -m pip install -e '.[dev]'
pytest
pytest -m 'not integration'
ruff check .
ruff format --check .
mypy src
python -m build
```

文档门禁：

```bash
npm --prefix docs install
npm --prefix docs run docs:build
```

VitePress 构建包含内部死链检查。文档界面还应在浏览器检查首页、导航、搜索、代码块、Mermaid 和深色/浅色主题。

## 真实环境集成测试

集成测试统一使用 `integration` marker，默认跳过。只有真实后端与环境变量准备完成后才显式运行：

```bash
export TAIYIFLOW_BASE_URL=https://your-platform.example.com
export TAIYIFLOW_API_KEY=your-api-key
export TAIYIFLOW_FLOW_ID=your-flow-id
pytest -m integration
```

不要在命令历史、CI 日志或测试报告中输出真实凭证。

## 待使用方验收清单

| 范围 | 验收重点 |
|------|----------|
| 真实对话 | WS/SSE、默认 Flow、多会话、执行控制与恢复 |
| 工具回报 | 权限审批、REST 结果回报、Outbox 补传与冲突 |
| 媒体 Flow | 文档/图像/音频/视频上传、专用理解 Flow、异步轮询 |
| 平台技能 | 技能包上传、来源优先级、管理工具与快照 |
| 浏览器 | HEADLESS Chromium、REMOTE_CDP、网络限制、截图理解 |
| MCP | 真实 Streamable HTTP 与 stdio 服务端兼容和取消 |
| 协同 Worker | ready/recent、任务对账、回报、审批、子任务与 drain |
| 第三方身份/SSO | registrar/validator、persistentToken 轮换与 `/xeai/sso` |
| 目标部署 | 目标 Python/OS、系统权限、资源限制、日志脱敏与单实例 |

已通过离线测试不等于上述项目完成。验收结果应单独记录，不能用增加 mock 用例替代。

## 权威计划

完整范围、阶段依赖、协议差异、测试矩阵和退出条件见仓库 `plans/001-python-sdk-implementation.md`。功能范围或可靠性语义变化应先更新该权威计划，不能只修改 README 或文档首页宣称完成。
