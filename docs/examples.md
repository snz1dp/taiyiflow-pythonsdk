# 示例程序

仓库示例只从环境变量读取配置，不硬编码密钥，也不打印令牌。运行示例会访问真实平台，不能替代自动化测试或真实环境验收记录。

## 通用环境变量

```bash
export TAIYIFLOW_BASE_URL=https://your-platform.example.com
export TAIYIFLOW_API_KEY=your-api-key
export TAIYIFLOW_FLOW_ID=your-flow-id  # 部分示例允许省略
```

## 示例列表

| 文件 | 内容 | 运行命令 |
|------|------|----------|
| `examples/minimal.py` | 最简异步流式对话 | `python examples/minimal.py` |
| `examples/multi_session.py` | 多会话并行消费 | `python examples/multi_session.py` |
| `examples/tool_takeover.py` | 工具 Hook 接管 | `python examples/tool_takeover.py` |
| `examples/execution_control.py` | 暂停、checkpoint 恢复 | `python examples/execution_control.py` |
| `examples/skills.py` | 本地 workspace 技能供应 | `python examples/skills.py` |
| `examples/browser.py` | HEADLESS 或 REMOTE_CDP 浏览器 | `python examples/browser.py` |
| `examples/worker.py` | 在现有 asyncio 应用中启动 Worker | `python examples/worker.py` |

## 执行控制示例

```bash
export TAIYIFLOW_STATE_DIR=./worker-state  # 可选文件 Outbox
python examples/execution_control.py
```

该示例暂停后查询当前 run 的可恢复 checkpoint，再调用 `resume()`。没有 checkpoint 时示例会明确失败，不会通过新建执行伪装恢复成功。

## 技能示例

```bash
export TAIYIFLOW_WORKSPACE=./workspace
export TAIYIFLOW_DEFAULT_SKILLS=my-skill,another-skill  # 可选
python examples/skills.py
```

默认读取 `<workspace>/.snz1dp/skills`。技能格式和来源优先级见 [技能配置与供应](skills.md)。

## 浏览器示例

未设置 CDP endpoint 时使用 HEADLESS：

```bash
export TAIYIFLOW_WORKSPACE=./workspace
export TAIYIFLOW_BROWSER_ALLOWED_HOSTS=example.com
python examples/browser.py
```

使用 REMOTE_CDP：

```bash
export TAIYIFLOW_CDP_ENDPOINT=wss://cdp.example.com/devtools/browser/id
export TAIYIFLOW_CDP_AUTHORIZATION='Bearer your-cdp-token'  # 可选
python examples/browser.py
```

Authorization 只通过环境变量注入。HEADLESS 需要自行准备 Playwright Chromium；真实 Chromium/CDP 行为仍待使用方验收。

## Worker 示例

```bash
python -m pip install -e '.[worker]'
export TAIYIFLOW_WORKER_CONFIG=./worker.yml
python examples/worker.py
```

完整 YAML 与扩展 SPI 见 [数字员工](digital-worker.md)。
