# 构建与部署

当前版本为 `0.1.0`，处于发布候选验收阶段，不表示已经发布。未经明确授权，仓库工具不会创建提交、推送、发布包或部署 Worker。

## Python 包构建

```bash
python -m pip install -e '.[dev]'
python -m build
```

构建产物位于 `dist/`。发布前应在干净虚拟环境分别安装并检查 wheel 与 sdist，不要把当前开发环境测试通过等同于目标部署平台验收。

## 文档站点

文档使用 VitePress、本地搜索与 Mermaid，界面文案和导航为中文。

```bash
npm --prefix docs install
npm --prefix docs run docs:dev
npm --prefix docs run docs:build
npm --prefix docs run docs:preview
```

默认发布目录为 `/taiyi/pythonsdk/docs/`，与 Java SDK 的 `/taiyi/javasdk/docs/` 采用相同的 `/taiyi/<sdk>/docs/` 路径规则。本地开发服务器和构建预览也使用该目录。

需要发布到其他子路径时，通过环境变量覆盖：

```bash
VITEPRESS_BASE_PATH=/your-docs-path/ npm --prefix docs run docs:build
```

GitHub Pages 形式可使用：

```bash
npm --prefix docs run docs:build:gh
```

构建输出为 `docs/.vitepress/dist`。无 `.git` 的构建环境自动禁用 lastUpdated，避免调用 git 失败。

## Worker 部署约束

1. 同一 workerId 只运行一个实例，不依赖 SDK 做选主或 fencing。
2. 使用独立低权限系统用户；Shell 与 MCP cwd 不是操作系统沙盒。
3. workspace、state 与加密凭证分目录管理，凭证和密钥位于 workspace 外。
4. API Key、JWT、persistentToken 和 CDP Authorization 通过受控密钥系统或环境变量注入。
5. 配置有界并发、队列、Outbox、超时和 drain，不改成无界值。
6. 平台不可达或任务状态不明时等待对账，不以重跑任务修复回报失败。
7. 对日志、异常、诊断和监控系统统一脱敏。
8. 真实 Chromium/CDP、MCP server、第三方身份与 SSO 分别验收。

## 文档部署边界

本仓库当前提供文档构建脚本，不复制 Java SDK 的 Spring Boot、Dockerfile、Nginx 或单实例容器配置。目标部署拓扑与运维策略应由使用方根据实际平台确定。
