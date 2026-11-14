---
layout: home

hero:
  name: "太乙智启 Python SDK"
  text: "平台智能体与数字员工开发手册"
  tagline: 使用原生 asyncio 驱动平台智能体多会话并行执行，或让 Python 进程成为常驻接活、请示、回报的数字员工
  actions:
    - theme: brand
      text: 快速开始
      link: /getting-started
    - theme: alt
      text: 智能体与会话
      link: /agent-and-session
    - theme: alt
      text: 数字员工
      link: /digital-worker

features:
  - title: 双形态运行时
    details: TaiyiAgent 主动驱动平台智能体；DigitalWorker 通过可选协同连接接收平台任务，一个 SDK 覆盖两种接入形态。
  - title: 原生异步多会话
    details: 基于 asyncio、异步迭代器与异步上下文管理器，会话间隔离，可独立暂停、恢复、取消和中断。
  - title: 钩子扩展体系
    details: 流式输出、工具接管、权限审批和会话生命周期四类 Hook，业务定制不侵入 SDK。
  - title: 工具、技能与 MCP
    details: 文件、搜索、Shell、媒体理解、SKILL、可选 Chromium，以及 Streamable HTTP 和 stdio MCP。
  - title: 安全可控
    details: READONLY、CONFIRM、AUTO 权限策略，workspace 路径约束、命令白名单和审批闭环。
  - title: 可靠性语义
    details: attach 与 checkpoint 恢复不创建新执行，Outbox 只补传结果不重跑工具，Worker 以平台状态对账。
---
