# 技能配置与供应

SKILL 供应链路把本地或平台技能按请求快照发送给平台。默认及显式请求的技能发送全文，其余可见技能发送摘要；attach/resume/retry/repeat 与子任务复用稳定快照，不重新扫描或扩张权限。

## 来源与优先级

| 优先级 | 来源 | 说明 |
|--------|------|------|
| 1 | workspace | 当前工作区技能 |
| 2 | user | 用户级技能 |
| 3 | compat | 兼容目录技能 |
| 4 | bundled | SDK 或使用方打包技能 |
| 5 | platform | 平台技能 |

高优先级同名来源优先。运行时同步模式当前只支持 `DISABLED`，不要配置自动同步并假设其已生效。

## 本地技能供应

```python
from pathlib import Path
from taiyiagent import TaiyiAgent
from taiyiagent.skills import (
    FileSystemSkillProvider,
    SkillRegistry,
    SkillSource,
    SkillSupplier,
    SkillSupplyPolicy,
)

workspace = Path("./workspace")
supplier = SkillSupplier(
    SkillRegistry(
        [
            FileSystemSkillProvider(
                workspace / ".snz1dp/skills",
                SkillSource.WORKSPACE,
            )
        ]
    ),
    SkillSupplyPolicy.local_only(),
)

agent = TaiyiAgent(
    ...,
    workspace=workspace,
    skill_supplier=supplier,
    default_skill_names=("my-skill",),
)
```

完整可运行示例见 `examples/skills.py`。

## 技能包与安全限制

技能 slug 使用小写字母、数字和连字符。归档只接受声明过的常规文件，并限制：

- 条目数量、单文件解压大小、总解压大小和路径深度；
- 路径穿越、绝对路径、隐藏条目、链接和不支持的文件类型；
- 异常压缩比与重复条目；
- 每个归档必须且只能包含一个带 frontmatter 的 `SKILL.md`。

技能上传 ZIP 的 multipart 字段以后端为准，使用 `package_file`，不是 Java 当前代码中的 `file`。

## 技能管理工具

同时提供 `skill_supplier` 与 `skill_manager` 时，可注册查询、读取、安装、更新、删除和显式同步工具。管理工具仍经过统一权限和 Hook 链，写操作需要审批。

技能可见性不等于工具授权：Flow Metadata、request mode、allowed tools 和权限策略仍共同过滤工具目录。

## Worker 配置

Worker 的技能与平台来源默认关闭：

```yaml
tools:
  skills:
    enabled: false
    platform-enabled: false
    workspace-dir: ./worker-workspace/.snz1dp/skills
    default-names: []
    bundled-resources: []
    tolerate-platform-failure: true
    sync-mode: DISABLED
```

还可配置 `user-dir` 与 `compat-dir`。启用平台技能和管理工具前，应完成真实平台技能上传、读取与权限验收。
