from __future__ import annotations

import asyncio
import os
from pathlib import Path

from taiyiagent import TaiyiAgent
from taiyiagent.skills import (
    FileSystemSkillProvider,
    SkillRegistry,
    SkillSource,
    SkillSupplier,
    SkillSupplyPolicy,
)


async def main() -> None:
    workspace = Path(os.environ.get("TAIYIFLOW_WORKSPACE", "./workspace"))
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
    async with TaiyiAgent(
        base_url=os.environ["TAIYIFLOW_BASE_URL"],
        api_key=os.environ["TAIYIFLOW_API_KEY"],
        flow_id=os.environ.get("TAIYIFLOW_FLOW_ID"),
        workspace=workspace,
        skill_supplier=supplier,
        default_skill_names=tuple(
            name.strip()
            for name in os.environ.get("TAIYIFLOW_DEFAULT_SKILLS", "").split(",")
            if name.strip()
        ),
    ) as agent:
        session = agent.session()
        async for event in session.chat("按当前可用技能完成这个任务，并说明使用了哪些技能"):
            if event.message_text is not None:
                print(event.message_text, end="", flush=True)
        print()


if __name__ == "__main__":
    asyncio.run(main())
