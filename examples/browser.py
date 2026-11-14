from __future__ import annotations

import asyncio
import os
from pathlib import Path

from taiyiagent import BrowserMode, BrowserSettings, TaiyiAgent


async def main() -> None:
    endpoint = os.environ.get("TAIYIFLOW_CDP_ENDPOINT")
    authorization = os.environ.get("TAIYIFLOW_CDP_AUTHORIZATION", "")
    settings = BrowserSettings(
        mode=BrowserMode.REMOTE_CDP if endpoint else BrowserMode.HEADLESS,
        remote_endpoint=endpoint,
        remote_headers={"Authorization": authorization} if authorization else {},
        allowed_hosts=frozenset(
            host.strip()
            for host in os.environ.get("TAIYIFLOW_BROWSER_ALLOWED_HOSTS", "").split(",")
            if host.strip()
        ),
    )
    async with TaiyiAgent(
        base_url=os.environ["TAIYIFLOW_BASE_URL"],
        api_key=os.environ["TAIYIFLOW_API_KEY"],
        flow_id=os.environ.get("TAIYIFLOW_FLOW_ID"),
        workspace=Path(os.environ.get("TAIYIFLOW_WORKSPACE", "./workspace")),
        enable_builtin_tools=False,
        browser_settings=settings,
    ) as agent:
        session = agent.session()
        async for event in session.chat("打开示例站点，概述页面内容并截图理解关键状态"):
            if event.message_text is not None:
                print(event.message_text, end="", flush=True)
        print()


if __name__ == "__main__":
    asyncio.run(main())
