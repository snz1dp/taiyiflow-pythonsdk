import asyncio
import os

from taiyiagent import TaiyiAgent


async def main() -> None:
    async with TaiyiAgent(
        base_url=os.environ["TAIYIFLOW_BASE_URL"],
        api_key=os.environ["TAIYIFLOW_API_KEY"],
        flow_id=os.environ["TAIYIFLOW_FLOW_ID"],
    ) as agent:
        session = agent.session()
        async for event in session.chat("你好，请介绍一下你自己"):
            if event.message_text is not None:
                print(event.message_text, end="", flush=True)
        print()


if __name__ == "__main__":
    asyncio.run(main())
