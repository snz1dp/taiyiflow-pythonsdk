import asyncio
import os

from taiyiagent import TaiyiAgent


async def ask(agent: TaiyiAgent, label: str, prompt: str) -> None:
    session = agent.session()
    async for event in session.chat(prompt):
        if event.message_text is not None:
            print(f"[{label}] {event.message_text}", end="", flush=True)
    print(f"\n[{label}] state={session.state.value}")


async def main() -> None:
    async with TaiyiAgent(
        base_url=os.environ["TAIYIFLOW_BASE_URL"],
        api_key=os.environ["TAIYIFLOW_API_KEY"],
        flow_id=os.environ["TAIYIFLOW_FLOW_ID"],
    ) as agent:
        await asyncio.gather(
            ask(agent, "first", "用一句话介绍 Python"),
            ask(agent, "second", "用一句话介绍 asyncio"),
        )


if __name__ == "__main__":
    asyncio.run(main())
