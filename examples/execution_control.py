import asyncio
import os

from taiyiagent import SessionState, TaiyiAgent


async def main() -> None:
    async with TaiyiAgent(
        base_url=os.environ["TAIYIFLOW_BASE_URL"],
        api_key=os.environ["TAIYIFLOW_API_KEY"],
        flow_id=os.environ["TAIYIFLOW_FLOW_ID"],
        recovery_policy="manual",
        state_dir=os.environ.get("TAIYIFLOW_STATE_DIR"),
    ) as agent:
        session = agent.session()
        stream = session.chat("请逐步分析这个问题，并在第一段后等待我恢复执行。")
        async for event in stream:
            if event.message_text is not None:
                print(event.message_text, end="", flush=True)
                await session.pause()
                break

        if session.state is SessionState.PAUSED:
            checkpoints = await agent.execution.list_resumable(agent.flow_id, session.session_id)
            checkpoint = next(
                (item for item in checkpoints if item.run_id == session.run_context.run_id),
                None,
            )
            if checkpoint is None:
                raise RuntimeError("No resumable checkpoint found")
            resumed = await session.resume(checkpoint)
            async for event in resumed:
                if event.message_text is not None:
                    print(event.message_text, end="", flush=True)
        print()


if __name__ == "__main__":
    asyncio.run(main())
