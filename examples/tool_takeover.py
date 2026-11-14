import asyncio
import os

from taiyiagent import TaiyiAgent, ToolCall, ToolExecutionHook, ToolResult


class LocalToolHook(ToolExecutionHook):
    async def takeover(self, session_id: str, tool_call: ToolCall) -> ToolResult | None:
        if tool_call.name != "local_echo":
            return None
        return ToolResult.ok(tool_call.id, {"echo": tool_call.arguments_dict})


async def main() -> None:
    async with TaiyiAgent(
        base_url=os.environ["TAIYIFLOW_BASE_URL"],
        api_key=os.environ["TAIYIFLOW_API_KEY"],
        flow_id=os.environ["TAIYIFLOW_FLOW_ID"],
        permission_policy="auto",
        tool_hooks=[LocalToolHook()],
    ) as agent:
        session = agent.session()
        async for event in session.chat("请调用 local_echo 工具，参数为 message=hello"):
            if event.message_text is not None:
                print(event.message_text, end="", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
