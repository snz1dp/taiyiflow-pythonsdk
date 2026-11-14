from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from ..errors import TaiyiFlowError
from ..models import ToolCall, ToolResult
from ..plugins.manager import PluginManager, ToolExecutionContext
from ..transport.capabilities import JsonObject, JsonValue

PROTOCOL_VERSION = "2025-03-26"


class McpError(TaiyiFlowError):
    pass


@dataclass(frozen=True, slots=True)
class McpTool:
    name: str
    description: str = ""
    input_schema: JsonObject = field(default_factory=lambda: {"type": "object"})


class McpClient(Protocol):
    async def list_tools(self) -> list[McpTool]: ...

    async def call_tool(self, tool_name: str, arguments: JsonObject) -> JsonObject: ...

    async def aclose(self) -> None: ...


def parse_tools(result: JsonObject) -> list[McpTool]:
    tools = result.get("tools")
    if not isinstance(tools, list):
        raise McpError("MCP tools/list result has no tools array")
    parsed: list[McpTool] = []
    for item in tools:
        if not isinstance(item, dict):
            raise McpError("MCP tool descriptor must be an object")
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            raise McpError("MCP tool descriptor has no name")
        description = item.get("description", "")
        schema = item.get("inputSchema", item.get("input_schema", {"type": "object"}))
        if not isinstance(description, str) or not isinstance(schema, dict):
            raise McpError("MCP tool descriptor has invalid metadata")
        parsed.append(McpTool(name, description, schema))
    return parsed


def parse_result(payload: JsonValue, request_id: int, method: str) -> JsonObject:
    if not isinstance(payload, dict) or payload.get("jsonrpc") != "2.0":
        raise McpError("Invalid MCP JSON-RPC response")
    response_id = payload.get("id")
    if isinstance(response_id, bool) or response_id != request_id:
        raise McpError("MCP response id does not match request")
    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message", "remote error")
        raise McpError(f"MCP {method} failed: {str(message)[:4096]}")
    if error is not None:
        raise McpError(f"MCP {method} failed: remote error")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise McpError(f"MCP {method} response has no object result")
    return result


async def register_tools(
    client: McpClient,
    server_name: str,
    plugin_manager: PluginManager,
) -> list[str]:
    if not server_name.strip():
        raise ValueError("server_name is required")
    registered: list[str] = []
    for tool in await client.list_tools():
        registered_name = f"c_{server_name}_{tool.name}"

        async def handler(
            context: ToolExecutionContext,
            call: ToolCall,
            tool_name: str = tool.name,
        ) -> ToolResult:
            result = await client.call_tool(tool_name, call.arguments_dict)
            return ToolResult.ok(call.id, result)

        plugin_manager.register(registered_name, handler)
        registered.append(registered_name)
    return registered
