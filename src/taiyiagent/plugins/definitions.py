from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .manager import ToolDefinition


def tool_definition(
    name: str,
    description: str,
    properties: Mapping[str, Any],
    *,
    required: Iterable[str] = (),
    feature: str | None = None,
) -> ToolDefinition:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": dict(properties),
        "additionalProperties": False,
    }
    required_names = list(required)
    if required_names:
        schema["required"] = required_names
    return ToolDefinition(name, description, schema, feature)
