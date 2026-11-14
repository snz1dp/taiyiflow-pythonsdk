from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from types import MappingProxyType
from typing import Any

from ..errors import ProtocolError


class ServiceConfig:
    def __init__(self, raw: Mapping[str, Any]) -> None:
        if not isinstance(raw, Mapping):
            raise ProtocolError("/api/v1/config must return a JSON object")
        self._raw = deepcopy(dict(raw))

    @property
    def raw(self) -> dict[str, Any]:
        return deepcopy(self._raw)

    @property
    def stream_protocol(self) -> str | None:
        return self._text("stream_protocol")

    def default_flow_id(self) -> str:
        return self._required_text("default_flow_id")

    def document_flow_id(self) -> str:
        return self._required_text("doc_unstructured_flow_id")

    def image_flow_id(self) -> str:
        return self._required_text("image_flow_id")

    def audio_flow_id(self) -> str:
        return self._required_text("audio_flow_id")

    def video_flow_id(self) -> str:
        return self._required_text("video_flow_id")

    def document_extensions(self) -> tuple[str, ...]:
        values = self._raw.get("doc_exts")
        if not isinstance(values, list):
            return ()
        extensions: dict[str, None] = {}
        for value in values:
            if not isinstance(value, str):
                continue
            extension = value.strip().lower().removeprefix(".")
            if extension:
                extensions[extension] = None
        return tuple(extensions)

    def max_file_size_upload(self) -> int | None:
        return self._positive_int("max_file_size_upload", None)

    def max_file_size_bytes(self) -> int | None:
        megabytes = self.max_file_size_upload()
        return None if megabytes is None else megabytes * 1024 * 1024

    def once_max_file_count(self) -> int | None:
        return self._positive_int("once_max_file_count", None)

    def _required_text(self, name: str) -> str:
        value = self._text(name)
        if value is None or not value.strip():
            raise ProtocolError(f"Server configuration is missing required field: {name}")
        return value.strip()

    def _text(self, name: str) -> str | None:
        value = self._raw.get(name)
        return value if isinstance(value, str) else None

    def _positive_int(self, name: str, fallback: int | None) -> int | None:
        value = self._raw.get(name)
        if value is None:
            return fallback
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ProtocolError(f"Server configuration field must be positive: {name}")
        return value


class FlowMetadata:
    _KNOWN_FIELDS = frozenset(
        {
            "id",
            "owner_appid",
            "name",
            "description",
            "enabled",
            "is_chat",
            "max_input_tokens",
            "welcome_message",
            "favicon",
            "logo",
            "avatar",
            "ai_name",
            "features",
            "questions",
            "knowledges",
            "sub_head",
            "custom_fields",
            "shadow_flows",
        }
    )

    def __init__(self, raw: Mapping[str, Any]) -> None:
        if not isinstance(raw, Mapping):
            raise ProtocolError("Flow metadata must be a JSON object")
        self._raw = deepcopy(dict(raw))
        self.id = self._text("id")
        self.owner_app_id = self._text("owner_appid")
        self.name = self._text("name")
        self.description = self._text("description")
        self.enabled = self._bool("enabled")
        self.is_chat = self._bool("is_chat")
        max_input_tokens = self._raw.get("max_input_tokens")
        self.max_input_tokens = (
            max_input_tokens
            if isinstance(max_input_tokens, int) and not isinstance(max_input_tokens, bool)
            else None
        )
        self.welcome_message = self._text("welcome_message")
        self.favicon = self._text("favicon")
        self.logo = self._text("logo")
        self.avatar = self._text("avatar")
        self.ai_name = self._text("ai_name")
        self.features = self._strings("features")
        self.questions = self._strings("questions")
        knowledges = self._raw.get("knowledges")
        self.knowledges = (
            tuple(deepcopy(item) for item in knowledges if isinstance(item, dict))
            if isinstance(knowledges, list)
            else ()
        )
        self.sub_head = self._text("sub_head")
        custom_fields = self._raw.get("custom_fields")
        self.custom_fields = deepcopy(custom_fields) if custom_fields is not None else None
        shadow_flows = self._raw.get("shadow_flows")
        self.shadow_flows = (
            tuple(deepcopy(item) for item in shadow_flows) if isinstance(shadow_flows, list) else ()
        )
        self.additional_properties = MappingProxyType(
            {
                key: deepcopy(value)
                for key, value in self._raw.items()
                if key not in self._KNOWN_FIELDS
            }
        )

    @property
    def raw(self) -> dict[str, Any]:
        return deepcopy(self._raw)

    def supports(self, feature: str) -> bool:
        return bool(feature) and feature in self.features

    def _text(self, name: str) -> str | None:
        value = self._raw.get(name)
        return str(value) if value is not None else None

    def _bool(self, name: str) -> bool | None:
        value = self._raw.get(name)
        return value if isinstance(value, bool) else None

    def _strings(self, name: str) -> tuple[str, ...]:
        value = self._raw.get(name)
        if not isinstance(value, list):
            return ()
        return tuple(item for item in value if isinstance(item, str))
