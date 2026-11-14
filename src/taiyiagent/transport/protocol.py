from __future__ import annotations

import asyncio
import logging

from ..models import StreamProtocol
from .rest import RestClient

logger = logging.getLogger(__name__)


class StreamProtocolResolver:
    def __init__(self, rest: RestClient, configured: StreamProtocol = StreamProtocol.AUTO) -> None:
        self._rest = rest
        self._configured = configured
        self._resolved = configured
        self._lock = asyncio.Lock()

    @property
    def automatic(self) -> bool:
        return self._configured is StreamProtocol.AUTO

    async def resolve(self) -> StreamProtocol:
        if self._resolved is not StreamProtocol.AUTO:
            return self._resolved
        async with self._lock:
            if self._resolved is StreamProtocol.AUTO:
                self._resolved = await self._read_protocol()
            return self._resolved

    async def refresh(self) -> StreamProtocol:
        if self._configured is not StreamProtocol.AUTO:
            return self._configured
        async with self._lock:
            self._resolved = await self._read_protocol()
            return self._resolved

    async def _read_protocol(self) -> StreamProtocol:
        try:
            config = await self._rest.get("/api/v1/config")
            value = config.get("stream_protocol") if isinstance(config, dict) else None
            if isinstance(value, str):
                normalized = value.strip().lower()
                if normalized == StreamProtocol.SSE.value:
                    return StreamProtocol.SSE
                if normalized == StreamProtocol.WEBSOCKET.value:
                    return StreamProtocol.WEBSOCKET
                logger.warning("Unknown stream protocol %r; falling back to WebSocket", value)
        except Exception as exc:
            logger.warning("Unable to read stream protocol; falling back to WebSocket: %s", exc)
        return StreamProtocol.WEBSOCKET
