from __future__ import annotations

import asyncio
import base64
import hashlib
from collections.abc import Awaitable, Callable
from typing import TypeVar

import httpx

from ..errors import ApiError, AuthenticationError, ConfigurationError, TransportError
from .credentials import CredentialStore, PersistentCredentials

T = TypeVar("T")


class WorkerAuth:
    def __init__(self) -> None:
        self._generation = 0
        self._refresh_lock = asyncio.Lock()
        self._last_source_generation: int | None = None
        self._last_refresh_error: AuthenticationError | None = None

    @staticmethod
    def api_key(api_key: str) -> WorkerAuth:
        return StaticWorkerAuth(api_key)

    @staticmethod
    def jwt(token: str) -> WorkerAuth:
        return StaticWorkerAuth(token)

    @staticmethod
    def persistent_token(
        token: str,
        device_id: str,
        store: CredentialStore,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> WorkerAuth:
        return PersistentWorkerAuth(token, device_id, store, http_client=http_client)

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def device_id(self) -> str | None:
        return None

    @property
    def supports_refresh(self) -> bool:
        return False

    def access_token(self) -> str:
        raise NotImplementedError

    async def authenticate(self, base_url: str) -> None:
        if not base_url.strip():
            raise ConfigurationError("base_url is required")
        if not self.supports_refresh:
            await self._authenticate(base_url)
            return
        async with self._refresh_lock:
            try:
                await self._authenticate(base_url)
            finally:
                self._generation += 1
                self._last_source_generation = None
                self._last_refresh_error = None

    async def refresh(self) -> bool:
        return await self.refresh_after_unauthorized(self.generation)

    async def refresh_after_unauthorized(self, observed_generation: int) -> bool:
        if not self.supports_refresh:
            return False
        async with self._refresh_lock:
            if observed_generation != self._generation:
                if self._last_source_generation == observed_generation:
                    if self._last_refresh_error is not None:
                        raise self._last_refresh_error
                    return bool(self.access_token())
                return bool(self.access_token())
            self._last_source_generation = observed_generation
            self._last_refresh_error = None
            try:
                await self._refresh()
                return True
            except AuthenticationError as exc:
                self._last_refresh_error = exc
                raise
            finally:
                self._generation += 1

    async def execute_with_retry(
        self,
        operation: Callable[[], Awaitable[T]],
    ) -> T:
        observed_generation = self.generation
        try:
            return await operation()
        except ApiError as exc:
            if exc.status_code != 401 or not await self.refresh_after_unauthorized(
                observed_generation
            ):
                raise
            return await operation()

    async def aclose(self) -> None:
        return None

    async def _authenticate(self, base_url: str) -> None:
        raise NotImplementedError

    async def _refresh(self) -> None:
        raise NotImplementedError


class StaticWorkerAuth(WorkerAuth):
    def __init__(self, token: str) -> None:
        super().__init__()
        if not token.strip():
            raise ConfigurationError("Authentication token is required")
        self._token = token

    def access_token(self) -> str:
        return self._token

    async def _authenticate(self, base_url: str) -> None:
        return None


class PersistentWorkerAuth(WorkerAuth):
    def __init__(
        self,
        token: str,
        device_id: str,
        store: CredentialStore,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__()
        self._store = store
        self._persistent_token = token.strip() or None
        self._device_id = device_id.strip() or None
        self._access_token: str | None = None
        self._base_url: str | None = None
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=10.0),
            follow_redirects=False,
        )
        if self._persistent_token is None:
            stored = store.load()
            if stored is not None:
                self._persistent_token = stored.persistent_token
                self._device_id = self._device_id or stored.device_id
                self._access_token = stored.access_token

    @property
    def supports_refresh(self) -> bool:
        return True

    @property
    def device_id(self) -> str | None:
        return self._device_id

    def access_token(self) -> str:
        return self._access_token or ""

    async def _authenticate(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")
        await self._exchange()

    async def _refresh(self) -> None:
        await self._exchange()

    async def _exchange(self) -> None:
        token = self._persistent_token
        device_id = self._device_id
        if not token or not device_id or self._base_url is None:
            raise AuthenticationError("Persistent token, device_id and base_url are required")
        try:
            salt_response = await self._client.get(
                self._base_url + "/xeai/sso/salt",
                params={"persistent_token": token},
            )
            salt = _successful_data(salt_response)
            ticket = _required_text(salt, "ticket")
            value = _required_text(salt, "value")
            signature = base64.b64encode(
                hashlib.sha256((device_id + value).encode()).digest()
            ).decode()
            token_response = await self._client.post(
                self._base_url + "/xeai/sso",
                data={
                    "persistent_token": token,
                    "salt_ticket": ticket,
                    "sign": signature,
                    "alg": "SHA-256",
                },
            )
            token_data = _successful_data(token_response)
            access_token = _required_text(token_data, "access_token")
            rotated = token_data.get("persistent_token")
            persistent_token = rotated if isinstance(rotated, str) and rotated.strip() else token
            self._store.save(PersistentCredentials(persistent_token, device_id, access_token))
            self._persistent_token = persistent_token
            self._access_token = access_token
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status in {400, 401, 403, 410}:
                self._clear_rejected()
                raise AuthenticationError("Persistent credentials were rejected") from exc
            raise AuthenticationError("Persistent credential exchange failed") from exc
        except _RejectedCredentials as exc:
            self._clear_rejected()
            raise AuthenticationError("Persistent credentials were rejected") from exc
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise AuthenticationError("Persistent credential exchange failed") from exc

    def _clear_rejected(self) -> None:
        self._persistent_token = None
        self._access_token = None
        self._store.clear()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()


class _RejectedCredentials(Exception):
    pass


def _successful_data(response: httpx.Response) -> dict[str, object]:
    response.raise_for_status()
    value = response.json()
    if not isinstance(value, dict) or value.get("code") != 0:
        raise _RejectedCredentials
    data = value.get("data")
    if not isinstance(data, dict):
        raise TransportError("Authentication response did not contain data")
    return data


def _required_text(data: dict[str, object], field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value.strip():
        raise TransportError("Authentication response was incomplete")
    return value
