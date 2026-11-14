import asyncio
import base64
import hashlib
import os
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
import respx

from taiyiagent.errors import ApiError, AuthenticationError, ConfigurationError
from taiyiagent.models import RecoveryPolicy
from taiyiagent.worker import (
    ApprovalChannel,
    AuthType,
    EncryptedFileCredentialStore,
    PersistentCredentials,
    WorkerAuth,
    WorkerConfig,
    WorkerRole,
)


class MemoryCredentialStore:
    def __init__(self, credentials: PersistentCredentials | None = None) -> None:
        self.credentials = credentials
        self.clear_calls = 0

    def load(self) -> PersistentCredentials | None:
        return self.credentials

    def save(self, credentials: PersistentCredentials) -> None:
        self.credentials = credentials

    def clear(self) -> None:
        self.credentials = None
        self.clear_calls += 1


@pytest.mark.asyncio
@respx.mock
async def test_persistent_auth_exchanges_rotates_and_shares_refresh() -> None:
    sso_calls = 0
    forms: list[dict[str, list[str]]] = []
    salt_tokens: list[str] = []

    def salt(request: httpx.Request) -> httpx.Response:
        salt_tokens.append(request.url.params["persistent_token"])
        return httpx.Response(
            200,
            json={"code": 0, "data": {"ticket": "ticket", "value": "salt"}},
        )

    def sso(request: httpx.Request) -> httpx.Response:
        nonlocal sso_calls
        sso_calls += 1
        forms.append(parse_qs(request.content.decode()))
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "access_token": f"access-{sso_calls}",
                    **({"persistent_token": "rotated"} if sso_calls == 1 else {}),
                },
            },
        )

    respx.get("https://example.test/xeai/sso/salt").mock(side_effect=salt)
    respx.post("https://example.test/xeai/sso").mock(side_effect=sso)
    store = MemoryCredentialStore()
    auth = WorkerAuth.persistent_token("persistent-/+?=初始", "device-1", store)
    await auth.authenticate("https://example.test")
    assert auth.access_token() == "access-1"
    assert store.credentials == PersistentCredentials("rotated", "device-1", "access-1")
    expected = base64.b64encode(hashlib.sha256(b"device-1salt").digest()).decode()
    assert forms[0]["sign"] == [expected]
    assert forms[0]["persistent_token"] == ["persistent-/+?=初始"]

    release = asyncio.Event()

    async def operation() -> str:
        token = auth.access_token()
        if token == "access-1":
            await release.wait()
            raise ApiError(401)
        return token

    pending = [asyncio.create_task(auth.execute_with_retry(operation)) for _ in range(8)]
    await asyncio.sleep(0)
    release.set()
    assert await asyncio.gather(*pending) == ["access-2"] * 8
    assert sso_calls == 2
    assert salt_tokens == ["persistent-/+?=初始", "rotated"]
    assert forms[1]["persistent_token"] == ["rotated"]
    await auth.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_persistent_auth_rejection_clears_but_server_error_preserves() -> None:
    respx.get("https://example.test/xeai/sso/salt").mock(
        return_value=httpx.Response(
            200, json={"code": 0, "data": {"ticket": "ticket", "value": "salt"}}
        )
    )
    sso = respx.post("https://example.test/xeai/sso").mock(
        side_effect=[
            httpx.Response(200, json={"code": 0, "data": {"access_token": "secret-access"}}),
            httpx.Response(503, json={"message": "persistent-secret"}),
            httpx.Response(401, json={"message": "persistent-secret"}),
        ]
    )
    store = MemoryCredentialStore()
    auth = WorkerAuth.persistent_token("persistent-secret", "device", store)
    await auth.authenticate("https://example.test")
    with pytest.raises(AuthenticationError) as unavailable:
        await auth.refresh()
    assert "persistent-secret" not in str(unavailable.value)
    assert "secret-access" not in str(unavailable.value)
    assert auth.access_token() == "secret-access"
    assert store.credentials is not None
    with pytest.raises(AuthenticationError) as rejected:
        await auth.refresh()
    assert "persistent-secret" not in str(rejected.value)
    assert auth.access_token() == ""
    assert store.credentials is None
    assert store.clear_calls == 1
    assert sso.call_count == 3
    await auth.aclose()


@pytest.mark.asyncio
async def test_static_auth_does_not_refresh_and_loads_stored_credentials() -> None:
    static = WorkerAuth.api_key("static-secret")
    assert await static.refresh() is False
    attempts = 0

    async def unauthorized() -> None:
        nonlocal attempts
        attempts += 1
        raise ApiError(401)

    with pytest.raises(ApiError):
        await static.execute_with_retry(unauthorized)
    assert attempts == 1
    stored = PersistentCredentials("persistent", "stored-device", "old-access")
    persistent = WorkerAuth.persistent_token("", "", MemoryCredentialStore(stored))
    assert persistent.device_id == "stored-device"
    assert persistent.access_token() == "old-access"
    await persistent.aclose()


def test_encrypted_store_round_trip_permissions_and_tamper_failure(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    store = EncryptedFileCredentialStore(
        tmp_path / "secure/credentials.bin",
        tmp_path / "secure/key.bin",
        workspace=workspace,
    )
    credentials = PersistentCredentials("persistent-secret", "device", "access-secret")
    store.save(credentials)
    first_ciphertext = (tmp_path / "secure/credentials.bin").read_bytes()
    assert store.load() == credentials
    store.save(credentials)
    assert first_ciphertext != (tmp_path / "secure/credentials.bin").read_bytes()
    assert repr(credentials) == "PersistentCredentials(<redacted>)"
    assert "persistent-secret" not in repr(credentials)
    if os.name == "posix":
        assert (tmp_path / "secure/credentials.bin").stat().st_mode & 0o777 == 0o600
        assert (tmp_path / "secure/key.bin").stat().st_mode & 0o777 == 0o600
    with (tmp_path / "secure/credentials.bin").open("ab") as output:
        output.write(b"tampered")
    with pytest.raises(AuthenticationError) as invalid:
        store.load()
    assert "persistent-secret" not in str(invalid.value)
    store.clear()
    assert store.load() is None


def test_encrypted_store_rejects_workspace_and_wrong_key(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError):
        EncryptedFileCredentialStore(
            tmp_path / "workspace/credentials", tmp_path / "key", workspace=tmp_path / "workspace"
        )
    credentials = tmp_path / "secure/credentials"
    key = tmp_path / "secure/key"
    store = EncryptedFileCredentialStore(credentials, key)
    store.save(PersistentCredentials("persistent", "device", "access"))
    key.write_bytes(b"wrong-key")
    with pytest.raises(AuthenticationError):
        store.load()


def test_worker_config_loads_yaml_defaults_and_env_secret(tmp_path: Path) -> None:
    config_path = tmp_path / "worker.yml"
    config_path.write_text(
        f"""
base-url: https://example.test
auth:
  type: API_KEY
  token-secret: TAIYIFLOW_API_KEY
identity:
  worker-id: worker-1
  display-name: Test Worker
  abilities: [shell, files]
workspace: {tmp_path / "workspace"}
state-dir: {tmp_path / "state"}
outbox:
  max-entries: 20
  max-entry-bytes: 4096
  max-total-bytes: 65536
  max-retries: 5
recovery-policy: AUTO
permission-policy: CONFIRM
approval:
  channel: PLATFORM
execution:
  concurrency: 5
  connect-timeout: PT20S
tools:
  builtin-tools: true
  allowed-shell-commands: [git]
  media: true
  skills:
    enabled: true
    platform-enabled: true
    default-names: [demo]
    sync-mode: DISABLED
  browser:
    mode: REMOTE_CDP
    action-timeout: PT10S
    max-pages-per-session: 4
    remote-endpoint-secret: TAIYIFLOW_CDP_ENDPOINT
    remote-authorization-secret: TAIYIFLOW_CDP_AUTHORIZATION
extensions:
  identity-validator: example.PlatformIdentityValidator
  worker-registrar: example.IamWorkerRegistrar
  task-handler: example.BusinessTaskHandler
  task-reporter: example.BusinessTaskReporter
  hooks: [example.MetricsHook]
""",
        encoding="utf-8",
    )
    config = WorkerConfig.load(config_path)
    assert config.auth.type is AuthType.API_KEY
    assert config.identity.role is WorkerRole.COLLABORATION_WORKER
    assert config.recovery_policy is RecoveryPolicy.AUTO
    assert config.approval.channel is ApprovalChannel.PLATFORM
    assert config.execution.connect_timeout == 20
    assert config.execution.request_timeout == 30
    assert config.outbox.max_entries == 20
    assert config.outbox.max_retries == 5
    assert config.extensions.identity_validator == "example.PlatformIdentityValidator"
    assert config.tools.builtin_tools
    assert config.tools.allowed_shell_commands == ("git",)
    assert config.tools.skills.default_names == ("demo",)
    assert config.tools.browser.action_timeout == 10
    browser = config.tools.browser.create_settings(
        {
            "TAIYIFLOW_CDP_ENDPOINT": "wss://browser.example/cdp?token=private",
            "TAIYIFLOW_CDP_AUTHORIZATION": "Bearer private",
        }
    )
    assert browser.redacted_remote_host() == "wss://browser.example"
    assert "private" not in repr(browser)
    auth = config.create_auth({"TAIYIFLOW_API_KEY": "actual-secret"})
    assert auth.access_token() == "actual-secret"
    assert "actual-secret" not in repr(config)
    with pytest.raises(ConfigurationError, match="Secret is missing"):
        config.create_auth({})


def test_persistent_config_requires_separate_store_and_device_fallback(tmp_path: Path) -> None:
    config_path = tmp_path / "worker.yml"
    config_path.write_text(
        f"""
base-url: https://example.test
auth:
  type: PERSISTENT_TOKEN
  token-secret: TAIYIFLOW_PERSISTENT_TOKEN
  credential-file: {tmp_path / "secure/credentials"}
  key-file: {tmp_path / "secure/key"}
identity:
  worker-id: worker-1
  display-name: Worker
workspace: {tmp_path / "workspace"}
device-id: device-1
""",
        encoding="utf-8",
    )
    config = WorkerConfig.load(config_path)
    auth = config.create_auth({"TAIYIFLOW_PERSISTENT_TOKEN": "persistent"})
    assert isinstance(auth, WorkerAuth)
    assert auth.device_id == "device-1"
    assert auth.access_token() == ""
