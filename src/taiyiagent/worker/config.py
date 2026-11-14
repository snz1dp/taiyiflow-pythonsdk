from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TypeVar

from ..browser import BrowserMode, BrowserSettings
from ..errors import ConfigurationError
from ..hooks import PermissionPolicy
from ..models import RecoveryPolicy
from ..skills import SkillSyncMode
from .auth import WorkerAuth
from .credentials import EncryptedFileCredentialStore
from .identity import WorkerRole


class AuthType(StrEnum):
    API_KEY = "api_key"
    JWT = "jwt"
    PERSISTENT_TOKEN = "persistent_token"  # noqa: S105


class ApprovalChannel(StrEnum):
    PLATFORM = "platform"
    LOCAL = "local"


@dataclass(frozen=True)
class WorkerAuthConfig:
    type: AuthType
    token_secret: str = ""
    device_id: str = ""
    credential_file: Path | None = None
    key_file: Path | None = None


@dataclass(frozen=True)
class WorkerIdentityConfig:
    worker_id: str = ""
    display_name: str = ""
    role: WorkerRole = WorkerRole.COLLABORATION_WORKER
    abilities: tuple[str, ...] = ()


@dataclass(frozen=True)
class WorkerApprovalConfig:
    channel: ApprovalChannel = ApprovalChannel.PLATFORM


@dataclass(frozen=True)
class WorkerExecutionConfig:
    concurrency: int = 3
    connect_timeout: float = 15.0
    drain_timeout: float = 30.0
    request_timeout: float = 30.0

    def __post_init__(self) -> None:
        if (
            self.concurrency <= 0
            or min(self.connect_timeout, self.drain_timeout, self.request_timeout) <= 0
        ):
            raise ConfigurationError("Worker concurrency and timeouts must be positive")


@dataclass(frozen=True)
class WorkerExtensionsConfig:
    identity_validator: str = ""
    worker_registrar: str = ""
    task_handler: str = ""
    task_reporter: str = ""
    approval_handler: str = ""
    approval_audit: str = ""
    subtask_mcp_resolver: str = ""
    hooks: tuple[str, ...] = ()


@dataclass(frozen=True)
class WorkerOutboxConfig:
    max_entries: int = 1000
    max_entry_bytes: int = 1024 * 1024
    max_total_bytes: int = 16 * 1024 * 1024
    max_retries: int = 32

    def __post_init__(self) -> None:
        if min(self.max_entries, self.max_entry_bytes, self.max_total_bytes, self.max_retries) <= 0:
            raise ConfigurationError("Worker outbox limits must be positive")


@dataclass(frozen=True)
class WorkerSkillsConfig:
    enabled: bool = False
    platform_enabled: bool = False
    workspace_dir: Path | None = None
    user_dir: Path | None = None
    compat_dir: Path | None = None
    default_names: tuple[str, ...] = ()
    bundled_resources: tuple[str, ...] = ()
    tolerate_platform_failure: bool = True
    sync_mode: SkillSyncMode = SkillSyncMode.DISABLED

    def __post_init__(self) -> None:
        if self.sync_mode is not SkillSyncMode.DISABLED:
            raise ConfigurationError("Only skills.sync-mode=DISABLED is supported by the runtime")


@dataclass(frozen=True)
class WorkerBrowserConfig:
    mode: BrowserMode = BrowserMode.DISABLED
    executable_path: Path | None = None
    launch_arguments: tuple[str, ...] = ()
    action_timeout: float = 15.0
    max_pages_per_session: int = 10
    max_read_text_characters: int = 8000
    max_console_messages: int = 200
    max_screenshot_bytes: int = 10 * 1024 * 1024
    execute_js_enabled: bool = False
    allowed_hosts: frozenset[str] = frozenset()
    allowed_cidrs: frozenset[str] = frozenset()
    remote_endpoint_secret: str = ""
    remote_authorization_secret: str = ""

    def create_settings(self, secrets: Mapping[str, str]) -> BrowserSettings:
        endpoint = secrets.get(self.remote_endpoint_secret) if self.remote_endpoint_secret else None
        headers: dict[str, str] = {}
        if self.remote_authorization_secret:
            authorization = secrets.get(self.remote_authorization_secret, "")
            if not authorization.strip():
                raise ConfigurationError(
                    f"Secret is missing or blank: {self.remote_authorization_secret}"
                )
            headers["Authorization"] = authorization
        return BrowserSettings(
            mode=self.mode,
            executable_path=self.executable_path,
            launch_arguments=self.launch_arguments,
            action_timeout=self.action_timeout,
            max_pages_per_session=self.max_pages_per_session,
            max_read_text_characters=self.max_read_text_characters,
            max_console_messages=self.max_console_messages,
            max_screenshot_bytes=self.max_screenshot_bytes,
            execute_js_enabled=self.execute_js_enabled,
            allowed_hosts=self.allowed_hosts,
            allowed_cidrs=self.allowed_cidrs,
            remote_endpoint=endpoint,
            remote_headers=headers,
        )


@dataclass(frozen=True)
class WorkerToolsConfig:
    builtin_tools: bool = False
    allowed_shell_commands: tuple[str, ...] = ()
    media: bool = False
    skills: WorkerSkillsConfig = field(default_factory=WorkerSkillsConfig)
    browser: WorkerBrowserConfig = field(default_factory=WorkerBrowserConfig)


@dataclass(frozen=True)
class WorkerConfig:
    base_url: str
    auth: WorkerAuthConfig
    identity: WorkerIdentityConfig
    workspace: Path
    client_id: str = ""
    device_id: str = ""
    recovery_policy: RecoveryPolicy = RecoveryPolicy.AUTO
    permission_policy: PermissionPolicy = PermissionPolicy.CONFIRM
    approval: WorkerApprovalConfig = field(default_factory=WorkerApprovalConfig)
    execution: WorkerExecutionConfig = field(default_factory=WorkerExecutionConfig)
    extensions: WorkerExtensionsConfig = field(default_factory=WorkerExtensionsConfig)
    state_dir: Path | None = None
    outbox: WorkerOutboxConfig = field(default_factory=WorkerOutboxConfig)
    tools: WorkerToolsConfig = field(default_factory=WorkerToolsConfig)

    def __post_init__(self) -> None:
        if not self.base_url.strip():
            raise ConfigurationError("base_url is required")

    @classmethod
    def load(cls, path: str | Path) -> WorkerConfig:
        try:
            import yaml
        except ImportError as exc:
            raise ConfigurationError(
                "Worker YAML configuration requires the 'worker' extra"
            ) from exc
        config_path = Path(path)
        try:
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            raise ConfigurationError("Unable to load worker YAML configuration") from exc
        if not isinstance(raw, dict):
            raise ConfigurationError("Worker configuration must be a mapping")
        values = _kebab_mapping(raw)
        auth_values = _section(values, "auth")
        identity_values = _section(values, "identity")
        try:
            return cls(
                base_url=_text(values, "base_url"),
                auth=WorkerAuthConfig(
                    type=_enum(AuthType, _text(auth_values, "type"), "auth.type"),
                    token_secret=_text(auth_values, "token_secret"),
                    device_id=_text(auth_values, "device_id"),
                    credential_file=_path(auth_values.get("credential_file")),
                    key_file=_path(auth_values.get("key_file")),
                ),
                identity=WorkerIdentityConfig(
                    worker_id=_text(identity_values, "worker_id"),
                    display_name=_text(identity_values, "display_name"),
                    role=_enum(
                        WorkerRole,
                        identity_values.get("role", WorkerRole.COLLABORATION_WORKER.value),
                        "identity.role",
                    ),
                    abilities=_strings(identity_values.get("abilities")),
                ),
                workspace=_required_path(values, "workspace"),
                client_id=_text(values, "client_id"),
                device_id=_text(values, "device_id"),
                recovery_policy=_enum(
                    RecoveryPolicy,
                    values.get("recovery_policy", RecoveryPolicy.AUTO.value),
                    "recovery-policy",
                ),
                permission_policy=_enum(
                    PermissionPolicy,
                    values.get("permission_policy", PermissionPolicy.CONFIRM.value),
                    "permission-policy",
                ),
                approval=WorkerApprovalConfig(
                    channel=_enum(
                        ApprovalChannel,
                        _section(values, "approval").get("channel", ApprovalChannel.PLATFORM.value),
                        "approval.channel",
                    )
                ),
                execution=_execution(values),
                extensions=_extensions(values),
                state_dir=_path(values.get("state_dir")),
                outbox=_outbox(values),
                tools=_tools(values),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, ConfigurationError):
                raise
            raise ConfigurationError("Invalid worker configuration") from exc

    def create_auth(
        self,
        secrets: Mapping[str, str],
    ) -> WorkerAuth:
        token = secrets.get(self.auth.token_secret, "") if self.auth.token_secret else ""
        if self.auth.type is AuthType.PERSISTENT_TOKEN:
            if self.auth.credential_file is None or self.auth.key_file is None:
                raise ConfigurationError(
                    "auth.credential-file and auth.key-file are required for persistent tokens"
                )
            store = EncryptedFileCredentialStore(
                self.auth.credential_file,
                self.auth.key_file,
                workspace=self.workspace,
            )
            return WorkerAuth.persistent_token(
                token,
                self.auth.device_id or self.device_id,
                store,
            )
        if not token.strip():
            raise ConfigurationError(
                f"Secret is missing or blank: {self.auth.token_secret or '<unset>'}"
            )
        if self.auth.type is AuthType.API_KEY:
            return WorkerAuth.api_key(token)
        return WorkerAuth.jwt(token)


E = TypeVar("E", bound=StrEnum)


def _section(values: dict[str, object], name: str) -> dict[str, object]:
    value = values.get(name, {})
    if not isinstance(value, dict):
        raise ConfigurationError(f"{name} must be a mapping")
    return _kebab_mapping(value)


def _kebab_mapping(values: dict[object, object]) -> dict[str, object]:
    return {str(key).replace("-", "_"): value for key, value in values.items()}


def _text(values: Mapping[str, object], name: str) -> str:
    value = values.get(name, "")
    return value if isinstance(value, str) else str(value)


def _path(value: object) -> Path | None:
    return Path(value) if isinstance(value, str) and value.strip() else None


def _required_path(values: Mapping[str, object], name: str) -> Path:
    path = _path(values.get(name))
    if path is None:
        raise ConfigurationError(f"{name} is required")
    return path


def _enum(enum_type: type[E], value: object, name: str) -> E:
    text = str(value)
    try:
        return enum_type(text.lower())
    except ValueError:
        try:
            return enum_type(text.upper())
        except ValueError as exc:
            raise ConfigurationError(f"Unsupported {name}") from exc


def _strings(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigurationError("Expected a list of strings")
    return tuple(value)


def _integer(value: object, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise ConfigurationError("Expected an integer")
    return int(value)


def _number(values: Mapping[str, object], name: str, default: float) -> float:
    value = values.get(name, default)
    if isinstance(value, bool):
        raise ConfigurationError(f"{name} must be numeric")
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        return _duration(value)
    raise ConfigurationError(f"{name} must be numeric")


def _duration(value: str) -> float:
    match = re.fullmatch(
        r"PT(?:(?P<hours>\d+(?:\.\d+)?)H)?(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
        r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?",
        value,
        flags=re.IGNORECASE,
    )
    if match is None or not any(match.groupdict().values()):
        raise ConfigurationError("Duration must use ISO-8601 PT format")
    return (
        float(match.group("hours") or 0) * 3600
        + float(match.group("minutes") or 0) * 60
        + float(match.group("seconds") or 0)
    )


def _execution(values: dict[str, object]) -> WorkerExecutionConfig:
    section = _section(values, "execution")
    return WorkerExecutionConfig(
        concurrency=_integer(section.get("concurrency"), 3),
        connect_timeout=_number(section, "connect_timeout", 15.0),
        drain_timeout=_number(section, "drain_timeout", 30.0),
        request_timeout=_number(section, "request_timeout", 30.0),
    )


def _extensions(values: dict[str, object]) -> WorkerExtensionsConfig:
    section = _section(values, "extensions")
    return WorkerExtensionsConfig(
        identity_validator=_text(section, "identity_validator"),
        worker_registrar=_text(section, "worker_registrar"),
        task_handler=_text(section, "task_handler"),
        task_reporter=_text(section, "task_reporter"),
        approval_handler=_text(section, "approval_handler"),
        approval_audit=_text(section, "approval_audit"),
        subtask_mcp_resolver=_text(section, "subtask_mcp_resolver"),
        hooks=_strings(section.get("hooks")),
    )


def _outbox(values: dict[str, object]) -> WorkerOutboxConfig:
    section = _section(values, "outbox")
    return WorkerOutboxConfig(
        max_entries=_integer(section.get("max_entries"), 1000),
        max_entry_bytes=_integer(section.get("max_entry_bytes"), 1024 * 1024),
        max_total_bytes=_integer(section.get("max_total_bytes"), 16 * 1024 * 1024),
        max_retries=_integer(section.get("max_retries"), 32),
    )


def _tools(values: dict[str, object]) -> WorkerToolsConfig:
    section = _section(values, "tools")
    skills = _section(section, "skills")
    browser = _section(section, "browser")
    return WorkerToolsConfig(
        builtin_tools=_boolean_value(section.get("builtin_tools"), False),
        allowed_shell_commands=_strings(section.get("allowed_shell_commands")),
        media=_boolean_value(section.get("media"), False),
        skills=WorkerSkillsConfig(
            enabled=_boolean_value(skills.get("enabled"), False),
            platform_enabled=_boolean_value(skills.get("platform_enabled"), False),
            workspace_dir=_path(skills.get("workspace_dir")),
            user_dir=_path(skills.get("user_dir")),
            compat_dir=_path(skills.get("compat_dir")),
            default_names=_strings(skills.get("default_names")),
            bundled_resources=_strings(skills.get("bundled_resources")),
            tolerate_platform_failure=_boolean_value(skills.get("tolerate_platform_failure"), True),
            sync_mode=_enum(
                SkillSyncMode,
                skills.get("sync_mode", SkillSyncMode.DISABLED.value),
                "skills.sync-mode",
            ),
        ),
        browser=WorkerBrowserConfig(
            mode=_enum(
                BrowserMode,
                browser.get("mode", BrowserMode.DISABLED.value),
                "browser.mode",
            ),
            executable_path=_path(browser.get("executable_path")),
            launch_arguments=_strings(browser.get("launch_arguments")),
            action_timeout=_number(browser, "action_timeout", 15.0),
            max_pages_per_session=_integer(browser.get("max_pages_per_session"), 10),
            max_read_text_characters=_integer(browser.get("max_read_text_characters"), 8000),
            max_console_messages=_integer(browser.get("max_console_messages"), 200),
            max_screenshot_bytes=_integer(browser.get("max_screenshot_bytes"), 10 * 1024 * 1024),
            execute_js_enabled=_boolean_value(browser.get("execute_js_enabled"), False),
            allowed_hosts=frozenset(_strings(browser.get("allowed_hosts"))),
            allowed_cidrs=frozenset(_strings(browser.get("allowed_cidrs"))),
            remote_endpoint_secret=_text(browser, "remote_endpoint_secret"),
            remote_authorization_secret=_text(browser, "remote_authorization_secret"),
        ),
    )


def _boolean_value(value: object, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ConfigurationError("Expected a boolean")
    return value
