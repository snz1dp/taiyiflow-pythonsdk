from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from enum import StrEnum
from pathlib import Path

from ..agent import TaiyiAgent
from ..browser import BrowserSettings
from ..collaboration import CollaborationPlugin, TaskCancel, TaskDispatch
from ..collaboration.client import ROLE_PRIMARY_CLI
from ..errors import ConfigurationError
from ..hooks import PermissionHook, PermissionPolicy, maybe_await
from ..models import RecoveryPolicy
from ..skills import (
    BundledSkillProvider,
    FileSystemSkillProvider,
    PlatformSkillProvider,
    SkillManager,
    SkillProvider,
    SkillRegistry,
    SkillSource,
    SkillSupplier,
    SkillSupplyPolicy,
    SkillTools,
)
from ..tools import FileReportOutbox, OutboxLimits
from ..transport import FileClient, JsonObject, JsonValue, MemoryClient, RestClient, UploadedFile
from .approvals import ApprovalAudit, WorkerApprovalHook
from .auth import WorkerAuth
from .config import ApprovalChannel, WorkerSkillsConfig
from .identity import (
    Ability,
    WorkerIdentity,
    WorkerIdentityValidator,
    WorkerRegistrar,
    WorkerRegistrationRequest,
    WorkerRole,
)
from .session_tasks import AgentSessionTaskHandler
from .subtasks import SubtaskMcpResolver, SubtaskOrchestrator, SubtaskTask
from .tasks import (
    PlatformTaskReporter,
    TaskExecutionState,
    TaskExecutor,
    TaskReporter,
    TaskReservation,
    WorkerTaskHandler,
)

log = logging.getLogger(__name__)


class WorkerState(StrEnum):
    ONLINE = "online"
    BUSY = "busy"
    PAUSED = "paused"
    OFFLINE = "offline"


class WorkerHook:
    async def on_ready(self, worker: DigitalWorker) -> None:
        pass

    async def on_task_start(self, task: TaskDispatch) -> None:
        pass

    async def on_task_finish(self, task: TaskDispatch, state: TaskExecutionState) -> None:
        pass

    async def on_error(self, stage: str, error: BaseException) -> None:
        pass

    async def on_stop(self) -> None:
        pass


class WorkerRuntime:
    def __init__(self, concurrency: int, *, queue_capacity: int = 128) -> None:
        if concurrency <= 0 or queue_capacity <= 0:
            raise ConfigurationError("Worker concurrency and queue capacity must be positive")
        self._concurrency = concurrency
        self._queue_capacity = queue_capacity
        self._semaphore = asyncio.Semaphore(concurrency)
        self._accepting = False
        self._terminated = False
        self._tasks: set[asyncio.Task[None]] = set()
        self._state = WorkerState.OFFLINE
        self._offline = asyncio.Event()
        self._offline.set()

    @property
    def state(self) -> WorkerState:
        return self._state

    @property
    def active_tasks(self) -> int:
        return len(self._tasks)

    def online(self) -> None:
        if not self._terminated:
            self._accepting = True
            self._offline.clear()
            self._state = WorkerState.BUSY if self._tasks else WorkerState.ONLINE

    def pause(self) -> None:
        if not self._terminated:
            self._accepting = False
            self._state = WorkerState.PAUSED

    def submit(
        self,
        operation: Callable[[], Coroutine[object, object, None]],
    ) -> asyncio.Task[None] | None:
        if (
            not self._accepting
            or self._terminated
            or len(self._tasks) >= self._concurrency + self._queue_capacity
        ):
            return None

        async def run() -> None:
            async with self._semaphore:
                await operation()

        runner = asyncio.create_task(run())
        self._tasks.add(runner)
        self._state = WorkerState.BUSY
        runner.add_done_callback(self._complete)
        return runner

    async def drain(self, wait_timeout: float) -> bool:
        self.pause()
        if not self._tasks:
            return True
        _, pending = await asyncio.wait(self._tasks, timeout=wait_timeout)
        return not pending

    async def aclose(self) -> None:
        self.pause()
        self._terminated = True
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._state = WorkerState.OFFLINE
        self._offline.set()

    async def wait(self) -> None:
        await self._offline.wait()

    def _complete(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if not self._terminated:
            self._state = (
                WorkerState.BUSY
                if self._tasks
                else WorkerState.ONLINE
                if self._accepting
                else WorkerState.PAUSED
            )


class DigitalWorker:
    def __init__(
        self,
        *,
        base_url: str,
        auth: WorkerAuth,
        workspace: str | Path,
        identity: WorkerIdentity | None = None,
        display_name: str = "",
        role: WorkerRole = WorkerRole.COLLABORATION_WORKER,
        abilities: frozenset[Ability] = frozenset(),
        worker_registrar: WorkerRegistrar | None = None,
        identity_validator: WorkerIdentityValidator,
        client_id: str | None = None,
        device_id: str | None = None,
        state_dir: str | Path | None = None,
        outbox_limits: OutboxLimits | None = None,
        concurrency: int = 3,
        recovery_policy: RecoveryPolicy = RecoveryPolicy.AUTO,
        permission_policy: PermissionPolicy = PermissionPolicy.CONFIRM,
        approval_channel: ApprovalChannel = ApprovalChannel.PLATFORM,
        approval_handler: PermissionHook | None = None,
        approval_audit: ApprovalAudit | None = None,
        task_handler: WorkerTaskHandler | None = None,
        task_reporter: TaskReporter | None = None,
        subtask_mcp_resolver: SubtaskMcpResolver | None = None,
        worker_hooks: list[WorkerHook] | None = None,
        connect_timeout: float = 15.0,
        drain_timeout: float = 30.0,
        request_timeout: float = 30.0,
        credential_headers: Mapping[str, str] | None = None,
        collaboration: CollaborationPlugin | None = None,
        enable_builtin_tools: bool = False,
        allowed_shell_commands: tuple[str, ...] = (),
        enable_media_tools: bool = False,
        skills: WorkerSkillsConfig | None = None,
        browser_settings: BrowserSettings | None = None,
    ) -> None:
        if identity is None and worker_registrar is None:
            raise ConfigurationError("identity or worker_registrar is required")
        if identity is None and not display_name.strip():
            raise ConfigurationError("display_name is required for worker registration")
        self.base_url = base_url.rstrip("/")
        self.auth = auth
        self.identity = identity
        self.display_name = display_name or (identity.display_name if identity else "")
        self.role = role
        self.abilities = abilities
        self.workspace = Path(workspace)
        self.enable_builtin_tools = enable_builtin_tools
        self.allowed_shell_commands = tuple(allowed_shell_commands)
        self.enable_media_tools = enable_media_tools
        self.skills_config = skills
        self.browser_settings = browser_settings
        self.state_dir = Path(state_dir) if state_dir is not None else None
        self.outbox_limits = outbox_limits
        self._outbox = (
            FileReportOutbox(self.state_dir, self.outbox_limits)
            if self.state_dir is not None
            else None
        )
        self.recovery_policy = recovery_policy
        self.permission_policy = permission_policy
        self.approval_channel = approval_channel
        self.client_id = client_id
        self.device_id = device_id
        self.instance_id: str | None = None
        self.connect_timeout = connect_timeout
        self.drain_timeout = drain_timeout
        self.request_timeout = request_timeout
        self._worker_registrar = worker_registrar
        self._identity_validator = identity_validator
        self._credential_headers = dict(credential_headers or {})
        self._approval_hook = WorkerApprovalHook(
            permission_policy,
            approval_channel,
            approval_handler=approval_handler,
            approval_audit=approval_audit,
        )
        self._subtask_mcp_resolver = subtask_mcp_resolver
        self._observed_auth_generation = auth.generation
        self._hooks = list(worker_hooks or [])
        self._runtime = WorkerRuntime(concurrency)
        self._rest = RestClient(
            self.base_url,
            access_token=self._access_token,
            timeout=request_timeout,
            credential_headers=credential_headers,
            on_unauthorized=(
                self._refresh_after_unauthorized if self.auth.supports_refresh else None
            ),
        )
        self.memories = MemoryClient(self._rest)
        self.files = FileClient(self._rest)
        self._reporter = task_reporter or PlatformTaskReporter(
            self.base_url,
            auth,
            timeout=request_timeout,
        )
        self._session_handler = AgentSessionTaskHandler(
            self._new_agent,
            on_control=self._control_changed,
            mcp_resolver=self._subtask_mcp_resolver,
        )
        self._custom_handler = task_handler
        self._executor = TaskExecutor(self, self._reporter)
        self._collaboration = collaboration
        self._ready: asyncio.Event | None = None
        self._started = False
        self._closed = False

    @property
    def state(self) -> WorkerState:
        return self._runtime.state

    @property
    def active_task_count(self) -> int:
        return self._runtime.active_tasks

    @property
    def is_connected(self) -> bool:
        return self._collaboration is not None and self._collaboration.is_connected

    async def execute(
        self,
        task: TaskDispatch,
        progress: Callable[[JsonValue], Awaitable[None]],
    ) -> JsonValue:
        if self._custom_handler is not None:
            return await self._custom_handler.execute(task, progress)
        if task.task_kind in {"remote_task", "dialogue_subtask"} or task.task_type == "schedule":
            return await self._session_handler.execute(task, progress)
        raise ConfigurationError(f"No worker task handler is configured for {task.task_kind}")

    async def start(self) -> None:
        if self._closed or self._started:
            raise ConfigurationError("Digital worker can only be started once")
        self._started = True
        try:
            await self.auth.authenticate(self.base_url)
            if self.identity is None:
                assert self._worker_registrar is not None
                registration = await maybe_await(
                    self._worker_registrar.register(
                        WorkerRegistrationRequest(self.display_name, self.role, self.abilities)
                    )
                )
                self.identity = registration.identity
            await maybe_await(
                self._identity_validator.validate(self.identity, self.role, self.abilities)
            )
            if self.role is WorkerRole.PRIMARY_CLI:
                self.instance_id = _load_or_create_instance_id(self.workspace)
            self.client_id = self.client_id or str(uuid.uuid4())
            self.device_id = self.device_id or self.auth.device_id
            self._ready = asyncio.Event()
            if self._collaboration is None:
                self._collaboration = CollaborationPlugin(
                    self.base_url,
                    access_token=self._access_token,
                    role=(
                        ROLE_PRIMARY_CLI if self.role is WorkerRole.PRIMARY_CLI else self.role.value
                    ),
                    client_id=self.client_id,
                    device_id=self.device_id or "",
                    instance_id=self.instance_id or "",
                    credential_headers=self._credential_headers,
                    connect_timeout=self.connect_timeout,
                )
            self._collaboration.on_task_dispatch(self._dispatch)
            self._collaboration.on_task_snapshot(self._executor.reconcile)
            self._collaboration.on_task_cancel(self._cancel)
            self._collaboration.on_connection_ready(self._connection_ready)
            self._collaboration.on_kicked(self._kicked)
            await self._collaboration.start()
            async with asyncio.timeout(self.connect_timeout):
                await self._ready.wait()
            await self._notify("on_ready", self)
        except Exception as exc:
            await self._notify_error("start", exc)
            await self.aclose()
            raise

    async def pause(self) -> None:
        self._runtime.pause()
        await self._executor.pause_all()

    async def resume(self) -> None:
        await self._executor.resume_all()
        self._runtime.online()

    async def drain(self, timeout: float) -> bool:  # noqa: ASYNC109
        return await self._runtime.drain(timeout)

    async def wait(self) -> None:
        await self._runtime.wait()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._collaboration is not None:
                await self._collaboration.aclose()
            if not await self._runtime.drain(self.drain_timeout):
                await self._notify_error("drain", TimeoutError("Worker drain timed out"))
        finally:
            await self._executor.cancel_all("worker-stop")
            await self._runtime.aclose()
            await self._session_handler.aclose()
            closer = getattr(self._reporter, "aclose", None)
            if closer is not None:
                await maybe_await(closer())
            await self._rest.aclose()
            await self.auth.aclose()
            await self._notify("on_stop")

    async def __aenter__(self) -> DigitalWorker:
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    def task_state(self, task: TaskDispatch) -> TaskExecutionState | None:
        return self._executor.state_of(task)

    async def upload_artifact(
        self,
        flow_id: str,
        session_id: str,
        file: str | Path,
    ) -> UploadedFile:
        return await self.files.upload(flow_id, session_id, file)

    def _dispatch(self, task: TaskDispatch) -> bool:
        if task.task_type == "primary":
            return self._runtime.submit(lambda: self._run_primary(task)) is not None
        reservation = self._executor.prepare(task)
        if not reservation.accepted:
            return False
        runner = self._runtime.submit(lambda: self._run_reserved(task, reservation))
        if runner is None:
            self._executor.reject(reservation)
            return False
        self._executor.attach(reservation, runner)
        return True

    async def _run_primary(self, task: TaskDispatch) -> None:
        await self._notify("on_task_start", task)
        state = TaskExecutionState.SUCCESS
        try:
            await self.execute(task, _ignore_progress)
        except Exception as exc:
            state = TaskExecutionState.FAILED
            await self._notify_error("task", exc)
        await self._notify("on_task_finish", task, state)

    async def _run_reserved(self, task: TaskDispatch, reservation: TaskReservation) -> None:
        await self._notify("on_task_start", task)
        try:
            state = await self._executor.execute(reservation)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state = TaskExecutionState.FAILED
            await self._notify_error("task", exc)
        await self._notify("on_task_finish", task, state)

    async def _cancel(self, cancel: TaskCancel) -> None:
        await self._executor.cancel(cancel)

    def _control_changed(self, task: TaskDispatch, control: object, registered: bool) -> None:
        if registered:
            self._executor.register_control(task, control)  # type: ignore[arg-type]
        else:
            self._executor.unregister_control(task, control)  # type: ignore[arg-type]

    async def _connection_ready(self, payload: JsonObject) -> None:
        await self._executor.flush_unknown_reports()
        self._runtime.online()
        if self._ready is not None:
            self._ready.set()

    async def _kicked(self, reason: str) -> None:
        await self._runtime.aclose()

    def _new_agent(self, flow_id: str) -> TaiyiAgent:
        agent = TaiyiAgent(
            base_url=self.base_url,
            flow_id=flow_id,
            access_token=self._access_token,
            client_id=self.client_id,
            device_id=self.device_id,
            workspace=self.workspace,
            outbox=self._outbox,
            timeout=self.request_timeout,
            recovery_policy=self.recovery_policy,
            permission_policy=self.permission_policy,
            permission_hook=self._approval_hook,
            credential_headers=self._credential_headers,
            on_unauthorized=(
                self._refresh_after_unauthorized if self.auth.supports_refresh else None
            ),
            enable_builtin_tools=self.enable_builtin_tools,
            allowed_shell_commands=self.allowed_shell_commands,
            enable_media_tools=self.enable_media_tools,
            browser_settings=self.browser_settings,
        )
        if self.skills_config is not None and self.skills_config.enabled:
            config = self.skills_config
            workspace_skills = config.workspace_dir or self.workspace / ".snz1dp/skills"
            providers: list[SkillProvider] = [
                FileSystemSkillProvider(workspace_skills, SkillSource.WORKSPACE)
            ]
            if config.user_dir is not None:
                providers.append(FileSystemSkillProvider(config.user_dir, SkillSource.USER))
            if config.compat_dir is not None:
                providers.append(FileSystemSkillProvider(config.compat_dir, SkillSource.COMPAT))
            if config.bundled_resources:
                providers.append(BundledSkillProvider("taiyiagent", config.bundled_resources))
            if config.platform_enabled:
                providers.append(PlatformSkillProvider(agent.skills))
            agent.skill_supplier = SkillSupplier(
                SkillRegistry(providers),
                SkillSupplyPolicy(
                    platform_enabled=config.platform_enabled,
                    tolerate_platform_failure=config.tolerate_platform_failure,
                ),
            )
            agent.default_skill_names = config.default_names
            skill_manager = SkillManager(workspace_skills, config.user_dir, agent.skills)
            SkillTools(agent.skill_supplier, skill_manager, config.default_names).register(
                agent.plugins
            )
        SubtaskOrchestrator(
            agent.rest,
            agent.plugins,
            self._execute_subtask,
            canceller=self._cancel_subtask,
            local_client_id=self.client_id,
            local_device_id=self.device_id,
        )
        return agent

    def _access_token(self) -> str:
        self._observed_auth_generation = self.auth.generation
        return self.auth.access_token()

    async def _refresh_after_unauthorized(self) -> None:
        await self.auth.refresh_after_unauthorized(self._observed_auth_generation)

    async def _execute_subtask(self, task: SubtaskTask) -> JsonObject:
        payload = dict(task.payload)
        payload.setdefault("task_kind", "dialogue_subtask")
        task_type = payload.get("task_type")
        dispatch = TaskDispatch.from_payload(
            payload,
            task_type if isinstance(task_type, str) else "task.dialogue_subtask.v1",
        )
        return await self._executor.execute_detached(dispatch)

    async def _cancel_subtask(self, task: SubtaskTask) -> None:
        await self._executor.cancel(
            TaskCancel(
                task_id=task.task_id,
                task_kind="dialogue_subtask",
                version=task.version,
                source_principal_type="worker",
                correlation_id="",
                cancel_request_id=f"subtask-cancel:{task.task_id}",
                cancel_requested_at="",
                payload={},
            )
        )

    async def _notify(self, method: str, *args: object) -> None:
        for hook in self._hooks:
            try:
                await maybe_await(getattr(hook, method)(*args))
            except Exception as exc:
                if method != "on_error":
                    await self._notify_error("hook", exc)

    async def _notify_error(self, stage: str, error: BaseException) -> None:
        for hook in self._hooks:
            try:
                await hook.on_error(stage, error)
            except Exception:
                log.warning("Worker error hook failed for %s", stage)


async def _ignore_progress(progress: JsonValue) -> None:
    return None


def _load_or_create_instance_id(workspace: Path) -> str:
    workspace.mkdir(parents=True, exist_ok=True)
    path = workspace / "instance-id"
    try:
        value = path.read_text(encoding="utf-8").strip()
        if value:
            return value
    except FileNotFoundError:
        pass
    value = str(uuid.uuid4())
    temporary = workspace / f".instance-id.{uuid.uuid4().hex}.tmp"
    temporary.write_text(value + "\n", encoding="utf-8")
    temporary.replace(path)
    return value
