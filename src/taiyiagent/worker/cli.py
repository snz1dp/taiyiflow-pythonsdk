from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import signal
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from ..errors import ConfigurationError, redact
from ..hooks import PermissionHook
from ..tools import OutboxLimits
from .approvals import ApprovalAudit
from .config import WorkerConfig
from .identity import Ability, WorkerIdentity, WorkerIdentityValidator, WorkerRegistrar
from .subtasks import SubtaskMcpResolver
from .tasks import TaskReporter, WorkerTaskHandler
from .worker import DigitalWorker, WorkerHook


def build_worker(config: WorkerConfig, secrets: Mapping[str, str]) -> DigitalWorker:
    extensions = config.extensions
    identity_validator = cast(
        WorkerIdentityValidator,
        _required(extensions.identity_validator, "identity-validator", "validate"),
    )
    registrar = cast(WorkerRegistrar | None, _optional(extensions.worker_registrar, "register"))
    approval_handler = cast(
        PermissionHook | None, _optional(extensions.approval_handler, "on_permission")
    )
    approval_audit = cast(ApprovalAudit | None, _optional(extensions.approval_audit, "record"))
    task_handler = cast(WorkerTaskHandler | None, _optional(extensions.task_handler, "execute"))
    task_reporter = cast(TaskReporter | None, _optional(extensions.task_reporter, "running"))
    mcp_resolver = cast(
        SubtaskMcpResolver | None, _optional(extensions.subtask_mcp_resolver, "resolve")
    )
    hooks = [cast(WorkerHook, _instance(path, "on_ready")) for path in extensions.hooks]
    identity = (
        WorkerIdentity(config.identity.worker_id, config.identity.display_name)
        if config.identity.worker_id.strip()
        else None
    )
    return DigitalWorker(
        base_url=config.base_url,
        auth=config.create_auth(secrets),
        workspace=config.workspace,
        identity=identity,
        display_name=config.identity.display_name,
        role=config.identity.role,
        abilities=frozenset(Ability(name) for name in config.identity.abilities),
        worker_registrar=registrar,
        identity_validator=identity_validator,
        client_id=config.client_id or None,
        device_id=config.device_id or None,
        state_dir=config.state_dir,
        outbox_limits=OutboxLimits(
            max_entries=config.outbox.max_entries,
            max_entry_bytes=config.outbox.max_entry_bytes,
            max_total_bytes=config.outbox.max_total_bytes,
            max_retries=config.outbox.max_retries,
        ),
        concurrency=config.execution.concurrency,
        recovery_policy=config.recovery_policy,
        permission_policy=config.permission_policy,
        approval_channel=config.approval.channel,
        approval_handler=approval_handler,
        approval_audit=approval_audit,
        task_handler=task_handler,
        task_reporter=task_reporter,
        subtask_mcp_resolver=mcp_resolver,
        worker_hooks=hooks,
        connect_timeout=config.execution.connect_timeout,
        drain_timeout=config.execution.drain_timeout,
        request_timeout=config.execution.request_timeout,
        enable_builtin_tools=config.tools.builtin_tools,
        allowed_shell_commands=config.tools.allowed_shell_commands,
        enable_media_tools=config.tools.media,
        skills=config.tools.skills,
        browser_settings=config.tools.browser.create_settings(secrets),
    )


async def run_worker(config_path: str | Path) -> None:
    worker = build_worker(WorkerConfig.load(config_path), dict(os.environ))
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_name, stopped.set)
        except (NotImplementedError, RuntimeError):
            pass
    try:
        await worker.start()
        stop_waiter = asyncio.create_task(stopped.wait())
        offline_waiter = asyncio.create_task(worker.wait())
        try:
            await asyncio.wait(
                {stop_waiter, offline_waiter},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for waiter in (stop_waiter, offline_waiter):
                waiter.cancel()
            await asyncio.gather(stop_waiter, offline_waiter, return_exceptions=True)
    finally:
        await worker.aclose()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="taiyiagent-worker")
    parser.add_argument(
        "--config",
        default=os.environ.get("TAIYIFLOW_WORKER_CONFIG", "worker.yml"),
        help="Worker YAML configuration path",
    )
    arguments = parser.parse_args(argv)
    try:
        asyncio.run(run_worker(arguments.config))
    except (ConfigurationError, OSError) as exc:
        print(f"Worker failed: {redact(exc)}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    return 0


def _optional(path: str, method: str) -> object | None:
    return _instance(path, method) if path.strip() else None


def _required(path: str, name: str, method: str) -> object:
    if not path.strip():
        raise ConfigurationError(f"extensions.{name} is required")
    return _instance(path, method)


def _instance(path: str, method: str) -> object:
    if not path.strip():
        raise ConfigurationError("Extension path is required")
    module_name, separator, attribute_name = path.partition(":")
    if not separator:
        module_name, separator, attribute_name = path.rpartition(".")
    if not module_name or not attribute_name:
        raise ConfigurationError(f"Invalid extension path: {path}")
    try:
        value: Any = getattr(importlib.import_module(module_name), attribute_name)
        if isinstance(value, type):
            value = value()
    except (ImportError, AttributeError, TypeError) as exc:
        raise ConfigurationError(f"Unable to load extension: {path}") from exc
    if not callable(getattr(value, method, None)):
        raise ConfigurationError(f"Extension {path} does not provide {method}()")
    return cast(object, value)


if __name__ == "__main__":
    raise SystemExit(main())
