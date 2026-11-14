from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine

from ..errors import ConfigurationError, redact
from ..hooks import maybe_await
from .client import CollaborationPlugin

log = logging.getLogger(__name__)

ScheduledTask = Callable[[], Awaitable[None] | None]


class SchedulePlugin:
    def __init__(self, collaboration: CollaborationPlugin) -> None:
        self._collaboration = collaboration
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._closed = False

    @property
    def task_count(self) -> int:
        return len(self._tasks)

    def schedule(
        self,
        task_id: str,
        task: ScheduledTask,
        *,
        initial_delay: float,
        period: float,
    ) -> None:
        if initial_delay < 0 or period <= 0:
            raise ConfigurationError("Schedule delay must be non-negative and period positive")

        async def run() -> None:
            await asyncio.sleep(initial_delay)
            while True:
                try:
                    await maybe_await(task())
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("Scheduled task failed: %s", task_id, exc_info=True)
                    await self._collaboration.send_task_error(task_id, redact(exc))
                await asyncio.sleep(period)

        self._replace(task_id, run)

    def schedule_once(self, task_id: str, task: ScheduledTask, *, delay: float) -> None:
        if delay < 0:
            raise ConfigurationError("Schedule delay must not be negative")

        async def run() -> None:
            try:
                await asyncio.sleep(delay)
                await maybe_await(task())
                await self._collaboration.send_task_result(task_id, "completed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("One-shot task failed: %s", task_id, exc_info=True)
                await self._collaboration.send_task_error(task_id, redact(exc))
            finally:
                current = asyncio.current_task()
                if self._tasks.get(task_id) is current:
                    self._tasks.pop(task_id, None)

        self._replace(task_id, run)

    def cancel(self, task_id: str) -> bool:
        task = self._tasks.pop(task_id, None)
        if task is None:
            return False
        task.cancel()
        return True

    def _replace(
        self, task_id: str, factory: Callable[[], Coroutine[object, object, None]]
    ) -> None:
        if self._closed:
            raise ConfigurationError("Schedule plugin is closed")
        if not task_id.strip():
            raise ConfigurationError("task_id is required")
        previous = self._tasks.pop(task_id, None)
        if previous is not None:
            previous.cancel()
        self._tasks[task_id] = asyncio.create_task(factory())

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
