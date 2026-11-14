from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from ..errors import ApiError, ReportConflictError, ToolExecutionError
from ..models import ToolCall, ToolResult
from ..transport import RestClient
from .outbox import (
    InMemoryReportOutbox,
    OutboxEntry,
    OutboxStatus,
    OutboxType,
    client_result_entry,
)


class ReportStatus(StrEnum):
    REPORTED = "reported"
    PENDING = "pending"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True, slots=True)
class ReportOutcome:
    status: ReportStatus
    entry: OutboxEntry | None
    error: Exception | None = None
    persisted: bool = True

    @property
    def terminal(self) -> bool:
        return self.status is not ReportStatus.PENDING


class ToolResultReporter:
    RESULT_PATH = "/api/v1/agents/client/call/result"

    def __init__(
        self,
        rest: RestClient,
        *,
        outbox: InMemoryReportOutbox | None = None,
        max_retries: int = 3,
        initial_retry_delay: float = 0.5,
    ) -> None:
        if max_retries < 0 or initial_retry_delay < 0:
            raise ValueError("Retry count and delay must be non-negative")
        self._rest = rest
        self.outbox = outbox or InMemoryReportOutbox()
        self._max_retries = max_retries
        self._initial_retry_delay = initial_retry_delay

    @staticmethod
    def build_payload(call: ToolCall, result: ToolResult) -> dict[str, Any]:
        return {
            "flow_id": call.flow_id,
            "session_id": call.session_id,
            "message_id": call.message_id,
            "run_id": call.run_id,
            "call_id": call.id,
            "success": result.success,
            "result": result.result,
            "error_message": result.error,
        }

    async def report(self, call: ToolCall, result: ToolResult) -> None:
        outcome = await self.report_persisted(call, result)
        if outcome.status is not ReportStatus.REPORTED:
            raise ToolExecutionError(
                f"Failed to report tool result for call {call.id}"
            ) from outcome.error

    async def report_persisted(self, call: ToolCall, result: ToolResult) -> ReportOutcome:
        return await self.report_payload(self.build_payload(call, result))

    async def report_payload(self, payload: dict[str, Any]) -> ReportOutcome:
        transient = client_result_entry(payload)
        try:
            entry = self.outbox.put(transient)
        except ReportConflictError as exc:
            dead = self.outbox.find(transient.id)
            return ReportOutcome(ReportStatus.DEAD_LETTER, dead, exc)
        except Exception as exc:
            outcome = await self._direct_once(transient)
            if outcome.status is not ReportStatus.REPORTED:
                return ReportOutcome(outcome.status, outcome.entry, outcome.error or exc, False)
            return outcome
        if entry.status is OutboxStatus.DEAD_LETTER:
            return ReportOutcome(ReportStatus.DEAD_LETTER, entry)
        return await self._deliver(entry, sleep_between_attempts=True)

    async def flush(self, *, force: bool = False) -> list[ReportOutcome]:
        outcomes: list[ReportOutcome] = []
        for entry in self.outbox.list(OutboxType.CLIENT_TOOL_RESULT):
            if not force and not entry.is_due:
                continue
            try:
                outcomes.append(await self._deliver(entry, sleep_between_attempts=False))
            except Exception as exc:
                outcomes.append(ReportOutcome(ReportStatus.PENDING, entry, exc))
        return outcomes

    async def flush_now(self) -> list[ReportOutcome]:
        return await self.flush(force=True)

    async def _deliver(
        self, original: OutboxEntry, *, sleep_between_attempts: bool
    ) -> ReportOutcome:
        entry = original
        for attempt in range(self._max_retries + 1):
            failure: Exception | None = None
            try:
                entry = self.outbox.mark_reporting(entry.id)
                if entry.status is OutboxStatus.DEAD_LETTER:
                    return ReportOutcome(ReportStatus.DEAD_LETTER, entry)
                await self._rest.post(self.RESULT_PATH, entry.payload)
                self.outbox.delete(entry.id)
                return ReportOutcome(ReportStatus.REPORTED, entry)
            except Exception as exc:
                failure = exc
                classification = self._classify(exc)
                try:
                    if classification == "delete":
                        self.outbox.delete(entry.id)
                        return ReportOutcome(ReportStatus.REPORTED, entry)
                    if classification == "dead_letter":
                        entry = self.outbox.mark_dead_letter(entry.id, self._safe_code(exc))
                        return ReportOutcome(ReportStatus.DEAD_LETTER, entry, exc)
                    entry = self.outbox.mark_retry(
                        entry.id,
                        self._safe_code(exc),
                        self._backoff(entry),
                    )
                    if entry.status is OutboxStatus.DEAD_LETTER:
                        return ReportOutcome(ReportStatus.DEAD_LETTER, entry, exc)
                except Exception as storage_error:
                    return ReportOutcome(ReportStatus.PENDING, entry, storage_error)
            if attempt < self._max_retries and sleep_between_attempts:
                await asyncio.sleep(self._initial_retry_delay * (2 ** min(attempt, 20)))
        return ReportOutcome(ReportStatus.PENDING, entry, failure)

    async def _direct_once(self, entry: OutboxEntry) -> ReportOutcome:
        try:
            await self._rest.post(self.RESULT_PATH, entry.payload)
            return ReportOutcome(ReportStatus.REPORTED, entry, persisted=False)
        except Exception as exc:
            classification = self._classify(exc)
            if classification == "delete":
                return ReportOutcome(ReportStatus.REPORTED, entry, persisted=False)
            status = (
                ReportStatus.DEAD_LETTER
                if classification == "dead_letter"
                else ReportStatus.PENDING
            )
            return ReportOutcome(status, entry, exc, False)

    @staticmethod
    def _classify(error: Exception) -> str:
        if not isinstance(error, ApiError):
            return "retry"
        if error.status_code == 400 and error.detail_code == "tool_call_paused":
            return "delete"
        if error.status_code in {401, 403, 408, 429} or error.status_code >= 500:
            return "retry"
        return "dead_letter"

    @staticmethod
    def _safe_code(error: Exception) -> str:
        return f"http-{error.status_code}" if isinstance(error, ApiError) else "network-error"

    def _backoff(self, entry: OutboxEntry) -> float:
        return float(min(300.0, self._initial_retry_delay * (2 ** min(20, entry.retry_count))))
