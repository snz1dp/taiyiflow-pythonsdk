from __future__ import annotations

import re
from typing import Any

from ..errors import ApiError, ReportConflictError
from ..models import ResumableCheckpoint
from .outbox import (
    InMemoryReportOutbox,
    OutboxEntry,
    OutboxStatus,
    OutboxType,
    PreparedToolResults,
    canonical,
    checkpoint_results_entry,
    entry_id,
    new_submission_id,
)


class CheckpointToolResultsAdapter:
    def __init__(self, outbox: InMemoryReportOutbox) -> None:
        self._outbox = outbox

    def prepare(
        self,
        checkpoint: ResumableCheckpoint,
        flow_id: str,
        session_id: str,
        pending_results: dict[str, dict[str, Any]],
    ) -> PreparedToolResults:
        existing = self._outbox.for_checkpoint(checkpoint.id)
        if len(existing) > 1:
            for entry in existing:
                self._outbox.mark_dead_letter(entry.id, "multiple-checkpoint-submissions")
            raise ReportConflictError(checkpoint.id)
        if existing:
            return self.reconcile(existing[0], checkpoint, flow_id, session_id)
        prepared = self.direct(checkpoint, flow_id, session_id, pending_results)
        if prepared.entry is None:
            return PreparedToolResults(None, [], True)
        entry = self._outbox.put(prepared.entry)
        if entry.status is OutboxStatus.DEAD_LETTER:
            raise ReportConflictError(entry.id)
        return PreparedToolResults(entry, canonical(entry.payload), True)

    def direct(
        self,
        checkpoint: ResumableCheckpoint,
        flow_id: str,
        session_id: str,
        pending_results: dict[str, dict[str, Any]],
    ) -> PreparedToolResults:
        results = [
            canonical(pending_results[call_id])
            for call_id in sorted(self._outstanding(checkpoint))
            if call_id in pending_results
        ]
        if not results:
            return PreparedToolResults(None, [], False)
        submission_id = new_submission_id(checkpoint.id, results)
        entry = checkpoint_results_entry(
            flow_id=flow_id,
            session_id=session_id,
            run_id=checkpoint.run_id,
            checkpoint_id=checkpoint.id,
            checkpoint_version=checkpoint.version,
            submission_id=submission_id,
            payload=results,
        )
        return PreparedToolResults(entry, results, False)

    def reconcile(
        self,
        entry: OutboxEntry,
        checkpoint: ResumableCheckpoint,
        flow_id: str,
        session_id: str,
    ) -> PreparedToolResults:
        if (
            entry.flow_id != flow_id
            or entry.session_id != session_id
            or entry.run_id != checkpoint.run_id
            or entry.checkpoint_id != checkpoint.id
        ):
            self._conflict(entry, "checkpoint-identity-conflict")
        if entry.submission_id == checkpoint.last_submission_id:
            if entry.submission_hash != checkpoint.last_submission_hash:
                self._conflict(entry, "submission-hash-conflict")
            self._outbox.delete(entry.id)
            return PreparedToolResults(None, [], True)
        if entry.checkpoint_version != checkpoint.version:
            self._conflict(entry, "checkpoint-version-conflict")
        if checkpoint.status in {"cancelled", "expired"}:
            self._conflict(entry, "checkpoint-archive-requires-reconciliation")
        outstanding = self._outstanding(checkpoint)
        for result in entry.payload:
            if not isinstance(result, dict) or result.get("call_id") not in outstanding:
                self._conflict(entry, "call-not-waiting-or-uncertain")
        return PreparedToolResults(entry, canonical(entry.payload), True)

    def accepted(self, entry: OutboxEntry | None) -> None:
        if entry is None:
            return
        self._outbox.delete(entry.id)
        if not isinstance(entry.payload, list):
            return
        for result in entry.payload:
            if not isinstance(result, dict):
                continue
            run_id = result.get("run_id")
            call_id = result.get("call_id")
            if isinstance(run_id, str) and isinstance(call_id, str):
                client_id = entry_id(OutboxType.CLIENT_TOOL_RESULT, run_id + "\0" + call_id)
                self._outbox.delete(client_id)

    def failed(self, entry: OutboxEntry, error: Exception) -> OutboxStatus:
        if self._deterministic(error):
            return self._outbox.mark_dead_letter(entry.id, self._safe_code(error)).status
        backoff = min(300.0, 0.5 * (2 ** min(20, entry.retry_count)))
        return self._outbox.mark_retry(entry.id, self._safe_code(error), backoff).status

    def _conflict(self, entry: OutboxEntry, reason: str) -> None:
        self._outbox.mark_dead_letter(entry.id, reason)
        raise ReportConflictError(entry.id)

    @staticmethod
    def _outstanding(checkpoint: ResumableCheckpoint) -> set[str]:
        return {*checkpoint.waiting_call_ids, *checkpoint.uncertain_call_ids}

    @staticmethod
    def _deterministic(error: Exception) -> bool:
        return (
            isinstance(error, ApiError)
            and error.status_code not in {401, 403, 408, 429}
            and error.status_code < 500
        )

    @staticmethod
    def _safe_code(error: Exception) -> str:
        if isinstance(error, ApiError):
            code = error.detail_code
            if code is not None and re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", code):
                return code
            return f"http-{error.status_code}"
        return "network-error"
