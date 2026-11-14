from __future__ import annotations

import builtins
import copy
import hashlib
import json
import os
import re
import time
import uuid
from dataclasses import asdict, dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Any

from ..errors import OutboxError, ReportConflictError


class OutboxType(StrEnum):
    CLIENT_TOOL_RESULT = "client_tool_result"
    CHECKPOINT_TOOL_RESULTS = "checkpoint_tool_results"


class OutboxStatus(StrEnum):
    PENDING = "pending"
    REPORTING = "reporting"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True, slots=True)
class OutboxLimits:
    max_entries: int = 1000
    max_entry_bytes: int = 1024 * 1024
    max_total_bytes: int = 16 * 1024 * 1024
    max_retries: int = 32

    def __post_init__(self) -> None:
        if min(self.max_entries, self.max_entry_bytes, self.max_total_bytes, self.max_retries) <= 0:
            raise ValueError("Outbox limits and retry budget must be positive")
        if self.max_entry_bytes > self.max_total_bytes:
            raise ValueError("Outbox entry limit must not exceed total limit")


@dataclass(slots=True)
class OutboxEntry:
    id: str
    type: OutboxType
    dedupe_key: str
    payload: Any
    status: OutboxStatus = OutboxStatus.PENDING
    flow_id: str | None = None
    session_id: str | None = None
    message_id: str | None = None
    run_id: str | None = None
    call_id: str | None = None
    checkpoint_id: str | None = None
    checkpoint_version: int = 0
    submission_id: str | None = None
    submission_hash: str | None = None
    created_at_epoch_ms: int = 0
    retry_count: int = 0
    next_attempt_at_epoch_ms: int = 0
    last_error: str | None = None

    def copy(self) -> OutboxEntry:
        return replace(self, payload=copy.deepcopy(self.payload))

    @property
    def is_due(self) -> bool:
        return self.status is OutboxStatus.PENDING and self.next_attempt_at_epoch_ms <= _now_ms()

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["type"] = self.type.value
        value["status"] = self.status.value
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> OutboxEntry:
        data = dict(value)
        data["type"] = OutboxType(data["type"])
        data["status"] = OutboxStatus(data["status"])
        return cls(**data)


@dataclass(frozen=True, slots=True)
class PreparedToolResults:
    entry: OutboxEntry | None
    tool_results: list[dict[str, Any]]
    persisted: bool

    @property
    def has_results(self) -> bool:
        return bool(self.tool_results)


def client_result_entry(payload: dict[str, Any]) -> OutboxEntry:
    run_id = _required(payload.get("run_id"), "run_id")
    call_id = _required(payload.get("call_id"), "call_id")
    dedupe_key = run_id + "\0" + call_id
    return OutboxEntry(
        id=entry_id(OutboxType.CLIENT_TOOL_RESULT, dedupe_key),
        type=OutboxType.CLIENT_TOOL_RESULT,
        dedupe_key=dedupe_key,
        flow_id=_text(payload.get("flow_id")),
        session_id=_text(payload.get("session_id")),
        message_id=_text(payload.get("message_id")),
        run_id=run_id,
        call_id=call_id,
        payload=canonical(payload),
        created_at_epoch_ms=_now_ms(),
    )


def checkpoint_results_entry(
    *,
    flow_id: str,
    session_id: str,
    run_id: str,
    checkpoint_id: str,
    checkpoint_version: int,
    submission_id: str,
    payload: list[dict[str, Any]],
) -> OutboxEntry:
    canonical_payload = canonical(payload)
    dedupe_key = (
        _required(checkpoint_id, "checkpoint_id") + "\0" + _required(submission_id, "submission_id")
    )
    return OutboxEntry(
        id=entry_id(OutboxType.CHECKPOINT_TOOL_RESULTS, dedupe_key),
        type=OutboxType.CHECKPOINT_TOOL_RESULTS,
        dedupe_key=dedupe_key,
        flow_id=_required(flow_id, "flow_id"),
        session_id=_required(session_id, "session_id"),
        run_id=_required(run_id, "run_id"),
        checkpoint_id=checkpoint_id,
        checkpoint_version=checkpoint_version,
        submission_id=submission_id,
        submission_hash=payload_hash(canonical_payload),
        payload=canonical_payload,
        created_at_epoch_ms=_now_ms(),
    )


def new_submission_id(checkpoint_id: str, payload: list[dict[str, Any]]) -> str:
    digest = bytearray(
        hashlib.md5(  # noqa: S324 - Java UUID.nameUUIDFromBytes compatibility, not security
            (checkpoint_id + "\0" + canonical_text(payload)).encode()
        ).digest()
    )
    digest[6] = (digest[6] & 0x0F) | 0x30
    digest[8] = (digest[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(digest)))


def entry_id(entry_type: OutboxType, dedupe_key: str) -> str:
    digest = hashlib.sha256(dedupe_key.encode()).hexdigest()
    return f"{entry_type.value}-{digest}"


def canonical(value: Any) -> Any:
    return json.loads(canonical_text(value))


def canonical_text(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise OutboxError("Outbox payload is not JSON serializable") from exc


def payload_hash(payload: Any) -> str:
    return hashlib.sha256(canonical_text(payload).encode()).hexdigest()


def safe_error(value: str | None) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.:-]", "_", value or "report-failed")
    return normalized[:128]


class InMemoryReportOutbox:
    def __init__(self, limits: OutboxLimits | None = None) -> None:
        self.limits = limits or OutboxLimits()
        self._entries: dict[str, OutboxEntry] = {}
        self._total_bytes = 0

    @property
    def is_persistent(self) -> bool:
        return False

    def put(self, proposed: OutboxEntry) -> OutboxEntry:
        value = self._validated(proposed)
        existing = self._entries.get(value.id)
        if existing is not None:
            if self._same_final_result(existing, value):
                return existing.copy()
            conflict = existing.copy()
            conflict.status = OutboxStatus.DEAD_LETTER
            conflict.last_error = "result-conflict"
            self._persist(conflict)
            self._entries[value.id] = conflict
            self._total_bytes = sum(self._entry_size(item) for item in self._entries.values())
            raise ReportConflictError(value.id)
        size = self._entry_size(value)
        if size > self.limits.max_entry_bytes:
            raise OutboxError("Outbox entry exceeds max-entry-bytes")
        if (
            len(self._entries) >= self.limits.max_entries
            or self._total_bytes + size > self.limits.max_total_bytes
        ):
            raise OutboxError("Outbox bounds exceeded")
        self._persist(value)
        self._entries[value.id] = value.copy()
        self._total_bytes += size
        return value.copy()

    def find(self, entry_id_value: str) -> OutboxEntry | None:
        entry = self._entries.get(entry_id_value)
        return entry.copy() if entry is not None else None

    def list(self, entry_type: OutboxType) -> builtins.list[OutboxEntry]:
        entries = [
            entry
            for entry in self._entries.values()
            if entry.type is entry_type and entry.status is not OutboxStatus.DEAD_LETTER
        ]
        return [
            entry.copy() for entry in sorted(entries, key=lambda item: item.created_at_epoch_ms)
        ]

    def list_all(self) -> builtins.list[OutboxEntry]:
        return [entry.copy() for entry in self._entries.values()]

    def for_checkpoint(self, checkpoint_id: str) -> builtins.list[OutboxEntry]:
        return [
            entry
            for entry in self.list(OutboxType.CHECKPOINT_TOOL_RESULTS)
            if entry.checkpoint_id == checkpoint_id
        ]

    def mark_reporting(self, entry_id_value: str) -> OutboxEntry:
        entry = self._required(entry_id_value)
        if entry.status is not OutboxStatus.DEAD_LETTER:
            entry.status = OutboxStatus.REPORTING
            self._update(entry)
        return entry

    def mark_retry(self, entry_id_value: str, error: str, backoff: float) -> OutboxEntry:
        entry = self._required(entry_id_value)
        if entry.status is not OutboxStatus.DEAD_LETTER:
            entry.retry_count += 1
            entry.last_error = safe_error(error)
            if entry.retry_count >= self.limits.max_retries:
                entry.status = OutboxStatus.DEAD_LETTER
                entry.last_error = "retry-budget-exhausted"
                entry.next_attempt_at_epoch_ms = 0
            else:
                entry.status = OutboxStatus.PENDING
                entry.next_attempt_at_epoch_ms = _now_ms() + max(0, int(backoff * 1000))
            self._update(entry)
        return entry

    def mark_dead_letter(self, entry_id_value: str, error: str) -> OutboxEntry:
        entry = self._required(entry_id_value)
        entry.status = OutboxStatus.DEAD_LETTER
        entry.last_error = safe_error(error)
        entry.next_attempt_at_epoch_ms = 0
        self._update(entry)
        return entry

    def delete(self, entry_id_value: str) -> None:
        entry = self._entries.get(entry_id_value)
        if entry is None:
            return
        self._remove(entry_id_value)
        del self._entries[entry_id_value]
        self._total_bytes -= self._entry_size(entry)

    def _update(self, entry: OutboxEntry) -> None:
        self._persist(entry)
        self._entries[entry.id] = entry.copy()
        self._total_bytes = sum(self._entry_size(item) for item in self._entries.values())

    def _required(self, entry_id_value: str) -> OutboxEntry:
        entry = self._entries.get(entry_id_value)
        if entry is None:
            raise OutboxError(f"Unknown outbox entry {entry_id_value}")
        return entry.copy()

    def _persist(self, entry: OutboxEntry) -> None:
        pass

    def _remove(self, entry_id_value: str) -> None:
        pass

    @staticmethod
    def _validated(proposed: OutboxEntry) -> OutboxEntry:
        if not proposed.id or not proposed.dedupe_key or proposed.payload is None:
            raise OutboxError("Invalid outbox entry")
        if proposed.id != entry_id(proposed.type, proposed.dedupe_key):
            raise OutboxError("Invalid outbox entry identity")
        value = proposed.copy()
        value.payload = canonical(value.payload)
        if (
            value.type is OutboxType.CHECKPOINT_TOOL_RESULTS
            and value.submission_hash != payload_hash(value.payload)
        ):
            raise OutboxError("Checkpoint submission hash mismatch")
        return value

    @staticmethod
    def _same_final_result(first: OutboxEntry, second: OutboxEntry) -> bool:
        return (
            first.type is second.type
            and first.dedupe_key == second.dedupe_key
            and canonical_text(first.payload) == canonical_text(second.payload)
            and first.submission_hash == second.submission_hash
        )

    @staticmethod
    def _entry_size(entry: OutboxEntry) -> int:
        return len(canonical_text(entry.to_dict()).encode())


class FileReportOutbox(InMemoryReportOutbox):
    def __init__(self, state_directory: str | Path, limits: OutboxLimits | None = None) -> None:
        self.directory = Path(state_directory).expanduser().absolute() / "report-outbox"
        self.quarantine_directory = self.directory / "quarantine"
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.quarantine_directory.mkdir(exist_ok=True, mode=0o700)
        os.chmod(self.directory, 0o700)
        os.chmod(self.quarantine_directory, 0o700)
        super().__init__(limits)
        self._load()

    @property
    def is_persistent(self) -> bool:
        return True

    def _path(self, entry_id_value: str) -> Path:
        return self.directory / f"{entry_id_value}.json"

    def _persist(self, entry: OutboxEntry) -> None:
        target = self._path(entry.id)
        temporary = self.directory / f".{target.name}.{uuid.uuid4()}.tmp"
        try:
            data = canonical_text(entry.to_dict()).encode()
            with temporary.open("xb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, target)
        except OSError as exc:
            raise OutboxError("Unable to persist report outbox entry") from exc
        finally:
            temporary.unlink(missing_ok=True)

    def _remove(self, entry_id_value: str) -> None:
        try:
            self._path(entry_id_value).unlink(missing_ok=True)
        except OSError as exc:
            raise OutboxError("Unable to delete report outbox entry") from exc

    def _load(self) -> None:
        for path in sorted(self.directory.glob("*.json")):
            try:
                if path.stat().st_size > self.limits.max_entry_bytes:
                    self._quarantine(path, "oversized")
                    continue
                value = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(value, dict):
                    raise ValueError("Outbox record must be an object")
                entry = self._validated(OutboxEntry.from_dict(value))
                if path != self._path(entry.id):
                    self._quarantine(path, "identity-mismatch")
                    continue
                if entry.status is OutboxStatus.REPORTING:
                    entry.status = OutboxStatus.PENDING
                    entry.last_error = "recovered-interrupted-report"
                    self._persist(entry)
                size = self._entry_size(entry)
                if (
                    len(self._entries) >= self.limits.max_entries
                    or self._total_bytes + size > self.limits.max_total_bytes
                ):
                    self._quarantine(path, "bounds-exceeded")
                    continue
                self._entries[entry.id] = entry
                self._total_bytes += size
            except Exception:
                self._quarantine(path, "corrupt")

    def _quarantine(self, path: Path, reason: str) -> None:
        try:
            target = self.quarantine_directory / f"{path.name}.{reason}-{uuid.uuid4()}"
            os.replace(path, target)
        except OSError as exc:
            raise OutboxError("Unable to quarantine damaged report outbox entry") from exc


def _now_ms() -> int:
    return int(time.time() * 1000)


def _required(value: Any, name: str) -> str:
    text = _text(value)
    if text is None:
        raise OutboxError(f"{name} is required for a report outbox entry")
    return text


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None
