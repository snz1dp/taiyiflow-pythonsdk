from __future__ import annotations

from dataclasses import dataclass

from ..transport.capabilities import JsonObject


@dataclass(frozen=True, slots=True)
class TaskDispatch:
    task_id: str
    task_kind: str
    task_type: str
    version: int
    flow_id: str
    input_value: str
    session_id: str
    child_session_id: str
    child_run_id: str
    source_session_id: str
    source_run_id: str
    source_message_id: str
    schedule_id: str
    run_record_id: str
    request_mode: str
    instance_id: str
    payload: JsonObject

    @classmethod
    def from_payload(cls, payload: JsonObject, task_type: str) -> TaskDispatch:
        command_value = payload.get("command_payload", payload.get("command"))
        command = command_value if isinstance(command_value, dict) else payload
        task_id = _text(payload, "task_id") or _text(payload, "id")
        child_session_id = _first_text(command, payload, "child_session_id")
        session_id = _first_text(command, payload, "session_id")
        if _text(payload, "task_kind") == "dialogue_subtask" and child_session_id:
            session_id = child_session_id
        return cls(
            task_id=task_id,
            task_kind=_text(payload, "task_kind"),
            task_type=task_type,
            version=_integer(payload.get("version")),
            flow_id=_first_text(command, payload, "flow_id"),
            input_value=_first_text(command, payload, "input_value")
            or _first_text(command, payload, "input")
            or _first_text(command, payload, "prompt"),
            session_id=session_id,
            child_session_id=child_session_id,
            child_run_id=_first_text(command, payload, "child_run_id"),
            source_session_id=_text(payload, "source_session_id"),
            source_run_id=_text(payload, "source_run_id"),
            source_message_id=_text(payload, "source_message_id"),
            schedule_id=_text(payload, "schedule_id"),
            run_record_id=_text(payload, "run_record_id"),
            request_mode=_first_text(command, payload, "request_mode"),
            instance_id=_text(payload, "instance_id"),
            payload=payload,
        )


@dataclass(frozen=True, slots=True)
class TaskCancel:
    task_id: str
    task_kind: str
    version: int
    source_principal_type: str
    correlation_id: str
    cancel_request_id: str
    cancel_requested_at: str
    payload: JsonObject

    @classmethod
    def from_payload(cls, payload: JsonObject) -> TaskCancel:
        return cls(
            task_id=_text(payload, "task_id"),
            task_kind=_text(payload, "task_kind"),
            version=_integer(payload.get("version")),
            source_principal_type=_text(payload, "source_principal_type"),
            correlation_id=_text(payload, "correlation_id"),
            cancel_request_id=_text(payload, "cancel_request_id"),
            cancel_requested_at=_text(payload, "cancel_requested_at"),
            payload=payload,
        )


@dataclass(frozen=True, slots=True)
class ReleasePush:
    version: str
    payload: JsonObject


def _text(payload: JsonObject, field: str) -> str:
    value = payload.get(field)
    return value if isinstance(value, str) else ""


def _first_text(primary: JsonObject, fallback: JsonObject, field: str) -> str:
    return _text(primary, field) or _text(fallback, field)


def _integer(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0
