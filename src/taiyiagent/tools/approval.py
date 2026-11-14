from __future__ import annotations

import asyncio
from dataclasses import dataclass

from ..hooks import PermissionDecision


@dataclass(frozen=True, slots=True)
class Approval:
    decision: PermissionDecision
    reason: str | None = None


class ApprovalBroker:
    def __init__(self) -> None:
        self._pending: dict[str, tuple[str, asyncio.Future[Approval]]] = {}

    async def wait_for(self, session_id: str, call_id: str, timeout_seconds: float) -> Approval:
        if call_id in self._pending:
            raise ValueError(f"Approval is already pending for tool call {call_id}")
        future: asyncio.Future[Approval] = asyncio.get_running_loop().create_future()
        self._pending[call_id] = (session_id, future)
        try:
            if timeout_seconds <= 0:
                return await future
            return await asyncio.wait_for(asyncio.shield(future), timeout_seconds)
        finally:
            current = self._pending.get(call_id)
            if current is not None and current[1] is future:
                del self._pending[call_id]

    def approve(self, call_id: str) -> bool:
        return self._complete(call_id, Approval(PermissionDecision.ALLOW))

    def deny(self, call_id: str, reason: str | None = None) -> bool:
        return self._complete(
            call_id,
            Approval(
                PermissionDecision.DENY,
                reason.strip() if reason and reason.strip() else "Permission denied by user",
            ),
        )

    def deny_all(self, reason: str, *, session_id: str | None = None) -> None:
        for pending_call_id, (pending_session_id, _) in list(self._pending.items()):
            if session_id is None or pending_session_id == session_id:
                self.deny(pending_call_id, reason)

    def _complete(self, call_id: str, approval: Approval) -> bool:
        pending = self._pending.pop(call_id, None)
        if pending is None:
            return False
        _, future = pending
        if future.done():
            return False
        future.set_result(approval)
        return True
