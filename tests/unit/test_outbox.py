from pathlib import Path

import httpx
import pytest
import respx

from taiyiagent import (
    CheckpointToolResultsAdapter,
    FileReportOutbox,
    InMemoryReportOutbox,
    OutboxLimits,
    OutboxStatus,
    ReportConflictError,
    ReportStatus,
    ResumableCheckpoint,
    ToolResultReporter,
)
from taiyiagent.tools.outbox import client_result_entry
from taiyiagent.transport import RestClient


def payload(result: str = "done") -> dict[str, object]:
    return {
        "flow_id": "flow-1",
        "session_id": "session-1",
        "message_id": "message-1",
        "run_id": "run-1",
        "call_id": "call-1",
        "success": True,
        "result": result,
    }


def checkpoint() -> ResumableCheckpoint:
    return ResumableCheckpoint(
        id="checkpoint-1",
        flow_id="flow-1",
        session_id="session-1",
        run_id="run-1",
        status="paused",
        version=4,
        waiting_call_ids=["call-1"],
    )


def test_file_store_recovers_interrupted_reports_and_quarantines_corruption(
    tmp_path: Path,
) -> None:
    first = FileReportOutbox(tmp_path)
    entry = first.put(client_result_entry(payload()))
    first.mark_reporting(entry.id)
    (first.directory / "broken.json").write_text("not-json", encoding="utf-8")

    recovered = FileReportOutbox(tmp_path)

    restored = recovered.find(entry.id)
    assert restored is not None
    assert restored.status is OutboxStatus.PENDING
    assert restored.last_error == "recovered-interrupted-report"
    assert len(recovered.list_all()) == 1
    assert len(list(recovered.quarantine_directory.iterdir())) == 1


def test_identical_replay_deduplicates_and_conflict_becomes_dead_letter() -> None:
    outbox = InMemoryReportOutbox()
    original = outbox.put(client_result_entry(payload()))
    reordered = {
        "call_id": "call-1",
        "run_id": "run-1",
        "result": "done",
        "success": True,
        "message_id": "message-1",
        "session_id": "session-1",
        "flow_id": "flow-1",
    }
    assert outbox.put(client_result_entry(reordered)).id == original.id

    with pytest.raises(ReportConflictError):
        outbox.put(client_result_entry(payload("other")))
    dead = outbox.find(original.id)
    assert dead is not None
    assert dead.status is OutboxStatus.DEAD_LETTER


@pytest.mark.asyncio
@respx.mock
async def test_network_failure_keeps_result_pending_and_flush_replays_same_payload() -> None:
    route = respx.post("https://example.test/api/v1/agents/client/call/result").mock(
        side_effect=[httpx.Response(500), httpx.Response(200, json={})]
    )
    rest = RestClient("https://example.test", "secret")
    reporter = ToolResultReporter(rest, max_retries=0, initial_retry_delay=0)

    outcome = await reporter.report_payload(payload())
    assert outcome.status is ReportStatus.PENDING
    assert outcome.entry is not None
    assert outcome.entry.last_error == "http-500"
    assert route.call_count == 1

    flushed = await reporter.flush_now()
    assert flushed[0].status is ReportStatus.REPORTED
    assert route.call_count == 2
    assert route.calls[0].request.content == route.calls[1].request.content
    assert reporter.outbox.list_all() == []
    await rest.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_persistence_failure_makes_only_one_direct_attempt() -> None:
    route = respx.post("https://example.test/api/v1/agents/client/call/result").mock(
        return_value=httpx.Response(500)
    )
    rest = RestClient("https://example.test", "secret")
    reporter = ToolResultReporter(
        rest,
        outbox=InMemoryReportOutbox(OutboxLimits(1, 128, 128, 1)),
        max_retries=3,
        initial_retry_delay=0,
    )

    outcome = await reporter.report_payload(payload())
    assert outcome.status is ReportStatus.PENDING
    assert not outcome.persisted
    assert route.call_count == 1
    await rest.aclose()


def test_checkpoint_retry_keeps_identity_and_acknowledged_replay_is_cleaned() -> None:
    outbox = InMemoryReportOutbox()
    adapter = CheckpointToolResultsAdapter(outbox)
    pending = {"call_id": "call-1", "result": "done"}

    first = adapter.prepare(checkpoint(), "flow-1", "session-1", {"call-1": pending})
    retry = adapter.prepare(checkpoint(), "flow-1", "session-1", {})
    assert first.entry is not None
    assert retry.entry is not None
    assert first.entry.submission_id == retry.entry.submission_id
    assert first.entry.submission_hash == retry.entry.submission_hash
    assert first.entry.payload == retry.entry.payload

    acknowledged_checkpoint = checkpoint().model_copy(
        update={
            "last_submission_id": first.entry.submission_id,
            "last_submission_hash": first.entry.submission_hash,
        }
    )
    acknowledged = adapter.prepare(acknowledged_checkpoint, "flow-1", "session-1", {})
    assert not acknowledged.has_results
    assert outbox.list_all() == []
