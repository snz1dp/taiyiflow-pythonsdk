import pytest
from pydantic import ValidationError

from taiyiagent.models import (
    ChatRequest,
    CloseCode,
    EventType,
    ResumableCheckpoint,
    StreamEvent,
    ToolCall,
    ToolResult,
    describe_close,
)


def test_chat_request_uses_backend_wire_aliases() -> None:
    request = ChatRequest(
        input_value="hello",
        session_id="session-1",
        files=["/workspace/a.txt"],
        reasoning=True,
        model="model-a",
        execution_mode="start",
        client_mcp_servers={"local": {"command": "python3"}},
    )

    wire = request.to_wire()

    assert wire["files"] == ["/workspace/a.txt"]
    assert wire["reasoning"] is True
    assert wire["model"] == "model-a"
    assert "input_files" not in wire
    assert "run_id" not in wire
    assert ChatRequest.from_wire(wire).to_wire() == wire


def test_chat_request_rejects_misspelled_outbound_fields() -> None:
    with pytest.raises(ValidationError):
        ChatRequest(sessionId="wrong")


def test_backend_extended_request_fields_round_trip() -> None:
    wire = {
        "input_value": "continue",
        "long_term_memory_enabled": True,
        "cross_session_memory_enabled": True,
        "relation_sessions": ["s-1", "s-2"],
        "client_skill_packages": {"skill": {"content": "instructions"}},
        "client_instructions": {"merged": "follow policy"},
        "agent_auto_loop_max_rounds": 2,
        "available_tools": [{"name": "read_file"}],
    }

    assert ChatRequest.from_wire(wire).to_wire() == wire


def test_stream_event_reads_only_text_chunk() -> None:
    event = StreamEvent(event="message", data={"chunk": "hello"})

    assert event.is_message
    assert event.message_text == "hello"
    assert event.is_type(EventType.MESSAGE)
    assert StreamEvent(event="message", data={"chunk": 1}).message_text is None
    assert (
        StreamEvent(event="error", data={"error": {"code": "bad"}}).error_message
        == "{'code': 'bad'}"
    )


def test_tool_event_parser_supports_function_and_command() -> None:
    event = StreamEvent(
        event="client_tool",
        data={
            "flow_id": "flow-event",
            "function": {"id": "call-1", "name": "read_file", "arguments": {"path": "a"}},
            "context": {"session_id": "session-event", "run_id": "run-1", "message_id": "m-1"},
        },
    )
    call = ToolCall.from_event(event, default_flow_id="flow", default_session_id="session")

    assert call.id == "call-1"
    assert call.flow_id == "flow-event"
    assert call.session_id == "session-event"
    assert call.run_id == "run-1"
    assert call.arguments_dict == {"path": "a"}

    command = StreamEvent(
        event="built_in",
        data={"command": {"id": "call-2", "arguments": {"command": "pwd"}}},
    )
    assert (
        ToolCall.from_event(command, default_flow_id="f", default_session_id="s").name
        == "shell_exec"
    )


def test_tool_result_constructors_match_wire_shape() -> None:
    assert ToolResult.ok("call-1", {"value": 1}).to_wire() == {
        "tool_call_id": "call-1",
        "success": True,
        "result": {"value": 1},
    }
    assert ToolResult.fail("call-1", "denied").to_wire() == {
        "tool_call_id": "call-1",
        "success": False,
        "error": "denied",
    }


def test_close_codes_cover_backend_contract() -> None:
    assert [code.value for code in CloseCode] == [1000, 4000, 4001, 4002, 4003, 4004, 4009]
    assert describe_close(4001) == "Authentication failed"
    assert describe_close(4009, "busy") == "Session execution conflict: busy"
    assert CloseCode.AUTH_FAILED.is_error
    assert not CloseCode.NORMAL.is_error


def test_checkpoint_side_effect_classification() -> None:
    base = {
        "id": "c",
        "flow_id": "f",
        "session_id": "s",
        "run_id": "r",
        "status": "paused",
        "version": 1,
    }
    assert not ResumableCheckpoint(**base).has_uncertain_side_effects
    assert ResumableCheckpoint(**base, pending_call_ids=["call-1"]).has_uncertain_side_effects
    assert ResumableCheckpoint(**base, uncertain_call_ids=["call-2"]).has_uncertain_side_effects
