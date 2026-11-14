import pytest

from taiyiagent.errors import ApiError, redact
from taiyiagent.hooks import PermissionDecision, PermissionPolicy
from taiyiagent.models import ToolCall


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("read_file", PermissionDecision.ALLOW),
        ("write_file", PermissionDecision.DENY),
        ("shell_exec", PermissionDecision.DENY),
        ("taiyiflow-in-chrome__read_text", PermissionDecision.ALLOW),
        ("taiyiflow-in-chrome__click", PermissionDecision.DENY),
    ],
)
def test_readonly_policy(name: str, expected: PermissionDecision) -> None:
    assert PermissionPolicy.READONLY.decide(ToolCall(name=name)) == expected


def test_permission_policies() -> None:
    write = ToolCall(name="write_file")
    assert PermissionPolicy.AUTO.decide(write) == PermissionDecision.ALLOW
    assert PermissionPolicy.CONFIRM.decide(write) == PermissionDecision.ASK
    assert PermissionPolicy.READONLY.decide(ToolCall(name="read_file")) == PermissionDecision.ALLOW


def test_api_error_preserves_structured_detail() -> None:
    error = ApiError(409, {"code": "session_conflict"})
    assert error.status_code == 409
    assert error.detail_code == "session_conflict"
    assert error.detail == {"code": "session_conflict"}


def test_error_text_redacts_credentials() -> None:
    text = redact("ws://host/path?__TOKEN__=secret Authorization: Bearer abc123")
    assert "secret" not in text
    assert "abc123" not in text
    assert "***" in text
