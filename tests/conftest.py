import pytest


@pytest.fixture(autouse=True)
def isolate_network_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "NO_PROXY",
        "no_proxy",
    ):
        monkeypatch.delenv(name, raising=False)
