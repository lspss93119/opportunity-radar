import pytest

from radar.collectors import hyperliquid


@pytest.fixture(autouse=True)
def isolated_hyperliquid_reconnect_budget(monkeypatch):
    """The process-wide reconnect budget must not leak waits between tests."""
    monkeypatch.setattr(
        hyperliquid, "SHARED_RECONNECT_BUDGET", hyperliquid.ReconnectBudget(0.0)
    )
