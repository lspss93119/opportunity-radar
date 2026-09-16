import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from radar.collectors.base import CollectorBatch
from radar.config import RadarConfig
from radar.models import MarketSnapshot
from radar.monitors.base import AlertRequest, Monitor
from radar.monitors.registry import MONITOR_FACTORIES, build_enabled_monitors
from radar.monitors.runner import MonitorRunner
from radar.state import RadarState

UTC = timezone.utc
NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)


class FakeMonitor:
    def __init__(
        self,
        name: str,
        interval_seconds: int,
        alerts: list[AlertRequest] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.name = name
        self.interval_seconds = interval_seconds
        self.alerts = [] if alerts is None else alerts
        self.error = error
        self.calls: list[tuple[datetime, RadarState]] = []

    async def evaluate(
        self, now: datetime, state: RadarState
    ) -> list[AlertRequest]:
        self.calls.append((now, state))
        if self.error is not None:
            raise self.error
        return list(self.alerts)


class BlockingMonitor(FakeMonitor):
    def __init__(self, started: asyncio.Event, release: asyncio.Event) -> None:
        super().__init__("blocking", 10)
        self.started = started
        self.release = release

    async def evaluate(
        self, now: datetime, state: RadarState
    ) -> list[AlertRequest]:
        self.calls.append((now, state))
        self.started.set()
        await self.release.wait()
        return []


def request(monitor: str, event_id: str) -> AlertRequest:
    return AlertRequest(
        monitor=monitor,
        event_id=event_id,
        created_at=NOW,
        payload={"event_id": event_id},
    )


def test_fake_monitor_structurally_satisfies_protocol_and_returns_alert():
    monitor = FakeMonitor("fake", 10, [request("fake", "one")])

    assert isinstance(monitor, Monitor)
    alerts = asyncio.run(monitor.evaluate(NOW, RadarState()))

    assert alerts == [request("fake", "one")]


def test_registry_uses_explicit_factory_and_respects_enabled_flag(monkeypatch):
    created_with = []

    def factory(settings):
        created_with.append(settings)
        return FakeMonitor("spread", settings.interval_seconds)

    monkeypatch.setitem(MONITOR_FACTORIES, "spread", factory)
    config = RadarConfig.model_validate(
        {"monitors": {"spread": {"enabled": True, "interval_seconds": 60}}}
    )
    monitors = build_enabled_monitors(config)

    assert len(monitors) == 1
    assert monitors[0].name == "spread"
    assert monitors[0].interval_seconds == 60
    assert created_with[0] is config.monitors.spread

    disabled_config = RadarConfig.model_validate(
        {"monitors": {"spread": {"enabled": False}}}
    )
    assert build_enabled_monitors(disabled_config) == ()


@pytest.mark.asyncio
async def test_enabled_monitor_runs_and_disabled_registry_monitor_does_not():
    enabled = FakeMonitor("enabled", 10)
    runner = MonitorRunner([enabled], RadarState())

    await runner.run_cycle(NOW)

    assert len(enabled.calls) == 1
    assert build_enabled_monitors(
        RadarConfig.model_validate({"monitors": {"spread": {"enabled": False}}}),
    ) == ()


@pytest.mark.asyncio
async def test_monitors_run_only_at_their_configured_cadences():
    monitor_a = FakeMonitor("a", 10)
    monitor_b = FakeMonitor("b", 60)
    runner = MonitorRunner([monitor_a, monitor_b], RadarState())

    await runner.run_cycle(NOW)
    await runner.run_cycle(NOW + timedelta(seconds=10))
    await runner.run_cycle(NOW + timedelta(seconds=20))
    await runner.run_cycle(NOW + timedelta(seconds=59))
    await runner.run_cycle(NOW + timedelta(seconds=60))

    assert [call[0] for call in monitor_a.calls] == [
        NOW,
        NOW + timedelta(seconds=10),
        NOW + timedelta(seconds=20),
        NOW + timedelta(seconds=59),
    ]
    assert [call[0] for call in monitor_b.calls] == [
        NOW,
        NOW + timedelta(seconds=60),
    ]


@pytest.mark.asyncio
async def test_time_jump_runs_once_without_catch_up_executions():
    monitor = FakeMonitor("jump", 10)
    runner = MonitorRunner([monitor], RadarState())

    await runner.run_cycle(NOW)
    await runner.run_cycle(NOW + timedelta(seconds=600))

    assert [call[0] for call in monitor.calls] == [
        NOW,
        NOW + timedelta(seconds=600),
    ]


@pytest.mark.asyncio
async def test_alerts_are_queued_in_monitor_and_alert_order():
    monitor_a = FakeMonitor("a", 10, [request("a", "a1"), request("a", "a2")])
    monitor_b = FakeMonitor("b", 10, [request("b", "b1")])
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = MonitorRunner([monitor_a, monitor_b], RadarState(), queue)

    await runner.run_cycle(NOW)

    assert [await queue.get(), await queue.get(), await queue.get()] == [
        request("a", "a1"),
        request("a", "a2"),
        request("b", "b1"),
    ]


@pytest.mark.asyncio
async def test_zero_alert_monitor_does_not_enqueue_anything():
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = MonitorRunner([FakeMonitor("quiet", 10)], RadarState(), queue)

    await runner.run_cycle(NOW)

    assert queue.empty()


@pytest.mark.asyncio
async def test_monitor_failure_isolated_and_reported():
    failures: list[tuple[str, Exception]] = []
    failing = FakeMonitor("failing", 10, error=RuntimeError("boom"))
    succeeding = FakeMonitor("succeeding", 10, [request("succeeding", "ok")])
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = MonitorRunner(
        [failing, succeeding],
        RadarState(),
        queue,
        error_handler=lambda name, error: failures.append((name, error)),
    )

    await runner.run_cycle(NOW)

    assert len(failing.calls) == 1
    assert len(succeeding.calls) == 1
    assert failures == [("failing", failing.error)]
    assert await queue.get() == request("succeeding", "ok")


@pytest.mark.asyncio
async def test_long_monitor_is_not_overlapped_or_backfilled():
    started = asyncio.Event()
    release = asyncio.Event()
    monitor = BlockingMonitor(started, release)
    runner = MonitorRunner([monitor], RadarState())

    first_cycle = asyncio.create_task(runner.run_cycle(NOW))
    await asyncio.wait_for(started.wait(), timeout=0.2)
    second_cycle = asyncio.create_task(
        runner.run_cycle(NOW + timedelta(seconds=10))
    )
    await second_cycle
    assert len(monitor.calls) == 1

    release.set()
    await first_cycle
    await runner.run_cycle(NOW + timedelta(seconds=20))
    assert len(monitor.calls) == 2


@pytest.mark.asyncio
async def test_concurrent_cycles_reserve_monitor_before_evaluation():
    started = asyncio.Event()
    release = asyncio.Event()
    monitor = BlockingMonitor(started, release)
    runner = MonitorRunner([monitor], RadarState())

    first_cycle = asyncio.create_task(runner.run_cycle(NOW))
    second_cycle = asyncio.create_task(runner.run_cycle(NOW))
    await asyncio.wait_for(started.wait(), timeout=0.2)
    await asyncio.sleep(0)

    assert len(monitor.calls) == 1

    release.set()
    await asyncio.gather(first_cycle, second_cycle)


@pytest.mark.asyncio
async def test_error_handler_failure_does_not_block_other_alerts():
    failing = FakeMonitor("failing", 10, error=RuntimeError("monitor failed"))
    succeeding = FakeMonitor("succeeding", 10, [request("succeeding", "ok")])
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()

    def broken_error_handler(name, error):
        raise RuntimeError("error handler failed")

    runner = MonitorRunner(
        [failing, succeeding],
        RadarState(),
        queue,
        error_handler=broken_error_handler,
    )

    await runner.run_cycle(NOW)

    assert await queue.get() == request("succeeding", "ok")


@pytest.mark.parametrize(
    ("monitors", "error"),
    [
        ([FakeMonitor("", 10)], "name"),
        ([FakeMonitor("zero", 0)], "interval"),
        ([FakeMonitor("negative", -1)], "interval"),
        ([FakeMonitor("bool", True)], "interval"),
        ([FakeMonitor("duplicate", 10), FakeMonitor("duplicate", 60)], "duplicate"),
        ([object()], "protocol"),
    ],
)
def test_runner_validates_monitor_contract(monitors, error):
    with pytest.raises((TypeError, ValueError), match=error):
        MonitorRunner(monitors, RadarState())


def test_alert_request_accepts_utc_and_nested_json_payload():
    alert = AlertRequest(
        monitor="fake",
        event_id="event-1",
        created_at=NOW,
        payload={"items": [{"ok": True}], "value": 1.5},
    )

    assert alert.created_at == NOW
    assert alert.payload["items"] == [{"ok": True}]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"created_at": datetime(2026, 9, 15, 12, 0)}, "created_at"),
        ({"monitor": ""}, "monitor"),
        ({"event_id": ""}, "event_id"),
        ({"payload": {"bad": object()}}, "payload"),
        ({"payload": {"bad": float("nan")}}, "payload"),
        ({"payload": {1: "non-string key"}}, "payload"),
    ],
)
def test_alert_request_rejects_invalid_values(kwargs, message):
    values = {
        "monitor": "fake",
        "event_id": "event-1",
        "created_at": NOW,
        "payload": {},
    }
    values.update(kwargs)

    with pytest.raises(ValueError, match=message):
        AlertRequest(**values)


def test_alert_request_is_frozen():
    alert = request("fake", "event-1")

    with pytest.raises((AttributeError, TypeError)):
        alert.event_id = "changed"


def test_framework_test_fixture_does_not_require_market_data_domain_fields():
    snapshot = MarketSnapshot(
        sample_time=NOW,
        observed_at=NOW,
        venue="lighter",
        venue_symbol="BTC",
        canonical_symbol="BTC",
        best_bid=99.0,
        best_bid_size=1.0,
        best_ask=100.0,
        best_ask_size=1.0,
    )
    state = RadarState()
    state.apply(CollectorBatch(market_snapshots=(snapshot,)))
    monitor = FakeMonitor("generic", 10, [request("generic", "event")])

    asyncio.run(monitor.evaluate(NOW, state))

    assert monitor.calls[0][1] is state
