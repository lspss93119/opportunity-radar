from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

import radar.history.manual_opportunity as history_module
from radar.alerts.manual_opportunity import ManualOpportunityAlertProcessor
from radar.alerts.worker import AlertRouter
from radar.collectors.base import CollectorBatch
from radar.config import AnomalyV2Config, ManualOpportunityConfig, SpreadMonitorConfig
from radar.history.manual_opportunity import BboRollingHistory
from radar.models import HourlyContext, MarketSnapshot
from radar.monitors.manual_opportunity import ManualOpportunityMonitor
from radar.monitors.runner import MonitorRunner
from radar.monitors.spread.monitor import SpreadMonitor
from radar.monitors.spread.models import SpreadPairKey
from radar.state import RadarState
from radar.storage.sqlite import SQLiteRuntimeStore


START = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
KEY = SpreadPairKey("QQQ", "arcus", "QQQ-USD", "lighter_robinhood", "QQQ")


class _SmallHistory(BboRollingHistory):
    def __init__(self) -> None:
        super().__init__(
            windows_seconds={"2h": 1_000, "24h": 1_000, "3d": 1_000},
            expected_interval_seconds=10,
            minimum_coverage=0.0,
        )


def _state(when: datetime, *, short_bid: float = 101.2) -> RadarState:
    state = RadarState()
    state.apply_market_batch(
        CollectorBatch(
            market_snapshots=(
                MarketSnapshot(
                    sample_time=when,
                    observed_at=when,
                    venue="arcus",
                    venue_symbol="QQQ-USD",
                    canonical_symbol="QQQ",
                    best_bid=99.0,
                    best_bid_size=1.0,
                    best_ask=100.0,
                    best_ask_size=1.0,
                ),
                MarketSnapshot(
                    sample_time=when,
                    observed_at=when,
                    venue="lighter_robinhood",
                    venue_symbol="QQQ",
                    canonical_symbol="QQQ",
                    best_bid=short_bid,
                    best_bid_size=1.0,
                    best_ask=short_bid + 1.0,
                    best_ask_size=1.0,
                ),
            )
        )
    )
    state.apply_context_batch(
        CollectorBatch(
            hourly_contexts=(
                HourlyContext(
                    sample_time=when - timedelta(hours=1),
                    observed_at=when - timedelta(hours=1),
                    venue="arcus",
                    venue_symbol="QQQ-USD",
                    canonical_symbol="QQQ",
                    volume_24h=2_000_000.0,
                ),
                HourlyContext(
                    sample_time=when - timedelta(hours=1),
                    observed_at=when - timedelta(hours=1),
                    venue="lighter_robinhood",
                    venue_symbol="QQQ",
                    canonical_symbol="QQQ",
                    volume_24h=1_500_000.0,
                ),
            )
        )
    )
    return state


@pytest.mark.asyncio
async def test_manual_monitor_runner_and_text_processor_have_no_slow_or_trading_path(
    monkeypatch,
):
    monkeypatch.setattr(history_module, "BboRollingHistory", _SmallHistory)
    manual = ManualOpportunityMonitor(
        ManualOpportunityConfig(),
        {"arcus": 0.0, "lighter_robinhood": 0.0},
    )
    manual.hydrate_history(
        {
            KEY: tuple(
                (START - timedelta(seconds=offset), 0.0)
                for offset in range(1_000, 0, -10)
            )
        }
    )
    spread = SpreadMonitor(
        SpreadMonitorConfig(),
        {"arcus": 0.0, "lighter_robinhood": 0.0},
    )
    queue = asyncio.Queue()
    runner = MonitorRunner((spread, manual), RadarState(), queue)

    for offset in range(0, 61, 10):
        when = START + timedelta(seconds=offset)
        runner.state = _state(when)
        await runner.run_cycle(when)

    initial = await queue.get()
    assert initial.monitor == "manual_opportunity"
    assert initial.payload["event_kind"] == "manual_initial"
    assert queue.empty()

    for offset in (70,):
        when = START + timedelta(seconds=offset)
        runner.state = _state(when, short_bid=102.0)
        await runner.run_cycle(when)
    expansion = await queue.get()
    assert expansion.payload["event_kind"] == "manual_expansion"

    class FakeTelegram:
        def __init__(self):
            self.text_calls: list[str] = []
            self.chart_calls: list[tuple[bytes, str]] = []
            self.network_calls = 0

        async def send_text(self, text: str) -> None:
            self.text_calls.append(text)

        async def send_chart(self, png: bytes, caption: str) -> None:
            self.chart_calls.append((png, caption))

    telegram = FakeTelegram()
    processor = ManualOpportunityAlertProcessor(telegram)  # type: ignore[arg-type]
    router = AlertRouter(
        spread_processor=processor,
        manual_processor=processor,
    )
    await router.process(initial)
    await router.process(expansion)

    assert len(telegram.text_calls) == 2
    assert telegram.chart_calls == []
    assert telegram.network_calls == 0


@pytest.mark.asyncio
async def test_disabled_anomaly_v2_persists_event_but_queues_nothing(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    config = SpreadMonitorConfig(
        anomaly_v2=AnomalyV2Config(
            enabled=True,
            telegram_enabled=False,
            confirmation_seconds=0,
            max_gap_seconds=20,
        ),
        stale_after_seconds=30,
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = SpreadMonitor(
            config,
            {"arcus": 0.0, "lighter_robinhood": 0.0},
            runtime_store=store,
            basis_window_seconds=100,
            basis_min_observations=1,
            basis_expected_interval_seconds=10,
        )
        monitor.hydrate_history(
            {
                KEY: tuple(
                    (START - timedelta(seconds=offset), 0.0)
                    for offset in range(100, 0, -10)
                )
            }
        )
        state = _state(START)
        state.apply_market_batch(
            CollectorBatch(
                market_snapshots=(
                    state.markets[0].model_copy(update={
                        "buy_10k_vwap": 100.0,
                        "sell_10k_vwap": 100.0,
                    }),
                    state.markets[1].model_copy(update={
                        "buy_10k_vwap": 100.0,
                        "sell_10k_vwap": 100.2,
                    }),
                )
            )
        )
        queue = asyncio.Queue()
        runner = MonitorRunner((monitor,), state, queue)

        await runner.run_cycle(START)

        assert queue.empty()
        assert store.get_monitor_state("spread", "anomaly_episodes_v2")
        assert store.list_opportunities(monitor_name="spread")


def test_example_config_makes_anomaly_telegram_gate_explicit():
    config = yaml.safe_load(
        (
            Path(__file__).parents[1] / "config" / "radar.example.yaml"
        ).read_text(encoding="utf-8")
    )

    anomaly = config["monitors"]["spread"]["anomaly_v2"]
    assert config["manual_opportunity"]["enabled"] is False
    assert anomaly["telegram_enabled"] is True
