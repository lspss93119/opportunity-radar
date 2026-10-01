from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from radar.collectors.base import CollectorBatch
from radar.config import AnomalyV2Config, SpreadMonitorConfig
from radar.models import MarketSnapshot
from radar.monitors.spread.monitor import SpreadMonitor
from radar.monitors.spread.models import SpreadPairKey
from radar.state import RadarState
from radar.storage.sqlite import SQLiteRuntimeStore


START = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def market(
    venue: str,
    sample_time: datetime,
    *,
    long_side: bool,
    spread_bps: float = 116.0,
) -> MarketSnapshot:
    if long_side:
        buy = 100.0
        sell = 99.0
    else:
        buy = 102.0
        sell = 100.0 * (1.0 + spread_bps / 10_000.0)
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=sample_time,
        venue=venue,
        venue_symbol="QQQ-USD" if venue == "arcus" else "QQQ",
        canonical_symbol="QQQ",
        best_bid=99.0,
        best_bid_size=100.0,
        best_ask=100.0,
        best_ask_size=100.0,
        buy_10k_vwap=buy,
        sell_10k_vwap=sell,
    )


def state_at(sample_time: datetime, *, spread_bps: float = 116.0) -> RadarState:
    state = RadarState()
    state.apply_market_batch(
        CollectorBatch(
            market_snapshots=(
                market("arcus", sample_time, long_side=True),
                market(
                    "lighter_robinhood",
                    sample_time,
                    long_side=False,
                    spread_bps=spread_bps,
                ),
            )
        )
    )
    return state


def make_monitor(
    *,
    store: SQLiteRuntimeStore | None = None,
    enabled: bool = True,
    confirmation_seconds: int = 60,
) -> SpreadMonitor:
    config = SpreadMonitorConfig(
        stale_after_seconds=120,
        anomaly_v2=AnomalyV2Config(
            enabled=enabled,
            deviation_bps=15.0,
            confirmation_seconds=confirmation_seconds,
            return_band_bps=5.0,
            max_gap_seconds=20,
            expansion_notify_step_bps=5.0,
        ),
    )
    return SpreadMonitor(
        config,
        {"arcus": 2.25, "lighter_robinhood": 0.0},
        runtime_store=store,
        basis_window_seconds=12_000,
        basis_min_observations=4,
        basis_expected_interval_seconds=10,
    )


def prime(monitor: SpreadMonitor, when: datetime = START) -> None:
    key = SpreadPairKey("QQQ", "arcus", "QQQ-USD", "lighter_robinhood", "QQQ")
    monitor.hydrate_history(
        {
            key: tuple(
                (when - timedelta(seconds=12_000 - index * 10), 100.0)
                for index in range(1_200)
            )
        }
    )


@pytest.mark.asyncio
async def test_v2_confirms_at_15_bps_after_60_seconds_and_emits_no_legacy_alert():
    monitor = make_monitor()
    prime(monitor)

    first = await monitor.evaluate(START, state_at(START))
    second = []
    for seconds in range(10, 61, 10):
        second = await monitor.evaluate(
            START + timedelta(seconds=seconds),
            state_at(START + timedelta(seconds=seconds)),
        )

    assert first == []
    assert [alert.payload["event_kind"] for alert in second] == ["anomaly_initial"]
    assert monitor._anomaly_v2 is not None
    active = monitor._anomaly_v2.active_states
    assert len(active) == 1
    assert active[0].episode.reference_mean_bps == pytest.approx(100.0)
    assert active[0].episode.confirmation_deviation_bps >= 15.0


@pytest.mark.asyncio
async def test_v2_expansion_is_post_confirmation_and_return_is_one_alert():
    monitor = make_monitor(confirmation_seconds=0)
    prime(monitor)
    await monitor.evaluate(START, state_at(START))
    expansion = await monitor.evaluate(
        START + timedelta(seconds=10),
            state_at(START + timedelta(seconds=10), spread_bps=122.0),
    )
    assert [alert.payload["event_kind"] for alert in expansion] == ["anomaly_expansion"]
    assert expansion[0].payload["lifetime_peak_deviation_bps"] == pytest.approx(22.0)

    returned = await monitor.evaluate(
        START + timedelta(seconds=20),
        state_at(START + timedelta(seconds=20), spread_bps=104.0),
    )
    assert [alert.payload["event_kind"] for alert in returned] == ["anomaly_return"]


@pytest.mark.asyncio
async def test_v2_restart_restores_active_episode_and_deduplicates_confirmation(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(store=store)
        prime(monitor)
        await monitor.evaluate(START, state_at(START))
        for seconds in range(10, 61, 10):
            await monitor.evaluate(
                START + timedelta(seconds=seconds),
                state_at(START + timedelta(seconds=seconds)),
            )

    with SQLiteRuntimeStore(database) as store:
        restored = make_monitor(store=store)
        prime(restored)
        alerts = await restored.evaluate(
            START + timedelta(seconds=70),
            state_at(START + timedelta(seconds=70)),
        )
        assert alerts == []
        assert restored._anomaly_v2 is not None
        assert restored._anomaly_v2.active_states[0].initial_event_emitted is True


def test_anomaly_v2_disabled_keeps_legacy_mode():
    monitor = make_monitor(enabled=False)
    assert monitor._anomaly_v2 is None
