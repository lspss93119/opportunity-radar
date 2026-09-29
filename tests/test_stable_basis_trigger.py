from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from radar.collectors.base import CollectorBatch
from radar.config import SpreadMonitorConfig
from radar.models import MarketSnapshot
from radar.monitors.spread.basis import RollingBasis
from radar.monitors.spread.monitor import SpreadMonitor
from radar.monitors.spread.models import SpreadPairKey
from radar.state import RadarState
from radar.storage.sqlite import SQLiteRuntimeStore

NOW = datetime(2026, 9, 28, 18, 30, tzinfo=UTC)
PAIR = SpreadPairKey("QQQ", "arcus", "QQQ-USD", "lighter_robinhood", "QQQ")


def make_snapshot(
    venue: str,
    when: datetime,
    raw_spread_bps: float,
    *,
    long_side: bool = False,
) -> MarketSnapshot:
    buy = 100.0 if venue == "arcus" else 100.0
    sell = 100.0 * (1.0 + raw_spread_bps / 10_000.0)
    if long_side:
        buy, sell = 100.0, 100.0
    return MarketSnapshot(
        sample_time=when,
        observed_at=when,
        venue=venue,
        venue_symbol="QQQ-USD" if venue == "arcus" else "QQQ",
        canonical_symbol="QQQ",
        best_bid=99.0,
        best_bid_size=1000.0,
        best_ask=100.0,
        best_ask_size=1000.0,
        buy_10k_vwap=buy,
        sell_10k_vwap=sell,
    )


def state_for(when: datetime, raw_spread_bps: float) -> RadarState:
    state = RadarState()
    state.apply(
        CollectorBatch(
            market_snapshots=(
                make_snapshot("arcus", when, raw_spread_bps, long_side=True),
                make_snapshot("lighter_robinhood", when, raw_spread_bps),
            )
        )
    )
    return state


def monitor(
    *,
    runtime_store: SQLiteRuntimeStore | None = None,
    fees_bps: dict[str, float] | None = None,
    **updates: object,
):
    values: dict[str, object] = {
        "candidate_net_bps": 10.0,
        "candidate_duration_seconds": 30,
        "alert_net_bps": 20.0,
        "alert_duration_seconds": 120,
        "interval_seconds": 10,
        "stale_after_seconds": 30,
        "primary_size_usd": 10_000,
        "top_n": 3,
    }
    values.update(updates)
    return SpreadMonitor(
        SpreadMonitorConfig.model_validate(values),
        {"arcus": 100.0, "lighter_robinhood": 100.0}
        if fees_bps is None
        else fees_bps,
        runtime_store=runtime_store,
        basis_window_seconds=6000,
        basis_min_observations=4,
        basis_expected_interval_seconds=10,
    )


def seed(monitor_instance: SpreadMonitor, values: list[float]) -> None:
    monitor_instance.hydrate_history(
        {
            PAIR: tuple(
                (
                    NOW - timedelta(seconds=max(10, 6000 - index * 10)),
                    value,
                )
                for index, value in enumerate(values)
            )
        }
    )


def test_rolling_basis_uses_prior_only_population_statistics_and_deduplicates():
    basis = RollingBasis(window_seconds=40, min_observations=4)
    basis.hydrate(
        [
            (NOW - timedelta(seconds=40), 10.0),
            (NOW - timedelta(seconds=30), 20.0),
            (NOW - timedelta(seconds=20), 30.0),
            (NOW - timedelta(seconds=10), 40.0),
        ]
    )

    stats = basis.observe(NOW, 50.0)
    duplicate = basis.observe(NOW, 50.0)

    assert stats.eligible is True
    assert stats.sample_count == 4
    assert stats.mean_bps == pytest.approx(25.0)
    assert stats.std_bps == pytest.approx(11.1803398875)
    assert duplicate.sample_count == 4
    assert basis.sample_count == 5


def test_rolling_basis_requires_full_window_and_coverage():
    basis = RollingBasis(window_seconds=40, min_observations=4)
    basis.hydrate(
        [
            (NOW - timedelta(seconds=30), 10.0),
            (NOW - timedelta(seconds=20), 10.0),
            (NOW - timedelta(seconds=10), 10.0),
        ]
    )

    stats = basis.stats_before(NOW)

    assert stats.eligible is False
    assert stats.sample_count == 3
    assert stats.mean_bps == pytest.approx(10.0)
    assert stats.std_bps == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_qqq_like_stable_deviation_alerts_after_sixty_seconds():
    instance = monitor()
    seed(instance, [9.0] * 600)

    assert await instance.evaluate(NOW, state_for(NOW, 30.0)) == []
    for offset in range(10, 60, 10):
        assert await instance.evaluate(
            NOW + timedelta(seconds=offset),
            state_for(NOW + timedelta(seconds=offset), 30.0),
        ) == []
    alerts = await instance.evaluate(
        NOW + timedelta(seconds=60), state_for(NOW + timedelta(seconds=60), 30.0)
    )

    assert len(alerts) == 1
    assert alerts[0].payload["rolling_mean_bps"] == pytest.approx(9.21)
    assert alerts[0].payload["rolling_std_bps"] == pytest.approx(2.0894736)
    assert alerts[0].payload["deviation_bps"] == pytest.approx(20.79)
    assert alerts[0].payload["signal_duration_seconds"] == 60


@pytest.mark.asyncio
async def test_legacy_threshold_fields_do_not_control_stable_trigger():
    instance = monitor(
        candidate_net_bps=10_000.0,
        candidate_duration_seconds=999,
        alert_net_bps=10_000.0,
        alert_duration_seconds=999,
    )
    seed(instance, [9.0] * 600)

    for offset in range(0, 60, 10):
        assert await instance.evaluate(
            NOW + timedelta(seconds=offset),
            state_for(NOW + timedelta(seconds=offset), 30.0),
        ) == []

    alerts = await instance.evaluate(
        NOW + timedelta(seconds=60), state_for(NOW + timedelta(seconds=60), 30.0)
    )

    assert len(alerts) == 1
    assert alerts[0].payload["signal_duration_seconds"] == 60
    assert instance.active_episodes[0].candidate_confirmed is False


@pytest.mark.asyncio
async def test_deviation_and_std_must_both_persist_for_alert():
    instance = monitor()
    seed(instance, [0.0, 10.0, 20.0, 30.0])

    for offset in range(0, 70, 10):
        assert await instance.evaluate(
            NOW + timedelta(seconds=offset),
            state_for(NOW + timedelta(seconds=offset), 31.0),
        ) == []


@pytest.mark.asyncio
async def test_persistence_break_resets_and_rearm_requires_below_fifteen():
    instance = monitor()
    seed(instance, [9.0] * 600)

    for offset in range(0, 70, 10):
        await instance.evaluate(
            NOW + timedelta(seconds=offset),
            state_for(NOW + timedelta(seconds=offset), 30.0),
        )
    assert instance.active_episodes[0].alerted is True
    assert await instance.evaluate(
        NOW + timedelta(seconds=70), state_for(NOW + timedelta(seconds=70), 30.0)
    ) == []
    assert await instance.evaluate(
        NOW + timedelta(seconds=80), state_for(NOW + timedelta(seconds=80), 14.0)
    ) == []
    instance.hydrate_history(
        {
            PAIR: tuple(
                (
                    NOW + timedelta(seconds=90)
                    - timedelta(seconds=6000 - index * 10),
                    9.0,
                )
                for index in range(600)
            )
        }
    )
    for offset in range(90, 150, 10):
        assert await instance.evaluate(
            NOW + timedelta(seconds=offset),
            state_for(NOW + timedelta(seconds=offset), 30.0),
        ) == []
    alerts = await instance.evaluate(
        NOW + timedelta(seconds=150), state_for(NOW + timedelta(seconds=150), 30.0)
    )

    assert len(alerts) == 1


@pytest.mark.asyncio
async def test_fee_does_not_block_trigger_and_theoretical_edge_is_display_context():
    instance = monitor()
    seed(instance, [9.0] * 600)
    for offset in range(0, 60, 10):
        await instance.evaluate(
            NOW + timedelta(seconds=offset),
            state_for(NOW + timedelta(seconds=offset), 30.0),
        )
    alerts = await instance.evaluate(
        NOW + timedelta(seconds=60), state_for(NOW + timedelta(seconds=60), 30.0)
    )

    assert len(alerts) == 1
    assert alerts[0].payload["round_trip_fee_bps"] == pytest.approx(400.0)
    assert alerts[0].payload["theoretical_edge_bps"] == pytest.approx(-379.21)


@pytest.mark.asyncio
async def test_missing_fee_does_not_block_raw_trigger_or_basis_eligibility():
    instance = monitor(fees_bps={})
    seed(instance, [9.0] * 600)
    for offset in range(0, 60, 10):
        assert await instance.evaluate(
            NOW + timedelta(seconds=offset),
            state_for(NOW + timedelta(seconds=offset), 30.0),
        ) == []

    alerts = await instance.evaluate(
        NOW + timedelta(seconds=60), state_for(NOW + timedelta(seconds=60), 30.0)
    )

    assert len(alerts) == 1
    assert alerts[0].payload["long_fee_bps"] is None
    assert alerts[0].payload["short_fee_bps"] is None
    assert alerts[0].payload["round_trip_fee_bps"] is None
    assert alerts[0].payload["theoretical_edge_bps"] is None


@pytest.mark.asyncio
async def test_alerted_episode_survives_restart_without_duplicate_alert(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        first = monitor(runtime_store=store)
        seed(first, [9.0] * 600)
        for offset in range(0, 60, 10):
            await first.evaluate(
                NOW + timedelta(seconds=offset),
                state_for(NOW + timedelta(seconds=offset), 30.0),
            )
        assert len(
            await first.evaluate(
                NOW + timedelta(seconds=60),
                state_for(NOW + timedelta(seconds=60), 30.0),
            )
        ) == 1

    with SQLiteRuntimeStore(database) as store:
        restarted = monitor(runtime_store=store)
        seed(restarted, [9.0] * 600)

        alerts = await restarted.evaluate(
            NOW + timedelta(seconds=70), state_for(NOW + timedelta(seconds=70), 30.0)
        )

    assert alerts == []
