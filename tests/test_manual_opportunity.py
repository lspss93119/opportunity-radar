from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from radar.config import ManualOpportunityConfig, RadarConfig
from radar.models import MarketSnapshot
from radar.monitors.manual_opportunity import (
    ManualOpportunityLifecycle,
    ManualOpportunityObservation,
    build_manual_observation,
    manual_opportunity_rejection_reason,
)
from radar.monitors.spread.models import SpreadPairKey
from radar.storage.sqlite import SQLiteRuntimeStore


START = datetime(2026, 9, 28, 10, 30, tzinfo=UTC)
KEY = SpreadPairKey("QQQ", "arcus", "QQQ-USD", "lighter_robinhood", "QQQ")
FEES = {"arcus": 2.25, "lighter_robinhood": 0.0}
CONFIG = ManualOpportunityConfig()


def observation(
    when: datetime,
    *,
    spread_bps: float = 20.0,
    means: tuple[float | None, float | None, float | None] = (0.0, 0.0, 0.0),
    long_volume: float | None = 1_000_000.0,
    short_volume: float | None = 1_000_000.0,
    key: SpreadPairKey = KEY,
    fees: dict[str, float] = FEES,
) -> ManualOpportunityObservation:
    return ManualOpportunityObservation(
        key=key,
        sample_time=when,
        long_observed_at=when,
        short_observed_at=when,
        long_best_ask=100.0,
        short_best_bid=100.0 * (1.0 + spread_bps / 10_000.0),
        mean_2h_bps=means[0],
        mean_24h_bps=means[1],
        mean_3d_bps=means[2],
        long_volume_24h=long_volume,
        short_volume_24h=short_volume,
        long_fee_bps=fees[key.long_venue],
        short_fee_bps=fees[key.short_venue],
    )


def test_bbo_observation_uses_long_ask_and_short_bid_not_vwap():
    long_snapshot = MarketSnapshot(
        sample_time=START,
        observed_at=START,
        venue="arcus",
        venue_symbol="QQQ-USD",
        canonical_symbol="QQQ",
        best_bid=99.0,
        best_bid_size=100.0,
        best_ask=100.0,
        best_ask_size=100.0,
        buy_10k_vwap=1.0,
        sell_10k_vwap=1.0,
    )
    short_snapshot = MarketSnapshot(
        sample_time=START,
        observed_at=START,
        venue="lighter_robinhood",
        venue_symbol="QQQ",
        canonical_symbol="QQQ",
        best_bid=101.0,
        best_bid_size=100.0,
        best_ask=102.0,
        best_ask_size=100.0,
        buy_10k_vwap=999.0,
        sell_10k_vwap=999.0,
    )

    result = build_manual_observation(
        long_snapshot,
        short_snapshot,
        mean_2h_bps=0.0,
        mean_24h_bps=0.0,
        mean_3d_bps=0.0,
        long_volume_24h=1_000_000.0,
        short_volume_24h=1_000_000.0,
        fees_bps=FEES,
    )

    assert result.current_spread_bps == pytest.approx(100.0)
    assert result.long_best_ask == 100.0
    assert result.short_best_bid == 101.0


@pytest.mark.parametrize(
    ("fees", "spread", "expected"),
    [
        ({"arcus": 2.25, "lighter_robinhood": 0.0}, 14.5, 10.0),
        ({"trade_xyz": 9.0, "hyperliquid": 4.5}, 37.0, 10.0),
    ],
)
def test_four_leg_taker_fee_and_expected_net_are_exact(
    fees: dict[str, float], spread: float, expected: float
):
    key = SpreadPairKey("QQQ", "arcus", "QQQ-USD", "lighter_robinhood", "QQQ")
    if "trade_xyz" in fees:
        key = SpreadPairKey("BTC", "trade_xyz", "xyz:BTC", "hyperliquid", "BTC")
    item = ManualOpportunityObservation(
        key=key,
        sample_time=START,
        long_observed_at=START,
        short_observed_at=START,
        long_best_ask=100.0,
        short_best_bid=100.0 * (1.0 + spread / 10_000.0),
        mean_2h_bps=0.0,
        mean_24h_bps=0.0,
        mean_3d_bps=0.0,
        long_volume_24h=1_000_000.0,
        short_volume_24h=1_000_000.0,
        long_fee_bps=fees[key.long_venue],
        short_fee_bps=fees[key.short_venue],
    )
    assert item.round_trip_fee_bps == pytest.approx(2 * sum(fees.values()))
    assert item.expected_net_at_a_bps == pytest.approx(expected)
    assert manual_opportunity_rejection_reason(item, CONFIG) is None


@pytest.mark.parametrize(
    "changed",
    [
        {"means": (0.0, None, 0.0)},
        {"means": (0.0, 0.0, 6.0)},
        {"long_volume": 999_999.99},
        {"short_volume": None},
    ],
)
def test_missing_or_unstable_baseline_and_volume_fail_closed(changed):
    item = observation(START, **changed)
    assert manual_opportunity_rejection_reason(item, CONFIG) is not None


def test_manual_lifecycle_confirms_after_strict_sixty_seconds():
    lifecycle = ManualOpportunityLifecycle(CONFIG, FEES)

    for offset in range(0, 61, 10):
        alerts = lifecycle.evaluate(observation(START + timedelta(seconds=offset)))

    assert len(alerts) == 1
    assert alerts[0].payload["event_kind"] == "manual_initial"
    assert lifecycle.active_episodes[0].confirmed_at == START + timedelta(seconds=60)


def test_one_failed_sample_resets_candidate_timer():
    lifecycle = ManualOpportunityLifecycle(CONFIG, FEES)
    for offset in (0, 10, 20):
        assert lifecycle.evaluate(observation(START + timedelta(seconds=offset))) == []
    assert lifecycle.evaluate(observation(START + timedelta(seconds=30), spread_bps=9.0)) == []
    for offset in range(40, 100, 10):
        assert lifecycle.evaluate(observation(START + timedelta(seconds=offset))) == []
    assert lifecycle.active_episodes[0].confirmed_at is None
    assert lifecycle.active_episodes[0].candidate_started_at == START + timedelta(seconds=40)
    assert lifecycle.evaluate(observation(START + timedelta(seconds=100)))


def test_continuity_gap_resets_candidate():
    lifecycle = ManualOpportunityLifecycle(CONFIG, FEES)
    lifecycle.evaluate(observation(START))
    assert lifecycle.evaluate(observation(START + timedelta(seconds=30))) == []
    assert lifecycle.active_episodes[0].candidate_started_at == START + timedelta(seconds=30)


def test_expansion_ladder_is_frozen_to_confirmation_basis():
    lifecycle = ManualOpportunityLifecycle(
        ManualOpportunityConfig(baseline_range_max_bps=5.0),
        {"arcus": 0.0, "lighter_robinhood": 0.0},
    )
    for offset in range(0, 61, 10):
        lifecycle.evaluate(
            observation(
                START + timedelta(seconds=offset),
                spread_bps=12.0,
                fees={"arcus": 0.0, "lighter_robinhood": 0.0},
            )
        )
    expansion = lifecycle.evaluate(
        observation(
            START + timedelta(seconds=70),
            spread_bps=16.0,
            fees={"arcus": 0.0, "lighter_robinhood": 0.0},
        )
    )
    assert expansion[0].payload["event_kind"] == "manual_expansion"
    assert expansion[0].payload["expansion_level_bps"] == pytest.approx(15.0)
    assert lifecycle.evaluate(
        observation(
            START + timedelta(seconds=80),
            spread_bps=21.0,
            fees={"arcus": 0.0, "lighter_robinhood": 0.0},
        )
    )[0].payload["expansion_level_bps"] == pytest.approx(20.0)
    assert lifecycle.evaluate(
        observation(
            START + timedelta(seconds=90),
            spread_bps=21.0,
            fees={"arcus": 0.0, "lighter_robinhood": 0.0},
        )
    ) == []
    assert lifecycle.evaluate(
        observation(
            START + timedelta(seconds=100),
            spread_bps=26.0,
            fees={"arcus": 0.0, "lighter_robinhood": 0.0},
        )
    )[0].payload["expansion_level_bps"] == pytest.approx(25.0)


def test_confirmed_episode_resolves_without_return_alert():
    lifecycle = ManualOpportunityLifecycle(CONFIG, FEES)
    for offset in range(0, 61, 10):
        lifecycle.evaluate(observation(START + timedelta(seconds=offset)))
    assert lifecycle.evaluate(observation(START + timedelta(seconds=70), spread_bps=9.0)) == []
    assert lifecycle.active_episodes == ()


def test_manual_state_restart_restores_frozen_basis_and_expansion_watermark(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        lifecycle = ManualOpportunityLifecycle(
            ManualOpportunityConfig(),
            {"arcus": 0.0, "lighter_robinhood": 0.0},
            runtime_store=store,
        )
        for offset in range(0, 61, 10):
            lifecycle.evaluate(
                observation(
                    START + timedelta(seconds=offset),
                    spread_bps=12.0,
                    fees={"arcus": 0.0, "lighter_robinhood": 0.0},
                )
            )
        lifecycle.evaluate(
            observation(
                START + timedelta(seconds=70),
                spread_bps=16.0,
                fees={"arcus": 0.0, "lighter_robinhood": 0.0},
            )
        )
        frozen = lifecycle.active_episodes[0].reference_a_bps
        watermark = lifecycle.active_episodes[0].highest_notified_level_bps

    with SQLiteRuntimeStore(database) as store:
        restored = ManualOpportunityLifecycle(
            ManualOpportunityConfig(),
            {"arcus": 0.0, "lighter_robinhood": 0.0},
            runtime_store=store,
        )
        assert restored.active_episodes[0].reference_a_bps == pytest.approx(frozen)
        assert restored.active_episodes[0].highest_notified_level_bps == pytest.approx(
            watermark
        )
        assert restored.evaluate(
            observation(
                START + timedelta(seconds=80),
                spread_bps=21.0,
                fees={"arcus": 0.0, "lighter_robinhood": 0.0},
            )
        )[0].payload["expansion_level_bps"] == pytest.approx(20.0)


def test_persistence_failure_rolls_back_confirmation_and_alert(monkeypatch, tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        lifecycle = ManualOpportunityLifecycle(
            ManualOpportunityConfig(), FEES, runtime_store=store
        )
        for offset in range(0, 60, 10):
            lifecycle.evaluate(observation(START + timedelta(seconds=offset)))

        original = store.set_monitor_state_and_append_opportunities
        failed = True

        def fail_once(*args, **kwargs):
            nonlocal failed
            if failed:
                failed = False
                raise RuntimeError("injected persistence failure")
            return original(*args, **kwargs)

        monkeypatch.setattr(store, "set_monitor_state_and_append_opportunities", fail_once)
        with pytest.raises(RuntimeError, match="injected persistence failure"):
            lifecycle.evaluate(observation(START + timedelta(seconds=60)))
        assert lifecycle.active_episodes[0].confirmed_at is None

        alert = lifecycle.evaluate(observation(START + timedelta(seconds=60)))
        assert len(alert) == 1
        events = store.list_opportunities(monitor_name="manual_opportunity")
        assert len(events) == 1
        assert events[0]["event_id"] == alert[0].event_id
        assert events[0]["event_type"] == "manual_initial"
        assert events[0]["event"] == alert[0].payload


def test_manual_config_is_disabled_and_does_not_change_anomaly_defaults():
    config = RadarConfig()
    assert config.manual_opportunity.enabled is False
    assert config.manual_opportunity.confirmation_seconds == 60
