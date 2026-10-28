from datetime import UTC, datetime

import pytest

from radar.collectors.base import CollectorBatch
from radar.config import MarketConfig, RadarConfig
from radar.models import HourlyContext, MarketSnapshot
from radar.monitors.manual_opportunity import ManualOpportunityMonitor
from radar.monitors.spread.factory import create_spread_monitor
from radar.state import RadarState


NOW = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)


def _snapshot(venue: str, symbol: str) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=NOW,
        observed_at=NOW,
        venue=venue,
        venue_symbol=symbol,
        canonical_symbol="BTC",
        best_bid=99.0,
        best_bid_size=1.0,
        best_ask=100.0,
        best_ask_size=1.0,
        buy_10k_vwap=100.0,
        sell_10k_vwap=101.0,
    )


def _context(venue: str, symbol: str) -> HourlyContext:
    return HourlyContext(
        sample_time=NOW,
        observed_at=NOW,
        venue=venue,
        venue_symbol=symbol,
        canonical_symbol="BTC",
        volume_24h=2_000_000.0,
    )


def _config(*, manual_enabled: bool = False) -> RadarConfig:
    return RadarConfig(
        fees_bps={"lighter": 0.0, "ondo": 2.5},
        markets=[
            MarketConfig(
                venue="lighter",
                venue_symbol="BTC",
                canonical_symbol="BTC",
            ),
            MarketConfig(
                venue="ondo",
                venue_symbol="BTC-USD.P",
                canonical_symbol="BTC",
                monitor_enabled=False,
            ),
        ],
        manual_opportunity={"enabled": manual_enabled},
    )


def _state() -> RadarState:
    state = RadarState()
    state.apply(
        CollectorBatch(
            market_snapshots=(
                _snapshot("lighter", "BTC"),
                _snapshot("ondo", "BTC-USD.P"),
            ),
            hourly_contexts=(
                _context("lighter", "BTC"),
                _context("ondo", "BTC-USD.P"),
            ),
        ),
        replace_context=True,
    )
    return state


def test_config_keeps_ondo_collectable_but_out_of_monitor_scope():
    config = _config()

    assert [market.venue for market in config.markets if market.enabled] == [
        "lighter",
        "ondo",
    ]
    assert config.monitor_market_identities() == {
        ("lighter", "BTC", "BTC"),
    }


@pytest.mark.asyncio
async def test_spread_monitor_does_not_build_ondo_route():
    monitor = create_spread_monitor(_config())

    assert await monitor.evaluate(NOW, _state()) == []
    assert monitor.active_episodes == ()
    assert monitor._basis_by_key == {}


@pytest.mark.asyncio
async def test_manual_monitor_does_not_build_ondo_route():
    config = _config(manual_enabled=True)
    monitor = ManualOpportunityMonitor(
        config.manual_opportunity,
        config.fees_bps,
        market_identities=config.monitor_market_identities(),
    )

    assert await monitor.evaluate(NOW, _state()) == []
    assert monitor._histories == {}
    assert monitor.active_episodes == ()
