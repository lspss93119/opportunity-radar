from datetime import datetime, timezone

import pytest

from radar.collectors.hyperliquid import HyperliquidCollector
from radar.collectors.lighter import LighterCollector
from radar.config import MarketConfig
from radar.pipeline import aligned_sample_time

UTC = timezone.utc


def configured_markets(venue: str) -> list[MarketConfig]:
    return [
        MarketConfig(venue=venue, venue_symbol=symbol, canonical_symbol=symbol)
        for symbol in ("BTC", "ETH", "SOL")
    ]


def configured_tsla_markets(venue: str, venue_symbol: str) -> list[MarketConfig]:
    return [
        MarketConfig(
            venue=venue,
            venue_symbol=venue_symbol,
            canonical_symbol="TSLA",
        )
    ]


def assert_live_market_batch(batch, venue: str, sample_time: datetime) -> None:
    assert {snapshot.canonical_symbol for snapshot in batch.market_snapshots} == {
        "BTC",
        "ETH",
        "SOL",
    }
    assert all(snapshot.venue == venue for snapshot in batch.market_snapshots)
    assert all(snapshot.sample_time == sample_time for snapshot in batch.market_snapshots)
    assert all(snapshot.observed_at >= sample_time for snapshot in batch.market_snapshots)
    assert all(snapshot.best_bid < snapshot.best_ask for snapshot in batch.market_snapshots)
    assert all(
        snapshot.buy_1k_vwap is not None and snapshot.sell_1k_vwap is not None
        for snapshot in batch.market_snapshots
    )
    assert {snapshot.canonical_symbol for snapshot in batch.funding_snapshots} == {
        "BTC",
        "ETH",
        "SOL",
    }
    assert {context.canonical_symbol for context in batch.hourly_contexts} == {
        "BTC",
        "ETH",
        "SOL",
    }


@pytest.mark.live
@pytest.mark.asyncio
async def test_hyperliquid_public_read_only_live_smoke():
    now = datetime.now(UTC)
    sample_time = aligned_sample_time(now, 10)
    batch = await HyperliquidCollector(configured_markets("hyperliquid")).collect(
        sample_time=sample_time,
        include_hourly_context=True,
    )

    assert_live_market_batch(batch, "hyperliquid", sample_time)


@pytest.mark.live
@pytest.mark.asyncio
async def test_lighter_public_read_only_live_smoke():
    now = datetime.now(UTC)
    sample_time = aligned_sample_time(now, 10)
    batch = await LighterCollector(configured_markets("lighter")).collect(
        sample_time=sample_time,
        include_hourly_context=True,
    )

    assert_live_market_batch(batch, "lighter", sample_time)


@pytest.mark.live
@pytest.mark.asyncio
async def test_trade_xyz_hip3_public_read_only_live_smoke_matches_lighter_tsla():
    now = datetime.now(UTC)
    sample_time = aligned_sample_time(now, 10)
    lighter_batch = await LighterCollector(
        configured_tsla_markets("lighter", "TSLA")
    ).collect(
        sample_time=sample_time,
        include_hourly_context=True,
    )
    hip3_batch = await HyperliquidCollector(
        configured_tsla_markets("trade_xyz", "xyz:TSLA"),
        venue="trade_xyz",
        dex="xyz",
    ).collect(
        sample_time=sample_time,
        include_hourly_context=True,
    )

    lighter = lighter_batch.market_snapshots[0]
    hip3 = hip3_batch.market_snapshots[0]
    assert lighter.venue == "lighter"
    assert lighter.venue_symbol == "TSLA"
    assert lighter.canonical_symbol == "TSLA"
    assert hip3.venue == "trade_xyz"
    assert hip3.venue_symbol == "xyz:TSLA"
    assert hip3.canonical_symbol == "TSLA"
    assert lighter.buy_1k_vwap is not None
    assert lighter.sell_1k_vwap is not None
    assert hip3.buy_1k_vwap is not None
    assert hip3.sell_1k_vwap is not None
    assert {item.canonical_symbol for item in lighter_batch.funding_snapshots} == {"TSLA"}
    assert {item.canonical_symbol for item in hip3_batch.funding_snapshots} == {"TSLA"}
    assert {item.canonical_symbol for item in lighter_batch.hourly_contexts} == {"TSLA"}
    assert {item.canonical_symbol for item in hip3_batch.hourly_contexts} == {"TSLA"}
    assert lighter.sample_time == hip3.sample_time == sample_time
    assert lighter.observed_at.tzinfo is not None
    assert hip3.observed_at.tzinfo is not None
