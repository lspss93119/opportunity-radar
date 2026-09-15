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
