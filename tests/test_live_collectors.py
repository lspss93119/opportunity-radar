import asyncio
from datetime import datetime, timezone

import pytest

from radar.collectors.base import CollectorBatch
from radar.collectors.arcus import ArcusCollector
from radar.collectors.backpack import BackpackCollector
from radar.collectors.hyperliquid import HyperliquidCollector
from radar.collectors.lighter import LighterCollector
from radar.config import MarketConfig
from radar.market_data import LatestMarketData
from radar.pipeline import MarketDataPipeline
from radar.state import RadarState

UTC = timezone.utc


def configured_markets(venue: str) -> list[MarketConfig]:
    return [
        MarketConfig(venue=venue, venue_symbol=symbol, canonical_symbol=symbol)
        for symbol in ("BTC", "ETH", "SOL")
    ]


def configured_lighter_robinhood_markets() -> list[MarketConfig]:
    return [
        MarketConfig(
            venue="lighter_robinhood",
            venue_symbol=symbol,
            canonical_symbol=symbol,
        )
        for symbol in (
            "BTC",
            "ETH",
            "SOL",
            "SNDK",
            "NVDA",
            "TSLA",
            "GOOGL",
            "AAPL",
            "META",
            "MU",
        )
    ]


def configured_tsla_markets(venue: str, venue_symbol: str) -> list[MarketConfig]:
    return [
        MarketConfig(
            venue=venue,
            venue_symbol=venue_symbol,
            canonical_symbol="TSLA",
        )
    ]


def configured_entropy_markets() -> list[MarketConfig]:
    return [
        MarketConfig(
            venue="entropy",
            venue_symbol="io:SNDK",
            canonical_symbol="SNDK",
        )
    ]


def configured_arcus_markets() -> list[MarketConfig]:
    return [
        MarketConfig(
            venue="arcus",
            venue_symbol=f"{symbol}-USD",
            canonical_symbol=symbol,
        )
        for symbol in ("SNDK", "NVDA", "TSLA", "HOOD", "GOOGL", "AAPL", "META", "MU")
    ]


def configured_backpack_markets() -> list[MarketConfig]:
    return [
        MarketConfig(
            venue="backpack",
            venue_symbol=f"{symbol}.US_USDC_PERP",
            canonical_symbol=symbol,
        )
        for symbol in ("SNDK", "NVDA", "TSLA", "HOOD", "GOOGL", "AAPL", "META", "MU")
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


async def collect_managed_live(
    collector,
    markets: list[MarketConfig],
    latest: LatestMarketData,
    expected_count: int,
):
    pipeline = MarketDataPipeline(
        [collector],
        RadarState(),
        markets=markets,
        latest_market_data=latest,
    )
    await collector.start()
    try:
        for _ in range(100):
            now = datetime.now(UTC)
            probe = await pipeline.collect_once(now=now)
            if len(probe.market_snapshots) == expected_count:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("live cache books were not populated")

        market_batch = await pipeline.collect_once(now=datetime.now(UTC))
        sample_time = market_batch.market_snapshots[0].sample_time
        hourly_batch = await collector.collect_hourly(sample_time=sample_time)
        return CollectorBatch(
            market_snapshots=market_batch.market_snapshots,
            funding_snapshots=hourly_batch.funding_snapshots,
            hourly_contexts=hourly_batch.hourly_contexts,
        )
    finally:
        await collector.stop()


@pytest.mark.live
@pytest.mark.asyncio
async def test_hyperliquid_public_read_only_live_smoke():
    markets = configured_markets("hyperliquid")
    latest = LatestMarketData()
    batch = await collect_managed_live(
        HyperliquidCollector(markets, latest_market_data=latest),
        markets,
        latest,
        len(markets),
    )

    assert_live_market_batch(
        batch, "hyperliquid", batch.market_snapshots[0].sample_time
    )


@pytest.mark.live
@pytest.mark.asyncio
async def test_lighter_public_read_only_live_smoke():
    markets = configured_markets("lighter")
    latest = LatestMarketData()
    batch = await collect_managed_live(
        LighterCollector(markets, latest_market_data=latest),
        markets,
        latest,
        3,
    )

    assert_live_market_batch(batch, "lighter", batch.market_snapshots[0].sample_time)


@pytest.mark.live
@pytest.mark.asyncio
async def test_lighter_robinhood_public_read_only_live_smoke():
    markets = configured_lighter_robinhood_markets()
    latest = LatestMarketData()
    expected_symbols = {
        "BTC",
        "ETH",
        "SOL",
        "SNDK",
        "NVDA",
        "TSLA",
        "GOOGL",
        "AAPL",
        "META",
        "MU",
    }
    batch = await collect_managed_live(
        LighterCollector(
            markets,
            venue="lighter_robinhood",
            base_url="https://api.rh.lighter.xyz",
            latest_market_data=latest,
        ),
        markets,
        latest,
        len(expected_symbols),
    )
    sample_time = batch.market_snapshots[0].sample_time

    assert {snapshot.canonical_symbol for snapshot in batch.market_snapshots} == expected_symbols
    assert {snapshot.venue_symbol for snapshot in batch.market_snapshots} == expected_symbols
    assert all(snapshot.venue == "lighter_robinhood" for snapshot in batch.market_snapshots)
    assert all(snapshot.sample_time == sample_time for snapshot in batch.market_snapshots)
    assert all(snapshot.observed_at >= sample_time for snapshot in batch.market_snapshots)
    assert all(snapshot.best_bid < snapshot.best_ask for snapshot in batch.market_snapshots)
    assert all(
        value is not None
        for snapshot in batch.market_snapshots
        for value in (
            snapshot.buy_1k_vwap,
            snapshot.sell_1k_vwap,
            snapshot.buy_5k_vwap,
            snapshot.sell_5k_vwap,
            snapshot.buy_10k_vwap,
            snapshot.sell_10k_vwap,
        )
    )
    assert {item.canonical_symbol for item in batch.funding_snapshots} == expected_symbols
    assert {context.canonical_symbol for context in batch.hourly_contexts} == expected_symbols


@pytest.mark.live
@pytest.mark.asyncio
async def test_trade_xyz_hip3_public_read_only_live_smoke_matches_lighter_tsla():
    lighter_markets = configured_tsla_markets("lighter", "TSLA")
    lighter_latest = LatestMarketData()
    lighter_batch = await collect_managed_live(
        LighterCollector(lighter_markets, latest_market_data=lighter_latest),
        lighter_markets,
        lighter_latest,
        1,
    )
    hip3_markets = configured_tsla_markets("trade_xyz", "xyz:TSLA")
    hip3_latest = LatestMarketData()
    hip3_batch = await collect_managed_live(
        HyperliquidCollector(
            hip3_markets,
            venue="trade_xyz",
            dex="xyz",
            latest_market_data=hip3_latest,
        ),
        hip3_markets,
        hip3_latest,
        1,
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
    assert lighter.sample_time == hip3.sample_time
    assert lighter.observed_at.tzinfo is not None
    assert hip3.observed_at.tzinfo is not None


@pytest.mark.live
@pytest.mark.asyncio
async def test_entropy_io_sndk_public_read_only_live_smoke():
    markets = configured_entropy_markets()
    latest = LatestMarketData()
    batch = await collect_managed_live(
        HyperliquidCollector(
            markets,
            venue="entropy",
            dex="io",
            latest_market_data=latest,
        ),
        markets,
        latest,
        1,
    )
    sample_time = batch.market_snapshots[0].sample_time

    assert {snapshot.canonical_symbol for snapshot in batch.market_snapshots} == {"SNDK"}
    snapshot = batch.market_snapshots[0]
    assert snapshot.venue == "entropy"
    assert snapshot.venue_symbol == "io:SNDK"
    assert snapshot.sample_time == sample_time
    assert snapshot.best_bid < snapshot.best_ask
    assert all(
        value is not None
        for value in (
            snapshot.buy_1k_vwap,
            snapshot.sell_1k_vwap,
            snapshot.buy_5k_vwap,
            snapshot.sell_5k_vwap,
            snapshot.buy_10k_vwap,
            snapshot.sell_10k_vwap,
        )
    )
    assert {item.canonical_symbol for item in batch.funding_snapshots} == {"SNDK"}
    assert {item.canonical_symbol for item in batch.hourly_contexts} == {"SNDK"}


@pytest.mark.live
@pytest.mark.asyncio
async def test_arcus_exact_equity_universe_public_read_only_live_smoke():
    markets = configured_arcus_markets()
    latest = LatestMarketData()
    expected_symbols = {"SNDK", "NVDA", "TSLA", "HOOD", "GOOGL", "AAPL", "META", "MU"}
    batch = await collect_managed_live(
        ArcusCollector(markets, latest_market_data=latest),
        markets,
        latest,
        len(expected_symbols),
    )
    sample_time = batch.market_snapshots[0].sample_time

    assert {snapshot.canonical_symbol for snapshot in batch.market_snapshots} == expected_symbols
    assert all(snapshot.venue == "arcus" for snapshot in batch.market_snapshots)
    assert all(snapshot.sample_time == sample_time for snapshot in batch.market_snapshots)
    assert all(snapshot.best_bid < snapshot.best_ask for snapshot in batch.market_snapshots)
    assert all(snapshot.observed_at >= sample_time for snapshot in batch.market_snapshots)
    vwap_availability = {
        size: sum(
            getattr(snapshot, f"buy_{size}_vwap") is not None
            and getattr(snapshot, f"sell_{size}_vwap") is not None
            for snapshot in batch.market_snapshots
        )
        for size in ("1k", "5k", "10k")
    }
    assert all(count == len(expected_symbols) for count in vwap_availability.values())
    assert {item.canonical_symbol for item in batch.funding_snapshots} == expected_symbols
    assert {item.canonical_symbol for item in batch.hourly_contexts} == expected_symbols


@pytest.mark.live
@pytest.mark.asyncio
async def test_backpack_exact_equity_universe_public_read_only_live_smoke():
    markets = configured_backpack_markets()
    latest = LatestMarketData()
    expected_symbols = {"SNDK", "NVDA", "TSLA", "HOOD", "GOOGL", "AAPL", "META", "MU"}
    batch = await collect_managed_live(
        BackpackCollector(markets, latest_market_data=latest),
        markets,
        latest,
        len(expected_symbols),
    )
    sample_time = batch.market_snapshots[0].sample_time

    assert {snapshot.canonical_symbol for snapshot in batch.market_snapshots} == expected_symbols
    assert all(snapshot.venue == "backpack" for snapshot in batch.market_snapshots)
    assert all(snapshot.sample_time == sample_time for snapshot in batch.market_snapshots)
    assert all(snapshot.observed_at >= sample_time for snapshot in batch.market_snapshots)
    assert all(snapshot.best_bid < snapshot.best_ask for snapshot in batch.market_snapshots)
    vwap_availability = {
        size: sum(
            getattr(snapshot, f"buy_{size}_vwap") is not None
            and getattr(snapshot, f"sell_{size}_vwap") is not None
            for snapshot in batch.market_snapshots
        )
        for size in ("1k", "5k", "10k")
    }
    assert all(count == len(expected_symbols) for count in vwap_availability.values())
    assert {snapshot.venue_symbol for snapshot in batch.market_snapshots} == {
        f"{symbol}.US_USDC_PERP" for symbol in expected_symbols
    }
    assert {item.canonical_symbol for item in batch.funding_snapshots} == expected_symbols
    assert {item.canonical_symbol for item in batch.hourly_contexts} == expected_symbols
