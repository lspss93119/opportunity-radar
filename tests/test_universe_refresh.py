from datetime import UTC, datetime

import pytest

from radar.app import RadarApplication
from radar.collectors.hyperliquid import HyperliquidCollector
from radar.collectors.lighter import LighterCollector
from radar.config import DiscoveryConfig, MarketConfig, RadarConfig
from radar.discovery import DiscoveryResult
from radar.models import QuotedMarketSnapshot
from radar.monitors.spread.models import SpreadPairKey
from radar.monitors.spread.monitor import SpreadMonitor
from radar.pipeline import MarketDataPipeline
from radar.state import RadarState

NOW = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)


def market(venue: str, symbol: str, *, discovered: bool = False) -> MarketConfig:
    return MarketConfig(
        venue=venue, venue_symbol=symbol, canonical_symbol=symbol, discovered=discovered
    )


class RecordingCollector:
    venue = "lighter"

    def __init__(self) -> None:
        self.received: list[tuple[str, ...]] = []

    async def set_markets(self, markets) -> None:
        self.received.append(tuple(m.venue_symbol for m in markets if m.venue == self.venue))


class FailingCollector:
    venue = "hyperliquid"

    async def set_markets(self, markets) -> None:
        raise RuntimeError("resubscribe failed")


class RecordingQuotedCollector:
    venue = "variational"

    def __init__(self) -> None:
        self.received: list[tuple[str, ...]] = []

    def set_markets(self, markets) -> None:
        self.received.append(tuple(m.venue_symbol for m in markets))


def quote(symbol: str) -> QuotedMarketSnapshot:
    return QuotedMarketSnapshot(
        quote_time=NOW,
        fetched_at=NOW,
        venue="variational",
        venue_symbol=symbol,
        canonical_symbol=symbol,
        mark_price=1.0,
        bid_1k=0.99,
        ask_1k=1.01,
        bid_100k=0.98,
        ask_100k=1.02,
        funding_rate=0.1,
        funding_interval_seconds=3600,
    )


@pytest.mark.asyncio
async def test_pipeline_set_universe_updates_collectors_and_prunes_quotes():
    collector = RecordingCollector()
    errors: list[tuple[str, Exception]] = []
    quoted = RecordingQuotedCollector()
    pipeline = MarketDataPipeline(
        [FailingCollector(), collector],
        RadarState(),
        quoted_market_collector=quoted,  # type: ignore[arg-type]
        collector_error_handler=lambda venue, error: errors.append((venue, error)),
    )
    pipeline._latest_quotes = {"BTC": quote("BTC"), "OLD": quote("OLD")}

    await pipeline.set_universe(
        [market("lighter", "BTC"), market("lighter", "VVV", discovered=True)],
        [market("variational", "BTC")],
    )

    # One collector failing to resubscribe must not block the others.
    assert [venue for venue, _ in errors] == ["hyperliquid"]
    assert collector.received == [("BTC", "VVV")]
    assert quoted.received == [("BTC",)]
    assert set(pipeline._latest_quotes) == {"BTC"}
    assert [m.venue_symbol for m in pipeline._markets] == ["BTC", "VVV"]


@pytest.mark.asyncio
async def test_hyperliquid_rebuilds_feed_only_when_coins_change():
    collector = HyperliquidCollector([market("hyperliquid", "BTC")])
    original_feed = collector._order_book_feed

    await collector.set_markets([market("hyperliquid", "BTC")])
    assert collector._order_book_feed is original_feed

    await collector.set_markets([market("hyperliquid", "BTC"), market("hyperliquid", "ZRO")])
    assert collector._order_book_feed is not original_feed
    assert collector._order_book_feed.coins == ("BTC", "ZRO")


@pytest.mark.asyncio
async def test_lighter_set_markets_replaces_venue_markets_before_start():
    collector = LighterCollector([market("lighter", "BTC")])

    await collector.set_markets(
        [market("lighter", "BTC"), market("lighter", "VVV"), market("hyperliquid", "ETH")]
    )

    assert [m.venue_symbol for m in collector._markets] == ["BTC", "VVV"]


def test_spread_monitor_drops_basis_for_feeds_leaving_the_universe():
    monitor = SpreadMonitor(RadarConfig().monitors.spread, {})
    kept = SpreadPairKey("BTC", "lighter", "BTC", "variational", "BTC")
    removed = SpreadPairKey("ZZZ", "lighter", "ZZZ", "variational", "ZZZ")
    monitor._basis_by_key = {kept: monitor._new_basis(), removed: monitor._new_basis()}

    monitor.set_market_identities(
        {("lighter", "BTC", "BTC"), ("variational", "BTC", "BTC")}
    )

    assert set(monitor._basis_by_key) == {kept}


class FakePipeline:
    def __init__(self) -> None:
        self.universes: list[tuple[int, int]] = []

    async def set_universe(self, markets, quoted_markets) -> None:
        self.universes.append((len(markets), len(quoted_markets)))


class FakeRunner:
    def __init__(self, monitor) -> None:
        self.monitors = (monitor,)


class FakeMonitor:
    def __init__(self) -> None:
        self.identities: list[set] = []

    def set_market_identities(self, identities) -> None:
        self.identities.append(set(identities))


def make_app(discover) -> tuple[RadarApplication, FakePipeline, FakeMonitor]:
    pipeline = FakePipeline()
    monitor = FakeMonitor()
    app = RadarApplication(
        pipeline=pipeline,  # type: ignore[arg-type]
        monitor_runner=FakeRunner(monitor),  # type: ignore[arg-type]
        alert_worker=object(),  # type: ignore[arg-type]
        storage=object(),  # type: ignore[arg-type]
        runtime_store=object(),  # type: ignore[arg-type]
        processor=object(),  # type: ignore[arg-type]
        config=RadarConfig(
            markets=[market("lighter", "BTC")],
            discovery=DiscoveryConfig(enabled=True),
        ),
        discover=discover,
    )
    return app, pipeline, monitor


@pytest.mark.asyncio
async def test_refresh_universe_applies_discovered_feeds_once():
    result = DiscoveryResult(
        markets=(market("hyperliquid", "BTC", discovered=True),),
        quoted_markets=(market("variational", "BTC", discovered=True),),
        exclusions=(),
    )

    async def discover(config):
        return result

    app, pipeline, monitor = make_app(discover)

    assert await app.refresh_universe() is True
    assert await app.refresh_universe() is True  # unchanged: nothing re-applied

    assert pipeline.universes == [(2, 1)]
    assert monitor.identities == [
        {
            ("lighter", "BTC", "BTC"),
            ("hyperliquid", "BTC", "BTC"),
            ("variational", "BTC", "BTC"),
        }
    ]
    assert len(app.config.markets) == 2


@pytest.mark.asyncio
async def test_refresh_universe_failure_keeps_last_known_good_universe():
    async def discover(config):
        raise TimeoutError("venue down")

    app, pipeline, monitor = make_app(discover)
    before = app.config

    assert await app.refresh_universe() is False
    assert app.config is before
    assert pipeline.universes == []
    assert monitor.identities == []
