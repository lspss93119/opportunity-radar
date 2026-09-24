import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from radar.collectors.base import Collector, CollectorBatch, CollectorLike, ManagedCollector
from radar.collectors.arcus import ArcusCollector
from radar.collectors.backpack import BackpackCollector
from radar.collectors.hyperliquid import HyperliquidCollector
from radar.collectors.lighter import LighterCollector
from radar.collectors.variational import VariationalCollector
from radar.config import MarketConfig, RadarConfig
from radar.market_data import LatestMarketData
from radar.models import HourlyContext, MarketSnapshot
from radar.models import QuotedMarketSnapshot
from radar.pipeline import (
    MarketDataPipeline,
    aligned_sample_time,
    hourly_sample_time,
)
from radar.state import RadarState
from radar.storage.parquet import ParquetStorage
from radar.vwap import BookLevel

UTC = timezone.utc
NOW = datetime(2026, 9, 15, 10, 0, 19, 876000, tzinfo=UTC)

ALL_SEVEN_MARKETS = (
    MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
    MarketConfig(
        venue="lighter_robinhood", venue_symbol="BTC", canonical_symbol="BTC"
    ),
    MarketConfig(
        venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
    ),
    MarketConfig(
        venue="trade_xyz", venue_symbol="xyz:TSLA", canonical_symbol="TSLA"
    ),
    MarketConfig(
        venue="entropy", venue_symbol="io:SNDK", canonical_symbol="SNDK"
    ),
    MarketConfig(
        venue="backpack",
        venue_symbol="SNDK.US_USDC_PERP",
        canonical_symbol="SNDK",
    ),
    MarketConfig(
        venue="arcus", venue_symbol="SNDK-USD", canonical_symbol="SNDK"
    ),
)


def all_seven_config(*, stale_after_seconds: int = 30) -> RadarConfig:
    return RadarConfig(
        sampling_seconds=10,
        fees_bps={
            "lighter_robinhood": 0.0,
            "trade_xyz": 9.0,
            "entropy": 9.0,
            "backpack": 5.0,
            "arcus": 2.25,
        },
        markets=list(ALL_SEVEN_MARKETS),
        monitors={"spread": {"stale_after_seconds": stale_after_seconds}},
    )


def pipeline_latest_market_data(pipeline: MarketDataPipeline) -> LatestMarketData:
    latest = getattr(pipeline.collectors[0], "_latest_market_data", None)
    assert isinstance(latest, LatestMarketData)
    return latest


def seed_pipeline_book(
    latest: LatestMarketData,
    market: MarketConfig,
    *,
    observed_at: datetime = NOW,
    base_size: float = 200.0,
) -> None:
    latest.update_book(
        venue=market.venue,
        venue_symbol=market.venue_symbol,
        bids=(BookLevel(price=99.0, base_size=base_size),),
        asks=(BookLevel(price=101.0, base_size=base_size),),
        observed_at=observed_at,
    )


def make_market(venue: str, observed_at: datetime, price: float) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=datetime(2026, 9, 15, 10, 0, 10, tzinfo=UTC),
        observed_at=observed_at,
        venue=venue,
        venue_symbol="BTC",
        canonical_symbol="BTC",
        best_bid=price - 1,
        best_bid_size=1,
        best_ask=price,
        best_ask_size=1,
    )


def test_aligned_sample_time_floors_to_ten_second_utc_boundary():
    assert aligned_sample_time(NOW, 10) == datetime(
        2026, 9, 15, 10, 0, 10, tzinfo=UTC
    )


def test_pipeline_rejects_non_ten_second_sampling():
    with pytest.raises(ValueError, match="10-second"):
        MarketDataPipeline([], RadarState(), sampling_seconds=5)


class FailingCollector:
    venue = "lighter"

    async def collect(self, *, sample_time: datetime, include_hourly_context: bool) -> CollectorBatch:
        raise RuntimeError("venue unavailable")


class SuccessfulCollector:
    venue = "hyperliquid"

    async def collect(self, *, sample_time: datetime, include_hourly_context: bool) -> CollectorBatch:
        return CollectorBatch(
            market_snapshots=(make_market("hyperliquid", NOW, 200),),
        )


class CadenceCollector:
    venue = "hyperliquid"

    def __init__(self):
        self.include_hourly_context_values: list[bool] = []

    async def collect(self, *, sample_time: datetime, include_hourly_context: bool) -> CollectorBatch:
        self.include_hourly_context_values.append(include_hourly_context)
        return CollectorBatch()


class LifecycleCollector(CadenceCollector):
    def __init__(self):
        super().__init__()
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class ManagedOnlyCollector:
    venue = "hyperliquid"

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def collect_hourly(self, *, sample_time: datetime) -> CollectorBatch:
        return CollectorBatch()


class BlockingHourlyCollector:
    venue = "lighter"

    def __init__(self) -> None:
        self.hourly_started = asyncio.Event()
        self.hourly_cancelled = asyncio.Event()
        self.stopped = False

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        assert self.hourly_cancelled.is_set()
        self.stopped = True

    async def collect_hourly(self, *, sample_time: datetime) -> CollectorBatch:
        self.hourly_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.hourly_cancelled.set()
            raise
        return CollectorBatch()


class HourlyCollectorStub:
    def __init__(self, venue: str, delay: float) -> None:
        self.venue = venue
        self.delay = delay

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def collect_hourly(self, *, sample_time: datetime) -> CollectorBatch:
        await asyncio.sleep(self.delay)
        return CollectorBatch(
            hourly_contexts=(
                HourlyContext(
                    sample_time=sample_time.replace(minute=0, second=0, microsecond=0),
                    observed_at=sample_time,
                    venue=self.venue,
                    venue_symbol="BTC",
                    canonical_symbol="BTC",
                    open_interest=1.0,
                    volume_24h=2.0,
                ),
            )
        )


class RecordingBatchStorage:
    def __init__(self) -> None:
        self.batches: list[CollectorBatch] = []

    def append(self, batch: CollectorBatch) -> None:
        self.batches.append(batch)


class NetworkOnlyCollector:
    venue = "lighter"

    async def collect(
        self, *, sample_time: datetime, include_hourly_context: bool
    ) -> CollectorBatch:
        raise AssertionError("cache-only sampling must not call collectors")


class BlockingQuotedCollector:
    def __init__(self, snapshot: QuotedMarketSnapshot) -> None:
        self.snapshot = snapshot
        self.poll_started = asyncio.Event()
        self.poll_cancelled = asyncio.Event()

    async def poll_once(self) -> tuple[QuotedMarketSnapshot, ...]:
        self.poll_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.poll_cancelled.set()
            raise
        return (self.snapshot,)


def make_quoted_snapshot() -> QuotedMarketSnapshot:
    return QuotedMarketSnapshot(
        quote_time=NOW,
        fetched_at=NOW,
        venue="variational",
        venue_symbol="AAPL",
        canonical_symbol="AAPL",
        mark_price=100.0,
        bid_1k=99.0,
        ask_1k=101.0,
        bid_100k=98.0,
        ask_100k=102.0,
        funding_rate=0.01,
        funding_interval_seconds=28_800,
        volume_24h=1.0,
        long_open_interest=2.0,
        short_open_interest=3.0,
    )


@pytest.mark.asyncio
async def test_pipeline_starts_and_stops_collectors_with_optional_lifecycle_hooks():
    collector = LifecycleCollector()
    pipeline = MarketDataPipeline([collector], RadarState(), sampling_seconds=10)

    await pipeline.start()
    await pipeline.stop()

    assert collector.started
    assert collector.stopped


@pytest.mark.asyncio
async def test_pipeline_stop_cancels_hourly_before_stopping_collectors():
    collector = BlockingHourlyCollector()
    pipeline = MarketDataPipeline(
        [collector],
        RadarState(),
        clock=lambda: datetime(2026, 9, 15, 10, 1, tzinfo=UTC),
    )

    await pipeline.start()
    await asyncio.wait_for(collector.hourly_started.wait(), timeout=0.2)
    await pipeline.stop()

    assert collector.hourly_cancelled.is_set()
    assert collector.stopped



def test_collector_protocol_is_structural():
    assert isinstance(SuccessfulCollector(), Collector)


def test_managed_collector_protocol_is_structural_and_distinct_from_legacy():
    collector = ManagedOnlyCollector()

    assert isinstance(collector, ManagedCollector)
    assert isinstance(collector, CollectorLike)


def test_context_application_preserves_the_authoritative_market_mapping():
    state = RadarState()
    market = make_market("lighter", NOW, 100)
    context = HourlyContext(
        sample_time=datetime(2026, 9, 15, 10, 0, tzinfo=UTC),
        observed_at=NOW,
        venue="lighter",
        venue_symbol="BTC",
        canonical_symbol="BTC",
        open_interest=1.0,
        volume_24h=2.0,
    )

    state.apply_market_batch(CollectorBatch(market_snapshots=(market,)))
    state.apply_context_batch(CollectorBatch(hourly_contexts=(context,)))

    assert state.get_market("lighter", "BTC") == market
    assert state.hourly_context == (context,)


@pytest.mark.asyncio
async def test_hourly_coordinator_merges_before_single_apply_and_append():
    first = HourlyCollectorStub("lighter", delay=0.02)
    second = HourlyCollectorStub("hyperliquid", delay=0.0)
    state = RadarState()
    configured_markets = (
        MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
        MarketConfig(
            venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
        ),
    )
    market = make_market("lighter", NOW, 100)
    state.apply_market_batch(CollectorBatch(market_snapshots=(market,)))
    storage = RecordingBatchStorage()
    pipeline = MarketDataPipeline(
        [first, second],
        state,
        markets=configured_markets,
        latest_market_data=LatestMarketData(),
        storage=storage,  # type: ignore[arg-type]
    )

    batch = await pipeline.collect_hourly_once(
        now=datetime(2026, 9, 15, 10, 1, tzinfo=UTC)
    )

    assert batch is not None
    assert {context.venue for context in batch.hourly_contexts} == {
        "lighter",
        "hyperliquid",
    }
    assert {context.venue for context in state.hourly_context} == {
        "lighter",
        "hyperliquid",
    }
    assert state.get_market("lighter", "BTC") == market
    assert storage.batches == [batch]


@pytest.mark.asyncio
async def test_collect_once_reads_latest_cache_without_invoking_collectors():
    latest = LatestMarketData()
    latest.update_book(
        venue="lighter",
        venue_symbol="BTC",
        bids=(BookLevel(price=99.0, base_size=200.0),),
        asks=(BookLevel(price=101.0, base_size=200.0),),
        observed_at=NOW,
    )
    state = RadarState()
    pipeline = MarketDataPipeline(
        [NetworkOnlyCollector()],
        state,
        markets=(
            MarketConfig(
                venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"
            ),
        ),
        latest_market_data=latest,
    )

    batch = await pipeline.collect_once(now=NOW)

    assert len(batch.market_snapshots) == 1
    assert batch.market_snapshots[0].observed_at == NOW
    assert state.get_market("lighter", "BTC") == batch.market_snapshots[0]


@pytest.mark.asyncio
async def test_variational_background_task_does_not_enter_market_state_and_stops_cleanly():
    collector = BlockingQuotedCollector(make_quoted_snapshot())
    pipeline = MarketDataPipeline(
        [],
        RadarState(),
        quoted_market_collector=collector,  # type: ignore[arg-type]
        quoted_market_poll_interval_seconds=0.01,
    )

    await pipeline.start()
    await asyncio.wait_for(collector.poll_started.wait(), timeout=0.2)
    batch = await pipeline.collect_once(now=NOW)
    assert batch.market_snapshots == ()
    assert pipeline.state.markets == ()

    await pipeline.stop()
    assert collector.poll_cancelled.is_set()


@pytest.mark.asyncio
async def test_arcus_stale_cache_is_omitted_by_cache_only_sampler():
    latest = LatestMarketData()
    latest.update_book(
        venue="arcus",
        venue_symbol="SNDK-USD",
        bids=(BookLevel(price=1876.19, base_size=100.0),),
        asks=(BookLevel(price=1876.2, base_size=100.0),),
        observed_at=datetime(2026, 9, 15, 9, 59, 39, tzinfo=UTC),
    )
    market = MarketConfig(
        venue="arcus", venue_symbol="SNDK-USD", canonical_symbol="SNDK"
    )
    pipeline = MarketDataPipeline(
        [],
        RadarState(),
        markets=[market],
        latest_market_data=latest,
    )

    batch = await pipeline.collect_once(
        now=datetime(2026, 9, 15, 10, 0, 10, tzinfo=UTC)
    )

    assert batch.market_snapshots == ()


@pytest.mark.asyncio
async def test_wait_for_market_feeds_polls_only_latest_cache():
    latest = LatestMarketData()
    pipeline = MarketDataPipeline(
        [NetworkOnlyCollector()],
        RadarState(),
        markets=(
            MarketConfig(
                venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"
            ),
        ),
        latest_market_data=latest,
        clock=lambda: NOW,
    )

    waiting = asyncio.create_task(
        pipeline.wait_for_market_feeds(
            "BTC", required_venues=1, timeout_seconds=0.2
        )
    )
    await asyncio.sleep(0)
    latest.update_book(
        venue="lighter",
        venue_symbol="BTC",
        bids=(BookLevel(price=99.0, base_size=200.0),),
        asks=(BookLevel(price=101.0, base_size=200.0),),
        observed_at=NOW,
    )

    await waiting


@pytest.mark.asyncio
async def test_wait_for_market_feeds_times_out_without_ready_cache_data():
    pipeline = MarketDataPipeline(
        [],
        RadarState(),
        markets=(
            MarketConfig(
                venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"
            ),
        ),
        clock=lambda: NOW,
    )

    with pytest.raises(TimeoutError, match="ready market feeds"):
        await pipeline.wait_for_market_feeds(
            "BTC", required_venues=1, timeout_seconds=0.01
        )


def test_pipeline_from_config_builds_the_two_configured_public_collectors():
    config = RadarConfig(
        sampling_seconds=10,
        markets=[
            MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
            MarketConfig(
                venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
            ),
        ],
    )

    pipeline = MarketDataPipeline.from_config(config)

    assert pipeline.sampling_seconds == 10
    assert [type(collector) for collector in pipeline.collectors] == [
        LighterCollector,
        HyperliquidCollector,
    ]
    assert isinstance(pipeline.collectors[0], ManagedCollector)


def test_pipeline_from_config_builds_independent_lighter_robinhood_collector():
    config = RadarConfig(
        sampling_seconds=10,
        fees_bps={"lighter_robinhood": 0.0},
        markets=[
            MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
            MarketConfig(
                venue="lighter_robinhood",
                venue_symbol="BTC",
                canonical_symbol="BTC",
            ),
            MarketConfig(
                venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
            ),
        ],
    )

    pipeline = MarketDataPipeline.from_config(config)

    assert [collector.venue for collector in pipeline.collectors] == [
        "lighter",
        "lighter_robinhood",
        "hyperliquid",
    ]
    assert isinstance(pipeline.collectors[1], LighterCollector)
    assert isinstance(pipeline.collectors[0], ManagedCollector)
    assert isinstance(pipeline.collectors[1], ManagedCollector)
    assert pipeline.collectors[1].base_url == "https://api.rh.lighter.xyz"


def test_pipeline_from_config_builds_trade_xyz_hyperliquid_collector_when_enabled():
    config = RadarConfig(
        sampling_seconds=10,
        fees_bps={"trade_xyz": 9.0},
        markets=[
            MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
            MarketConfig(
                venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
            ),
            MarketConfig(
                venue="trade_xyz",
                venue_symbol="xyz:TSLA",
                canonical_symbol="TSLA",
            ),
        ],
    )

    pipeline = MarketDataPipeline.from_config(config)

    assert [collector.venue for collector in pipeline.collectors] == [
        "lighter",
        "hyperliquid",
        "trade_xyz",
    ]
    assert isinstance(pipeline.collectors[2], HyperliquidCollector)
    assert pipeline.collectors[2].dex == "xyz"


def test_pipeline_from_config_builds_entropy_and_arcus_collectors_when_enabled():
    config = RadarConfig(
        sampling_seconds=10,
        fees_bps={"trade_xyz": 9.0, "entropy": 9.0, "arcus": 2.25},
        markets=[
            MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
            MarketConfig(
                venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
            ),
            MarketConfig(
                venue="trade_xyz",
                venue_symbol="xyz:TSLA",
                canonical_symbol="TSLA",
            ),
            MarketConfig(
                venue="entropy",
                venue_symbol="io:SNDK",
                canonical_symbol="SNDK",
            ),
            MarketConfig(
                venue="arcus",
                venue_symbol="SNDK-USD",
                canonical_symbol="SNDK",
            ),
        ],
    )

    pipeline = MarketDataPipeline.from_config(config)

    assert [collector.venue for collector in pipeline.collectors] == [
        "lighter",
        "hyperliquid",
        "trade_xyz",
        "entropy",
        "arcus",
    ]
    assert isinstance(pipeline.collectors[3], HyperliquidCollector)
    assert pipeline.collectors[3].dex == "io"
    assert isinstance(pipeline.collectors[4], ArcusCollector)


def test_pipeline_from_config_builds_backpack_collector_when_enabled():
    config = RadarConfig(
        sampling_seconds=10,
        fees_bps={"backpack": 5.0},
        markets=[
            MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
            MarketConfig(
                venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
            ),
            MarketConfig(
                venue="backpack",
                venue_symbol="SNDK.US_USDC_PERP",
                canonical_symbol="SNDK",
            ),
        ],
    )

    pipeline = MarketDataPipeline.from_config(config)

    assert [collector.venue for collector in pipeline.collectors] == [
        "lighter",
        "hyperliquid",
        "backpack",
    ]
    assert isinstance(pipeline.collectors[2], BackpackCollector)


def test_pipeline_from_config_keeps_variational_quotes_out_of_market_collectors():
    config = RadarConfig(
        quoted_markets=[
            MarketConfig(
                venue="variational", venue_symbol="AAPL", canonical_symbol="AAPL"
            ),
            MarketConfig(
                venue="variational", venue_symbol="US500", canonical_symbol="SPY"
            ),
        ]
    )

    pipeline = MarketDataPipeline.from_config(config)

    assert all(collector.venue != "variational" for collector in pipeline.collectors)
    assert isinstance(pipeline._quoted_market_collector, VariationalCollector)
    assert pipeline._quoted_market_collector._markets == tuple(config.quoted_markets)


def test_pipeline_from_config_injects_one_cache_and_preserves_all_feed_identity():
    pipeline = MarketDataPipeline.from_config(
        all_seven_config(stale_after_seconds=7)
    )

    latest = pipeline_latest_market_data(pipeline)
    assert pipeline._latest_market_data is latest
    assert pipeline._stale_after_seconds == 7
    assert [collector.venue for collector in pipeline.collectors] == [
        "lighter",
        "lighter_robinhood",
        "hyperliquid",
        "trade_xyz",
        "entropy",
        "arcus",
        "backpack",
    ]
    assert all(
        getattr(collector, "_latest_market_data", None) is latest
        for collector in pipeline.collectors
    )
    configured_identity = [
        (market.venue, market.venue_symbol)
        for collector in pipeline.collectors
        for market in getattr(collector, "_markets")
    ]
    assert set(configured_identity) == {
        (market.venue, market.venue_symbol) for market in ALL_SEVEN_MARKETS
    }
    assert len(configured_identity) == len(ALL_SEVEN_MARKETS)


@pytest.mark.asyncio
async def test_all_seven_venue_pipeline_samples_only_ready_fresh_shared_books():
    pipeline = MarketDataPipeline.from_config(all_seven_config())
    latest = pipeline_latest_market_data(pipeline)
    ready_markets = (
        ALL_SEVEN_MARKETS[0],
        ALL_SEVEN_MARKETS[2],
        ALL_SEVEN_MARKETS[5],
        ALL_SEVEN_MARKETS[6],
    )
    for market in ready_markets:
        seed_pipeline_book(latest, market)

    batch = await pipeline.collect_once(now=NOW)

    expected = {
        (market.venue, market.venue_symbol) for market in ready_markets
    }
    observed = {
        (snapshot.venue, snapshot.venue_symbol)
        for snapshot in batch.market_snapshots
    }
    assert observed == expected
    assert {
        (snapshot.venue, snapshot.venue_symbol)
        for snapshot in pipeline.state.markets
    } == expected
    assert all(
        snapshot.sample_time == datetime(2026, 9, 15, 10, 0, 10, tzinfo=UTC)
        for snapshot in batch.market_snapshots
    )


@pytest.mark.parametrize(
    "failure",
    [
        "stale",
        "future",
        "missing_10k_depth",
        "mismatched_hyperliquid_coin",
        "backpack_gap",
        "lighter_reconnect",
        "arcus_timeout",
        "metadata_failure",
    ],
)
@pytest.mark.asyncio
async def test_all_seven_venue_pipeline_fails_closed_without_synthetic_samples(
    failure: str,
):
    pipeline = MarketDataPipeline.from_config(all_seven_config())
    latest = pipeline_latest_market_data(pipeline)
    target_by_failure = {
        "stale": ALL_SEVEN_MARKETS[0],
        "future": ALL_SEVEN_MARKETS[1],
        "missing_10k_depth": ALL_SEVEN_MARKETS[2],
        "mismatched_hyperliquid_coin": ALL_SEVEN_MARKETS[2],
        "backpack_gap": ALL_SEVEN_MARKETS[5],
        "lighter_reconnect": ALL_SEVEN_MARKETS[0],
        "arcus_timeout": ALL_SEVEN_MARKETS[6],
        "metadata_failure": ALL_SEVEN_MARKETS[4],
    }
    target = target_by_failure[failure]
    for market in ALL_SEVEN_MARKETS:
        seed_pipeline_book(latest, market)

    all_identities = {
        (market.venue, market.venue_symbol) for market in ALL_SEVEN_MARKETS
    }
    before_failure = await pipeline.collect_once(now=NOW)
    assert {
        (snapshot.venue, snapshot.venue_symbol)
        for snapshot in before_failure.market_snapshots
    } == all_identities

    if failure == "stale":
        seed_pipeline_book(
            latest,
            target,
            observed_at=NOW - timedelta(seconds=31),
        )
    elif failure == "future":
        seed_pipeline_book(
            latest,
            target,
            observed_at=NOW + timedelta(seconds=1),
        )
    elif failure == "missing_10k_depth":
        seed_pipeline_book(latest, target, base_size=0.1)
    elif failure == "mismatched_hyperliquid_coin":
        latest.update_book(
            venue="hyperliquid",
            venue_symbol="BTC-UNCONFIGURED",
            bids=(BookLevel(price=99.0, base_size=200.0),),
            asks=(BookLevel(price=101.0, base_size=200.0),),
            observed_at=NOW,
        )
        latest.invalidate(
            venue=target.venue,
            venue_symbol=target.venue_symbol,
        )
    elif failure in {"backpack_gap", "lighter_reconnect", "arcus_timeout"}:
        latest.invalidate(venue=target.venue, venue_symbol=target.venue_symbol)
    elif failure == "metadata_failure":
        latest.invalidate(venue=target.venue, venue_symbol=target.venue_symbol)
        latest.update_metadata(
            venue=target.venue,
            venue_symbol=target.venue_symbol,
            mark_price=101.0,
            index_price=100.0,
        )

    batch = await pipeline.collect_once(now=NOW)

    expected = {
        (market.venue, market.venue_symbol)
        for market in ALL_SEVEN_MARKETS
        if market != target
    }
    observed = {
        (snapshot.venue, snapshot.venue_symbol)
        for snapshot in batch.market_snapshots
    }
    assert observed == expected
    assert {
        (snapshot.venue, snapshot.venue_symbol)
        for snapshot in pipeline.state.markets
    } == expected
    target_view = latest._views[(target.venue, target.venue_symbol)]
    if failure in {
        "mismatched_hyperliquid_coin",
        "backpack_gap",
        "lighter_reconnect",
        "arcus_timeout",
        "metadata_failure",
    }:
        assert not target_view.ready


@pytest.mark.asyncio
async def test_empty_market_batch_clears_old_market_snapshot_in_state():
    latest = LatestMarketData()
    state = RadarState()
    old_lighter = make_market("lighter", datetime(2026, 9, 15, 9, 59, tzinfo=UTC), 100)
    state.apply_market_batch(CollectorBatch(market_snapshots=(old_lighter,)))

    pipeline = MarketDataPipeline(
        [],
        state,
        markets=(
            MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
        ),
        latest_market_data=latest,
        clock=lambda: NOW,
    )
    batch = await pipeline.collect_once(now=NOW)

    assert batch.market_snapshots == ()
    assert state.get_market("lighter", "BTC") is None


@pytest.mark.asyncio
async def test_failed_hourly_collector_is_reported_while_other_batch_remains_usable():
    failures: list[tuple[str, Exception]] = []
    state = RadarState()
    pipeline = MarketDataPipeline(
        [FailingCollector(), HourlyCollectorStub("hyperliquid", delay=0.0)],
        state,
        sampling_seconds=10,
        collector_error_handler=lambda venue, error: failures.append((venue, error)),
    )

    batch = await pipeline.collect_hourly_once(
        now=datetime(2026, 9, 15, 10, 1, tzinfo=UTC)
    )

    assert batch is not None
    assert failures[0][0] == "lighter"
    assert str(failures[0][1]) == "venue unavailable"
    assert [context.venue for context in batch.hourly_contexts] == ["hyperliquid"]
    assert [context.venue for context in state.hourly_context] == ["hyperliquid"]


@pytest.mark.asyncio
async def test_hourly_error_handler_failure_does_not_stop_successful_batch():
    state = RadarState()

    def broken_handler(venue: str, error: Exception) -> None:
        raise RuntimeError("logging failed")

    pipeline = MarketDataPipeline(
        [FailingCollector(), HourlyCollectorStub("hyperliquid", delay=0.0)],
        state,
        sampling_seconds=10,
        collector_error_handler=broken_handler,
    )

    batch = await pipeline.collect_hourly_once(
        now=datetime(2026, 9, 15, 10, 1, tzinfo=UTC)
    )

    assert batch is not None
    assert [context.venue for context in batch.hourly_contexts] == ["hyperliquid"]
    assert [context.venue for context in state.hourly_context] == ["hyperliquid"]


@pytest.mark.asyncio
async def test_pipeline_hands_collected_batch_to_optional_storage(tmp_path):
    latest = LatestMarketData()
    latest.update_book(
        venue="hyperliquid",
        venue_symbol="BTC",
        bids=(BookLevel(price=199.0, base_size=200.0),),
        asks=(BookLevel(price=200.0, base_size=200.0),),
        observed_at=NOW,
    )
    storage = ParquetStorage(tmp_path / "data")
    pipeline = MarketDataPipeline(
        [],
        RadarState(),
        markets=(
            MarketConfig(
                venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
            ),
        ),
        latest_market_data=latest,
        sampling_seconds=10,
        clock=lambda: NOW,
        storage=storage,
    )

    await pipeline.collect_once(now=NOW)

    assert storage.pending_count == 1
    assert pipeline.flush_storage(now=NOW) == 1
    assert storage.pending_count == 0


@pytest.mark.asyncio
async def test_pipeline_delays_hourly_context_until_after_grace_and_repeats_per_hour():
    collector = CadenceCollector()
    pipeline = MarketDataPipeline([collector], RadarState(), sampling_seconds=10)

    await pipeline.collect_hourly_once(
        now=datetime(2026, 9, 15, 10, 0, 0, tzinfo=UTC)
    )
    await pipeline.collect_hourly_once(
        now=datetime(2026, 9, 15, 10, 0, 50, tzinfo=UTC)
    )
    await pipeline.collect_hourly_once(
        now=datetime(2026, 9, 15, 10, 1, 0, tzinfo=UTC)
    )
    await pipeline.collect_hourly_once(
        now=datetime(2026, 9, 15, 10, 1, 10, tzinfo=UTC)
    )
    await pipeline.collect_hourly_once(
        now=datetime(2026, 9, 15, 11, 1, 0, tzinfo=UTC)
    )

    assert collector.include_hourly_context_values == [True, True]


@pytest.mark.asyncio
async def test_pipeline_collects_hourly_context_immediately_after_mid_hour_start():
    collector = CadenceCollector()
    pipeline = MarketDataPipeline([collector], RadarState(), sampling_seconds=10)

    await pipeline.collect_hourly_once(
        now=datetime(2026, 9, 15, 10, 37, tzinfo=UTC)
    )

    assert collector.include_hourly_context_values == [True]


class HourlyContextCollector(CadenceCollector):
    async def collect(
        self, *, sample_time: datetime, include_hourly_context: bool
    ) -> CollectorBatch:
        self.include_hourly_context_values.append(include_hourly_context)
        if not include_hourly_context:
            return CollectorBatch()
        return CollectorBatch(
            hourly_contexts=(
                HourlyContext(
                    sample_time=hourly_sample_time(sample_time),
                    observed_at=sample_time,
                    venue=self.venue,
                    venue_symbol="BTC",
                    canonical_symbol="BTC",
                    open_interest=1.0,
                    volume_24h=2.0,
                ),
            )
        )


@pytest.mark.asyncio
async def test_hourly_context_sample_time_stays_at_hour_boundary_after_grace():
    collector = HourlyContextCollector()
    pipeline = MarketDataPipeline([collector], RadarState(), sampling_seconds=10)

    batch = await pipeline.collect_hourly_once(
        now=datetime(2026, 9, 15, 10, 1, 10, tzinfo=UTC)
    )

    assert batch is not None
    assert batch.hourly_contexts[0].sample_time == datetime(
        2026, 9, 15, 10, 0, tzinfo=UTC
    )


def test_state_clears_context_when_a_context_collection_round_has_no_data():
    state = RadarState()
    state.apply(
        CollectorBatch(
            funding_snapshots=(),
            hourly_contexts=(),
        ),
        replace_context=True,
    )

    assert state.funding == ()
    assert state.hourly_context == ()
