import asyncio
from datetime import datetime, timezone

import pytest

from radar.collectors.base import Collector, CollectorBatch
from radar.collectors.hyperliquid import HyperliquidCollector
from radar.collectors.lighter import LighterCollector
from radar.config import MarketConfig, RadarConfig
from radar.pipeline import MarketDataPipeline, aligned_sample_time
from radar.state import RadarState
from radar.models import MarketSnapshot

UTC = timezone.utc
NOW = datetime(2026, 9, 15, 10, 0, 19, 876000, tzinfo=UTC)


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


class BlockingCollector:
    def __init__(self, venue: str, started: set[str], all_started: asyncio.Event, release: asyncio.Event):
        self.venue = venue
        self._started = started
        self._all_started = all_started
        self._release = release
        self.sample_times: list[datetime] = []

    async def collect(self, *, sample_time: datetime, include_hourly_context: bool) -> CollectorBatch:
        self.sample_times.append(sample_time)
        self._started.add(self.venue)
        if len(self._started) == 2:
            self._all_started.set()
        await self._release.wait()
        return CollectorBatch(
            market_snapshots=(make_market(self.venue, NOW, 100),),
        )


@pytest.mark.asyncio
async def test_pipeline_collects_all_venues_concurrently_with_one_sample_time():
    started: set[str] = set()
    all_started = asyncio.Event()
    release = asyncio.Event()
    collectors = [
        BlockingCollector("lighter", started, all_started, release),
        BlockingCollector("hyperliquid", started, all_started, release),
    ]
    pipeline = MarketDataPipeline(collectors, RadarState(), sampling_seconds=10)

    task = asyncio.create_task(pipeline.collect_once(now=NOW))
    await asyncio.wait_for(all_started.wait(), timeout=0.2)
    assert not task.done()
    release.set()

    batch = await task
    expected_sample_time = datetime(2026, 9, 15, 10, 0, 10, tzinfo=UTC)
    assert [snapshot.venue for snapshot in batch.market_snapshots] == [
        "lighter",
        "hyperliquid",
    ]
    assert all(collector.sample_times == [expected_sample_time] for collector in collectors)


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


def test_collector_protocol_is_structural():
    assert isinstance(SuccessfulCollector(), Collector)


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


@pytest.mark.asyncio
async def test_failed_venue_does_not_leave_old_market_snapshot_in_state():
    state = RadarState()
    old_lighter = make_market("lighter", datetime(2026, 9, 15, 9, 59, tzinfo=UTC), 100)
    state.apply(CollectorBatch(market_snapshots=(old_lighter,)))

    pipeline = MarketDataPipeline(
        [FailingCollector(), SuccessfulCollector()], state, sampling_seconds=10
    )
    await pipeline.collect_once(now=NOW)

    assert state.get_market("lighter", "BTC") is None
    assert state.get_market("hyperliquid", "BTC").best_ask == 200


@pytest.mark.asyncio
async def test_pipeline_requests_hourly_context_only_once_per_utc_hour():
    collector = CadenceCollector()
    pipeline = MarketDataPipeline([collector], RadarState(), sampling_seconds=10)

    await pipeline.collect_once(now=NOW)
    await pipeline.collect_once(
        now=datetime(2026, 9, 15, 10, 0, 29, tzinfo=UTC)
    )
    await pipeline.collect_once(
        now=datetime(2026, 9, 15, 11, 0, 0, tzinfo=UTC)
    )

    assert collector.include_hourly_context_values == [True, False, True]


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
