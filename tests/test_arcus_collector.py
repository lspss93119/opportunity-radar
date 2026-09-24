import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from radar.collectors.arcus import (
    ArcusCollector,
    parse_arcus_funding_rates,
    parse_arcus_l2_order_book,
    parse_arcus_markets,
)
from radar.config import MarketConfig
from radar.market_data import LatestMarketData
from radar.pipeline import MarketDataPipeline
from radar.state import RadarState

UTC = timezone.utc
FIXTURES = Path(__file__).parent / "fixtures" / "arcus"
SAMPLE_TIME = datetime(2026, 9, 15, 10, 0, 10, tzinfo=UTC)
OBSERVED_AT = datetime(2026, 9, 15, 10, 0, 10, 654000, tzinfo=UTC)


def load_fixture(name: str):
    with (FIXTURES / name).open(encoding="utf-8") as handle:
        return json.load(handle)


def configured_markets() -> list[MarketConfig]:
    return [
        MarketConfig(
            venue="arcus",
            venue_symbol=symbol,
            canonical_symbol=symbol.removesuffix("-USD"),
        )
        for symbol in ("SNDK-USD", "NVDA-USD")
    ]


def test_arcus_markets_parser_keeps_online_perpetuals_and_omits_offline_records():
    markets = parse_arcus_markets(load_fixture("markets.json"))

    assert markets["SNDK-USD"].market_id == 33
    assert markets["SNDK-USD"].mark_price == 1875.54
    assert markets["SNDK-USD"].oracle_price == 1875.5
    assert markets["SNDK-USD"].open_interest == 117.5023163
    assert markets["SNDK-USD"].volume_24h == 1149567.4
    assert "SPY-USD" in markets
    assert "AAPL-USD" not in markets


def test_arcus_order_book_parser_reads_array_levels_and_sorts_sides():
    bids, asks = parse_arcus_l2_order_book(load_fixture("l2_sndk_usd.json"))

    assert [(level.price, level.base_size) for level in bids] == [
        (1876.19, 100.0),
        (1875.33, 100.0),
    ]
    assert [(level.price, level.base_size) for level in asks] == [
        (1876.2, 100.0),
        (1876.6, 100.0),
    ]

    with pytest.raises(ValueError, match="levels"):
        parse_arcus_l2_order_book({"bids": [], "asks": []})
    with pytest.raises(ValueError, match="level"):
        parse_arcus_l2_order_book({"bids": [["1876.0"]], "asks": [["1877.0", "1"]]})


def test_arcus_funding_parser_uses_latest_api_timestamp():
    point = parse_arcus_funding_rates(
        load_fixture("funding_sndk_usd.json"), expected_market_id=33
    )

    assert point is not None
    assert point.funding_rate == pytest.approx(0.000005034722222222)
    assert point.effective_time == datetime.fromtimestamp(1790128800, tz=UTC)


class FixtureTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict | None]] = []

    async def __call__(self, url: str, *, method: str, json_body=None, params=None):
        self.calls.append((url, method, params))
        if url == ArcusCollector.MARKETS_URL:
            return load_fixture("markets.json")
        if url.startswith(ArcusCollector.L2_ORDER_BOOK_URL):
            symbol = url.rsplit("/", 1)[-1].lower().replace("-", "_")
            return load_fixture(f"l2_{symbol}.json")
        if url == ArcusCollector.FUNDING_RATES_URL:
            if params["market"] == "SNDK-USD":
                return load_fixture("funding_sndk_usd.json")
            return {"fundingRates": []}
        raise AssertionError(f"unexpected request: {url} {params}")


class BlockingMetadataTransport(FixtureTransport):
    def __init__(self) -> None:
        super().__init__()
        self.request_started = asyncio.Event()
        self.release_metadata = asyncio.Event()

    async def __call__(self, url: str, *, method: str, json_body=None, params=None):
        if url == ArcusCollector.MARKETS_URL:
            self.request_started.set()
            await self.release_metadata.wait()
        return await super().__call__(
            url,
            method=method,
            json_body=json_body,
            params=params,
        )


class MetadataFailureTransport(FixtureTransport):
    def __init__(self) -> None:
        super().__init__()
        self.fail_metadata = False

    async def __call__(self, url: str, *, method: str, json_body=None, params=None):
        if self.fail_metadata and url == ArcusCollector.MARKETS_URL:
            raise OSError("Arcus metadata unavailable")
        return await super().__call__(
            url,
            method=method,
            json_body=json_body,
            params=params,
        )


@pytest.mark.asyncio
async def test_arcus_background_refresh_does_not_block_cache_sampler():
    transport = BlockingMetadataTransport()
    latest = LatestMarketData()
    market = MarketConfig(
        venue="arcus", venue_symbol="SNDK-USD", canonical_symbol="SNDK"
    )
    collector = ArcusCollector(
        [market],
        request_json=transport,
        latest_market_data=latest,
        refresh_interval_seconds=60.0,
    )
    pipeline = MarketDataPipeline(
        [collector],
        RadarState(),
        markets=[market],
        latest_market_data=latest,
    )

    await collector.start()
    try:
        await asyncio.wait_for(transport.request_started.wait(), timeout=0.2)
        requests_before_sample = len(transport.calls)
        batch = await asyncio.wait_for(
            pipeline.collect_once(
                now=datetime(2026, 9, 15, 10, 0, 10, tzinfo=UTC)
            ),
            timeout=0.2,
        )
        assert batch.market_snapshots == ()
        assert len(transport.calls) == requests_before_sample
    finally:
        transport.release_metadata.set()
        await collector.stop()


@pytest.mark.asyncio
async def test_arcus_refresh_publishes_complete_book_with_rest_observation_time():
    transport = FixtureTransport()
    latest = LatestMarketData()
    collector = ArcusCollector(
        configured_markets()[:1],
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        latest_market_data=latest,
    )

    await collector.refresh_once()

    batch = latest.build_batch(
        configured_markets()[:1],
        sample_time=SAMPLE_TIME,
        now=OBSERVED_AT,
        stale_after_seconds=30,
    )
    assert len(batch.market_snapshots) == 1
    assert batch.market_snapshots[0].observed_at == OBSERVED_AT
    assert batch.market_snapshots[0].observed_at != SAMPLE_TIME


@pytest.mark.asyncio
async def test_arcus_metadata_failure_retains_prior_cache_book():
    transport = MetadataFailureTransport()
    latest = LatestMarketData()
    collector = ArcusCollector(
        configured_markets()[:1],
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        latest_market_data=latest,
    )

    await collector.refresh_once()
    transport.fail_metadata = True
    await collector.refresh_once()

    batch = latest.build_batch(
        configured_markets()[:1],
        sample_time=SAMPLE_TIME,
        now=OBSERVED_AT,
        stale_after_seconds=30,
    )
    assert len(batch.market_snapshots) == 1
    assert batch.market_snapshots[0].observed_at == OBSERVED_AT
    assert batch.market_snapshots[0].mark_price == 1875.54
    assert batch.market_snapshots[0].index_price == 1875.5


@pytest.mark.asyncio
async def test_arcus_refresh_omits_only_failed_market():
    transport = FixtureTransport()
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        if url.endswith("NVDA-USD"):
            raise OSError("Arcus book unavailable")
        return await transport(url, method=method, json_body=json_body, params=params)

    latest = LatestMarketData()
    collector = ArcusCollector(
        configured_markets(),
        request_json=failing_transport,
        clock=lambda: OBSERVED_AT,
        error_handler=lambda venue, error: failures.append((venue, error)),
        latest_market_data=latest,
    )

    await collector.refresh_once()

    batch = latest.build_batch(
        configured_markets(),
        sample_time=SAMPLE_TIME,
        now=OBSERVED_AT,
        stale_after_seconds=30,
    )
    assert [snapshot.canonical_symbol for snapshot in batch.market_snapshots] == [
        "SNDK"
    ]
    assert [(venue, str(error)) for venue, error in failures] == [
        ("arcus", "Arcus book unavailable")
    ]


@pytest.mark.asyncio
async def test_arcus_refresh_keeps_hourly_funding_and_context_separate():
    transport = FixtureTransport()
    latest = LatestMarketData()
    collector = ArcusCollector(
        configured_markets(),
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        latest_market_data=latest,
    )

    await collector.refresh_once()
    batch = await collector.collect_hourly(sample_time=SAMPLE_TIME)

    assert batch.market_snapshots == ()
    assert len(batch.funding_snapshots) == 1
    assert batch.funding_snapshots[0].canonical_symbol == "SNDK"
    assert [context.canonical_symbol for context in batch.hourly_contexts] == [
        "SNDK",
        "NVDA",
    ]
    assert batch.hourly_contexts[0].observed_at == OBSERVED_AT


@pytest.mark.asyncio
async def test_arcus_collector_normalizes_market_funding_context_and_executable_vwap():
    transport = FixtureTransport()
    collector = ArcusCollector(
        configured_markets(),
        request_json=transport,
        clock=lambda: OBSERVED_AT,
    )

    batch = await collector.collect(
        sample_time=SAMPLE_TIME, include_hourly_context=True
    )

    assert [snapshot.canonical_symbol for snapshot in batch.market_snapshots] == [
        "SNDK",
        "NVDA",
    ]
    sndk = batch.market_snapshots[0]
    assert sndk.venue == "arcus"
    assert sndk.venue_symbol == "SNDK-USD"
    assert sndk.best_bid == 1876.19
    assert sndk.best_ask == 1876.2
    assert sndk.mark_price == 1875.54
    assert sndk.index_price == 1875.5
    assert sndk.buy_1k_vwap == pytest.approx(1876.2)
    assert sndk.sell_1k_vwap == pytest.approx(1876.19)
    assert sndk.buy_5k_vwap is not None
    assert sndk.sell_5k_vwap is not None
    assert sndk.buy_10k_vwap is not None
    assert sndk.sell_10k_vwap is not None
    assert sndk.observed_at == OBSERVED_AT

    assert len(batch.funding_snapshots) == 1
    funding = batch.funding_snapshots[0]
    assert funding.canonical_symbol == "SNDK"
    assert funding.effective_time == datetime.fromtimestamp(1790128800, tz=UTC)
    assert funding.next_funding_time == datetime.fromtimestamp(1790132400, tz=UTC)
    assert funding.observed_at == OBSERVED_AT

    assert [context.canonical_symbol for context in batch.hourly_contexts] == [
        "SNDK",
        "NVDA",
    ]
    assert batch.hourly_contexts[0].sample_time == datetime(
        2026, 9, 15, 10, 0, tzinfo=UTC
    )
    assert batch.hourly_contexts[0].open_interest == 117.5023163
    assert batch.hourly_contexts[0].volume_24h == 1149567.4

    assert sum(url == ArcusCollector.MARKETS_URL for url, _, _ in transport.calls) == 1
    assert sum(url.startswith(ArcusCollector.L2_ORDER_BOOK_URL) for url, _, _ in transport.calls) == 2
    assert sum(url == ArcusCollector.FUNDING_RATES_URL for url, _, _ in transport.calls) == 2
    assert all(method == "GET" for _, method, _ in transport.calls)


@pytest.mark.asyncio
async def test_arcus_collector_omits_only_symbol_when_its_book_request_fails():
    transport = FixtureTransport()
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        if url.endswith("NVDA-USD"):
            raise OSError("Arcus book unavailable")
        return await transport(url, method=method, json_body=json_body, params=params)

    collector = ArcusCollector(
        configured_markets(),
        request_json=failing_transport,
        clock=lambda: OBSERVED_AT,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )
    batch = await collector.collect(
        sample_time=SAMPLE_TIME, include_hourly_context=False
    )

    assert [snapshot.canonical_symbol for snapshot in batch.market_snapshots] == ["SNDK"]
    assert [(venue, str(error)) for venue, error in failures] == [
        ("arcus", "Arcus book unavailable")
    ]


@pytest.mark.asyncio
async def test_arcus_collector_returns_empty_batch_when_metadata_fails():
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        raise OSError("Arcus metadata unavailable")

    collector = ArcusCollector(
        configured_markets(),
        request_json=failing_transport,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )

    batch = await collector.collect(
        sample_time=SAMPLE_TIME, include_hourly_context=True
    )

    assert batch.market_snapshots == ()
    assert batch.funding_snapshots == ()
    assert batch.hourly_contexts == ()
    assert [(venue, str(error)) for venue, error in failures] == [
        ("arcus", "Arcus metadata unavailable")
    ]


@pytest.mark.asyncio
async def test_arcus_collector_marks_unfilled_vwap_targets_unavailable():
    class SparseTransport(FixtureTransport):
        async def __call__(self, url: str, *, method: str, json_body=None, params=None):
            if url.endswith("SNDK-USD"):
                return {
                    "bids": [["1876.19", "0.1"]],
                    "asks": [["1876.2", "0.1"]],
                }
            return await super().__call__(url, method=method, json_body=json_body, params=params)

    collector = ArcusCollector(
        configured_markets()[:1],
        request_json=SparseTransport(),
        clock=lambda: OBSERVED_AT,
    )

    batch = await collector.collect(
        sample_time=SAMPLE_TIME, include_hourly_context=False
    )

    snapshot = batch.market_snapshots[0]
    assert snapshot.buy_1k_vwap is None
    assert snapshot.sell_1k_vwap is None
    assert snapshot.buy_5k_vwap is None
    assert snapshot.sell_5k_vwap is None
    assert snapshot.buy_10k_vwap is None
    assert snapshot.sell_10k_vwap is None
