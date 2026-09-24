import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from radar.collectors.backpack import (
    BackpackCollector,
    parse_backpack_depth,
    parse_backpack_funding_rates,
    parse_backpack_markets,
    parse_backpack_mark_prices,
    parse_backpack_open_interest,
    parse_backpack_tickers,
)
from radar.config import MarketConfig
from radar.market_data import LatestMarketData
from radar.pipeline import MarketDataPipeline
from radar.state import RadarState

UTC = timezone.utc
FIXTURES = Path(__file__).parent / "fixtures" / "backpack"
SAMPLE_TIME = datetime(2026, 9, 23, 10, 0, 10, tzinfo=UTC)
OBSERVED_AT = datetime(2026, 9, 23, 10, 0, 10, 654000, tzinfo=UTC)


def load_fixture(name: str):
    with (FIXTURES / name).open(encoding="utf-8") as handle:
        return json.load(handle)


def configured_markets() -> list[MarketConfig]:
    return [
        MarketConfig(
            venue="backpack",
            venue_symbol=symbol,
            canonical_symbol=canonical,
        )
        for symbol, canonical in (
            ("SNDK.US_USDC_PERP", "SNDK"),
            ("NVDA.US_USDC_PERP", "NVDA"),
        )
    ]


def configured_crypto_market() -> list[MarketConfig]:
    return [
        MarketConfig(
            venue="backpack",
            venue_symbol="BTC_USDC_PERP",
            canonical_symbol="BTC",
        )
    ]


def test_backpack_markets_parser_keeps_visible_open_stock_and_crypto_perps():
    markets = parse_backpack_markets(load_fixture("markets.json"))

    assert sorted(markets) == [
        "BTC_USDC_PERP",
        "NVDA.US_USDC_PERP",
        "SNDK.US_USDC_PERP",
    ]
    assert markets["SNDK.US_USDC_PERP"].base_symbol == "SNDK.US"
    assert markets["BTC_USDC_PERP"].base_symbol == "BTC"


def test_backpack_markets_parser_fails_closed_when_visibility_is_missing():
    assert parse_backpack_markets(
        [
            {
                "baseSymbol": "BTC",
                "marketType": "PERP",
                "orderBookState": "Open",
                "symbol": "BTC_USDC_PERP",
            }
        ]
    ) == {}


def test_backpack_depth_parser_sorts_levels_and_reads_array_pairs():
    bids, asks = parse_backpack_depth(load_fixture("depth_sndk_us_usdc_perp.json"))

    assert [(level.price, level.base_size) for level in bids] == [
        (99.0, 20.0),
        (98.0, 100.0),
    ]
    assert [(level.price, level.base_size) for level in asks] == [
        (100.0, 20.0),
        (101.0, 100.0),
    ]


def test_backpack_bulk_context_parsers_keep_normalized_values():
    mark_prices = parse_backpack_mark_prices(load_fixture("mark_prices.json"))
    open_interest = parse_backpack_open_interest(load_fixture("open_interest.json"))
    tickers = parse_backpack_tickers(load_fixture("tickers.json"))
    funding = parse_backpack_funding_rates(
        load_fixture("funding_sndk_us_usdc_perp.json"),
        expected_symbol="SNDK.US_USDC_PERP",
    )

    assert mark_prices["SNDK.US_USDC_PERP"].mark_price == 100.5
    assert mark_prices["SNDK.US_USDC_PERP"].index_price == 100.25
    assert mark_prices["SNDK.US_USDC_PERP"].next_funding_time == datetime(
        2026, 9, 23, 5, 0, tzinfo=UTC
    )
    assert mark_prices["SNDK.US_USDC_PERP"].funding_rate == pytest.approx(0.0000125)
    assert open_interest["SNDK.US_USDC_PERP"] == 12.5
    assert tickers["SNDK.US_USDC_PERP"] == 123456.7
    assert funding is not None
    assert funding.effective_time == datetime(2026, 9, 23, 6, 0, tzinfo=UTC)
    assert funding.funding_rate == pytest.approx(0.0000125)


class FixtureTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict | None]] = []

    async def __call__(self, url: str, *, method: str, json_body=None, params=None):
        self.calls.append((url, method, params))
        if url == BackpackCollector.MARKETS_URL:
            return load_fixture("markets.json")
        if url == BackpackCollector.MARK_PRICES_URL:
            return load_fixture("mark_prices.json")
        if url == BackpackCollector.OPEN_INTEREST_URL:
            return load_fixture("open_interest.json")
        if url == BackpackCollector.TICKERS_URL:
            return load_fixture("tickers.json")
        if url == BackpackCollector.FUNDING_RATES_URL:
            symbol = params["symbol"]
            return load_fixture(
                "funding_sndk_us_usdc_perp.json"
                if symbol == "SNDK.US_USDC_PERP"
                else "funding_nvda_us_usdc_perp.json"
            )
        if url == BackpackCollector.DEPTH_URL:
            symbol = params["symbol"]
            return load_fixture(
                "depth_sndk_us_usdc_perp.json"
                if symbol == "SNDK.US_USDC_PERP"
                else "depth_nvda_us_usdc_perp.json"
            )
        raise AssertionError(f"unexpected request: {url} {params}")


class FixtureWebSocket:
    def __init__(self, messages: list[dict]) -> None:
        self.sent: list[dict] = []
        self.closed = False
        self._queue: asyncio.Queue[str | None] = asyncio.Queue()
        for message in messages:
            self.push(message)

    def push(self, message: dict) -> None:
        self._queue.put_nowait(json.dumps(message))

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._queue.put_nowait(None)

    async def __aenter__(self) -> "FixtureWebSocket":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    def __aiter__(self) -> "FixtureWebSocket":
        return self

    async def __anext__(self) -> str:
        message = await self._queue.get()
        if message is None:
            raise StopAsyncIteration
        return message


def depth_update(
    symbol: str,
    *,
    first_update_id: int,
    final_update_id: int,
    bids: tuple[tuple[float, float], ...] = (),
    asks: tuple[tuple[float, float], ...] = (),
) -> dict:
    return {
        "stream": f"depth.{symbol}",
        "data": {
            "e": "depth",
            "s": symbol,
            "U": first_update_id,
            "u": final_update_id,
            "b": [[str(price), str(size)] for price, size in bids],
            "a": [[str(price), str(size)] for price, size in asks],
        },
    }


async def wait_until(predicate) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("predicate was not satisfied")


def collector_websocket() -> FixtureWebSocket:
    return FixtureWebSocket(
        [
            {"id": 1, "result": None},
            depth_update(
                "SNDK.US_USDC_PERP",
                first_update_id=12345,
                final_update_id=12346,
                bids=((99.0, 20.0),),
                asks=((100.0, 20.0), (101.0, 100.0)),
            ),
            depth_update(
                "NVDA.US_USDC_PERP",
                first_update_id=12346,
                final_update_id=12347,
                bids=((199.0, 20.0),),
                asks=((200.0, 20.0), (201.0, 100.0)),
            ),
        ]
    )


def cache_pipeline(
    collector: BackpackCollector,
    markets: list[MarketConfig],
    latest: LatestMarketData,
) -> MarketDataPipeline:
    return MarketDataPipeline(
        [collector],
        RadarState(),
        markets=markets,
        latest_market_data=latest,
        clock=lambda: OBSERVED_AT,
    )


@pytest.mark.asyncio
async def test_backpack_collector_uses_ws_books_and_keeps_depth_out_of_sampling():
    transport = FixtureTransport()
    websocket = collector_websocket()
    latest = LatestMarketData()
    collector = BackpackCollector(
        configured_markets(),
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=lambda _url: websocket,
        latest_market_data=latest,
    )

    await collector.start()
    try:
        await wait_until(
            lambda: all(
                collector._order_book_feed.snapshot(symbol) is not None
                for symbol in ("SNDK.US_USDC_PERP", "NVDA.US_USDC_PERP")
            )
        )
        depth_calls_after_start = sum(
            url == BackpackCollector.DEPTH_URL for url, _, _ in transport.calls
        )
        pipeline = cache_pipeline(collector, configured_markets(), latest)
        market_batch = await pipeline.collect_once(now=OBSERVED_AT)
        hourly_batch = await collector.collect_hourly(sample_time=SAMPLE_TIME)
    finally:
        await collector.stop()

    assert depth_calls_after_start == 2
    assert sum(url == BackpackCollector.DEPTH_URL for url, _, _ in transport.calls) == 2
    assert [snapshot.canonical_symbol for snapshot in market_batch.market_snapshots] == [
        "SNDK",
        "NVDA",
    ]
    assert market_batch.market_snapshots[0].buy_10k_vwap is not None
    assert market_batch.market_snapshots[0].sell_10k_vwap is not None
    assert len(hourly_batch.funding_snapshots) == 1
    assert [context.canonical_symbol for context in hourly_batch.hourly_contexts] == [
        "SNDK",
        "NVDA",
    ]
    assert websocket.sent == [
        {
            "method": "SUBSCRIBE",
            "params": ["depth.SNDK.US_USDC_PERP", "depth.NVDA.US_USDC_PERP"],
        }
    ]


class MetadataFailureTransport(FixtureTransport):
    def __init__(self) -> None:
        super().__init__()
        self.fail_metadata = False

    async def __call__(self, url: str, *, method: str, json_body=None, params=None):
        if self.fail_metadata and url in {
            BackpackCollector.MARKETS_URL,
            BackpackCollector.MARK_PRICES_URL,
            BackpackCollector.OPEN_INTEREST_URL,
            BackpackCollector.TICKERS_URL,
        }:
            raise OSError("Backpack metadata refresh unavailable")
        return await super().__call__(
            url,
            method=method,
            json_body=json_body,
            params=params,
        )


@pytest.mark.asyncio
async def test_backpack_metadata_failure_preserves_ready_cache_book():
    transport = MetadataFailureTransport()
    websocket = FixtureWebSocket(
        [
            {"id": 1, "result": None},
            depth_update(
                "BTC_USDC_PERP",
                first_update_id=12346,
                final_update_id=12347,
                bids=((199.0, 20.0),),
                asks=((200.0, 20.0), (201.0, 100.0)),
            ),
        ]
    )
    market = MarketConfig(
        venue="backpack", venue_symbol="BTC_USDC_PERP", canonical_symbol="BTC"
    )
    latest = LatestMarketData()
    collector = BackpackCollector(
        [market],
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=lambda _url: websocket,
        latest_market_data=latest,
    )

    await collector.start()
    try:
        await wait_until(
            lambda: collector._order_book_feed.snapshot(market.venue_symbol) is not None
        )
        pipeline = cache_pipeline(collector, [market], latest)
        before = await pipeline.collect_once(now=OBSERVED_AT)
        transport.fail_metadata = True
        assert await collector._refresh_metadata_once() is False
        after = await pipeline.collect_once(now=OBSERVED_AT)
    finally:
        await collector.stop()

    assert len(before.market_snapshots) == 1
    assert len(after.market_snapshots) == 1
    assert after.market_snapshots[0].observed_at == OBSERVED_AT
    assert after.market_snapshots[0].mark_price == before.market_snapshots[0].mark_price
    assert after.market_snapshots[0].index_price == before.market_snapshots[0].index_price


@pytest.mark.asyncio
async def test_backpack_collector_normalizes_market_funding_context_and_vwap():
    transport = FixtureTransport()
    websocket = collector_websocket()
    collector = BackpackCollector(
        configured_markets(),
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=lambda _url: websocket,
        latest_market_data=LatestMarketData(),
    )

    latest = collector._latest_market_data
    assert isinstance(latest, LatestMarketData)
    pipeline = cache_pipeline(collector, configured_markets(), latest)
    try:
        await collector.start()
        await wait_until(
            lambda: all(
                collector._order_book_feed.snapshot(symbol) is not None
                for symbol in ("SNDK.US_USDC_PERP", "NVDA.US_USDC_PERP")
            )
        )
        market_batch = await pipeline.collect_once(now=OBSERVED_AT)
        hourly_batch = await collector.collect_hourly(sample_time=SAMPLE_TIME)
    finally:
        await collector.stop()

    assert [snapshot.canonical_symbol for snapshot in market_batch.market_snapshots] == [
        "SNDK",
        "NVDA",
    ]
    sndk = market_batch.market_snapshots[0]
    assert sndk.venue == "backpack"
    assert sndk.venue_symbol == "SNDK.US_USDC_PERP"
    assert sndk.best_bid == 99.0
    assert sndk.best_ask == 100.0
    assert sndk.mark_price == 100.5
    assert sndk.index_price == 100.25
    assert sndk.buy_1k_vwap == pytest.approx(100.0)
    assert sndk.sell_1k_vwap == pytest.approx(99.0)
    assert sndk.buy_5k_vwap is not None
    assert sndk.sell_5k_vwap is not None
    assert sndk.buy_10k_vwap is not None
    assert sndk.sell_10k_vwap is not None
    assert sndk.observed_at == OBSERVED_AT

    assert len(hourly_batch.funding_snapshots) == 1
    funding = hourly_batch.funding_snapshots[0]
    assert funding.canonical_symbol == "SNDK"
    assert funding.effective_time == datetime(2026, 9, 23, 6, 0, tzinfo=UTC)
    assert funding.next_funding_time == datetime(2026, 9, 23, 5, 0, tzinfo=UTC)
    assert funding.observed_at == OBSERVED_AT

    assert [context.canonical_symbol for context in hourly_batch.hourly_contexts] == [
        "SNDK",
        "NVDA",
    ]
    assert hourly_batch.hourly_contexts[0].sample_time == datetime(
        2026, 9, 23, 10, 0, tzinfo=UTC
    )
    assert hourly_batch.hourly_contexts[0].open_interest == 12.5
    assert hourly_batch.hourly_contexts[0].volume_24h == 123456.7

    assert sum(url == BackpackCollector.MARKETS_URL for url, _, _ in transport.calls) == 1
    assert sum(url == BackpackCollector.DEPTH_URL for url, _, _ in transport.calls) == 2
    assert sum(url == BackpackCollector.MARK_PRICES_URL for url, _, _ in transport.calls) == 1
    assert sum(url == BackpackCollector.OPEN_INTEREST_URL for url, _, _ in transport.calls) == 1
    assert sum(url == BackpackCollector.TICKERS_URL for url, _, _ in transport.calls) == 1
    assert sum(url == BackpackCollector.FUNDING_RATES_URL for url, _, _ in transport.calls) == 2
    assert all(method == "GET" for _, method, _ in transport.calls)


@pytest.mark.asyncio
async def test_backpack_collector_collects_configured_crypto_perp():
    websocket = FixtureWebSocket(
        [
            {"id": 1, "result": None},
            depth_update(
                "BTC_USDC_PERP",
                first_update_id=12346,
                final_update_id=12347,
                bids=((199.0, 20.0),),
                asks=((200.0, 20.0), (201.0, 100.0)),
            ),
        ]
    )
    latest = LatestMarketData()
    collector = BackpackCollector(
        configured_crypto_market(),
        request_json=FixtureTransport(),
        clock=lambda: OBSERVED_AT,
        websocket_connect=lambda _url: websocket,
        latest_market_data=latest,
    )

    pipeline = cache_pipeline(collector, configured_crypto_market(), latest)
    try:
        await collector.start()
        await wait_until(
            lambda: collector._order_book_feed.snapshot("BTC_USDC_PERP") is not None
        )
        batch = await pipeline.collect_once(now=OBSERVED_AT)
    finally:
        await collector.stop()

    assert len(batch.market_snapshots) == 1
    assert batch.market_snapshots[0].venue_symbol == "BTC_USDC_PERP"
    assert batch.market_snapshots[0].canonical_symbol == "BTC"


@pytest.mark.asyncio
async def test_backpack_collector_omits_only_symbol_when_depth_fails():
    transport = FixtureTransport()
    websocket = collector_websocket()
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        if url == BackpackCollector.DEPTH_URL and params["symbol"] == "NVDA.US_USDC_PERP":
            raise OSError("Backpack book unavailable")
        return await transport(url, method=method, json_body=json_body, params=params)

    latest = LatestMarketData()
    collector = BackpackCollector(
        configured_markets(),
        request_json=failing_transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=lambda _url: websocket,
        latest_market_data=latest,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )
    pipeline = cache_pipeline(collector, configured_markets(), latest)
    try:
        await collector.start()
        await wait_until(
            lambda: collector._order_book_feed.snapshot("SNDK.US_USDC_PERP") is not None
        )
        batch = await pipeline.collect_once(now=OBSERVED_AT)
    finally:
        await collector.stop()

    assert [snapshot.canonical_symbol for snapshot in batch.market_snapshots] == ["SNDK"]
    assert [(venue, str(error)) for venue, error in failures] == [
        ("backpack", "Backpack book unavailable")
    ]


@pytest.mark.asyncio
async def test_backpack_collector_returns_empty_batch_when_markets_request_fails():
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        raise OSError("Backpack metadata unavailable")

    latest = LatestMarketData()
    collector = BackpackCollector(
        configured_markets(),
        request_json=failing_transport,
        websocket_connect=lambda _url: FixtureWebSocket(),
        latest_market_data=latest,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )

    pipeline = cache_pipeline(collector, configured_markets(), latest)
    try:
        await collector.start()
        batch = await pipeline.collect_once(now=OBSERVED_AT)
        hourly_batch = await collector.collect_hourly(sample_time=SAMPLE_TIME)
    finally:
        await collector.stop()

    assert batch.market_snapshots == ()
    assert hourly_batch.funding_snapshots == ()
    assert hourly_batch.hourly_contexts == ()
    assert ("backpack", "Backpack metadata unavailable") in [
        (venue, str(error)) for venue, error in failures
    ]


@pytest.mark.asyncio
async def test_backpack_collector_marks_unfilled_vwap_targets_unavailable():
    transport = FixtureTransport()
    websocket = FixtureWebSocket(
        [
            {"id": 1, "result": None},
            depth_update(
                "SNDK.US_USDC_PERP",
                first_update_id=12345,
                final_update_id=12346,
            ),
        ]
    )

    async def sparse_transport(url: str, *, method: str, json_body=None, params=None):
        if url == BackpackCollector.DEPTH_URL:
            return {
                "lastUpdateId": "12345",
                "bids": [["99", "0.1"]],
                "asks": [["100", "0.1"]],
            }
        return await transport(url, method=method, json_body=json_body, params=params)

    latest = LatestMarketData()
    collector = BackpackCollector(
        configured_markets()[:1],
        request_json=sparse_transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=lambda _url: websocket,
        latest_market_data=latest,
    )

    pipeline = cache_pipeline(collector, configured_markets()[:1], latest)
    try:
        await collector.start()
        await wait_until(
            lambda: collector._order_book_feed.snapshot("SNDK.US_USDC_PERP") is not None
        )
        batch = await pipeline.collect_once(now=OBSERVED_AT)
    finally:
        await collector.stop()

    assert batch.market_snapshots == ()
