import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from radar.collectors.base import CollectorBatch
from radar.collectors.hyperliquid import (
    HyperliquidCollector,
    parse_hyperliquid_l2_book,
    parse_hyperliquid_meta_and_asset_ctxs,
)
from radar.config import MarketConfig
from radar.market_data import LatestMarketData
from radar.pipeline import MarketDataPipeline
from radar.state import RadarState

UTC = timezone.utc
FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"
SAMPLE_TIME = datetime(2026, 9, 15, 10, 0, 10, tzinfo=UTC)
OBSERVED_AT = datetime(2026, 9, 15, 10, 0, 10, 321000, tzinfo=UTC)


def load_fixture(name: str):
    with (FIXTURES / name).open(encoding="utf-8") as handle:
        return json.load(handle)


def configured_markets() -> list[MarketConfig]:
    return [
        MarketConfig(venue="hyperliquid", venue_symbol=symbol, canonical_symbol=symbol)
        for symbol in ("BTC", "ETH", "SOL")
    ]


def configured_hip3_markets() -> list[MarketConfig]:
    return [
        MarketConfig(
            venue="trade_xyz",
            venue_symbol="xyz:TSLA",
            canonical_symbol="TSLA",
        ),
        MarketConfig(
            venue="trade_xyz",
            venue_symbol="xyz:NVDA",
            canonical_symbol="NVDA",
        ),
    ]


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


def load_l2_fixture(coin: str) -> dict:
    if coin == "xyz:TSLA":
        return load_fixture("hip3_l2_xyz_tsla.json")
    if coin == "io:SNDK":
        return load_fixture_from_entropy("l2_io_sndk.json")
    return load_fixture(f"l2_{coin.lower()}.json")


def fixture_websocket(
    coins: tuple[str, ...], *, include_coins: tuple[str, ...] | None = None
):
    included = set(coins if include_coins is None else include_coins)
    messages = [
        {
            "channel": "subscriptionResponse",
            "data": {
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": coin},
            },
        }
        for coin in coins
    ]
    messages.extend(
        {"channel": "l2Book", "data": load_l2_fixture(coin)}
        for coin in coins
        if coin in included
    )
    websocket = FixtureWebSocket(messages)
    return websocket, lambda _url, **_kwargs: websocket


async def wait_for_books(collector: HyperliquidCollector, coins: tuple[str, ...]) -> None:
    for _ in range(100):
        if all(collector._order_book_feed.snapshot(coin) is not None for coin in coins):
            return
        await asyncio.sleep(0)
    raise AssertionError("fixture websocket books were not populated")


async def wait_until(predicate) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("predicate was not satisfied")


def cache_pipeline(
    collector: HyperliquidCollector,
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


def test_hyperliquid_meta_contexts_are_mapped_by_universe_order():
    contexts = parse_hyperliquid_meta_and_asset_ctxs(
        load_fixture("meta_and_asset_ctxs.json")
    )

    assert contexts["BTC"].mark_price == 100.0
    assert contexts["BTC"].index_price == 99.9
    assert contexts["ETH"].open_interest == 234.5
    assert contexts["SOL"].volume_24h == 678901.0


def test_hyperliquid_l2_parser_orders_sides_and_rejects_missing_levels():
    bids, asks = parse_hyperliquid_l2_book(load_fixture("l2_btc.json"))

    assert [(level.price, level.base_size) for level in bids] == [(99.0, 60.0), (98.0, 60.0)]
    assert [(level.price, level.base_size) for level in asks] == [(100.0, 60.0), (101.0, 60.0)]

    with pytest.raises(ValueError, match="levels"):
        parse_hyperliquid_l2_book({"coin": "BTC", "levels": [[]]})
    with pytest.raises(ValueError, match="coin"):
        parse_hyperliquid_l2_book(
            load_fixture("l2_btc.json"), expected_coin="ETH"
        )


class FixtureTransport:
    def __init__(self):
        self.calls: list[tuple[str, str, dict | None]] = []

    async def __call__(self, url: str, *, method: str, json_body=None, params=None):
        self.calls.append((url, method, json_body))
        if json_body["type"] == "metaAndAssetCtxs":
            return load_fixture("meta_and_asset_ctxs.json")
        if json_body["type"] == "l2Book":
            return load_fixture(f"l2_{json_body['coin'].lower()}.json")
        if json_body["type"] == "fundingHistory":
            return load_fixture(f"funding_{json_body['coin'].lower()}.json")
        raise AssertionError(f"unexpected request: {json_body}")


@pytest.mark.asyncio
async def test_hyperliquid_collector_normalizes_market_funding_and_hourly_context():
    transport = FixtureTransport()
    websocket, websocket_connect = fixture_websocket(("BTC", "ETH", "SOL"))
    latest = LatestMarketData()
    collector = HyperliquidCollector(
        configured_markets(),
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=websocket_connect,
        latest_market_data=latest,
    )

    markets = configured_markets()
    pipeline = cache_pipeline(collector, markets, latest)
    try:
        await collector.start()
        await wait_for_books(collector, ("BTC", "ETH", "SOL"))
        market_batch = await pipeline.collect_once(now=OBSERVED_AT)
        batch = await collector.collect_hourly(sample_time=SAMPLE_TIME)
    finally:
        await collector.stop()

    assert batch.market_snapshots == ()
    assert [snapshot.canonical_symbol for snapshot in market_batch.market_snapshots] == [
        "BTC",
        "ETH",
        "SOL",
    ]
    btc = market_batch.market_snapshots[0]
    assert btc.best_bid == 99.0
    assert btc.best_bid_size == 60.0
    assert btc.best_ask == 100.0
    assert btc.best_ask_size == 60.0
    assert btc.mark_price == 100.0
    assert btc.index_price == 99.9
    assert btc.buy_10k_vwap == pytest.approx(10000 / (60 + 4000 / 101))
    assert btc.sell_10k_vwap == pytest.approx(10000 / (60 + 4060 / 98))
    assert btc.sample_time == SAMPLE_TIME
    assert btc.observed_at == OBSERVED_AT
    assert btc.observed_at != btc.sample_time

    assert len(batch.funding_snapshots) == 3
    btc_funding = batch.funding_snapshots[0]
    assert btc_funding.funding_rate == pytest.approx(0.0002)
    assert btc_funding.effective_time == datetime.fromtimestamp(
        1789470000, tz=UTC
    )
    assert btc_funding.next_funding_time is None

    btc_context = batch.hourly_contexts[0]
    assert btc_context.sample_time == datetime(2026, 9, 15, 10, 0, tzinfo=UTC)
    assert btc_context.open_interest == 123.4
    assert btc_context.volume_24h == 456789.0
    assert btc_context.observed_at == OBSERVED_AT

    assert {url for url, _, _ in transport.calls} == {HyperliquidCollector.INFO_URL}
    assert all(method == "POST" for _, method, _ in transport.calls)
    assert sum(body["type"] == "metaAndAssetCtxs" for _, _, body in transport.calls) == 1
    assert sum(body["type"] == "l2Book" for _, _, body in transport.calls) == 0
    assert sum(body["type"] == "fundingHistory" for _, _, body in transport.calls) == 3
    assert [
        body for _, _, body in transport.calls if body["type"] == "metaAndAssetCtxs"
    ] == [{"type": "metaAndAssetCtxs"}]


@pytest.mark.asyncio
async def test_hyperliquid_collector_omits_symbol_until_its_ws_snapshot_arrives():
    transport = FixtureTransport()
    websocket, websocket_connect = fixture_websocket(
        ("BTC", "ETH", "SOL"), include_coins=("BTC", "SOL")
    )
    failures: list[tuple[str, Exception]] = []

    latest = LatestMarketData()
    collector = HyperliquidCollector(
        configured_markets(),
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=websocket_connect,
        latest_market_data=latest,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )
    pipeline = cache_pipeline(collector, configured_markets(), latest)
    try:
        await collector.start()
        await wait_for_books(collector, ("BTC", "SOL"))
        batch = await pipeline.collect_once(now=OBSERVED_AT)
    finally:
        await collector.stop()

    assert {snapshot.canonical_symbol for snapshot in batch.market_snapshots} == {
        "BTC",
        "SOL",
    }
    assert failures == []
    assert sum(body["type"] == "l2Book" for _, _, body in transport.calls) == 0


@pytest.mark.asyncio
async def test_hyperliquid_collector_reports_metadata_failure():
    websocket, websocket_connect = fixture_websocket(("BTC", "ETH", "SOL"))
    failures: list[tuple[str, Exception]] = []
    latest = LatestMarketData()

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        raise OSError("metadata unavailable")

    collector = HyperliquidCollector(
        configured_markets(),
        request_json=failing_transport,
        websocket_connect=websocket_connect,
        latest_market_data=latest,
        clock=lambda: OBSERVED_AT,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )

    pipeline = cache_pipeline(collector, configured_markets(), latest)
    try:
        await collector.start()
        await wait_for_books(collector, ("BTC", "ETH", "SOL"))
        batch = await collector.collect_hourly(sample_time=SAMPLE_TIME)
        market_batch = await pipeline.collect_once(now=OBSERVED_AT)
    finally:
        await collector.stop()

    assert batch == CollectorBatch()
    assert len(market_batch.market_snapshots) == 3
    assert [(venue, str(error)) for venue, error in failures] == [
        ("hyperliquid", "metadata unavailable")
    ]


@pytest.mark.asyncio
async def test_hyperliquid_invalid_cache_publication_clears_local_book():
    transport = FixtureTransport()
    websocket = FixtureWebSocket(
        [
            {
                "channel": "subscriptionResponse",
                "data": {
                    "method": "subscribe",
                    "subscription": {"type": "l2Book", "coin": "BTC"},
                },
            },
            {
                "channel": "l2Book",
                "data": {
                    "coin": "BTC",
                    "levels": [
                        [{"px": "101", "sz": "1", "n": 1}],
                        [{"px": "100", "sz": "1", "n": 1}],
                    ],
                },
            },
        ]
    )
    failures: list[tuple[str, Exception]] = []
    latest = LatestMarketData()
    market = MarketConfig(venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC")
    collector = HyperliquidCollector(
        [market],
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=lambda _url, **_kwargs: websocket,
        latest_market_data=latest,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )

    pipeline = cache_pipeline(collector, [market], latest)
    try:
        await collector.start()
        await wait_until(lambda: bool(failures))
        assert collector._order_book_feed.snapshot("BTC") is None
        assert (await pipeline.collect_once(now=OBSERVED_AT)).market_snapshots == ()
    finally:
        await collector.stop()


class Hip3FixtureTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def __call__(self, url: str, *, method: str, json_body=None, params=None):
        assert json_body is not None
        self.calls.append(json_body)
        request_type = json_body["type"]
        if request_type == "metaAndAssetCtxs":
            return load_fixture("hip3_meta_and_asset_ctxs.json")
        if request_type == "l2Book" and json_body["coin"] == "xyz:TSLA":
            return load_fixture("hip3_l2_xyz_tsla.json")
        if request_type == "fundingHistory" and json_body["coin"] == "xyz:TSLA":
            return load_fixture("hip3_funding_xyz_tsla.json")
        raise AssertionError(f"unexpected HIP-3 request: {json_body}")


class EntropyFixtureTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def __call__(self, url: str, *, method: str, json_body=None, params=None):
        assert json_body is not None
        self.calls.append(json_body)
        request_type = json_body["type"]
        if request_type == "metaAndAssetCtxs":
            return load_fixture_from_entropy("meta_and_asset_ctxs.json")
        if request_type == "l2Book" and json_body["coin"] == "io:SNDK":
            return load_fixture_from_entropy("l2_io_sndk.json")
        if request_type == "fundingHistory" and json_body["coin"] == "io:SNDK":
            return load_fixture_from_entropy("funding_io_sndk.json")
        raise AssertionError(f"unexpected Entropy request: {json_body}")


def load_fixture_from_entropy(name: str):
    with (Path(__file__).parent / "fixtures" / "entropy" / name).open(
        encoding="utf-8"
    ) as handle:
        return json.load(handle)


@pytest.mark.asyncio
async def test_trade_xyz_collector_adds_dex_only_to_metadata_and_normalizes_hip3_data():
    transport = Hip3FixtureTransport()
    websocket, websocket_connect = fixture_websocket(("xyz:TSLA",))
    market = configured_hip3_markets()[:1]
    latest = LatestMarketData()
    collector = HyperliquidCollector(
        market,
        venue="trade_xyz",
        dex="xyz",
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=websocket_connect,
        latest_market_data=latest,
    )

    pipeline = cache_pipeline(collector, market, latest)
    try:
        await collector.start()
        await wait_for_books(collector, ("xyz:TSLA",))
        market_batch = await pipeline.collect_once(now=OBSERVED_AT)
        batch = await collector.collect_hourly(sample_time=SAMPLE_TIME)
    finally:
        await collector.stop()

    assert batch.market_snapshots == ()
    snapshot = market_batch.market_snapshots[0]
    assert snapshot.venue == "trade_xyz"
    assert snapshot.venue_symbol == "xyz:TSLA"
    assert snapshot.canonical_symbol == "TSLA"
    assert snapshot.best_bid == 100.0
    assert snapshot.best_ask == 101.0
    assert snapshot.buy_1k_vwap is not None
    assert snapshot.sell_1k_vwap is not None
    assert snapshot.buy_5k_vwap is not None
    assert snapshot.sell_5k_vwap is not None
    assert snapshot.buy_10k_vwap is not None
    assert snapshot.sell_10k_vwap is not None

    funding = batch.funding_snapshots[0]
    assert funding.venue == "trade_xyz"
    assert funding.venue_symbol == "xyz:TSLA"
    assert funding.canonical_symbol == "TSLA"
    assert funding.funding_rate == pytest.approx(0.0002)
    assert funding.effective_time == datetime.fromtimestamp(1789470000, tz=UTC)

    context = batch.hourly_contexts[0]
    assert context.venue == "trade_xyz"
    assert context.venue_symbol == "xyz:TSLA"
    assert context.canonical_symbol == "TSLA"
    assert context.open_interest == 123.4
    assert context.volume_24h == 456789.0

    metadata_requests = [
        body for body in transport.calls if body["type"] == "metaAndAssetCtxs"
    ]
    assert metadata_requests == [{"type": "metaAndAssetCtxs", "dex": "xyz"}]
    assert all(
        "dex" not in body
        for body in transport.calls
        if body["type"] in {"l2Book", "fundingHistory"}
    )
    assert sum(body["type"] == "l2Book" for body in transport.calls) == 0


@pytest.mark.asyncio
async def test_entropy_collector_uses_io_namespace_and_maps_sndk():
    transport = EntropyFixtureTransport()
    websocket, websocket_connect = fixture_websocket(("io:SNDK",))
    market = [
        MarketConfig(
            venue="entropy",
            venue_symbol="io:SNDK",
            canonical_symbol="SNDK",
        )
    ]
    latest = LatestMarketData()
    collector = HyperliquidCollector(
        market,
        venue="entropy",
        dex="io",
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=websocket_connect,
        latest_market_data=latest,
    )

    pipeline = cache_pipeline(collector, market, latest)
    try:
        await collector.start()
        await wait_for_books(collector, ("io:SNDK",))
        market_batch = await pipeline.collect_once(now=OBSERVED_AT)
        batch = await collector.collect_hourly(sample_time=SAMPLE_TIME)
    finally:
        await collector.stop()

    assert batch.market_snapshots == ()
    assert len(market_batch.market_snapshots) == 1
    snapshot = market_batch.market_snapshots[0]
    assert snapshot.venue == "entropy"
    assert snapshot.venue_symbol == "io:SNDK"
    assert snapshot.canonical_symbol == "SNDK"
    assert snapshot.buy_10k_vwap is not None
    assert snapshot.sell_10k_vwap is not None
    assert batch.funding_snapshots[0].effective_time == datetime.fromtimestamp(
        1790128800, tz=UTC
    )
    assert batch.hourly_contexts[0].canonical_symbol == "SNDK"

    assert [
        body for body in transport.calls if body["type"] == "metaAndAssetCtxs"
    ] == [{"type": "metaAndAssetCtxs", "dex": "io"}]
    assert all(
        "dex" not in body
        for body in transport.calls
        if body["type"] in {"l2Book", "fundingHistory"}
    )
    assert sum(body["type"] == "l2Book" for body in transport.calls) == 0


@pytest.mark.asyncio
async def test_trade_xyz_ws_failure_omits_only_unready_symbol_and_keeps_trade_venue():
    transport = Hip3FixtureTransport()
    websocket, websocket_connect = fixture_websocket(
        ("xyz:TSLA", "xyz:NVDA"), include_coins=("xyz:TSLA",)
    )
    latest = LatestMarketData()

    collector = HyperliquidCollector(
        configured_hip3_markets(),
        venue="trade_xyz",
        dex="xyz",
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=websocket_connect,
        latest_market_data=latest,
    )

    pipeline = cache_pipeline(collector, configured_hip3_markets(), latest)
    try:
        await collector.start()
        await wait_for_books(collector, ("xyz:TSLA",))
        batch = await pipeline.collect_once(now=OBSERVED_AT)
    finally:
        await collector.stop()

    assert [(snapshot.venue_symbol, snapshot.canonical_symbol) for snapshot in batch.market_snapshots] == [
        ("xyz:TSLA", "TSLA")
    ]
    assert sum(body["type"] == "l2Book" for body in transport.calls) == 0


@pytest.mark.asyncio
async def test_trade_xyz_metadata_failure_reports_logical_venue_and_returns_empty_batch():
    websocket, websocket_connect = fixture_websocket(("xyz:TSLA",))
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        raise OSError("HIP-3 metadata unavailable")

    collector = HyperliquidCollector(
        configured_hip3_markets(),
        venue="trade_xyz",
        dex="xyz",
        request_json=failing_transport,
        websocket_connect=websocket_connect,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )

    try:
        await collector.start()
        batch = await collector.collect_hourly(sample_time=SAMPLE_TIME)
    finally:
        await collector.stop()

    assert batch == CollectorBatch()
    assert [(venue, str(error)) for venue, error in failures] == [
        ("trade_xyz", "HIP-3 metadata unavailable")
    ]
