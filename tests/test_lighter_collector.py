import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from radar.collectors.lighter import (
    LighterCollector,
    parse_lighter_fundings,
    parse_lighter_order_book_details,
    parse_lighter_order_book_orders,
)
from radar.config import MarketConfig
from radar.market_data import LatestMarketData
from radar.pipeline import MarketDataPipeline
from radar.state import RadarState

UTC = timezone.utc
FIXTURES = Path(__file__).parent / "fixtures" / "lighter"
SAMPLE_TIME = datetime(2026, 9, 15, 10, 0, 10, tzinfo=UTC)
OBSERVED_AT = datetime(2026, 9, 15, 10, 0, 10, 654000, tzinfo=UTC)
ROBINHOOD_BASE_URL = "https://api.rh.lighter.xyz"


def load_fixture(name: str):
    with (FIXTURES / name).open(encoding="utf-8") as handle:
        return json.load(handle)


def configured_markets(venue: str = "lighter") -> list[MarketConfig]:
    return [
        MarketConfig(venue=venue, venue_symbol=symbol, canonical_symbol=symbol)
        for symbol in ("BTC", "ETH", "SOL")
    ]


def test_lighter_details_parser_maps_perpetual_market_ids_and_context():
    markets = parse_lighter_order_book_details(load_fixture("order_book_details.json"))

    assert markets["BTC"].market_id == 1
    assert markets["ETH"].mark_price == 2000.0
    assert markets["SOL"].index_price == 99.5
    assert markets["SOL"].open_interest == 345.6
    assert markets["SOL"].volume_24h == 678901.0


def test_lighter_details_parser_accepts_null_spot_list_for_perp_filter():
    payload = load_fixture("order_book_details.json")
    payload["spot_order_book_details"] = None

    markets = parse_lighter_order_book_details(payload)

    assert set(markets) == {"BTC", "ETH", "SOL"}


def test_lighter_order_parser_aggregates_and_sorts_orders():
    bids, asks = parse_lighter_order_book_orders(load_fixture("order_book_btc.json"))

    assert [(level.price, level.base_size) for level in bids] == [(99.0, 60.0), (98.0, 60.0)]
    assert [(level.price, level.base_size) for level in asks] == [(100.0, 60.0), (101.0, 60.0)]

    with pytest.raises(ValueError, match="code"):
        parse_lighter_order_book_orders({"code": 500, "asks": [], "bids": []})


def test_lighter_funding_parser_uses_latest_settlement_rate_and_timestamp():
    point = parse_lighter_fundings(load_fixture("fundings_btc.json"))

    assert point is not None
    assert point.funding_rate == pytest.approx(0.000011)
    assert point.effective_time == datetime.fromtimestamp(1789470000, tz=UTC)


@pytest.mark.parametrize(
    ("direction", "expected_rate"),
    [("long", 0.000045), ("short", -0.000045)],
)
def test_lighter_funding_parser_normalizes_percentage_rate_and_direction(
    direction: str, expected_rate: float
):
    point = parse_lighter_fundings(
        {
            "code": 200,
            "fundings": [
                {
                    "timestamp": 1789470000,
                    "rate": "0.0045",
                    "direction": direction,
                }
            ],
        }
    )

    assert point is not None
    assert point.funding_rate == pytest.approx(expected_rate)


@pytest.mark.parametrize("direction", ["long", "short"])
def test_lighter_funding_parser_preserves_zero_rate(
    direction: str,
):
    point = parse_lighter_fundings(
        {
            "code": 200,
            "fundings": [
                {
                    "timestamp": 1789470000,
                    "rate": "0",
                    "direction": direction,
                }
            ],
        }
    )

    assert point is not None
    assert point.funding_rate == 0.0


@pytest.mark.parametrize("direction", [None, "sideways", 1])
def test_lighter_funding_parser_rejects_unknown_or_malformed_direction(
    direction: object,
):
    with pytest.raises(ValueError, match="direction"):
        parse_lighter_fundings(
            {
                "code": 200,
                "fundings": [
                    {
                        "timestamp": 1789470000,
                        "rate": "0.0045",
                        "direction": direction,
                    }
                ],
            }
        )


class FixtureTransport:
    def __init__(self, base_url: str = LighterCollector.BASE_URL):
        self.calls: list[tuple[str, str, dict | None]] = []
        normalized_base_url = base_url.rstrip("/")
        self.order_book_details_url = f"{normalized_base_url}/api/v1/orderBookDetails"
        self.order_book_orders_url = f"{normalized_base_url}/api/v1/orderBookOrders"
        self.fundings_url = f"{normalized_base_url}/api/v1/fundings"

    async def __call__(self, url: str, *, method: str, json_body=None, params=None):
        self.calls.append((url, method, params))
        if url == self.order_book_details_url:
            return load_fixture("order_book_details.json")
        if url == self.order_book_orders_url:
            market_id = params["market_id"]
            symbol = {1: "btc", 0: "eth", 2: "sol"}[market_id]
            return load_fixture(f"order_book_{symbol}.json")
        if url == self.fundings_url:
            market_id = params["market_id"]
            symbol = {1: "btc", 0: "eth", 2: "sol"}[market_id]
            return load_fixture(f"fundings_{symbol}.json")
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


def ws_snapshot(symbol: str, market_id: int, nonce: int = 1) -> dict:
    payload = load_fixture(f"order_book_{symbol.lower()}.json")
    return {
        "type": "subscribed/order_book",
        "channel": f"order_book:{market_id}",
        "order_book": {
            "code": 0,
            "asks": [
                {"price": order["price"], "size": order["remaining_base_amount"]}
                for order in payload["asks"]
            ],
            "bids": [
                {"price": order["price"], "size": order["remaining_base_amount"]}
                for order in payload["bids"]
            ],
            "nonce": nonce,
        },
    }


def ws_zero_delete_all_asks(market_id: int, *, begin_nonce: int = 1, nonce: int = 2):
    return {
        "type": "update/order_book",
        "channel": f"order_book:{market_id}",
        "order_book": {
            "code": 0,
            "asks": [
                {"price": "100", "size": "0"},
                {"price": "101", "size": "0"},
            ],
            "bids": [],
            "begin_nonce": begin_nonce,
            "nonce": nonce,
        },
    }


def fixture_websocket(*, include_symbols: tuple[str, ...] = ("btc", "eth", "sol")):
    market_ids = {"btc": 1, "eth": 0, "sol": 2}
    websocket = FixtureWebSocket(
        [{"type": "connected"}]
        + [ws_snapshot(symbol, market_ids[symbol]) for symbol in include_symbols]
    )
    return websocket, lambda _url: websocket


async def wait_for_books(collector: LighterCollector, market_ids: tuple[int, ...]) -> None:
    for _ in range(100):
        if all(collector._order_book_feed.snapshot(market_id) is not None for market_id in market_ids):
            return
        await asyncio.sleep(0)
    raise AssertionError("fixture websocket books were not populated")


def cache_pipeline(
    collector: LighterCollector,
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


@pytest.mark.parametrize(
    ("venue", "base_url"),
    [
        ("lighter", LighterCollector.BASE_URL),
        ("lighter_robinhood", ROBINHOOD_BASE_URL),
    ],
)
@pytest.mark.asyncio
async def test_lighter_collector_normalizes_market_funding_and_hourly_context(
    venue: str, base_url: str
):
    transport = FixtureTransport(base_url)
    websocket, websocket_connect = fixture_websocket()
    latest = LatestMarketData()
    collector = LighterCollector(
        configured_markets(venue),
        venue=venue,
        base_url=base_url,
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=websocket_connect,
        latest_market_data=latest,
    )

    markets = configured_markets(venue)
    pipeline = cache_pipeline(collector, markets, latest)
    try:
        await collector.start()
        await wait_for_books(collector, (1, 0, 2))
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
    assert btc.buy_1k_vwap == pytest.approx(100.0)
    assert btc.sell_1k_vwap == pytest.approx(99.0)
    assert btc.buy_5k_vwap == pytest.approx(100.0)
    assert btc.sell_5k_vwap == pytest.approx(99.0)
    assert btc.buy_10k_vwap == pytest.approx(10000 / (60 + 4000 / 101))
    assert btc.sell_10k_vwap == pytest.approx(10000 / (60 + 4060 / 98))
    assert all(snapshot.venue == venue for snapshot in market_batch.market_snapshots)
    assert collector.base_url == base_url
    assert collector.ws_url == (
        "wss://api.rh.lighter.xyz/stream"
        if venue == "lighter_robinhood"
        else "wss://mainnet.zklighter.elliot.ai/stream"
    )
    assert btc.sample_time == SAMPLE_TIME
    assert btc.observed_at == OBSERVED_AT
    assert btc.observed_at != btc.sample_time

    assert len(batch.funding_snapshots) == 3
    assert batch.funding_snapshots[0].funding_rate == pytest.approx(0.000011)
    assert batch.funding_snapshots[0].effective_time == datetime.fromtimestamp(
        1789470000, tz=UTC
    )
    assert batch.funding_snapshots[0].next_funding_time is None

    btc_context = batch.hourly_contexts[0]
    assert btc_context.sample_time == datetime(2026, 9, 15, 10, 0, tzinfo=UTC)
    assert btc_context.open_interest == 123.4
    assert btc_context.volume_24h == 456789.0
    assert btc_context.observed_at == OBSERVED_AT

    assert all(method == "GET" for _, method, _ in transport.calls)
    assert sum(url == transport.order_book_details_url for url, _, _ in transport.calls) == 1
    assert sum(url == transport.order_book_orders_url for url, _, _ in transport.calls) == 0
    assert sum(url == transport.fundings_url for url, _, _ in transport.calls) == 3


@pytest.mark.parametrize(
    ("venue", "base_url"),
    [
        ("lighter", LighterCollector.BASE_URL),
        ("lighter_robinhood", ROBINHOOD_BASE_URL),
    ],
)
@pytest.mark.asyncio
async def test_lighter_collector_omits_symbol_when_its_book_request_fails(
    venue: str, base_url: str
):
    transport = FixtureTransport(base_url)
    websocket, websocket_connect = fixture_websocket(include_symbols=("btc", "sol"))
    failures: list[tuple[str, Exception]] = []
    latest = LatestMarketData()

    collector = LighterCollector(
        configured_markets(venue),
        venue=venue,
        base_url=base_url,
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=websocket_connect,
        latest_market_data=latest,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )
    markets = configured_markets(venue)
    pipeline = cache_pipeline(collector, markets, latest)
    try:
        await collector.start()
        await wait_for_books(collector, (1, 2))
        batch = await pipeline.collect_once(now=OBSERVED_AT)
    finally:
        await collector.stop()

    assert {snapshot.canonical_symbol for snapshot in batch.market_snapshots} == {
        "BTC",
        "SOL",
    }
    assert failures == []


@pytest.mark.parametrize(
    ("venue", "base_url"),
    [
        ("lighter", LighterCollector.BASE_URL),
        ("lighter_robinhood", ROBINHOOD_BASE_URL),
    ],
)
@pytest.mark.asyncio
async def test_lighter_collector_reports_market_details_failure(
    venue: str, base_url: str
):
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        raise OSError("market details unavailable")

    collector = LighterCollector(
        configured_markets(venue),
        venue=venue,
        base_url=base_url,
        request_json=failing_transport,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )

    await collector.start()
    await collector.stop()

    assert [(venue, str(error)) for venue, error in failures] == [
        (venue, "market details unavailable")
    ]


class SequencedDetailsTransport(FixtureTransport):
    def __init__(self, base_url: str = LighterCollector.BASE_URL) -> None:
        super().__init__(base_url)
        self.fail_details = False

    async def __call__(self, url: str, *, method: str, json_body=None, params=None):
        if url == self.order_book_details_url and self.fail_details:
            raise OSError("market details unavailable")
        return await super().__call__(url, method=method, json_body=json_body, params=params)


@pytest.mark.parametrize(
    ("venue", "base_url"),
    [
        ("lighter", LighterCollector.BASE_URL),
        ("lighter_robinhood", ROBINHOOD_BASE_URL),
    ],
)
@pytest.mark.asyncio
async def test_lighter_metadata_failure_preserves_ready_cache_book(
    venue: str, base_url: str
):
    transport = SequencedDetailsTransport(base_url)
    websocket, websocket_connect = fixture_websocket(include_symbols=("btc",))
    latest = LatestMarketData()
    collector = LighterCollector(
        [MarketConfig(venue=venue, venue_symbol="BTC", canonical_symbol="BTC")],
        venue=venue,
        base_url=base_url,
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=websocket_connect,
        latest_market_data=latest,
    )

    markets = [MarketConfig(venue=venue, venue_symbol="BTC", canonical_symbol="BTC")]
    pipeline = cache_pipeline(collector, markets, latest)
    try:
        await collector.start()
        await wait_for_books(collector, (1,))
        transport.fail_details = True
        await collector._refresh_metadata_once()
        batch = await pipeline.collect_once(now=OBSERVED_AT)
    finally:
        await collector.stop()

    assert len(batch.market_snapshots) == 1
    assert batch.market_snapshots[0].observed_at == OBSERVED_AT


@pytest.mark.parametrize(
    ("venue", "base_url"),
    [
        ("lighter", LighterCollector.BASE_URL),
        ("lighter_robinhood", ROBINHOOD_BASE_URL),
    ],
)
@pytest.mark.asyncio
async def test_lighter_invalid_zero_delete_invalidates_previous_cache_view(
    venue: str, base_url: str
):
    transport = FixtureTransport(base_url)
    websocket, websocket_connect = fixture_websocket(include_symbols=("btc",))
    latest = LatestMarketData()
    market = MarketConfig(venue=venue, venue_symbol="BTC", canonical_symbol="BTC")
    collector = LighterCollector(
        [market],
        venue=venue,
        base_url=base_url,
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=websocket_connect,
        latest_market_data=latest,
    )

    pipeline = cache_pipeline(collector, [market], latest)
    try:
        await collector.start()
        await wait_for_books(collector, (1,))
        before = await pipeline.collect_once(now=OBSERVED_AT)
        assert len(before.market_snapshots) == 1

        websocket.push(ws_zero_delete_all_asks(1))
        after = None
        for _ in range(100):
            after = await pipeline.collect_once(now=OBSERVED_AT)
            if after.market_snapshots == ():
                break
            await asyncio.sleep(0)
        else:
            raise AssertionError("invalid publication left a ready cache view")
    finally:
        await collector.stop()

    assert after is not None
    assert after.market_snapshots == ()
    assert latest._views[(venue, "BTC")].ready is False


@pytest.mark.parametrize(
    ("venue", "base_url"),
    [
        ("lighter", LighterCollector.BASE_URL),
        ("lighter_robinhood", ROBINHOOD_BASE_URL),
    ],
)
@pytest.mark.asyncio
async def test_lighter_initial_discovery_recovers_before_starting_feed(
    venue: str, base_url: str
):
    transport = SequencedDetailsTransport(base_url)
    websocket, websocket_connect = fixture_websocket(include_symbols=("btc",))
    connections: list[str] = []

    def connect(url: str):
        connections.append(url)
        return websocket_connect(url)

    latest = LatestMarketData()
    market = MarketConfig(venue=venue, venue_symbol="BTC", canonical_symbol="BTC")
    collector = LighterCollector(
        [market],
        venue=venue,
        base_url=base_url,
        request_json=transport,
        clock=lambda: OBSERVED_AT,
        websocket_connect=connect,
        latest_market_data=latest,
    )

    transport.fail_details = True
    pipeline = cache_pipeline(collector, [market], latest)
    try:
        await collector.start()
        assert collector._order_book_feed.market_ids == ()
        assert connections == []
        assert (await pipeline.collect_once(now=OBSERVED_AT)).market_snapshots == ()

        transport.fail_details = False
        await collector._refresh_metadata_once()
        await wait_for_books(collector, (1,))

        batch = await pipeline.collect_once(now=OBSERVED_AT)
    finally:
        await collector.stop()

    assert connections == [collector.ws_url]
    assert len(batch.market_snapshots) == 1
    assert websocket.sent == [
        {"type": "subscribe", "channel": "order_book/1"}
    ]
