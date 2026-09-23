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
    collector = LighterCollector(
        configured_markets(venue),
        venue=venue,
        base_url=base_url,
        request_json=transport,
        clock=lambda: OBSERVED_AT,
    )

    batch = await collector.collect(
        sample_time=SAMPLE_TIME, include_hourly_context=True
    )

    assert [snapshot.canonical_symbol for snapshot in batch.market_snapshots] == [
        "BTC",
        "ETH",
        "SOL",
    ]
    btc = batch.market_snapshots[0]
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
    assert all(snapshot.venue == venue for snapshot in batch.market_snapshots)
    assert collector.base_url == base_url
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
    assert sum(url == transport.order_book_orders_url for url, _, _ in transport.calls) == 3
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
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        if url == transport.order_book_orders_url and params["market_id"] == 0:
            raise OSError("temporary outage")
        return await transport(url, method=method, json_body=json_body, params=params)

    collector = LighterCollector(
        configured_markets(venue),
        venue=venue,
        base_url=base_url,
        request_json=failing_transport,
        clock=lambda: OBSERVED_AT,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )
    batch = await collector.collect(
        sample_time=SAMPLE_TIME, include_hourly_context=False
    )

    assert {snapshot.canonical_symbol for snapshot in batch.market_snapshots} == {
        "BTC",
        "SOL",
    }
    assert [(venue, str(error)) for venue, error in failures] == [
        (venue, "temporary outage")
    ]


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

    batch = await collector.collect(
        sample_time=SAMPLE_TIME, include_hourly_context=False
    )

    assert batch.market_snapshots == ()
    assert [(venue, str(error)) for venue, error in failures] == [
        (venue, "market details unavailable")
    ]
