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


def test_backpack_markets_parser_keeps_visible_open_stock_perps_only():
    markets = parse_backpack_markets(load_fixture("markets.json"))

    assert sorted(markets) == ["NVDA.US_USDC_PERP", "SNDK.US_USDC_PERP"]
    assert markets["SNDK.US_USDC_PERP"].base_symbol == "SNDK.US"


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


@pytest.mark.asyncio
async def test_backpack_collector_normalizes_market_funding_context_and_vwap():
    transport = FixtureTransport()
    collector = BackpackCollector(
        configured_markets(),
        request_json=transport,
        clock=lambda: OBSERVED_AT,
    )

    batch = await collector.collect(
        sample_time=SAMPLE_TIME,
        include_hourly_context=True,
    )

    assert [snapshot.canonical_symbol for snapshot in batch.market_snapshots] == [
        "SNDK",
        "NVDA",
    ]
    sndk = batch.market_snapshots[0]
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

    assert len(batch.funding_snapshots) == 1
    funding = batch.funding_snapshots[0]
    assert funding.canonical_symbol == "SNDK"
    assert funding.effective_time == datetime(2026, 9, 23, 6, 0, tzinfo=UTC)
    assert funding.next_funding_time == datetime(2026, 9, 23, 5, 0, tzinfo=UTC)
    assert funding.observed_at == OBSERVED_AT

    assert [context.canonical_symbol for context in batch.hourly_contexts] == [
        "SNDK",
        "NVDA",
    ]
    assert batch.hourly_contexts[0].sample_time == datetime(
        2026, 9, 23, 10, 0, tzinfo=UTC
    )
    assert batch.hourly_contexts[0].open_interest == 12.5
    assert batch.hourly_contexts[0].volume_24h == 123456.7

    assert sum(url == BackpackCollector.MARKETS_URL for url, _, _ in transport.calls) == 1
    assert sum(url == BackpackCollector.DEPTH_URL for url, _, _ in transport.calls) == 2
    assert sum(url == BackpackCollector.MARK_PRICES_URL for url, _, _ in transport.calls) == 1
    assert sum(url == BackpackCollector.OPEN_INTEREST_URL for url, _, _ in transport.calls) == 1
    assert sum(url == BackpackCollector.TICKERS_URL for url, _, _ in transport.calls) == 1
    assert sum(url == BackpackCollector.FUNDING_RATES_URL for url, _, _ in transport.calls) == 2
    assert all(method == "GET" for _, method, _ in transport.calls)


@pytest.mark.asyncio
async def test_backpack_collector_omits_only_symbol_when_depth_fails():
    transport = FixtureTransport()
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        if url == BackpackCollector.DEPTH_URL and params["symbol"] == "NVDA.US_USDC_PERP":
            raise OSError("Backpack book unavailable")
        return await transport(url, method=method, json_body=json_body, params=params)

    collector = BackpackCollector(
        configured_markets(),
        request_json=failing_transport,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )
    batch = await collector.collect(
        sample_time=SAMPLE_TIME,
        include_hourly_context=False,
    )

    assert [snapshot.canonical_symbol for snapshot in batch.market_snapshots] == ["SNDK"]
    assert [(venue, str(error)) for venue, error in failures] == [
        ("backpack", "Backpack book unavailable")
    ]


@pytest.mark.asyncio
async def test_backpack_collector_returns_empty_batch_when_markets_request_fails():
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        raise OSError("Backpack metadata unavailable")

    collector = BackpackCollector(
        configured_markets(),
        request_json=failing_transport,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )

    batch = await collector.collect(
        sample_time=SAMPLE_TIME,
        include_hourly_context=True,
    )

    assert batch.market_snapshots == ()
    assert batch.funding_snapshots == ()
    assert batch.hourly_contexts == ()
    assert [(venue, str(error)) for venue, error in failures] == [
        ("backpack", "Backpack metadata unavailable")
    ]


@pytest.mark.asyncio
async def test_backpack_collector_marks_unfilled_vwap_targets_unavailable():
    transport = FixtureTransport()

    async def sparse_transport(url: str, *, method: str, json_body=None, params=None):
        if url == BackpackCollector.DEPTH_URL:
            return {
                "bids": [["99", "0.1"]],
                "asks": [["100", "0.1"]],
            }
        return await transport(url, method=method, json_body=json_body, params=params)

    collector = BackpackCollector(
        configured_markets()[:1],
        request_json=sparse_transport,
        clock=lambda: OBSERVED_AT,
    )

    batch = await collector.collect(
        sample_time=SAMPLE_TIME,
        include_hourly_context=False,
    )

    snapshot = batch.market_snapshots[0]
    assert snapshot.buy_1k_vwap is None
    assert snapshot.sell_1k_vwap is None
    assert snapshot.buy_5k_vwap is None
    assert snapshot.sell_5k_vwap is None
    assert snapshot.buy_10k_vwap is None
    assert snapshot.sell_10k_vwap is None
