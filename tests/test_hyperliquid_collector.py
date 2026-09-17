import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from radar.collectors.hyperliquid import (
    HyperliquidCollector,
    parse_hyperliquid_l2_book,
    parse_hyperliquid_meta_and_asset_ctxs,
)
from radar.config import MarketConfig

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
    collector = HyperliquidCollector(
        configured_markets(), request_json=transport, clock=lambda: OBSERVED_AT
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

    assert {url for url, _, _ in transport.calls} == {
        HyperliquidCollector.INFO_URL
    }
    assert all(method == "POST" for _, method, _ in transport.calls)
    assert sum(body["type"] == "metaAndAssetCtxs" for _, _, body in transport.calls) == 1
    assert sum(body["type"] == "l2Book" for _, _, body in transport.calls) == 3
    assert sum(body["type"] == "fundingHistory" for _, _, body in transport.calls) == 3


@pytest.mark.asyncio
async def test_hyperliquid_collector_omits_symbol_when_its_book_request_fails():
    transport = FixtureTransport()
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        if json_body.get("type") == "l2Book" and json_body.get("coin") == "ETH":
            raise OSError("temporary outage")
        return await transport(url, method=method, json_body=json_body, params=params)

    collector = HyperliquidCollector(
        configured_markets(),
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
        ("hyperliquid", "temporary outage")
    ]


@pytest.mark.asyncio
async def test_hyperliquid_collector_reports_metadata_failure():
    failures: list[tuple[str, Exception]] = []

    async def failing_transport(url: str, *, method: str, json_body=None, params=None):
        raise OSError("metadata unavailable")

    collector = HyperliquidCollector(
        configured_markets(),
        request_json=failing_transport,
        error_handler=lambda venue, error: failures.append((venue, error)),
    )

    batch = await collector.collect(
        sample_time=SAMPLE_TIME, include_hourly_context=False
    )

    assert batch.market_snapshots == ()
    assert [(venue, str(error)) for venue, error in failures] == [
        ("hyperliquid", "metadata unavailable")
    ]
