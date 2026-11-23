from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from radar.collectors.variational import (
    VARIATIONAL_STATS_URL,
    VariationalCollector,
    parse_variational_stats,
    variational_funding_snapshot,
)
from radar.config import MarketConfig
from radar.models import QuotedMarketSnapshot

FETCHED_AT = datetime(2026, 9, 24, 12, 0, 40, tzinfo=UTC)
FIXTURE = Path(__file__).parent / "fixtures" / "variational" / "stats.json"


def load_payload() -> dict[str, object]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def configured_markets() -> tuple[MarketConfig, ...]:
    return (
        MarketConfig(
            venue="variational", venue_symbol="AAPL", canonical_symbol="AAPL"
        ),
        MarketConfig(
            venue="variational", venue_symbol="US500", canonical_symbol="SPY"
        ),
    )


def test_parse_variational_stats_filters_configured_markets_and_preserves_quote_times():
    snapshots = parse_variational_stats(
        load_payload(),
        configured_markets(),
        fetched_at=FETCHED_AT,
    )

    assert len(snapshots) == 2
    by_symbol = {snapshot.venue_symbol: snapshot for snapshot in snapshots}
    assert by_symbol["AAPL"].canonical_symbol == "AAPL"
    assert by_symbol["AAPL"].quote_time == datetime(
        2026, 9, 24, 12, 0, 20, tzinfo=UTC
    )
    assert by_symbol["AAPL"].fetched_at == FETCHED_AT
    assert by_symbol["AAPL"].bid_1k == pytest.approx(100.0)
    assert by_symbol["AAPL"].ask_100k == pytest.approx(101.5)
    assert by_symbol["AAPL"].bid_1m is None
    assert by_symbol["AAPL"].funding_rate == pytest.approx(0.044923)
    assert by_symbol["AAPL"].funding_interval_seconds == 28_800
    assert by_symbol["US500"].canonical_symbol == "SPY"
    assert by_symbol["US500"].bid_1m == pytest.approx(760.0)


def test_interval_zero_is_excluded_from_normalized_perpetual_snapshots():
    markets = configured_markets() + (
        MarketConfig(
            venue="variational", venue_symbol="USOILP", canonical_symbol="USOILP"
        ),
    )

    snapshots = parse_variational_stats(
        load_payload(), markets, fetched_at=FETCHED_AT
    )

    assert {snapshot.venue_symbol for snapshot in snapshots} == {"AAPL", "US500"}


def test_malformed_listing_isolated_from_valid_listing():
    payload = load_payload()
    listings = payload["listings"]
    assert isinstance(listings, list)
    listings.append(
        {
            "ticker": "BROKEN",
            "name": "Broken",
            "mark_price": "100",
            "volume_24h": "1",
            "open_interest": {
                "long_open_interest": "1",
                "short_open_interest": "1",
            },
            "funding_rate": "0",
            "funding_interval_s": 28_800,
            "quotes": {
                "updated_at": "2026-09-24T12:00:20Z",
                "size_1k": {"bid": "100"},
                "size_100k": {"bid": "99", "ask": "101"},
            },
        }
    )
    markets = configured_markets() + (
        MarketConfig(
            venue="variational", venue_symbol="BROKEN", canonical_symbol="BROKEN"
        ),
    )

    snapshots = parse_variational_stats(payload, markets, fetched_at=FETCHED_AT)

    assert {snapshot.venue_symbol for snapshot in snapshots} == {"AAPL", "US500"}


def test_us500_requires_explicit_spy_identity():
    payload = load_payload()
    listings = payload["listings"]
    assert isinstance(listings, list)
    us500 = next(listing for listing in listings if listing["ticker"] == "US500")
    us500["name"] = "S&P 500 Index Proxy"

    snapshots = parse_variational_stats(
        payload, configured_markets(), fetched_at=FETCHED_AT
    )

    assert {snapshot.venue_symbol for snapshot in snapshots} == {"AAPL"}


@pytest.mark.parametrize(
    "updated_at",
    [
        "2026-09-24T11:57:59Z",
        "2026-09-24T12:00:41Z",
    ],
)
def test_stale_or_future_quotes_fail_closed(updated_at: str):
    payload = load_payload()
    listings = payload["listings"]
    assert isinstance(listings, list)
    aapl = next(listing for listing in listings if listing["ticker"] == "AAPL")
    aapl["quotes"]["updated_at"] = updated_at

    snapshots = parse_variational_stats(
        payload, configured_markets(), fetched_at=FETCHED_AT
    )

    assert {snapshot.venue_symbol for snapshot in snapshots} == {"US500"}


@pytest.mark.asyncio
async def test_collector_deduplicates_unchanged_quote_time_and_accepts_newer_quote():
    payload = load_payload()
    current_payload = copy.deepcopy(payload)
    current_fetched_at = FETCHED_AT

    async def request_json(url: str, **kwargs: object) -> dict[str, object]:
        assert url == VARIATIONAL_STATS_URL
        return copy.deepcopy(current_payload)

    collector = VariationalCollector(
        configured_markets(),
        request_json=request_json,
        clock=lambda: current_fetched_at,
    )

    first = await collector.poll_once()
    second = await collector.poll_once()
    assert len(first) == 2
    assert second == ()

    listings = current_payload["listings"]
    assert isinstance(listings, list)
    for listing in listings:
        listing["quotes"]["updated_at"] = (
            datetime.fromisoformat(listing["quotes"]["updated_at"].replace("Z", "+00:00"))
            + timedelta(seconds=30)
        ).isoformat().replace("+00:00", "Z")

    current_fetched_at = FETCHED_AT + timedelta(seconds=30)
    third = await collector.poll_once()
    assert len(third) == 2
    assert all(snapshot.quote_time > first[0].quote_time for snapshot in third)


@pytest.mark.asyncio
async def test_collector_endpoint_failure_publishes_no_synthetic_quote():
    calls = 0

    async def request_json(url: str, **kwargs: object) -> dict[str, object]:
        nonlocal calls
        calls += 1
        raise OSError("endpoint unavailable")

    collector = VariationalCollector(
        configured_markets(), request_json=request_json, clock=lambda: FETCHED_AT
    )

    assert await collector.poll_once() == ()
    assert calls == 1


def test_quoted_snapshot_is_not_an_existing_market_snapshot():
    payload = load_payload()
    snapshots = parse_variational_stats(
        payload, configured_markets(), fetched_at=FETCHED_AT
    )

    assert all(isinstance(snapshot, QuotedMarketSnapshot) for snapshot in snapshots)


@pytest.mark.parametrize(
    ("annualized_rate", "interval_seconds", "live_interval_rate"),
    [
        # Live /metadata/v2/stats rows (2026-11-23): v1 funding_rate equals v2
        # funding.last.annualized_rate, and v2 interval_rate is per interval.
        (0.09482, 28_800, 0.00008659),  # BTC, 8h
        (0.1095, 14_400, 0.000050),  # VVV, 4h
        (-87.3054, 3_600, -0.009966),  # ACE, 1h, shorts pay longs
    ],
)
def test_variational_funding_rate_is_annualized_and_converts_per_interval(
    annualized_rate, interval_seconds, live_interval_rate
):
    quote = QuotedMarketSnapshot(
        quote_time=FETCHED_AT,
        fetched_at=FETCHED_AT,
        venue="variational",
        venue_symbol="X",
        canonical_symbol="X",
        mark_price=1.0,
        bid_1k=0.99,
        ask_1k=1.01,
        bid_100k=0.98,
        ask_100k=1.02,
        funding_rate=annualized_rate,
        funding_interval_seconds=interval_seconds,
    )

    funding = variational_funding_snapshot(quote)

    assert funding.interval_seconds == interval_seconds
    assert funding.funding_rate == pytest.approx(live_interval_rate, rel=1e-3)
    assert funding.apr == pytest.approx(annualized_rate)
    assert funding.effective_time == FETCHED_AT
