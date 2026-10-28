from datetime import datetime, timezone

import pytest

from radar.collectors.ondo import (
    OndoCollector,
    parse_ondo_book,
    parse_ondo_contracts,
)
from radar.config import MarketConfig, RadarConfig
from radar.pipeline import MarketDataPipeline

NOW = datetime(2026, 10, 28, 8, 0, tzinfo=timezone.utc)
MARKET = MarketConfig(venue="ondo", venue_symbol="BTC-USD.P", canonical_symbol="BTC")
CONTRACTS = {
    "success": True,
    "result": [{
        "market": "BTC-USD.P",
        "disabled": False,
        "isClosed": False,
        "indexPrice": "70000",
        "lastPrice": "70001",
        "openInterest": "8",
        "usdVolume": "1000000",
    }],
}
DEPTH = {
    "success": True,
    "result": {
        "market": "BTC-USD.P",
        "time": "2026-10-28T08:00:00Z",
        "bids": [["69999", "3"], ["69998", "4"]],
        "asks": [["70001", "3"], ["70002", "4"]],
    },
}
FUNDING = {
    "success": True,
    "result": {"market": "BTC-USD.P", "rate": "0.0001",
               "intervalEnds": "2026-10-28T09:00:00Z"},
}


def test_ondo_parsers_reject_bad_envelopes_and_crossed_books():
    assert "BTC-USD.P" in parse_ondo_contracts(CONTRACTS)
    for response in ({}, {"success": False, "result": DEPTH["result"]}):
        with pytest.raises(ValueError):
            parse_ondo_book(response, symbol="BTC-USD.P")
    crossed = {"success": True, "result": {**DEPTH["result"], "bids": [["70002", "1"]]}}
    with pytest.raises(ValueError, match="crossed"):
        parse_ondo_book(crossed, symbol="BTC-USD.P")


@pytest.mark.asyncio
async def test_ondo_collects_radar_snapshots_with_injected_public_rest():
    called = []
    async def request(url, *, method="GET", params=None):
        called.append((url, params))
        if url.endswith("/contracts"):
            return CONTRACTS
        if url.endswith("/depth"):
            return DEPTH
        if url.endswith("/funding_rates"):
            return FUNDING
        raise AssertionError(url)

    errors = []
    collector = OndoCollector([MARKET], request_json=request,
                               clock=lambda: NOW,
                               error_handler=lambda venue, e: errors.append((venue, e)))
    batch = await collector.collect(sample_time=NOW, include_hourly_context=True)
    assert not errors
    assert len(batch.market_snapshots) == 1
    row = batch.market_snapshots[0]
    assert row.venue == "ondo"
    assert row.best_bid == 69999
    assert row.best_ask == 70001
    assert row.buy_10k_vwap is not None
    assert row.sell_10k_vwap is not None
    assert len(batch.funding_snapshots) == 1
    assert batch.funding_snapshots[0].funding_rate == 0.0001
    assert len(batch.hourly_contexts) == 1
    assert all(url.startswith("https://api.ondoperps.xyz/v1/perps/") for url, _ in called)


@pytest.mark.asyncio
async def test_ondo_invalid_book_fails_closed():
    async def request(url, *, method="GET", params=None):
        if url.endswith("/contracts"):
            return CONTRACTS
        return {"success": True, "result": {**DEPTH["result"], "asks": []}}
    errors = []
    batch = await OndoCollector([MARKET], request_json=request,
                                clock=lambda: NOW,
                                error_handler=lambda venue, error: errors.append(error)
                                ).collect(sample_time=NOW, include_hourly_context=False)
    assert batch.market_snapshots == ()
    assert len(errors) == 1


def test_pipeline_only_registers_ondo_when_enabled():
    empty = MarketDataPipeline.from_config(RadarConfig())
    assert all(c.venue != "ondo" for c in empty.collectors)
    config = RadarConfig(markets=[MARKET], fees_bps={"ondo": 3.0})
    pipeline = MarketDataPipeline.from_config(config)
    assert sum(c.venue == "ondo" for c in pipeline.collectors) == 1


@pytest.mark.asyncio
async def test_ondo_contracts_failure_invalidates_cached_book():
    from radar.market_data import LatestMarketData
    from radar.vwap import BookLevel

    latest = LatestMarketData()
    latest.update_book(
        venue="ondo", venue_symbol="BTC-USD.P",
        bids=[BookLevel(price=69999, base_size=1)],
        asks=[BookLevel(price=70001, base_size=1)],
        observed_at=NOW,
    )

    async def failing_request(url, *, method="GET", params=None):
        raise RuntimeError("contract endpoint unavailable")

    collector = OndoCollector(
        [MARKET], latest_market_data=latest, request_json=failing_request,
        clock=lambda: NOW, error_handler=lambda venue, error: None,
    )
    batch = await collector.collect(sample_time=NOW, include_hourly_context=False)
    assert not batch.market_snapshots
    assert latest.ready_venues(
        markets=[MARKET], canonical_symbol="BTC", now=NOW, stale_after_seconds=30,
    ) == set()
