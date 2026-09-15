from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from radar.models import FundingSnapshot, HourlyContext, MarketSnapshot

UTC = timezone.utc
NOW = datetime(2026, 9, 15, 10, 0, tzinfo=UTC)


def make_market(**overrides):
    data = {
        "sample_time": NOW,
        "observed_at": NOW,
        "venue": "lighter",
        "venue_symbol": "BTC",
        "canonical_symbol": "BTC",
        "best_bid": 99.0,
        "best_bid_size": 2.0,
        "best_ask": 100.0,
        "best_ask_size": 3.0,
        "mark_price": 99.5,
        "index_price": 99.4,
        "buy_1k_vwap": 100.1,
        "sell_1k_vwap": 98.9,
        "buy_5k_vwap": 100.2,
        "sell_5k_vwap": 98.8,
        "buy_10k_vwap": 100.3,
        "sell_10k_vwap": 98.7,
    }
    data.update(overrides)
    return MarketSnapshot(**data)


def test_market_snapshot_accepts_valid_utc_data():
    snap = make_market()
    assert snap.canonical_symbol == "BTC"
    assert snap.sample_time.tzinfo is not None


def test_market_snapshot_rejects_naive_timestamp():
    with pytest.raises(ValidationError):
        make_market(sample_time=datetime(2026, 9, 15, 10, 0))


def test_market_snapshot_rejects_crossed_or_locked_bbo():
    with pytest.raises(ValidationError):
        make_market(best_bid=100.0, best_ask=100.0)


def test_market_snapshot_rejects_negative_size():
    with pytest.raises(ValidationError):
        make_market(best_bid_size=-1.0)


def test_funding_snapshot_requires_utc_times():
    snap = FundingSnapshot(
        effective_time=NOW,
        observed_at=NOW,
        venue="lighter",
        venue_symbol="BTC",
        canonical_symbol="BTC",
        funding_rate=0.0001,
        next_funding_time=NOW,
    )
    assert snap.funding_rate == pytest.approx(0.0001)

    with pytest.raises(ValidationError):
        FundingSnapshot(
            effective_time=datetime(2026, 9, 15, 10, 0),
            observed_at=NOW,
            venue="lighter",
            venue_symbol="BTC",
            canonical_symbol="BTC",
            funding_rate=0.0001,
        )

    for invalid_rate in (float("nan"), float("inf"), -float("inf")):
        with pytest.raises(ValidationError):
            FundingSnapshot(
                effective_time=NOW,
                observed_at=NOW,
                venue="lighter",
                venue_symbol="BTC",
                canonical_symbol="BTC",
                funding_rate=invalid_rate,
            )


def test_hourly_context_rejects_negative_oi_or_volume():
    with pytest.raises(ValidationError):
        HourlyContext(
            sample_time=NOW,
            observed_at=NOW,
            venue="lighter",
            venue_symbol="BTC",
            canonical_symbol="BTC",
            open_interest=-1,
            volume_24h=100,
        )
