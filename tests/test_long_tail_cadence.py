from datetime import UTC, datetime, timedelta

import pytest

from radar.alerts.spread import (
    format_anomaly_alert,
    format_spread_alert,
    parse_anomaly_alert,
    parse_spread_alert,
)
from radar.collectors.base import CollectorBatch
from radar.config import DiscoveryConfig, MarketConfig, RadarConfig, SpreadMonitorConfig
from radar.discovery import Listing, select_universe
from radar.history.spread import AnomalyConfirmationContext, HistoricalSpreadContext
from radar.models import MarketSnapshot
from radar.monitors.base import AlertRequest
from radar.monitors.spread.models import SpreadPairKey, build_spread_candidates, spread_by_size
from radar.monitors.spread.monitor import SpreadMonitor
from radar.pipeline import MarketDataPipeline
from radar.state import RadarState
from radar.storage.parquet import ParquetStorage

NOW = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)  # divisible by 30s and 60s
LONG = ("lighter", "VVV", "VVV")
SHORT = ("hyperliquid", "VVV", "VVV")
KEY = SpreadPairKey("VVV", "lighter", "VVV", "hyperliquid", "VVV")


def snapshot(venue: str, sample_time: datetime, *, buy: float, sell: float, deep: bool = False):
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=sample_time,
        venue=venue,
        venue_symbol="VVV",
        canonical_symbol="VVV",
        best_bid=sell,
        best_bid_size=100.0,
        best_ask=buy,
        best_ask_size=100.0,
        buy_1k_vwap=buy,
        sell_1k_vwap=sell,
        buy_5k_vwap=buy * 1.001 if deep else None,
        sell_5k_vwap=sell * 0.999 if deep else None,
    )


def test_spread_monitor_defaults_to_one_thousand_dollar_evaluation():
    assert SpreadMonitorConfig().primary_size_usd == 1_000


def test_discovery_marks_long_tail_and_rfq_feeds_for_slow_history():
    listings = [
        Listing("lighter", "BTC", "BTC", 100.0, 50_000_000),
        Listing("hyperliquid", "BTC", "BTC", 100.0, 4_999_999),
        Listing("variational", "BTC", "BTC", 100.0, 90_000_000),
    ]
    result = select_universe(
        listings, RadarConfig(discovery=DiscoveryConfig(enabled=True))
    )

    cadence = {m.venue: m.sample_seconds for m in (*result.markets, *result.quoted_markets)}
    assert cadence == {"lighter": 10, "hyperliquid": 30, "variational": 30}


def test_yaml_rfq_markets_default_to_thirty_seconds_unless_explicit():
    config = RadarConfig(
        quoted_markets=[
            MarketConfig(venue="variational", venue_symbol="BTC", canonical_symbol="BTC"),
            MarketConfig(
                venue="variational", venue_symbol="ETH", canonical_symbol="ETH", sample_seconds=10
            ),
        ]
    )
    assert config.market_sample_seconds() == {
        ("variational", "BTC", "BTC"): 30,
        ("variational", "ETH", "ETH"): 10,
    }


def make_monitor() -> SpreadMonitor:
    return SpreadMonitor(
        SpreadMonitorConfig(),
        {},
        market_identities={LONG, SHORT},
        market_sample_seconds={LONG: 10, SHORT: 30},
    )


@pytest.mark.asyncio
async def test_long_tail_pair_evaluates_every_10s_but_records_on_its_cadence():
    monitor = make_monitor()
    for step in range(6):  # 60 seconds of 10s samples
        sample_time = NOW + timedelta(seconds=10 * step)
        state = RadarState()
        state.apply_market_batch(
            CollectorBatch(
                market_snapshots=(
                    snapshot("lighter", sample_time, buy=100.0, sell=99.9),
                    snapshot("hyperliquid", sample_time, buy=100.2, sell=100.1),
                )
            )
        )
        await monitor.evaluate(sample_time, state)

    basis = monitor._basis_by_key[KEY]
    assert basis.sample_count == 2  # only the :00 and :30 samples
    assert basis.expected_interval_seconds == 30
    # Same 80% coverage requirement as a 10s pair: 6912 * 10 / 30.
    assert basis.min_observations == 2304


def test_hydration_keeps_only_points_on_the_pair_cadence():
    monitor = make_monitor()
    points = tuple((NOW + timedelta(seconds=10 * step), 1.0) for step in range(9))

    monitor.hydrate_history({KEY: points})

    assert monitor._basis_by_key[KEY].sample_count == 3


def test_cadence_change_restarts_basis_for_affected_pairs_only():
    monitor = make_monitor()
    other = SpreadPairKey("BTC", "lighter", "BTC", "hyperliquid", "BTC")
    identities = {LONG, SHORT, ("lighter", "BTC", "BTC"), ("hyperliquid", "BTC", "BTC")}
    monitor.set_market_identities(identities, {LONG: 10, SHORT: 30})
    monitor._basis_by_key = {KEY: monitor._new_basis(KEY), other: monitor._new_basis(other)}

    monitor.set_market_identities(identities, {LONG: 10, SHORT: 10})

    assert set(monitor._basis_by_key) == {other}


@pytest.mark.asyncio
async def test_pipeline_persists_long_tail_rows_only_on_cadence(tmp_path):
    storage = ParquetStorage(tmp_path / "data")
    pipeline = MarketDataPipeline(
        [],
        RadarState(),
        storage=storage,
        markets=[
            MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
            MarketConfig(
                venue="lighter", venue_symbol="VVV", canonical_symbol="VVV", sample_seconds=30
            ),
        ],
    )
    def batch(sample_time):
        return CollectorBatch(
            market_snapshots=tuple(
                snapshot("lighter", sample_time, buy=100.0, sell=99.9).model_copy(
                    update={"venue_symbol": symbol, "canonical_symbol": symbol}
                )
                for symbol in ("BTC", "VVV")
            )
        )

    on_cadence = pipeline._on_storage_cadence(batch(NOW), NOW)
    off_cadence = pipeline._on_storage_cadence(
        batch(NOW + timedelta(seconds=10)), NOW + timedelta(seconds=10)
    )

    assert [s.venue_symbol for s in on_cadence.market_snapshots] == ["BTC", "VVV"]
    assert [s.venue_symbol for s in off_cadence.market_snapshots] == ["BTC"]


def test_spread_by_size_reports_thin_books_as_none():
    snapshots = [
        snapshot("lighter", NOW, buy=100.0, sell=99.9, deep=True),
        snapshot("hyperliquid", NOW, buy=100.2, sell=100.1),
    ]
    spreads = spread_by_size(snapshots, KEY)

    assert spreads["1000"] == pytest.approx((100.1 / 100.0 - 1) * 10_000)
    assert spreads["5000"] is None  # Hyperliquid book has no $5k depth
    assert spreads["10000"] is None
    candidates = build_spread_candidates(
        snapshots, NOW, primary_size_usd=1_000, stale_after_seconds=30, fees_bps={},
        require_fees=False,
    )
    assert {c.key for c in candidates} >= {KEY}


def spread_alert_payload(**overrides):
    payload = {
        "canonical_symbol": "VVV",
        "long_venue": "lighter",
        "long_venue_symbol": "VVV",
        "short_venue": "hyperliquid",
        "short_venue_symbol": "VVV",
        "primary_size_usd": 1_000,
        "long_buy_vwap": 100.0,
        "short_sell_vwap": 100.3,
        "raw_spread_bps": 30.0,
        "long_fee_bps": 0.0,
        "short_fee_bps": 4.5,
        "net_spread_bps": 25.5,
        "rolling_mean_bps": 2.0,
        "rolling_std_bps": 1.0,
        "deviation_bps": 28.0,
        "signal_duration_seconds": 120,
        "observed_at_skew_seconds": 0.5,
        "round_trip_fee_bps": 9.0,
        "theoretical_edge_bps": 19.0,
        "sample_time": NOW.isoformat(),
        "candidate_duration_seconds": 30,
        "alert_duration_seconds": 120,
        "funding_context": {"long": None, "short": None},
        "spread_by_size_bps": {"1000": 30.0, "5000": 12.5, "10000": None},
    }
    payload.update(overrides)
    return AlertRequest(monitor="spread", event_id="e", created_at=NOW, payload=payload)


def test_spread_alert_shows_size_context_and_thin_depth():
    details = parse_spread_alert(spread_alert_payload())
    message = format_spread_alert(details, HistoricalSpreadContext.empty())

    assert "各金額原始價差 (bps): $1k +30.00 | $5k +12.50 | $10k 深度不足" in message


def test_spread_alert_without_size_context_omits_the_line():
    payload = spread_alert_payload()
    del payload.payload["spread_by_size_bps"]
    message = format_spread_alert(parse_spread_alert(payload), HistoricalSpreadContext.empty())

    assert "各金額原始價差" not in message


def test_anomaly_alert_shows_size_context():
    request = AlertRequest(
        monitor="spread",
        event_id="e",
        created_at=NOW,
        payload={
            "event_kind": "anomaly_initial",
            "episode_id": "episode",
            "canonical_symbol": "VVV",
            "long_venue": "lighter",
            "long_venue_symbol": "VVV",
            "short_venue": "hyperliquid",
            "short_venue_symbol": "VVV",
            "primary_size_usd": 1_000,
            "sample_time": NOW.isoformat(),
            "candidate_started_at": NOW.isoformat(),
            "confirmed_at": NOW.isoformat(),
            "reference_mean_bps": 2.0,
            "reference_std_bps": 1.0,
            "lifetime_peak_spread_bps": 30.0,
            "lifetime_peak_deviation_bps": 28.0,
            "lifetime_peak_at": NOW.isoformat(),
            "current_spread_bps": 30.0,
            "current_deviation_bps": 28.0,
            "long_buy_vwap": 100.0,
            "short_sell_vwap": 100.3,
            "raw_spread_bps": 30.0,
            "observed_at_skew_seconds": 0.5,
            "spread_by_size_bps": {"1000": 30.0, "5000": None, "10000": None},
        },
    )
    message = format_anomaly_alert(
        parse_anomaly_alert(request), AnomalyConfirmationContext.empty()
    )

    assert "Size bps  $1k +30.00 | $5k thin | $10k thin" in message
