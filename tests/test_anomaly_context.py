from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from radar.history.anomaly_replay import _ContextHistory
from radar.history.spread import SpreadHistory
from radar.models import MarketSnapshot
from radar.storage.parquet import ParquetStorage


CONFIRMED = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def _market(
    venue: str,
    sample_time: datetime,
    *,
    observed_at: datetime | None = None,
    buy: float = 100.0,
    sell: float = 101.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=sample_time if observed_at is None else observed_at,
        venue=venue,
        venue_symbol="QQQ-USD" if venue == "arcus" else "QQQ",
        canonical_symbol="QQQ",
        best_bid=99.0,
        best_bid_size=10.0,
        best_ask=100.0,
        best_ask_size=10.0,
        buy_10k_vwap=buy,
        sell_10k_vwap=sell,
    )


def _write_full_context(tmp_path) -> None:
    storage = ParquetStorage(tmp_path / "data")
    start = CONFIRMED - timedelta(hours=48)
    for index in range(48 * 60 * 60 // 10):
        timestamp = start + timedelta(seconds=index * 10)
        storage.append(_market("arcus", timestamp))
        storage.append(_market("lighter_robinhood", timestamp))
    storage.append(_market("arcus", CONFIRMED))
    storage.append(_market("lighter_robinhood", CONFIRMED))
    storage.flush(now=CONFIRMED)


def test_query_anomaly_context_is_prior_only_and_replay_compatible(tmp_path):
    _write_full_context(tmp_path)

    context = SpreadHistory(tmp_path / "data").query_anomaly_context(
        canonical_symbol="QQQ",
        long_venue="arcus",
        long_venue_symbol="QQQ-USD",
        short_venue="lighter_robinhood",
        short_venue_symbol="QQQ",
        primary_size_usd=10_000,
        confirmed_at=CONFIRMED,
        stale_after_seconds=30,
    )

    history = _ContextHistory()
    start = CONFIRMED - timedelta(hours=48)
    for index in range(48 * 60 * 60 // 10):
        history.append(start + timedelta(seconds=index * 10), 100.0)
    replay_12 = history.stats_before(CONFIRMED, 12 * 60 * 60)
    replay_24 = history.stats_before(CONFIRMED, 24 * 60 * 60)
    replay_48 = history.stats_before(CONFIRMED, 48 * 60 * 60)

    assert context.stats_12h.sample_count == replay_12.sample_count
    assert context.stats_24h.sample_count == replay_24.sample_count
    assert context.stats_48h.sample_count == replay_48.sample_count
    assert context.stats_12h.mean_bps == pytest.approx(100.0)
    assert context.stats_12h.eligible is True
    assert context.stats_24h.eligible is True
    assert context.stats_48h.eligible is True


def test_query_anomaly_context_deduplicates_and_rejects_lookahead_and_stale_rows(tmp_path):
    storage = ParquetStorage(tmp_path / "data")
    start = CONFIRMED - timedelta(hours=12)
    for index in range(12 * 60 * 60 // 10):
        timestamp = start + timedelta(seconds=index * 10)
        storage.append(_market("arcus", timestamp))
        storage.append(_market("lighter_robinhood", timestamp))
    duplicate_time = CONFIRMED - timedelta(seconds=10)
    storage.append(
        _market(
            "arcus",
            duplicate_time,
            observed_at=duplicate_time + timedelta(seconds=1),
            buy=99.0,
        )
    )
    storage.append(
        _market(
            "lighter_robinhood",
            duplicate_time,
            observed_at=duplicate_time + timedelta(seconds=31),
            sell=103.0,
        )
    )
    storage.append(_market("arcus", CONFIRMED, buy=50.0))
    storage.append(_market("lighter_robinhood", CONFIRMED, sell=150.0))
    storage.flush(now=CONFIRMED)

    context = SpreadHistory(tmp_path / "data").query_anomaly_context(
        canonical_symbol="QQQ",
        long_venue="arcus",
        long_venue_symbol="QQQ-USD",
        short_venue="lighter_robinhood",
        short_venue_symbol="QQQ",
        primary_size_usd=10_000,
        confirmed_at=CONFIRMED,
        stale_after_seconds=30,
    )

    assert context.stats_12h.sample_count == 4_319
    assert context.stats_12h.eligible is True
    assert context.stats_12h.mean_bps == pytest.approx(100.0)
