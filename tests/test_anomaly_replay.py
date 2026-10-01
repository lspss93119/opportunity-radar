from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from radar.config import MarketConfig, RadarConfig
from radar.history.anomaly_replay import ReplayParameters, replay_market_data
from radar.models import MarketSnapshot
from radar.storage.parquet import ParquetStorage


START = datetime(2026, 9, 28, 0, 0, tzinfo=UTC)


def make_config() -> RadarConfig:
    return RadarConfig(
        fees_bps={"arcus": 0.0, "lighter_robinhood": 0.0},
        markets=[
            MarketConfig(
                venue="arcus",
                venue_symbol="QQQ-USD",
                canonical_symbol="QQQ",
            ),
            MarketConfig(
                venue="lighter_robinhood",
                venue_symbol="QQQ",
                canonical_symbol="QQQ",
            ),
        ]
    )


def snapshot(
    *,
    venue: str,
    venue_symbol: str,
    sample_time: datetime,
    raw_spread_bps: float,
) -> MarketSnapshot:
    long_side = venue == "arcus"
    price = 100.0 if long_side else 100.0 + raw_spread_bps / 100.0
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=sample_time,
        venue=venue,
        venue_symbol=venue_symbol,
        canonical_symbol="QQQ",
        best_bid=99.0,
        best_bid_size=200.0,
        best_ask=101.0,
        best_ask_size=200.0,
        buy_1k_vwap=100.0,
        sell_1k_vwap=100.0,
        buy_5k_vwap=100.0,
        sell_5k_vwap=price,
        buy_10k_vwap=100.0,
        sell_10k_vwap=price,
    )


def write_market_data(tmp_path, snapshots: list[MarketSnapshot]) -> None:
    storage = ParquetStorage(tmp_path / "data")
    for item in snapshots:
        storage.append(item)
    storage.flush(now=START)


def baseline_snapshots() -> list[MarketSnapshot]:
    return [
        snapshot(
            venue=venue,
            venue_symbol=symbol,
            sample_time=START - timedelta(hours=24) + timedelta(seconds=10 * index),
            raw_spread_bps=0.0,
        )
        for index in range(6_912)
        for venue, symbol in (("arcus", "QQQ-USD"), ("lighter_robinhood", "QQQ"))
    ]


def test_replay_uses_prior_only_24h_basis_and_exact_pair_identity(tmp_path):
    write_market_data(
        tmp_path,
        baseline_snapshots()
        + [
            snapshot(
                venue="arcus",
                venue_symbol="QQQ-USD",
                sample_time=START,
                raw_spread_bps=12.0,
            ),
            snapshot(
                venue="lighter_robinhood",
                venue_symbol="QQQ",
                sample_time=START,
                raw_spread_bps=12.0,
            ),
        ],
    )

    result = replay_market_data(
        data_root=tmp_path / "data",
        config=make_config(),
        start=START,
        end=START + timedelta(seconds=10),
        parameters=ReplayParameters(
            anomaly_deviation_bps=10.0,
            confirmation_seconds=0,
            return_band_bps=5.0,
            max_gap_seconds=20,
        ),
    )

    assert len(result.episodes) == 1
    episode = result.episodes[0]
    assert episode.pair_key.long_venue == "arcus"
    assert episode.pair_key.short_venue == "lighter_robinhood"
    assert episode.episode.reference_mean_bps == 0.0
    assert episode.episode.confirmation_deviation_bps == pytest.approx(12.0)
    assert episode.confirmation_context.stats_24h.mean_bps == 0.0
    assert episode.confirmation_context.stats_24h.sample_count == 6_912


def test_replay_does_not_interpolate_a_missing_sample_gap(tmp_path):
    write_market_data(
        tmp_path,
        baseline_snapshots()
        + [
            snapshot(
                venue=venue,
                venue_symbol=symbol,
                sample_time=START,
                raw_spread_bps=12.0,
            )
            for venue, symbol in (("arcus", "QQQ-USD"), ("lighter_robinhood", "QQQ"))
        ]
        + [
            snapshot(
                venue=venue,
                venue_symbol=symbol,
                sample_time=START + timedelta(seconds=30),
                raw_spread_bps=12.0,
            )
            for venue, symbol in (("arcus", "QQQ-USD"), ("lighter_robinhood", "QQQ"))
        ],
    )

    result = replay_market_data(
        data_root=tmp_path / "data",
        config=make_config(),
        start=START,
        end=START + timedelta(seconds=40),
        parameters=ReplayParameters(
            anomaly_deviation_bps=10.0,
            confirmation_seconds=0,
            return_band_bps=5.0,
            max_gap_seconds=20,
        ),
    )

    assert len(result.episodes) == 1
    assert result.episodes[0].episode.resolution_reason == "data_gap"
    assert result.episodes[0].episode.ended_at == START


def test_replay_can_filter_exact_direction(tmp_path):
    write_market_data(
        tmp_path,
        baseline_snapshots()
        + [
            snapshot(
                venue=venue,
                venue_symbol=symbol,
                sample_time=START,
                raw_spread_bps=12.0,
            )
            for venue, symbol in (("arcus", "QQQ-USD"), ("lighter_robinhood", "QQQ"))
        ],
    )

    result = replay_market_data(
        data_root=tmp_path / "data",
        config=make_config(),
        start=START,
        end=START + timedelta(seconds=10),
        parameters=ReplayParameters(confirmation_seconds=0),
        long_venue="lighter_robinhood",
        long_venue_symbol="QQQ",
        short_venue="arcus",
        short_venue_symbol="QQQ-USD",
    )

    assert result.episodes == ()
