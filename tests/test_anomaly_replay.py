from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from radar.config import MarketConfig, RadarConfig
from radar.history import anomaly_replay
from radar.history.anomaly_replay import (
    ReplayParameters,
    replay_market_data,
    replay_market_data_multi,
)
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
            MarketConfig(
                venue="other",
                venue_symbol="QQQ",
                canonical_symbol="QQQ",
            ),
        ]
    )


def make_multi_symbol_config() -> RadarConfig:
    config = make_config()
    config.markets.extend(
        [
            MarketConfig(
                venue="arcus",
                venue_symbol="ABC-USD",
                canonical_symbol="ABC",
            ),
            MarketConfig(
                venue="lighter_robinhood",
                venue_symbol="ABC",
                canonical_symbol="ABC",
            ),
        ]
    )
    return config


def snapshot(
    *,
    venue: str,
    venue_symbol: str,
    sample_time: datetime,
    raw_spread_bps: float,
    observed_at: datetime | None = None,
) -> MarketSnapshot:
    long_side = venue == "arcus"
    price = 100.0 if long_side else 100.0 + raw_spread_bps / 100.0
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=sample_time if observed_at is None else observed_at,
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


def test_replay_accepts_observation_a_few_milliseconds_after_sample_time(tmp_path):
    observed_at = START + timedelta(milliseconds=5)
    write_market_data(
        tmp_path,
        baseline_snapshots()
        + [
            snapshot(
                venue=venue,
                venue_symbol=symbol,
                sample_time=START,
                observed_at=observed_at,
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
    )

    assert len(result.episodes) == 1
    assert result.episodes[0].episode.confirmation_deviation_bps == pytest.approx(12.0)


def test_replay_keeps_latest_observed_row_for_same_feed_and_sample(tmp_path):
    first_observed_at = START
    latest_observed_at = START + timedelta(milliseconds=5)
    write_market_data(
        tmp_path,
        baseline_snapshots()
        + [
            snapshot(
                venue=venue,
                venue_symbol=symbol,
                sample_time=START,
                observed_at=first_observed_at,
                raw_spread_bps=12.0,
            )
            for venue, symbol in (("arcus", "QQQ-USD"), ("lighter_robinhood", "QQQ"))
        ]
        + [
            snapshot(
                venue=venue,
                venue_symbol=symbol,
                sample_time=START,
                observed_at=latest_observed_at,
                raw_spread_bps=14.0,
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
    )

    assert len(result.episodes) == 1
    assert result.episodes[0].episode.confirmation_deviation_bps == pytest.approx(14.0)


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


def test_exact_direction_filters_prune_unneeded_feeds_before_query(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    def fake_iter_market_rows(**kwargs):
        captured.update(kwargs)
        yield from ()

    monkeypatch.setattr(anomaly_replay, "_iter_market_rows", fake_iter_market_rows)

    replay_market_data(
        data_root=tmp_path / "data",
        config=make_config(),
        start=START,
        end=START + timedelta(seconds=10),
        parameters=ReplayParameters(confirmation_seconds=0),
        symbol="QQQ",
        long_venue="arcus",
        long_venue_symbol="QQQ-USD",
        short_venue="lighter_robinhood",
        short_venue_symbol="QQQ",
    )

    assert captured["feeds_by_symbol"] == {
        "QQQ": (("arcus", "QQQ-USD"), ("lighter_robinhood", "QQQ"))
    }


def test_context_history_derives_all_windows_from_one_bounded_history():
    history = anomaly_replay._ContextHistory()
    for index in range(17_280):
        history.append(
            START - timedelta(hours=48) + timedelta(seconds=10 * index),
            float(index),
        )

    stats_12h = history.stats_before(START, 12 * 60 * 60)
    stats_24h = history.stats_before(START, 24 * 60 * 60)
    stats_48h = history.stats_before(START, 48 * 60 * 60)

    assert stats_12h.sample_count == 4_320
    assert stats_24h.sample_count == 8_640
    assert stats_48h.sample_count == 17_280
    assert stats_12h.mean_bps == pytest.approx(15_119.5)
    assert stats_24h.mean_bps == pytest.approx(12_959.5)
    assert stats_48h.mean_bps == pytest.approx(8_639.5)
    assert stats_12h.min_bps == 12_960.0
    assert stats_12h.max_bps == 17_280.0 - 1.0
    assert stats_12h.eligible
    assert stats_24h.eligible
    assert stats_48h.eligible


def test_context_history_discards_expired_points_without_changing_window_stats():
    history = anomaly_replay._ContextHistory(max_window_seconds=60)
    for index in range(100):
        history.append(START + timedelta(seconds=10 * index), float(index))

    stats = history.stats_before(START + timedelta(seconds=1_000), 60)

    assert history.point_count == 7
    assert stats.sample_count == 6
    assert stats.mean_bps == pytest.approx(96.5)


def test_context_statistics_are_not_computed_without_confirmation(monkeypatch):
    calls: list[int] = []
    original = anomaly_replay._ContextHistory.stats_before

    def record_stats(self, sample_time, window_seconds):
        calls.append(window_seconds)
        return original(self, sample_time, window_seconds)

    monkeypatch.setattr(anomaly_replay._ContextHistory, "stats_before", record_stats)

    def fake_iter_market_rows(**kwargs):
        yield "QQQ", START, {
            ("arcus", "QQQ-USD"): (START, 100.0, 100.0),
            ("lighter_robinhood", "QQQ"): (START, 100.0, 100.0),
        }

    monkeypatch.setattr(anomaly_replay, "_iter_market_rows", fake_iter_market_rows)
    replay_market_data_multi(
        data_root=Path("/unused"),
        config=make_config(),
        start=START,
        end=START + timedelta(seconds=10),
        parameter_sets={"A": ReplayParameters()},
    )

    assert calls == []


def test_multi_configuration_replay_shares_one_stream_and_keeps_trackers_independent(
    monkeypatch,
):
    calls = 0

    def fake_iter_market_rows(**kwargs):
        nonlocal calls
        calls += 1
        for index in range(6_912):
            sample_time = START - timedelta(hours=24) + timedelta(seconds=10 * index)
            yield "QQQ", sample_time, {
                ("arcus", "QQQ-USD"): (sample_time, 100.0, 100.0),
                ("lighter_robinhood", "QQQ"): (sample_time, 100.0, 100.0),
            }
        sample_time = START
        yield "QQQ", sample_time, {
            ("arcus", "QQQ-USD"): (sample_time, 100.0, 100.0),
            ("lighter_robinhood", "QQQ"): (sample_time, 100.0, 101.2),
        }

    monkeypatch.setattr(anomaly_replay, "_iter_market_rows", fake_iter_market_rows)
    result = replay_market_data_multi(
        data_root=Path("/unused"),
        config=make_config(),
        start=START,
        end=START + timedelta(seconds=10),
        parameter_sets={
            "A": ReplayParameters(anomaly_deviation_bps=10.0, confirmation_seconds=0),
            "B": ReplayParameters(anomaly_deviation_bps=150.0, confirmation_seconds=0),
        },
    )

    assert calls == 1
    assert len(result.results["A"].episodes) == 1
    assert result.results["B"].episodes == ()


def test_single_configuration_delegates_to_shared_replay(monkeypatch):
    rows = [
        (
            "QQQ",
            START,
            {
                ("arcus", "QQQ-USD"): (START, 100.0, 100.0),
                ("lighter_robinhood", "QQQ"): (START, 100.0, 101.2),
            },
        )
    ]

    def fake_iter_market_rows(**kwargs):
        yield from rows

    monkeypatch.setattr(anomaly_replay, "_iter_market_rows", fake_iter_market_rows)
    single = replay_market_data(
        data_root=Path("/unused"),
        config=make_config(),
        start=START,
        end=START + timedelta(seconds=10),
        parameters=ReplayParameters(confirmation_seconds=0),
    )
    multi = replay_market_data_multi(
        data_root=__import__("pathlib").Path("/unused"),
        config=make_config(),
        start=START,
        end=START + timedelta(seconds=10),
        parameter_sets={"default": ReplayParameters(confirmation_seconds=0)},
    )

    assert single.episodes == multi.results["default"].episodes


def test_shared_replay_does_not_share_basis_or_tracker_state_between_symbols(
    monkeypatch,
):
    def fake_iter_market_rows(**kwargs):
        for index in range(6_912):
            sample_time = START - timedelta(hours=24) + timedelta(seconds=10 * index)
            yield "QQQ", sample_time, {
                ("arcus", "QQQ-USD"): (sample_time, 100.0, 100.0),
                ("lighter_robinhood", "QQQ"): (sample_time, 100.0, 100.0),
            }
        sample_time = START
        yield "QQQ", sample_time, {
            ("arcus", "QQQ-USD"): (sample_time, 100.0, 100.0),
            ("lighter_robinhood", "QQQ"): (sample_time, 100.0, 101.2),
        }
        yield "ABC", sample_time, {
            ("arcus", "ABC-USD"): (sample_time, 100.0, 100.0),
            ("lighter_robinhood", "ABC"): (sample_time, 100.0, 101.2),
        }

    monkeypatch.setattr(anomaly_replay, "_iter_market_rows", fake_iter_market_rows)
    result = replay_market_data_multi(
        data_root=Path("/unused"),
        config=make_multi_symbol_config(),
        start=START,
        end=START + timedelta(seconds=10),
        parameter_sets={"A": ReplayParameters(confirmation_seconds=0)},
    )

    episodes = result.results["A"].episodes
    assert [episode.episode.pair_key.canonical_symbol for episode in episodes] == [
        "QQQ"
    ]


def test_partition_stream_preserves_order_across_date_boundaries(tmp_path):
    day_one = datetime(2026, 9, 28, 23, 59, 50, tzinfo=UTC)
    day_two = datetime(2026, 9, 29, 0, 0, 0, tzinfo=UTC)
    write_market_data(
        tmp_path,
        [
            snapshot(
                venue=venue,
                venue_symbol=symbol,
                sample_time=sample_time,
                raw_spread_bps=0.0,
            )
            for sample_time in (day_one, day_two)
            for venue, symbol in (("arcus", "QQQ-USD"), ("lighter_robinhood", "QQQ"))
        ],
    )

    rows = list(
        anomaly_replay._iter_market_rows(
            data_root=tmp_path / "data",
            feeds_by_symbol={
                "QQQ": (("arcus", "QQQ-USD"), ("lighter_robinhood", "QQQ"))
            },
            start=day_one,
            end=day_two + timedelta(seconds=10),
        )
    )

    assert [sample_time for _symbol, sample_time, _rows in rows] == [day_one, day_two]
