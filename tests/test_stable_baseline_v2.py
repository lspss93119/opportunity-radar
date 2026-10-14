from __future__ import annotations

from datetime import UTC, datetime, timedelta

from radar.history.stable_baseline import Pair
from radar.history.stable_baseline_v2 import (
    AnalysisParams,
    MarketObservation,
    PairPoint,
    _attribute_pair_slot,
    _classify_observation,
    iter_pair_points,
    _sampling_loss_count,
)


NOW = datetime(2026, 10, 1, 12, tzinfo=UTC)
PAIR = Pair("TEST", "venue_a", "TEST", "venue_b", "TEST")
MARKETS = {
    ("venue_a", "TEST", "TEST"),
    ("venue_b", "TEST", "TEST"),
}
PARAMS = AnalysisParams()


def observation(*, venue: str = "venue_a", sample_time: datetime = NOW,
                observed_at: datetime = NOW, buy: float = 101.0,
                sell: float = 99.0) -> MarketObservation:
    return MarketObservation(
        sample_time=sample_time,
        observed_at=observed_at,
        venue=venue,
        venue_symbol="TEST",
        symbol="TEST",
        buy=buy,
        sell=sell,
    )


def test_classify_observation_separates_invalid_vwap_and_staleness():
    assessment = _classify_observation(
        observation(observed_at=NOW - timedelta(seconds=13), buy=0.0)
    )

    assert assessment.valid is False
    assert assessment.reasons == ("stale", "invalid_vwap")


def test_classify_observation_rejects_observation_after_sample():
    assessment = _classify_observation(
        observation(observed_at=NOW + timedelta(seconds=1))
    )

    assert assessment.valid is False
    assert assessment.reasons == ("observed_after_sample",)


def test_pair_slot_attributes_missing_leg_and_sample_time_mismatch():
    missing = _attribute_pair_slot(
        PairPoint(sample_time=NOW, pair=PAIR, a=observation(), b=None), PARAMS
    )
    mismatch = _attribute_pair_slot(
        PairPoint(
            sample_time=NOW,
            pair=PAIR,
            a=observation(),
            b=observation(
                venue="venue_b", sample_time=NOW + timedelta(seconds=10),
            ),
        ),
        PARAMS,
    )

    assert missing.valid_sync is False
    assert missing.reasons == ("missing_b",)
    assert mismatch.valid_sync is False
    assert mismatch.reasons == ("sample_time_mismatch",)


def test_pair_slot_attributes_observed_at_skew_without_calling_it_market_instability():
    assessment = _attribute_pair_slot(
        PairPoint(
            sample_time=NOW,
            pair=PAIR,
            a=observation(),
            b=observation(
                venue="venue_b", observed_at=NOW - timedelta(seconds=4),
            ),
        ),
        PARAMS,
    )

    assert assessment.valid_sync is False
    assert assessment.reasons == ("observed_at_skew",)
    assert assessment.skew_seconds == 4.0


def test_sampling_loss_counts_valid_ten_second_slots_not_seen_at_sixty_seconds():
    ten_second = {NOW + timedelta(seconds=offset) for offset in (0, 10, 20, 30, 40, 50)}
    sixty_second = {NOW}

    assert _sampling_loss_count(ten_second, sixty_second) == 5


def test_iter_pair_points_keeps_latest_duplicate_observation(tmp_path):
    import duckdb

    parquet_path = tmp_path / "market.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE quotes AS SELECT * FROM (VALUES
              (TIMESTAMPTZ '2026-10-01 12:00:00+00', TIMESTAMPTZ '2026-10-01 11:59:59+00', 'venue_a', 'TEST', 'TEST', 101.0, 99.0),
              (TIMESTAMPTZ '2026-10-01 12:00:00+00', TIMESTAMPTZ '2026-10-01 12:00:00+00', 'venue_a', 'TEST', 'TEST', 102.0, 100.0),
              (TIMESTAMPTZ '2026-10-01 12:00:00+00', TIMESTAMPTZ '2026-10-01 11:59:58+00', 'venue_b', 'TEST', 'TEST', 101.0, 99.0)
            ) AS t(sample_time, observed_at, venue, venue_symbol, canonical_symbol,
                   buy_1k_vwap, sell_1k_vwap)
            """
        )
        connection.execute("COPY quotes TO ? (FORMAT PARQUET)", [str(parquet_path)])
    finally:
        connection.close()

    points = list(
        iter_pair_points(
            [str(parquet_path)],
            configured=MARKETS,
            size_usd=1000,
            sample_seconds=60,
        )
    )

    assert len(points) == 1
    assert points[0].a is not None
    assert points[0].a.observed_at == NOW
    assert points[0].a.buy == 102.0
