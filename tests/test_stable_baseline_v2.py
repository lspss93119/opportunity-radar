from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from radar.history.stable_baseline import Pair
from radar.history.stable_baseline_v2 import (
    AnalysisParams,
    DailySeries,
    MarketObservation,
    PairPoint,
    _attribute_pair_slot,
    build_baseline_rows,
    causal_baseline,
    _classify_observation,
    build_daily_series,
    iter_pair_points,
    _sampling_loss_count,
    us_session_regime,
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


def daily(day: date, values: list[float], coverage: float = 1.0) -> DailySeries:
    return DailySeries(
        day=day,
        values={
            "mid": tuple(values),
            "exec_buy_a_sell_b": tuple(values),
            "exec_buy_b_sell_a": tuple(values),
        },
        sample_count=1440,
        coverage=coverage,
    )


def test_causal_baseline_uses_only_completed_days_before_today():
    history = {
        date(2026, 10, 1): daily(date(2026, 10, 1), [100.0]),
        date(2026, 10, 2): daily(date(2026, 10, 2), [102.0]),
        date(2026, 10, 3): daily(date(2026, 10, 3), [1000.0]),
    }

    reference = causal_baseline(
        history,
        today=date(2026, 10, 3),
        basis_type="mid",
        lookback_days=7,
        min_days=2,
        use_daily_medians=True,
        stable_range_bps=12.0,
    )

    assert reference is not None
    assert reference.baseline_bps == 101.0
    assert reference.days_used == 2
    assert reference.stable is True


def test_build_baseline_rows_reports_drift_and_insufficient_regime_history():
    history = {
        date(2026, 10, 1): daily(date(2026, 10, 1), [100.0]),
        date(2026, 10, 2): daily(date(2026, 10, 2), [101.0]),
        date(2026, 10, 3): daily(date(2026, 10, 3), [102.0]),
        date(2026, 10, 4): daily(date(2026, 10, 4), [103.0]),
        date(2026, 10, 5): daily(date(2026, 10, 5), [104.0]),
        date(2026, 10, 6): daily(date(2026, 10, 6), [105.0]),
    }

    rows = build_baseline_rows(PAIR, history, params=PARAMS, regime="us_regular")
    seven_day = next(row for row in rows if row["window"] == "7d" and row["basis_type"] == "mid")

    assert seven_day["status"] == "stable"
    assert seven_day["eligible_windows"] == 1
    assert seven_day["median_drift_bps_per_day"] > 0

    insufficient = build_baseline_rows(
        PAIR,
        {date(2026, 10, 1): daily(date(2026, 10, 1), [100.0])},
        params=PARAMS,
        regime="us_closed",
    )
    assert {row["status"] for row in insufficient} == {"insufficient_data"}


def test_us_session_regime_handles_regular_extended_and_closed_hours():
    assert us_session_regime(datetime(2026, 10, 1, 14, tzinfo=UTC)) == "us_regular"
    assert us_session_regime(datetime(2026, 10, 1, 12, tzinfo=UTC)) == "us_extended"
    assert us_session_regime(datetime(2026, 10, 3, 14, tzinfo=UTC)) == "us_closed"
