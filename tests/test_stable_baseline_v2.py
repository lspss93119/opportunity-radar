from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from radar.history.stable_baseline import Pair
from radar.history.stable_baseline_v2 import (
    AnalysisParams,
    AnomalyEpisode,
    BaselineReference,
    DailySeries,
    MarketObservation,
    PairPoint,
    _attribute_pair_slot,
    build_baseline_rows,
    causal_baseline,
    collect_data_quality,
    _classify_observation,
    build_daily_series,
    iter_pair_points,
    _sampling_loss_count,
    build_funnel_rows,
    classify_candidate,
    detect_episodes,
    estimate_cost_scenarios,
    summarize_episodes,
    write_v2_outputs,
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


def test_session_regime_coverage_uses_regime_specific_expected_slots():
    start = datetime(2026, 10, 1, 13, 30, tzinfo=UTC)
    points = [
        pair_point(start + timedelta(minutes=offset), 0.0)
        for offset in range(390)
    ]

    history = build_daily_series(iter(points), params=PARAMS, regime="us_regular")

    assert history[start.date()].sample_count == 390
    assert history[start.date()].coverage == pytest.approx(1.0)


def baseline_reference(value: float = 100.0) -> BaselineReference:
    return BaselineReference(
        baseline_bps=value,
        q90_q10_bps=2.0,
        days_used=7,
        stable=True,
        drift_bps_per_day=0.0,
        days=tuple(date(2026, 9, day) for day in range(20, 27)),
    )


def pair_point(ts: datetime, basis_bps: float) -> PairPoint:
    a = observation(sample_time=ts, observed_at=ts, buy=101.0, sell=99.0)
    b_mid = 100.0 * (1.0 + basis_bps / 10000.0)
    b = observation(
        venue="venue_b",
        sample_time=ts,
        observed_at=ts,
        buy=b_mid + 1.0,
        sell=b_mid - 1.0,
    )
    return PairPoint(sample_time=ts, pair=PAIR, a=a, b=b)


def test_sustained_anomaly_is_one_episode_and_records_convergence_windows():
    start = datetime(2026, 10, 1, 12, tzinfo=UTC)
    points = [
        pair_point(start + timedelta(minutes=offset), 123.0)
        for offset in range(10)
    ]
    points += [
        pair_point(start + timedelta(minutes=10), 100.0),
    ]

    episodes = detect_episodes(
        points,
        baseline_by_day={start.date(): baseline_reference()},
        basis_type="mid",
        threshold_bps=20.0,
        direction=1,
        params=PARAMS,
    )

    assert len(episodes) == 1
    episode = episodes[0]
    assert episode.confirmed is True
    assert episode.status == "converged"
    assert episode.converged_15m is True
    assert episode.converged_1h is True
    assert episode.converged_4h is True
    assert episode.peak_deviation_bps == pytest.approx(23.0, abs=0.1)


def test_anomaly_rearms_only_after_the_prior_episode_is_no_longer_active():
    start = datetime(2026, 10, 1, 12, tzinfo=UTC)
    points = [
        pair_point(start, 121.0),
        pair_point(start + timedelta(minutes=1), 122.0),
        pair_point(start + timedelta(minutes=2), 108.0),
        pair_point(start + timedelta(minutes=3), 125.0),
        pair_point(start + timedelta(minutes=4), 126.0),
        pair_point(start + timedelta(minutes=5), 100.0),
    ]

    episodes = detect_episodes(
        points,
        baseline_by_day={start.date(): baseline_reference()},
        basis_type="mid",
        threshold_bps=20.0,
        direction=1,
        params=PARAMS,
    )

    assert [episode.status for episode in episodes] == ["no_convergence", "converged"]
    assert [episode.confirmed for episode in episodes] == [True, True]


def test_data_gap_censors_episode_and_restored_quote_is_not_convergence():
    start = datetime(2026, 10, 1, 12, tzinfo=UTC)
    points = [
        pair_point(start, 121.0),
        pair_point(start + timedelta(minutes=1), 122.0),
        pair_point(start + timedelta(minutes=4), 100.0),
    ]

    episodes = detect_episodes(
        points,
        baseline_by_day={start.date(): baseline_reference()},
        basis_type="mid",
        threshold_bps=20.0,
        direction=1,
        params=PARAMS,
    )

    assert len(episodes) == 1
    assert episodes[0].status == "censored_data_gap"
    assert episodes[0].converged_15m is None


def test_episode_summary_keeps_unknown_convergence_out_of_success_denominator():
    start = datetime(2026, 10, 1, 12, tzinfo=UTC)
    episodes = [
        AnomalyEpisode(
            pair=PAIR,
            basis_type="mid",
            threshold_bps=20.0,
            direction=1,
            start=start,
            end=start + timedelta(minutes=1),
            baseline_bps=100.0,
            peak_deviation_bps=25.0,
            sample_count=2,
            confirmed=True,
            status="censored_data_gap",
            convergence_seconds=None,
            converged_15m=None,
            converged_1h=None,
            converged_4h=None,
            gap_seconds=180.0,
        ),
        AnomalyEpisode(
            pair=PAIR,
            basis_type="mid",
            threshold_bps=20.0,
            direction=1,
            start=start + timedelta(hours=1),
            end=start + timedelta(hours=1, minutes=5),
            baseline_bps=100.0,
            peak_deviation_bps=25.0,
            sample_count=2,
            confirmed=True,
            status="converged",
            convergence_seconds=300.0,
            converged_15m=True,
            converged_1h=True,
            converged_4h=True,
            gap_seconds=0.0,
        ),
    ]

    summary = summarize_episodes(episodes)

    assert summary["anomaly_episodes"] == 2
    assert summary["converged_15m_known"] == 1
    assert summary["converged_15m_count"] == 1
    assert summary["converged_15m_rate"] == 1.0


def test_cost_scenarios_use_four_fills_and_do_not_treat_funding_as_zero_cost():
    scenarios = estimate_cost_scenarios(
        entry_edge_bps=30.0,
        exit_edge_bps=5.0,
        venue_a="venue_a",
        venue_b="venue_b",
        fees={"venue_a": 1.0, "venue_b": 2.0},
        notionals=(1000.0, 5000.0),
        extra_slippage_bps=(0.0, 2.0),
    )

    no_slip_1k = next(
        row for row in scenarios
        if row["notional_per_leg"] == 1000.0 and row["extra_slippage_bps"] == 0.0
    )
    slip_5k = next(
        row for row in scenarios
        if row["notional_per_leg"] == 5000.0 and row["extra_slippage_bps"] == 2.0
    )
    assert no_slip_1k["gross_bps"] == 25.0
    assert no_slip_1k["fee_bps"] == 6.0
    assert no_slip_1k["slippage_bps"] == 0.0
    assert no_slip_1k["net_bps"] == 19.0
    assert slip_5k["slippage_bps"] == 8.0
    assert slip_5k["funding_included"] is False


def test_candidate_classification_prioritizes_evidence_over_total_pnl():
    assert classify_candidate({
        "valid_sync_samples": 1000,
        "coverage_eligible": True,
        "stable_baseline": True,
        "confirmed_anomalies": 4,
        "observed_convergence": 3,
        "executable_convergence": 2,
        "economically_viable_1000_2bps": 1,
    })["classification"] == "A"
    assert classify_candidate({
        "valid_sync_samples": 1000,
        "coverage_eligible": True,
        "stable_baseline": True,
        "confirmed_anomalies": 1,
        "observed_convergence": 0,
        "executable_convergence": 0,
        "economically_viable_1000_2bps": 0,
    })["classification"] == "B"
    assert classify_candidate({
        "valid_sync_samples": 0,
        "coverage_eligible": False,
        "stable_baseline": False,
        "confirmed_anomalies": 100,
        "observed_convergence": 100,
        "executable_convergence": 100,
        "economically_viable_1000_2bps": 100,
    })["classification"] == "C"
    assert classify_candidate({
        "valid_sync_samples": 1000,
        "coverage_eligible": True,
        "stable_baseline": False,
        "confirmed_anomalies": 10,
        "observed_convergence": 10,
        "executable_convergence": 10,
        "economically_viable_1000_2bps": 10,
    })["classification"] == "D"


def test_funnel_reports_global_stage_counts_and_exclusion_reasons():
    rows = build_funnel_rows([
        {
            "pair_id": "one",
            "configured": True,
            "valid_sync": True,
            "coverage_eligible": True,
            "stable_baseline": True,
            "anomaly_episodes": 2,
            "confirmed_anomalies": 1,
            "observed_convergence": 1,
            "executable_convergence": 1,
            "economically_viable": 0,
            "exclusion_reason": "cost_gate",
        },
        {
            "pair_id": "two",
            "configured": True,
            "valid_sync": False,
            "coverage_eligible": False,
            "stable_baseline": False,
            "anomaly_episodes": 0,
            "confirmed_anomalies": 0,
            "observed_convergence": 0,
            "executable_convergence": 0,
            "economically_viable": 0,
            "exclusion_reason": "skew",
        },
    ])

    valid_row = next(row for row in rows if row["scope"] == "global" and row["stage"] == "valid_synchronized_pairs")
    cost_row = next(row for row in rows if row["scope"] == "global" and row["stage"] == "economically_viable_candidates")
    assert valid_row["candidate_count"] == 1
    assert cost_row["candidate_count"] == 0
    assert cost_row["excluded_count"] == 2
    assert "cost_gate" in cost_row["exclusion_reasons"]


def test_v2_writer_emits_requested_files_with_stable_headers(tmp_path: Path):
    write_v2_outputs(
        tmp_path,
        quality_rows=[{"record_type": "system", "metric": "raw_rows", "count": 1}],
        baseline_rows=[{"window": "7d", "basis_type": "mid", "status": "stable"}],
        episode_rows=[{"record_type": "summary", "threshold_bps": 20.0}],
        funnel_rows=[{"scope": "global", "stage": "configured_pairs", "candidate_count": 1}],
        ranking_rows=[{"classification": "B", "symbol": "TEST"}],
    )

    assert {
        "data_quality_report.csv",
        "baseline_stability.csv",
        "anomaly_convergence.csv",
        "opportunity_funnel.csv",
        "candidate_ranking.csv",
    } <= {path.name for path in tmp_path.iterdir()}


def test_collect_data_quality_attributes_pair_without_sql_alias_error(tmp_path: Path):
    import duckdb

    parquet_path = tmp_path / "quality.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE quotes AS SELECT * FROM (VALUES
              (TIMESTAMPTZ '2026-10-01 12:00:00+00', TIMESTAMPTZ '2026-10-01 12:00:00+00', 'venue_a', 'TEST', 'TEST', 101.0, 99.0),
              (TIMESTAMPTZ '2026-10-01 12:00:00+00', TIMESTAMPTZ '2026-10-01 12:00:00+00', 'venue_b', 'TEST', 'TEST', 102.0, 100.0)
            ) AS t(sample_time, observed_at, venue, venue_symbol, canonical_symbol,
                   buy_1k_vwap, sell_1k_vwap)
            """
        )
        connection.execute("COPY quotes TO ? (FORMAT PARQUET)", [str(parquet_path)])
    finally:
        connection.close()

    _, metrics = collect_data_quality(
        [str(parquet_path)], MARKETS, params=AnalysisParams(),
    )

    assert metrics[PAIR]["valid_sync_samples"] == 1
