from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from radar.history.stable_baseline import Params, Quote, scan

FEES = {"a": 0.0, "b": 0.0}
MARKETS = {("a", "ONE", "ONE"), ("b", "ONE", "ONE")}
PARAMS = Params(min_baseline_days=2, baseline_days=3, min_daily_coverage=0.001,
                deviation_bps=20.0, stable_range_bps=12.0, min_expected_net_bps=1.0)


def quote(ts, venue, basis_bps=100.0, age=0, spread=.002):
    mid = 100 if venue == "a" else 100 * (1 + basis_bps / 10000)
    return Quote(ts, ts - timedelta(seconds=age), venue, "ONE", "ONE", mid + spread/2, mid - spread/2)


def rows(*, anomaly=155.0, baseline=100.0, bad_skew=False):
    all_rows = []
    start = datetime(2026, 10, 1, 12, tzinfo=UTC)
    for d in range(2):
        for m in range(3):
            ts = start + timedelta(days=d, minutes=m)
            all_rows += [quote(ts, "a"), quote(ts, "b", baseline + d * 0.1)]
    for m, spread_bps in enumerate([anomaly, anomaly, anomaly, baseline + 1, baseline + 1]):
        ts = start + timedelta(days=2, minutes=m)
        all_rows += [quote(ts, "a"), quote(ts, "b", spread_bps, age=7 if bad_skew else 0)]
    return sorted(all_rows, key=lambda x: (x.sample_time, x.venue))


def run(data, params=PARAMS, fees=FEES):
    return scan(lambda: iter(data), configured=MARKETS, fees=fees, params=params)


def test_prior_completed_days_only_and_next_tick_execution():
    result = run(rows())
    assert result["summary"]["configured_pairs"] == 1
    assert result["summary"]["closed_trades"] == 1
    trade, = result["trades"]
    day3 = datetime(2026, 10, 3, 12, tzinfo=UTC)
    assert trade["entry_utc"] == (day3 + timedelta(minutes=2)).isoformat()
    assert trade["exit_utc"] == (day3 + timedelta(minutes=4)).isoformat()
    assert trade["exit_reason"] == "converged"
    assert trade["net_usd"] > 0
    assert trade["reference_bps"] == pytest.approx(100.05, abs=.1)


def test_skew_drops_fake_arbitrage():
    result = run(rows(bad_skew=True))
    assert result["summary"]["closed_trades"] == 0


def test_baseline_drift_ineligible():
    result = run(rows(baseline=100, anomaly=155), params=Params(
        min_baseline_days=2, baseline_days=3, min_daily_coverage=.001,
        stable_range_bps=0.01))
    assert result["summary"]["closed_trades"] == 0


def test_missing_fee_fail_closed():
    with pytest.raises(ValueError, match="missing or invalid venue fee"):
        run(rows(), fees={"a": 0.0})


def test_slippage_and_fee_are_charged_at_four_fills():
    no_fees, = run(rows())["trades"]
    params = Params(min_baseline_days=2, baseline_days=3, min_daily_coverage=.001,
                    min_expected_net_bps=1, extra_slippage_bps_per_order=1)
    with_cost, = run(rows(), params=params, fees={"a": 1.0, "b": 1.0})["trades"]
    assert with_cost["fee_usd"] == pytest.approx(.4, abs=.01)
    assert with_cost["slippage_usd"] == pytest.approx(.4)
    assert no_fees["net_usd"] - with_cost["net_usd"] == pytest.approx(.8, abs=.01)


def test_all_configured_pairs_reported_even_without_quotes():
    other = {("a", "TWO", "TWO"), ("b", "TWO", "TWO")}
    result = scan(lambda: iter(rows()), configured=MARKETS | other, fees=FEES, params=PARAMS)
    assert result["summary"]["configured_pairs"] == 2
    assert next(r for r in result["pairs"] if r["symbol"] == "TWO")["status"] == "insufficient_history"


def test_forward_fill_not_invented_on_gap():
    data = rows()
    gap_minute = datetime(2026, 10, 3, 12, 1, tzinfo=UTC)
    assert run([x for x in data if x.sample_time != gap_minute])["summary"]["closed_trades"] == 0
