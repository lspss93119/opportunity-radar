from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from radar.history.manual_opportunity import (
    BboRollingHistory,
    ManualOpportunityHistory,
    load_recent_bbo_history,
)
from radar.monitors.spread.models import SpreadPairKey


START = datetime(2026, 9, 28, tzinfo=UTC)


def test_bbo_history_means_are_strictly_prior_and_use_requested_windows():
    history = BboRollingHistory(
        windows_seconds={"2h": 20, "24h": 30, "3d": 40},
        expected_interval_seconds=10,
        minimum_coverage=0.0,
    )
    for index, value in enumerate((1.0, 2.0, 3.0)):
        stats = history.observe(START + timedelta(seconds=index * 10), value)
    assert stats["2h"].mean_bps == pytest.approx(1.5)
    assert stats["24h"].mean_bps == pytest.approx(1.5)
    assert stats["3d"].mean_bps == pytest.approx(1.5)


def test_bbo_history_marks_insufficient_coverage_unavailable():
    history = BboRollingHistory(
        windows_seconds={"2h": 100, "24h": 100, "3d": 100},
        expected_interval_seconds=10,
        minimum_coverage=0.8,
    )
    stats = history.observe(START, 10.0)
    assert stats["2h"].available is False
    assert stats["24h"].available is False
    assert stats["3d"].available is False


def test_bbo_history_mutation_can_restore_the_exact_previous_state():
    history = BboRollingHistory(
        windows_seconds={"2h": 20, "24h": 30, "3d": 40},
        expected_interval_seconds=10,
        minimum_coverage=0.0,
    )
    points = [
        (START, 1.0),
        (START + timedelta(seconds=10), 2.0),
        (START + timedelta(seconds=20), 3.0),
    ]
    history.hydrate(points)
    expected = BboRollingHistory(
        windows_seconds={"2h": 20, "24h": 30, "3d": 40},
        expected_interval_seconds=10,
        minimum_coverage=0.0,
    )
    expected.hydrate(points)

    _stats, mutation = history.observe_with_rollback(
        START + timedelta(seconds=30), 4.0
    )
    mutation.rollback()

    assert history._last_sample_time == expected._last_sample_time
    assert history._points == expected._points
    assert history._sums == expected._sums


def _market_schema() -> pa.Schema:
    timestamp = pa.timestamp("us", tz="UTC")
    return pa.schema(
        [
            pa.field("sample_time", timestamp),
            pa.field("observed_at", timestamp),
            pa.field("venue", pa.string()),
            pa.field("venue_symbol", pa.string()),
            pa.field("canonical_symbol", pa.string()),
            pa.field("best_bid", pa.float64()),
            pa.field("best_ask", pa.float64()),
        ]
    )


def _write_market_rows(data_root: Path, rows: list[dict[str, object]]) -> Path:
    partition = data_root / "market" / "date=2026-10-05"
    partition.mkdir(parents=True)
    path = partition / "part-0000.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=_market_schema()), path)
    return path


def test_load_recent_bbo_history_is_streaming_prior_only_and_route_isolated(tmp_path):
    as_of = datetime(2026, 10, 5, 0, 0, 30, tzinfo=UTC)
    t0 = datetime(2026, 10, 5, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=10)
    t2 = t0 + timedelta(seconds=20)
    t3 = as_of
    rows = [
        {
            "sample_time": t0,
            "observed_at": t0 - timedelta(seconds=1),
            "venue": "arcus",
            "venue_symbol": "QQQ-USD",
            "canonical_symbol": "QQQ",
            "best_bid": 99.0,
            "best_ask": 100.0,
        },
        {
            "sample_time": t0,
            "observed_at": t0,
            "venue": "arcus",
            "venue_symbol": "QQQ-USD",
            "canonical_symbol": "QQQ",
            "best_bid": 99.0,
            "best_ask": 101.0,
        },
        {
            "sample_time": t0,
            "observed_at": t0,
            "venue": "lighter_robinhood",
            "venue_symbol": "QQQ",
            "canonical_symbol": "QQQ",
            "best_bid": 101.5,
            "best_ask": 102.0,
        },
        {
            "sample_time": t1,
            "observed_at": as_of + timedelta(seconds=1),
            "venue": "arcus",
            "venue_symbol": "QQQ-USD",
            "canonical_symbol": "QQQ",
            "best_bid": 99.0,
            "best_ask": 101.0,
        },
        {
            "sample_time": t1,
            "observed_at": t1,
            "venue": "lighter_robinhood",
            "venue_symbol": "QQQ",
            "canonical_symbol": "QQQ",
            "best_bid": 101.5,
            "best_ask": 102.0,
        },
        {
            "sample_time": t2,
            "observed_at": t2,
            "venue": "arcus",
            "venue_symbol": "QQQ-USD",
            "canonical_symbol": "QQQ",
            "best_bid": 99.0,
            "best_ask": 100.0,
        },
        {
            "sample_time": t2,
            "observed_at": t2,
            "venue": "lighter_robinhood",
            "venue_symbol": "QQQ",
            "canonical_symbol": "QQQ",
            "best_bid": 101.0,
            "best_ask": 102.0,
        },
        {
            "sample_time": t3,
            "observed_at": t3,
            "venue": "arcus",
            "venue_symbol": "QQQ-USD",
            "canonical_symbol": "QQQ",
            "best_bid": 99.0,
            "best_ask": 100.0,
        },
        {
            "sample_time": t3,
            "observed_at": t3,
            "venue": "lighter_robinhood",
            "venue_symbol": "QQQ",
            "canonical_symbol": "QQQ",
            "best_bid": 101.0,
            "best_ask": 102.0,
        },
        {
            "sample_time": t0,
            "observed_at": t0,
            "venue": "disabled",
            "venue_symbol": "QQQ",
            "canonical_symbol": "QQQ",
            "best_bid": 200.0,
            "best_ask": 201.0,
        },
    ]
    path = _write_market_rows(tmp_path / "data", rows)
    before = hashlib.sha256(path.read_bytes()).digest()
    long_key = SpreadPairKey("QQQ", "arcus", "QQQ-USD", "lighter_robinhood", "QQQ")
    reverse_key = SpreadPairKey(
        "QQQ", "lighter_robinhood", "QQQ", "arcus", "QQQ-USD"
    )

    result = load_recent_bbo_history(
        tmp_path / "data",
        allowed_feeds={
            ("arcus", "QQQ-USD", "QQQ"),
            ("lighter_robinhood", "QQQ", "QQQ"),
        },
        as_of=as_of,
    )

    assert list(result) == [long_key, reverse_key]
    assert [point[0] for point in result[long_key]] == [t0, t2]
    assert result[long_key][0][1] == pytest.approx(
        (101.5 / 101.0 - 1.0) * 10_000.0
    )
    assert result[reverse_key][0][1] == pytest.approx(
        (99.0 / 102.0 - 1.0) * 10_000.0
    )
    assert all(timestamp < as_of for points in result.values() for timestamp, _ in points)
    assert hashlib.sha256(path.read_bytes()).digest() == before


def test_manual_opportunity_history_queries_exact_route_without_lookahead(tmp_path):
    as_of = datetime(2026, 10, 5, 0, 0, 30, tzinfo=UTC)
    t0 = datetime(2026, 10, 5, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=10)
    rows = [
        {
            "sample_time": t0,
            "observed_at": t0,
            "venue": "arcus",
            "venue_symbol": "QQQ-USD",
            "canonical_symbol": "QQQ",
            "best_bid": 99.0,
            "best_ask": 100.0,
        },
        {
            "sample_time": t0,
            "observed_at": t0,
            "venue": "lighter_robinhood",
            "venue_symbol": "QQQ",
            "canonical_symbol": "QQQ",
            "best_bid": 101.0,
            "best_ask": 102.0,
        },
        {
            "sample_time": t1,
            "observed_at": as_of + timedelta(seconds=1),
            "venue": "arcus",
            "venue_symbol": "QQQ-USD",
            "canonical_symbol": "QQQ",
            "best_bid": 99.0,
            "best_ask": 100.0,
        },
        {
            "sample_time": t1,
            "observed_at": t1,
            "venue": "lighter_robinhood",
            "venue_symbol": "QQQ",
            "canonical_symbol": "QQQ",
            "best_bid": 101.0,
            "best_ask": 102.0,
        },
    ]
    path = _write_market_rows(tmp_path / "data", rows)
    before = hashlib.sha256(path.read_bytes()).digest()

    result = ManualOpportunityHistory(tmp_path / "data").query(
        canonical_symbol="QQQ",
        long_venue="arcus",
        long_venue_symbol="QQQ-USD",
        short_venue="lighter_robinhood",
        short_venue_symbol="QQQ",
        as_of=as_of,
    )

    assert [point.sample_time for point in result.points] == [t0]
    assert result.points[0].raw_spread_bps == pytest.approx(100.0)
    assert hashlib.sha256(path.read_bytes()).digest() == before
