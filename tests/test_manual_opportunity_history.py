from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta
import hashlib
import math
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from radar.history.manual_opportunity import (
    BboHistoryArrays,
    BboRollingHistory,
    ManualOpportunityHistory,
    load_recent_bbo_history,
)
from radar.monitors.spread.models import SpreadPairKey


START = datetime(2026, 9, 28, tzinfo=UTC)


class _ReferenceBboRollingHistory:
    """The pre-optimization deque implementation used as an oracle."""

    def __init__(
        self,
        *,
        windows_seconds: dict[str, int],
        expected_interval_seconds: int,
        minimum_coverage: float,
    ) -> None:
        self.windows_seconds = dict(windows_seconds)
        self.expected_interval_seconds = expected_interval_seconds
        self.minimum_coverage = minimum_coverage
        self._points = {name: deque() for name in windows_seconds}
        self._sums = {name: 0.0 for name in windows_seconds}
        self._last_sample_time: datetime | None = None

    def hydrate(self, points: list[tuple[datetime, float]]) -> None:
        self._points = {name: deque() for name in self.windows_seconds}
        self._sums = {name: 0.0 for name in self.windows_seconds}
        self._last_sample_time = None
        values = {timestamp: float(value) for timestamp, value in points}
        ordered = sorted(values.items())
        if not ordered:
            return
        latest_timestamp = ordered[-1][0]
        for name, window_seconds in self.windows_seconds.items():
            cutoff = latest_timestamp - timedelta(seconds=window_seconds)
            for timestamp, value in ordered:
                if timestamp >= cutoff:
                    self._points[name].append((timestamp, value))
                    self._sums[name] += value
        self._last_sample_time = latest_timestamp

    def observe_with_rollback(self, sample_time: datetime, value: float):
        if self._last_sample_time is not None:
            assert sample_time >= self._last_sample_time
        previous_last = self._last_sample_time
        previous_sums = self._sums.copy()
        removed: dict[str, tuple[datetime, float] | None] = {}
        pruned: dict[str, tuple[tuple[datetime, float], ...]] = {}
        results = {}
        for name, window_seconds in self.windows_seconds.items():
            points = self._points[name]
            removed[name] = (
                points.pop() if points and points[-1][0] == sample_time else None
            )
            cutoff = sample_time - timedelta(seconds=window_seconds)
            expired = []
            while points and points[0][0] < cutoff:
                expired.append(points.popleft())
                self._sums[name] -= expired[-1][1]
            pruned[name] = tuple(expired)
            count = len(points)
            expected_slots = window_seconds / self.expected_interval_seconds
            coverage = min(1.0, count / expected_slots)
            mean = self._sums[name] / count if count else None
            oldest = points[0][0] if points else None
            required_count = math.ceil(expected_slots * self.minimum_coverage)
            covers_start = self.minimum_coverage == 0 or (
                oldest is not None and oldest <= cutoff
            )
            available = mean is not None and count >= required_count and covers_start
            results[name] = (count, coverage, mean if available else None, available)
        for name in self.windows_seconds:
            self._points[name].append((sample_time, value))
            self._sums[name] += value
        self._last_sample_time = sample_time

        def rollback() -> None:
            for name in self.windows_seconds:
                points = self._points[name]
                points.pop()
                expired = pruned[name]
                points.extendleft(reversed(expired))
                if removed[name] is not None:
                    points.append(removed[name])
            self._sums = previous_sums.copy()
            self._last_sample_time = previous_last

        return results, rollback


def _stats_tuple(stats) -> tuple[object, ...]:
    return tuple(
        (item.sample_count, item.coverage, item.mean_bps, item.available)
        for item in stats.values()
    )


def _assert_stats_equal(actual, expected) -> None:
    assert list(actual) == list(expected)
    for name in actual:
        actual_item = actual[name]
        expected_item = expected[name]
        assert actual_item.sample_count == expected_item[0]
        assert actual_item.coverage == pytest.approx(expected_item[1])
        if expected_item[2] is None:
            assert actual_item.mean_bps is None
        else:
            assert actual_item.mean_bps == pytest.approx(expected_item[2])
        assert actual_item.available == expected_item[3]


def test_bbo_history_randomized_equivalence_to_pre_optimization_reference():
    import random

    windows = {"2h": 200, "24h": 500, "3d": 900}
    for seed in range(3):
        rng = random.Random(seed)
        points = [
            (START + timedelta(seconds=offset), rng.uniform(-50, 50))
            for offset in (0, 10, 10, 40, 100, 800)
        ]
        optimized = BboRollingHistory(
            windows_seconds=windows,
            expected_interval_seconds=10,
            minimum_coverage=0.6,
        )
        reference = _ReferenceBboRollingHistory(
            windows_seconds=windows,
            expected_interval_seconds=10,
            minimum_coverage=0.6,
        )
        optimized.hydrate(points)
        reference.hydrate(points)
        for _ in range(1_300):
            timestamp = (reference._last_sample_time or START) + timedelta(
                seconds=rng.choice((0, 0, 10, 20, 100))
            )
            value = rng.uniform(-100, 100)
            actual, actual_mutation = optimized.observe_with_rollback(timestamp, value)
            expected, rollback = reference.observe_with_rollback(timestamp, value)
            _assert_stats_equal(actual, expected)
            if rng.random() < 0.23:
                actual_mutation.rollback()
                rollback()
                assert optimized._last_sample_time == reference._last_sample_time
                assert list(optimized._points["3d"]) == list(reference._points["3d"])


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


def test_bbo_history_hydrate_bounds_each_window_to_latest_point():
    history = BboRollingHistory(
        windows_seconds={"2h": 20, "24h": 40, "3d": 60},
        expected_interval_seconds=10,
        minimum_coverage=0.0,
    )
    points = [
        (START + timedelta(seconds=offset), float(offset))
        for offset in range(0, 70, 10)
    ]

    history.hydrate(points)

    assert [timestamp for timestamp, _value in history._points["2h"]] == [
        START + timedelta(seconds=offset) for offset in (40, 50, 60)
    ]
    assert [timestamp for timestamp, _value in history._points["24h"]] == [
        START + timedelta(seconds=offset) for offset in (20, 30, 40, 50, 60)
    ]
    assert [timestamp for timestamp, _value in history._points["3d"]] == [
        START + timedelta(seconds=offset) for offset in range(0, 70, 10)
    ]


def test_bbo_history_bounded_hydration_matches_legacy_after_first_observation():
    points = [
        (START + timedelta(seconds=offset), float(offset * 2))
        for offset in range(0, 70, 10)
    ]
    legacy = BboRollingHistory(
        windows_seconds={"2h": 20, "24h": 40, "3d": 60},
        expected_interval_seconds=10,
        minimum_coverage=0.8,
    )
    optimized = BboRollingHistory(
        windows_seconds={"2h": 20, "24h": 40, "3d": 60},
        expected_interval_seconds=10,
        minimum_coverage=0.8,
    )
    values = {timestamp: value for timestamp, value in points}
    for timestamp, value in sorted(values.items()):
        legacy._append(timestamp, value)
    optimized.hydrate(points)

    expected = legacy.observe(START + timedelta(seconds=70), 140.0)
    actual = optimized.observe(START + timedelta(seconds=70), 140.0)

    assert actual == expected
    assert optimized._points == legacy._points
    assert optimized._sums == pytest.approx(legacy._sums)
    assert optimized._last_sample_time == legacy._last_sample_time


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

    _stats, mutation = history.observe_with_rollback(START + timedelta(seconds=30), 4.0)
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
    reverse_key = SpreadPairKey("QQQ", "lighter_robinhood", "QQQ", "arcus", "QQQ-USD")

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
    assert result[long_key][0][1] == pytest.approx((101.5 / 101.0 - 1.0) * 10_000.0)
    assert result[reverse_key][0][1] == pytest.approx((99.0 / 102.0 - 1.0) * 10_000.0)
    assert isinstance(result[long_key], BboHistoryArrays)
    assert result[long_key].timestamps.typecode == "q"
    assert result[long_key].values.typecode == "d"
    assert all(
        timestamp < as_of for points in result.values() for timestamp, _ in points
    )
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
