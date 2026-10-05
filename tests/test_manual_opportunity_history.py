from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from radar.history.manual_opportunity import BboRollingHistory


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
