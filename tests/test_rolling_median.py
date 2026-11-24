from datetime import UTC, datetime, timedelta
import random
import statistics

import pytest

from radar.monitors.spread.basis import RollingMedian

T0 = datetime(2026, 11, 24, tzinfo=UTC)


def median_basis(window: int = 100, min_obs: int = 3, interval: int = 10) -> RollingMedian:
    return RollingMedian(
        window_seconds=window, min_observations=min_obs, expected_interval_seconds=interval
    )


def at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


def test_median_uses_prior_points_only():
    basis = median_basis()
    for index, value in enumerate([5.0, 1.0, 9.0]):
        basis.observe(at(10 * index), value)

    stats = basis.observe(at(30), 100.0)  # the current point is excluded

    assert stats.mean_bps == 5.0
    assert stats.sample_count == 3
    assert basis.stats_before(at(40)).mean_bps == pytest.approx((5.0 + 9.0) / 2)


def test_window_slides_and_old_points_leave_the_median():
    basis = median_basis(window=30)
    for index, value in enumerate([100.0, 100.0, 1.0, 2.0, 3.0]):
        basis.observe(at(10 * index), value)

    # Window [20, 50): only 1, 2, 3 remain.
    assert basis.stats_before(at(50)).mean_bps == 2.0


def test_eligibility_requires_full_window_coverage():
    basis = median_basis(window=100, min_obs=3)
    for index in range(4):
        basis.observe(at(10 * index), 1.0)

    assert not basis.stats_before(at(40)).eligible  # window not yet covered
    for index in range(4, 12):
        basis.observe(at(10 * index), 1.0)
    assert basis.stats_before(at(120)).eligible


def test_repeated_timestamp_replaces_its_value():
    basis = median_basis()
    basis.observe(at(0), 1.0)
    basis.observe(at(10), 2.0)
    basis.observe(at(10), 50.0)

    assert basis.stats_before(at(20)).mean_bps == pytest.approx((1.0 + 50.0) / 2)


def test_rollback_keeps_sorted_window_consistent():
    basis = median_basis(window=30)
    for index, value in enumerate([4.0, 8.0, 6.0]):
        basis.observe(at(10 * index), value)
    before = basis.stats_before(at(30)).mean_bps

    _stats, mutation = basis.observe_with_rollback(at(40), 100.0)  # also prunes
    mutation.rollback()

    assert basis.stats_before(at(30)).mean_bps == before
    assert basis._sorted == sorted(basis._values.values())


def test_matches_statistics_median_on_random_walk():
    rng = random.Random(7)
    basis = median_basis(window=600, min_obs=1)
    history: list[tuple[int, float]] = []
    for step in range(400):
        value = rng.gauss(0, 10)
        stats = basis.observe(at(10 * step), value)
        window = [v for t, v in history if t >= 10 * step - 600]
        if window:
            assert stats.mean_bps == pytest.approx(statistics.median(window))
        history.append((10 * step, value))


def test_hydrate_rebuilds_the_window():
    basis = median_basis()
    basis.observe(at(0), 999.0)
    basis.hydrate([(at(10), 3.0), (at(20), 1.0), (at(30), 2.0)])

    assert basis.stats_before(at(40)).mean_bps == 2.0
    assert basis._sorted == [1.0, 2.0, 3.0]
