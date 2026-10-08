from copy import deepcopy
from datetime import UTC, datetime, timedelta
import math

import pytest

from radar.monitors.spread.basis import RollingBasis, RollingBasisStats


START = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def _state(basis: RollingBasis) -> tuple[object, ...]:
    return (
        tuple(basis._times),
        dict(basis._values),
        basis._sum,
        basis._sum_squares,
        basis._last_sample_time,
    )


def _hydrated_basis(*, window_seconds: int = 40) -> RollingBasis:
    basis = RollingBasis(
        window_seconds=window_seconds,
        min_observations=1,
        expected_interval_seconds=10,
    )
    basis.hydrate(
        [
            (START + timedelta(seconds=offset), float(offset + 1))
            for offset in range(0, 50, 10)
        ]
    )
    return basis


def test_observe_with_rollback_restores_append_and_is_idempotent():
    basis = _hydrated_basis()
    before = _state(basis)

    stats, mutation = basis.observe_with_rollback(START + timedelta(seconds=50), 99.0)

    assert stats.sample_count == 4
    assert basis.sample_count == 5
    mutation.rollback()
    mutation.rollback()

    assert _state(basis) == before


def test_rollback_restores_pruned_points_and_appended_value():
    basis = _hydrated_basis(window_seconds=30)
    before = _state(basis)

    _stats, mutation = basis.observe_with_rollback(START + timedelta(seconds=50), 99.0)

    assert tuple(basis._times) == tuple(
        START + timedelta(seconds=offset) for offset in (20, 30, 40, 50)
    )
    mutation.rollback()

    assert _state(basis) == before


def test_rollback_restores_multiple_pruned_points_in_chronological_order():
    basis = _hydrated_basis(window_seconds=20)
    before = _state(basis)

    _stats, mutation = basis.observe_with_rollback(START + timedelta(seconds=50), 99.0)

    assert tuple(basis._times) == tuple(
        START + timedelta(seconds=offset) for offset in (30, 40, 50)
    )
    mutation.rollback()

    assert _state(basis) == before


def test_rollback_restores_duplicate_timestamp_value_and_sums():
    basis = RollingBasis(window_seconds=40, min_observations=1)
    basis.hydrate(
        [
            (START, 10.0),
            (START + timedelta(seconds=10), 20.0),
            (START + timedelta(seconds=20), 30.0),
        ]
    )
    before = _state(basis)

    stats, mutation = basis.observe_with_rollback(START + timedelta(seconds=20), 99.0)

    assert stats.sample_count == 2
    assert basis._values[START + timedelta(seconds=20)] == 99.0
    mutation.rollback()

    assert _state(basis) == before


def test_failed_observation_leaves_basis_unchanged():
    basis = _hydrated_basis()
    before = _state(basis)

    with pytest.raises(ValueError, match="sample_time must not move backwards"):
        basis.observe_with_rollback(START - timedelta(seconds=1), 99.0)

    assert _state(basis) == before


def test_hydrate_observe_with_rollback_matches_legacy_observe():
    points = [
        (START + timedelta(seconds=offset), float(offset * 3 - 2))
        for offset in range(0, 50, 10)
    ]
    legacy = RollingBasis(window_seconds=30, min_observations=1)
    optimized = deepcopy(legacy)
    legacy.hydrate(points)
    optimized.hydrate(points)

    legacy_stats = legacy.observe(START + timedelta(seconds=50), 17.5)
    optimized_stats, mutation = optimized.observe_with_rollback(
        START + timedelta(seconds=50), 17.5
    )

    assert optimized_stats == legacy_stats
    assert _state(optimized) == _state(legacy)
    mutation.rollback()
    assert optimized.sample_count == 5


def test_rollback_rejects_intervening_mutation():
    basis = _hydrated_basis()
    _stats, mutation = basis.observe_with_rollback(START + timedelta(seconds=50), 99.0)
    basis.observe_with_rollback(START + timedelta(seconds=60), 100.0)

    with pytest.raises(RuntimeError, match="changed before rollback"):
        mutation.rollback()


def test_mutation_journal_matches_reference_over_more_than_one_rolling_window():
    window_seconds = 24 * 60 * 60
    interval_seconds = 10
    min_observations = 4
    initial = [
        (
            START - timedelta(seconds=(8_640 - index) * interval_seconds),
            100.0 + (index % 13) * 0.25,
        )
        for index in range(8_640)
    ]
    optimized = RollingBasis(
        window_seconds=window_seconds,
        min_observations=min_observations,
        expected_interval_seconds=interval_seconds,
    )
    optimized.hydrate(initial)
    reference_times = [timestamp for timestamp, _value in initial]
    reference_values = {timestamp: value for timestamp, value in initial}
    reference_sum = sum(reference_values.values())
    reference_sum_squares = sum(value * value for value in reference_values.values())
    reference_last = reference_times[-1]

    def reference_observe(
        timestamp: datetime,
        value: float,
    ) -> RollingBasisStats:
        nonlocal reference_sum, reference_sum_squares, reference_last
        if timestamp < reference_last:
            raise ValueError("sample_time must not move backwards")
        cutoff = timestamp - timedelta(seconds=window_seconds)
        while reference_times and reference_times[0] < cutoff:
            old_timestamp = reference_times.pop(0)
            old_value = reference_values.pop(old_timestamp)
            reference_sum -= old_value
            reference_sum_squares -= old_value * old_value
        excluded = reference_values.get(timestamp)
        sample_count = len(reference_values) - (1 if excluded is not None else 0)
        total = reference_sum - (excluded if excluded is not None else 0.0)
        squares = reference_sum_squares - (
            excluded * excluded if excluded is not None else 0.0
        )
        mean = total / sample_count if sample_count else None
        std = None
        if sample_count:
            std = math.sqrt(max(0.0, squares / sample_count - mean * mean))
        oldest = next((item for item in reference_times if item < timestamp), None)
        coverage = sample_count / (window_seconds / interval_seconds)
        if oldest is None or oldest > cutoff:
            coverage = min(coverage, 0.0)
        stats = RollingBasisStats(
            sample_count=sample_count,
            coverage=coverage,
            mean_bps=mean,
            std_bps=std,
            min_observations=min_observations,
        )
        if excluded is not None:
            reference_sum += value - excluded
            reference_sum_squares += value * value - excluded * excluded
            reference_values[timestamp] = value
        else:
            reference_times.append(timestamp)
            reference_values[timestamp] = value
            reference_sum += value
            reference_sum_squares += value * value
            reference_last = timestamp
        return stats

    for index in range(9_001):
        timestamp = START + timedelta(seconds=index * interval_seconds)
        value = 90.0 + (index % 17) * 0.5
        expected = reference_observe(timestamp, value)
        actual, _mutation = optimized.observe_with_rollback(timestamp, value)
        assert actual.sample_count == expected.sample_count
        assert actual.coverage == pytest.approx(expected.coverage)
        assert actual.mean_bps == pytest.approx(expected.mean_bps)
        assert actual.std_bps == pytest.approx(expected.std_bps)
        assert actual.eligible is expected.eligible

        if index == 4_000:
            duplicate_value = 123.75
            expected = reference_observe(timestamp, duplicate_value)
            actual, _mutation = optimized.observe_with_rollback(timestamp, duplicate_value)
            assert actual.sample_count == expected.sample_count
            assert actual.mean_bps == pytest.approx(expected.mean_bps)
            assert actual.std_bps == pytest.approx(expected.std_bps)

    assert tuple(optimized._times) == tuple(reference_times)
    assert optimized._values == reference_values
    assert optimized._sum == pytest.approx(reference_sum)
    assert optimized._sum_squares == pytest.approx(reference_sum_squares)
    assert optimized._last_sample_time == reference_last
