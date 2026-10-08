from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import math
from typing import Iterable


ROLLING_WINDOW_SECONDS = 24 * 60 * 60
EXPECTED_INTERVAL_SECONDS = 10
MIN_HISTORY_OBSERVATIONS = 6_912


@dataclass(frozen=True)
class RollingBasisStats:
    sample_count: int
    coverage: float
    mean_bps: float | None
    std_bps: float | None
    min_observations: int

    @property
    def eligible(self) -> bool:
        return (
            self.sample_count >= self.min_observations
            and self.mean_bps is not None
            and self.std_bps is not None
            and self.coverage >= 0.8
        )


@dataclass
class RollingBasisMutation:
    """Small inverse journal for one successful rolling-basis observation."""

    basis: "RollingBasis"
    sample_time: datetime
    sample_value: float
    previous_last_sample_time: datetime | None
    previous_sum: float
    previous_sum_squares: float
    previous_value: float | None
    had_previous_value: bool
    pruned_points: tuple[tuple[datetime, float], ...] = ()
    append_applied: bool = False
    _rolled_back: bool = False

    def rollback(self) -> None:
        """Restore the exact state captured before this observation.

        Rollback is intentionally idempotent.  If another observation changed
        the basis first, refusing to guess is safer than corrupting history.
        """
        if self._rolled_back:
            return

        if self.append_applied:
            current_value = self.basis._values.get(self.sample_time)
            if (
                current_value != self.sample_value
                or not self.basis._times
                or self.basis._times[-1] != self.sample_time
            ):
                raise RuntimeError("rolling basis changed before rollback")
            if self.had_previous_value:
                if self.sample_time not in self.basis._values:
                    raise RuntimeError("rolling basis changed before rollback")
                assert self.previous_value is not None
                self.basis._values[self.sample_time] = self.previous_value
            else:
                self.basis._times.pop()
                del self.basis._values[self.sample_time]

        for timestamp, value in reversed(self.pruned_points):
            if timestamp in self.basis._values:
                raise RuntimeError("rolling basis changed before rollback")
            self.basis._times.appendleft(timestamp)
            self.basis._values[timestamp] = value

        self.basis._values = {
            timestamp: self.basis._values[timestamp]
            for timestamp in self.basis._times
        }
        self.basis._sum = self.previous_sum
        self.basis._sum_squares = self.previous_sum_squares
        self.basis._last_sample_time = self.previous_last_sample_time
        self._rolled_back = True


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _finite(value: float) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("raw spread must be finite")
    return result


class RollingBasis:
    """Bounded prior-only rolling statistics for one directional pair."""

    def __init__(
        self,
        *,
        window_seconds: int = ROLLING_WINDOW_SECONDS,
        min_observations: int = MIN_HISTORY_OBSERVATIONS,
        expected_interval_seconds: int = EXPECTED_INTERVAL_SECONDS,
    ) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if min_observations <= 0:
            raise ValueError("min_observations must be positive")
        if expected_interval_seconds <= 0:
            raise ValueError("expected_interval_seconds must be positive")
        self.window_seconds = window_seconds
        self.min_observations = min_observations
        self.expected_interval_seconds = expected_interval_seconds
        self._times: deque[datetime] = deque()
        self._values: dict[datetime, float] = {}
        self._sum = 0.0
        self._sum_squares = 0.0
        self._last_sample_time: datetime | None = None

    @property
    def sample_count(self) -> int:
        return len(self._values)

    def hydrate(self, points: Iterable[tuple[datetime, float]]) -> None:
        """Replace local history with sorted, deduplicated prior observations."""
        values: dict[datetime, float] = {}
        for sample_time, raw_spread_bps in points:
            timestamp = _as_utc(sample_time, "sample_time")
            values[timestamp] = _finite(raw_spread_bps)

        self._times.clear()
        self._values.clear()
        self._sum = 0.0
        self._sum_squares = 0.0
        self._last_sample_time = None
        for timestamp in sorted(values):
            self._append(timestamp, values[timestamp])

    def stats_before(self, sample_time: datetime) -> RollingBasisStats:
        """Return statistics strictly before ``sample_time``."""
        timestamp = _as_utc(sample_time, "sample_time")
        self._prune(timestamp - timedelta(seconds=self.window_seconds))
        return self._stats_before_pruned(timestamp)

    def _stats_before_pruned(self, timestamp: datetime) -> RollingBasisStats:
        """Calculate prior-only statistics after the window has been pruned."""
        if (
            self._last_sample_time is not None
            and timestamp < self._last_sample_time
        ):
            raise ValueError("sample_time must not move backwards")

        excluded = self._values.get(timestamp)
        sample_count = len(self._values) - (1 if excluded is not None else 0)
        total = self._sum - (excluded if excluded is not None else 0.0)
        squares = self._sum_squares - (
            excluded * excluded if excluded is not None else 0.0
        )
        mean: float | None = None
        std: float | None = None
        if sample_count > 0:
            mean = total / sample_count
            variance = max(0.0, squares / sample_count - mean * mean)
            std = math.sqrt(variance)

        oldest = next(
            (value for value in self._times if value < timestamp),
            None,
        )
        coverage = sample_count / (
            self.window_seconds / self.expected_interval_seconds
        )
        if oldest is None or oldest > timestamp - timedelta(
            seconds=self.window_seconds
        ):
            coverage = min(coverage, 0.0)
        return RollingBasisStats(
            sample_count=sample_count,
            coverage=coverage,
            mean_bps=mean,
            std_bps=std,
            min_observations=self.min_observations,
        )

    def observe(self, sample_time: datetime, raw_spread_bps: float) -> RollingBasisStats:
        """Evaluate the prior window, then append the current observation."""
        stats, _mutation = self.observe_with_rollback(sample_time, raw_spread_bps)
        return stats

    def observe_with_rollback(
        self,
        sample_time: datetime,
        raw_spread_bps: float,
    ) -> tuple[RollingBasisStats, RollingBasisMutation]:
        """Observe one point and return a bounded inverse mutation journal."""
        timestamp = _as_utc(sample_time, "sample_time")
        value = _finite(raw_spread_bps)
        if (
            self._last_sample_time is not None
            and timestamp < self._last_sample_time
        ):
            raise ValueError("sample_time must not move backwards")

        had_previous_value = timestamp in self._values
        previous_value = self._values.get(timestamp)
        mutation = RollingBasisMutation(
            basis=self,
            sample_time=timestamp,
            sample_value=value,
            previous_last_sample_time=self._last_sample_time,
            previous_sum=self._sum,
            previous_sum_squares=self._sum_squares,
            previous_value=previous_value,
            had_previous_value=had_previous_value,
        )
        try:
            mutation.pruned_points = self._prune(
                timestamp - timedelta(seconds=self.window_seconds)
            )
            stats = self._stats_before_pruned(timestamp)
            self._append(timestamp, value)
            mutation.append_applied = True
            return stats, mutation
        except Exception:
            mutation.rollback()
            raise

    def _append(self, sample_time: datetime, raw_spread_bps: float) -> None:
        previous = self._values.get(sample_time)
        if previous is not None:
            self._sum += raw_spread_bps - previous
            self._sum_squares += raw_spread_bps * raw_spread_bps - previous * previous
            self._values[sample_time] = raw_spread_bps
            return
        if self._last_sample_time is not None and sample_time < self._last_sample_time:
            raise ValueError("sample_time must not move backwards")
        self._times.append(sample_time)
        self._values[sample_time] = raw_spread_bps
        self._sum += raw_spread_bps
        self._sum_squares += raw_spread_bps * raw_spread_bps
        self._last_sample_time = sample_time

    def _prune(self, cutoff: datetime) -> tuple[tuple[datetime, float], ...]:
        removed: list[tuple[datetime, float]] = []
        while self._times and self._times[0] < cutoff:
            timestamp = self._times.popleft()
            value = self._values.pop(timestamp)
            self._sum -= value
            self._sum_squares -= value * value
            removed.append((timestamp, value))
        return tuple(removed)
