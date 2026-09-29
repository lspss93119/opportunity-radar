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
        stats = self.stats_before(sample_time)
        self._append(_as_utc(sample_time, "sample_time"), _finite(raw_spread_bps))
        return stats

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

    def _prune(self, cutoff: datetime) -> None:
        while self._times and self._times[0] < cutoff:
            timestamp = self._times.popleft()
            value = self._values.pop(timestamp)
            self._sum -= value
            self._sum_squares -= value * value
