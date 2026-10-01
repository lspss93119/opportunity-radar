from __future__ import annotations

import argparse
from array import array
import csv
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import math
from pathlib import Path
import resource
from statistics import median, quantiles
import time
from collections.abc import Iterator, Mapping
from typing import Any

import duckdb  # type: ignore[import-untyped]

from radar.config import RadarConfig, load_config
from radar.monitors.spread.anomaly import (
    AnomalyEpisode,
    AnomalyObservation,
    AnomalyParameters,
    AnomalyTracker,
)
from radar.monitors.spread.basis import (
    EXPECTED_INTERVAL_SECONDS,
    ROLLING_WINDOW_SECONDS,
    RollingBasis,
)
from radar.monitors.spread.models import SpreadPairKey


QUERY_BATCH_SIZE = 8_192
CONTEXT_WINDOW_SECONDS = {
    "12h": 12 * 60 * 60,
    "24h": ROLLING_WINDOW_SECONDS,
    "48h": 48 * 60 * 60,
}


@dataclass(frozen=True)
class ReplayParameters:
    anomaly_deviation_bps: float = 10.0
    confirmation_seconds: int = 60
    return_band_bps: float = 5.0
    max_gap_seconds: int = 20

    def as_anomaly_parameters(self) -> AnomalyParameters:
        return AnomalyParameters(
            anomaly_deviation_bps=self.anomaly_deviation_bps,
            confirmation_seconds=self.confirmation_seconds,
            return_band_bps=self.return_band_bps,
            max_gap_seconds=self.max_gap_seconds,
        )


RESEARCH_PARAMETER_SETS = {
    "A": ReplayParameters(
        anomaly_deviation_bps=10.0,
        confirmation_seconds=60,
        return_band_bps=5.0,
        max_gap_seconds=20,
    ),
    "B": ReplayParameters(
        anomaly_deviation_bps=10.0,
        confirmation_seconds=120,
        return_band_bps=5.0,
        max_gap_seconds=20,
    ),
    "C": ReplayParameters(
        anomaly_deviation_bps=15.0,
        confirmation_seconds=60,
        return_band_bps=5.0,
        max_gap_seconds=20,
    ),
}


@dataclass(frozen=True)
class ReplayWindowStats:
    sample_count: int
    mean_bps: float | None
    std_bps: float | None
    min_bps: float | None
    max_bps: float | None
    eligible: bool


@dataclass(frozen=True)
class ConfirmationContext:
    stats_12h: ReplayWindowStats
    stats_24h: ReplayWindowStats
    stats_48h: ReplayWindowStats


@dataclass(frozen=True)
class ReplayEpisode:
    episode: AnomalyEpisode
    confirmation_context: ConfirmationContext

    @property
    def pair_key(self) -> SpreadPairKey:
        return self.episode.pair_key

    def to_row(self) -> dict[str, object]:
        episode = self.episode
        context = self.confirmation_context
        row: dict[str, object] = {
            "canonical_symbol": episode.pair_key.canonical_symbol,
            "long_venue": episode.pair_key.long_venue,
            "long_venue_symbol": episode.pair_key.long_venue_symbol,
            "short_venue": episode.pair_key.short_venue,
            "short_venue_symbol": episode.pair_key.short_venue_symbol,
            "candidate_started_at": episode.candidate_started_at.isoformat(),
            "confirmed_at": _time_string(episode.confirmed_at),
            "lifetime_peak_at": episode.lifetime_peak_at.isoformat(),
            "ended_at": _time_string(episode.ended_at),
            "resolution_reason": episode.resolution_reason,
            "reference_mean_bps": episode.reference_mean_bps,
            "reference_std_bps": episode.reference_std_bps,
            "confirmation_spread_bps": episode.confirmation_spread_bps,
            "confirmation_deviation_bps": episode.confirmation_deviation_bps,
            "lifetime_peak_spread_bps": episode.lifetime_peak_spread_bps,
            "lifetime_peak_deviation_bps": episode.lifetime_peak_deviation_bps,
            "post_confirmation_peak_spread_bps": episode.post_confirmation_peak_spread_bps,
            "post_confirmation_peak_deviation_bps": episode.post_confirmation_peak_deviation_bps,
            "post_confirmation_peak_at": _time_string(
                episode.post_confirmation_peak_at
            ),
            "post_confirmation_expansion_bps": episode.post_confirmation_expansion_bps,
            "confirmation_to_peak_seconds": episode.confirmation_to_peak_seconds,
            "total_duration_seconds": episode.total_duration_seconds,
            "post_confirmation_alive_seconds": episode.post_confirmation_alive_seconds,
            "end_spread_bps": episode.end_spread_bps,
            "end_deviation_bps": episode.end_deviation_bps,
        }
        for name, stats in (
            ("12h", context.stats_12h),
            ("24h", context.stats_24h),
            ("48h", context.stats_48h),
        ):
            row.update(
                {
                    f"{name}_sample_count": stats.sample_count,
                    f"{name}_mean_bps": stats.mean_bps,
                    f"{name}_std_bps": stats.std_bps,
                    f"{name}_min_bps": stats.min_bps,
                    f"{name}_max_bps": stats.max_bps,
                }
            )
        return row


@dataclass(frozen=True)
class ReplayResult:
    episodes: tuple[ReplayEpisode, ...]
    elapsed_seconds: float
    peak_rss_bytes: int
    processed_symbols: tuple[str, ...]
    processed_pairs: int = 0


@dataclass(frozen=True)
class ReplayMultiResult:
    results: Mapping[str, ReplayResult]
    elapsed_seconds: float
    peak_rss_bytes: int
    processed_symbols: tuple[str, ...]
    processed_pairs: int


class _ContextHistory:
    """One bounded 48-hour history with lazy prior-window statistics."""

    def __init__(self, max_window_seconds: int = CONTEXT_WINDOW_SECONDS["48h"]) -> None:
        self.max_window_seconds = max_window_seconds
        self._times = array("q")
        self._values = array("d")
        self._start_index = 0

    def append(self, sample_time: datetime, value: float) -> None:
        timestamp = _as_utc(sample_time, "sample_time")
        timestamp_us = _epoch_microseconds(timestamp)
        if self._times and timestamp_us < self._times[-1]:
            raise ValueError("sample_time must not move backwards")
        self._prune(timestamp_us - self.max_window_seconds * 1_000_000)
        self._times.append(timestamp_us)
        self._values.append(float(value))

    def stats_before(self, sample_time: datetime, window_seconds: int) -> ReplayWindowStats:
        timestamp = _as_utc(sample_time, "sample_time")
        timestamp_us = _epoch_microseconds(timestamp)
        if window_seconds <= 0 or window_seconds > self.max_window_seconds:
            raise ValueError("window_seconds must fit the context history")
        cutoff_us = timestamp_us - window_seconds * 1_000_000
        count = 0
        total = 0.0
        total_squares = 0.0
        mean: float | None = None
        std: float | None = None
        minimum: float | None = None
        maximum: float | None = None
        has_anchor = False
        for index in range(self._start_index, len(self._times)):
            value_time = self._times[index]
            value = self._values[index]
            if value_time >= timestamp_us:
                break
            if value_time < cutoff_us:
                continue
            count += 1
            total += value
            total_squares += value * value
            minimum = value if minimum is None else min(minimum, value)
            maximum = value if maximum is None else max(maximum, value)
            if value_time == cutoff_us:
                has_anchor = True
        if count:
            mean = total / count
            std = math.sqrt(max(0.0, total_squares / count - mean * mean))
        expected_count = window_seconds // EXPECTED_INTERVAL_SECONDS
        minimum_count = math.ceil(expected_count * 0.8)
        coverage = count / expected_count
        eligible = (
            count >= minimum_count
            and coverage >= 0.8
            and has_anchor
            and mean is not None
            and std is not None
        )
        return ReplayWindowStats(
            sample_count=count,
            mean_bps=mean,
            std_bps=std,
            min_bps=minimum,
            max_bps=maximum,
            eligible=eligible,
        )

    @property
    def expected_count(self) -> int:
        return self.max_window_seconds // EXPECTED_INTERVAL_SECONDS

    @property
    def point_count(self) -> int:
        return len(self._times) - self._start_index

    def _prune(self, cutoff_us: int) -> None:
        while (
            self._start_index < len(self._times)
            and self._times[self._start_index] < cutoff_us
        ):
            self._start_index += 1
        if self._start_index >= 4_096 and self._start_index * 2 >= len(self._times):
            self._times = self._times[self._start_index :]
            self._values = self._values[self._start_index :]
            self._start_index = 0


@dataclass
class _TrackerState:
    tracker: AnomalyTracker
    confirmation_contexts: dict[str, ConfirmationContext]


@dataclass
class _PairState:
    key: SpreadPairKey
    basis: RollingBasis
    context_history: _ContextHistory
    trackers: dict[str, _TrackerState]


def replay_market_data(
    *,
    data_root: Path,
    config: RadarConfig,
    start: datetime,
    end: datetime,
    parameters: ReplayParameters,
    symbol: str | None = None,
    long_venue: str | None = None,
    long_venue_symbol: str | None = None,
    short_venue: str | None = None,
    short_venue_symbol: str | None = None,
) -> ReplayResult:
    """Replay one configuration using the shared multi-configuration path."""
    result = replay_market_data_multi(
        data_root=data_root,
        config=config,
        start=start,
        end=end,
        parameter_sets={"default": parameters},
        symbol=symbol,
        long_venue=long_venue,
        long_venue_symbol=long_venue_symbol,
        short_venue=short_venue,
        short_venue_symbol=short_venue_symbol,
    )
    return result.results["default"]


def replay_market_data_multi(
    *,
    data_root: Path,
    config: RadarConfig,
    start: datetime,
    end: datetime,
    parameter_sets: Mapping[str, ReplayParameters],
    symbol: str | None = None,
    long_venue: str | None = None,
    long_venue_symbol: str | None = None,
    short_venue: str | None = None,
    short_venue_symbol: str | None = None,
) -> ReplayMultiResult:
    """Replay independent configurations over one shared market-data stream."""
    started = time.perf_counter()
    start_utc = _as_utc(start, "start")
    end_utc = _as_utc(end, "end")
    if end_utc <= start_utc:
        raise ValueError("end must be after start")
    if not parameter_sets:
        raise ValueError("parameter_sets must not be empty")
    anomaly_parameters = {
        name: parameters.as_anomaly_parameters()
        for name, parameters in parameter_sets.items()
    }
    feeds_by_symbol = _configured_feeds(
        config,
        symbol=symbol,
        long_venue=long_venue,
        long_venue_symbol=long_venue_symbol,
        short_venue=short_venue,
        short_venue_symbol=short_venue_symbol,
    )
    states_by_symbol = {
        canonical_symbol: _pair_states(
            canonical_symbol,
            feeds,
            anomaly_parameters,
            long_venue=long_venue,
            long_venue_symbol=long_venue_symbol,
            short_venue=short_venue,
            short_venue_symbol=short_venue_symbol,
        )
        for canonical_symbol, feeds in feeds_by_symbol.items()
    }
    warm_start = start_utc - timedelta(seconds=CONTEXT_WINDOW_SECONDS["48h"])
    for canonical_symbol, sample_time, rows in _iter_market_rows(
        data_root=data_root,
        feeds_by_symbol=feeds_by_symbol,
        start=warm_start,
        end=end_utc,
    ):
        pair_states = states_by_symbol.get(canonical_symbol)
        if pair_states:
            _process_sample(
                sample_time,
                rows,
                pair_states,
                stale_after_seconds=config.monitors.spread.stale_after_seconds,
            )

    elapsed_seconds = time.perf_counter() - started
    peak_rss_bytes = _peak_rss_bytes()
    processed_symbols = tuple(sorted(feeds_by_symbol))
    processed_pairs = sum(len(states) for states in states_by_symbol.values())
    results: dict[str, ReplayResult] = {}
    for name in parameter_sets:
        episodes: list[ReplayEpisode] = []
        for pair_states in states_by_symbol.values():
            _collect_replay_episodes(
                episodes,
                pair_states,
                configuration=name,
                start=start_utc,
                end=end_utc,
            )
        episodes.sort(
            key=lambda item: (
                item.episode.confirmed_at or item.episode.candidate_started_at,
                item.episode.episode_id,
            )
        )
        results[name] = ReplayResult(
            episodes=tuple(episodes),
            elapsed_seconds=elapsed_seconds,
            peak_rss_bytes=peak_rss_bytes,
            processed_symbols=processed_symbols,
            processed_pairs=processed_pairs,
        )
    return ReplayMultiResult(
        results=results,
        elapsed_seconds=elapsed_seconds,
        peak_rss_bytes=peak_rss_bytes,
        processed_symbols=processed_symbols,
        processed_pairs=processed_pairs,
    )


def write_replay_csv(path: Path, result: ReplayResult) -> None:
    rows = [episode.to_row() for episode in result.episodes]
    if not rows:
        path.write_text("\n", encoding="utf-8")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _configured_feeds(
    config: RadarConfig,
    *,
    symbol: str | None,
    long_venue: str | None,
    long_venue_symbol: str | None,
    short_venue: str | None,
    short_venue_symbol: str | None,
) -> dict[str, tuple[tuple[str, str], ...]]:
    configured: dict[str, set[tuple[str, str]]] = {}
    for market in config.markets:
        if not market.enabled or (symbol is not None and market.canonical_symbol != symbol):
            continue
        configured.setdefault(market.canonical_symbol, set()).add(
            (market.venue, market.venue_symbol)
        )
    feeds: dict[str, set[tuple[str, str]]]
    if all(
        value is None
        for value in (
            long_venue,
            long_venue_symbol,
            short_venue,
            short_venue_symbol,
        )
    ):
        feeds = configured
    else:
        feeds = {}
        for canonical_symbol, configured_feeds in configured.items():
            for long_venue_name, long_symbol in configured_feeds:
                for short_venue_name, short_symbol in configured_feeds:
                    if long_venue_name.lower() == short_venue_name.lower():
                        continue
                    key = SpreadPairKey(
                        canonical_symbol=canonical_symbol,
                        long_venue=long_venue_name,
                        long_venue_symbol=long_symbol,
                        short_venue=short_venue_name,
                        short_venue_symbol=short_symbol,
                    )
                    if not _matches_filters(
                        key,
                        long_venue=long_venue,
                        long_venue_symbol=long_venue_symbol,
                        short_venue=short_venue,
                        short_venue_symbol=short_venue_symbol,
                    ):
                        continue
                    feeds.setdefault(canonical_symbol, set()).update(
                        {(long_venue_name, long_symbol), (short_venue_name, short_symbol)}
                    )
    return {
        canonical_symbol: tuple(sorted(symbols))
        for canonical_symbol, symbols in feeds.items()
    }


def _pair_states(
    canonical_symbol: str,
    feeds: tuple[tuple[str, str], ...],
    parameter_sets: Mapping[str, AnomalyParameters],
    *,
    long_venue: str | None,
    long_venue_symbol: str | None,
    short_venue: str | None,
    short_venue_symbol: str | None,
) -> dict[SpreadPairKey, _PairState]:
    states: dict[SpreadPairKey, _PairState] = {}
    for long_venue_name, long_symbol in feeds:
        for short_venue_name, short_symbol in feeds:
            if long_venue_name.lower() == short_venue_name.lower():
                continue
            key = SpreadPairKey(
                canonical_symbol=canonical_symbol,
                long_venue=long_venue_name,
                long_venue_symbol=long_symbol,
                short_venue=short_venue_name,
                short_venue_symbol=short_symbol,
            )
            if not _matches_filters(
                key,
                long_venue=long_venue,
                long_venue_symbol=long_venue_symbol,
                short_venue=short_venue,
                short_venue_symbol=short_venue_symbol,
            ):
                continue
            states[key] = _PairState(
                key=key,
                basis=RollingBasis(),
                context_history=_ContextHistory(),
                trackers={
                    name: _TrackerState(
                        tracker=AnomalyTracker(key, parameters),
                        confirmation_contexts={},
                    )
                    for name, parameters in parameter_sets.items()
                },
            )
    return states


def _matches_filters(
    key: SpreadPairKey,
    *,
    long_venue: str | None,
    long_venue_symbol: str | None,
    short_venue: str | None,
    short_venue_symbol: str | None,
) -> bool:
    return all(
        value is None or actual == value
        for value, actual in (
            (long_venue, key.long_venue),
            (long_venue_symbol, key.long_venue_symbol),
            (short_venue, key.short_venue),
            (short_venue_symbol, key.short_venue_symbol),
        )
    )


def _iter_market_rows(
    *,
    data_root: Path,
    feeds_by_symbol: dict[str, tuple[tuple[str, str], ...]],
    start: datetime,
    end: datetime,
) -> Iterator[
    tuple[str, datetime, dict[tuple[str, str], tuple[datetime, object, object]]]
]:
    market_partitions = _market_partitions_for_window(data_root, start=start, end=end)
    if not market_partitions:
        return
    feed_descriptors = [
        (canonical_symbol, venue, venue_symbol)
        for canonical_symbol, feeds in sorted(feeds_by_symbol.items())
        for venue, venue_symbol in feeds
    ]
    if not feed_descriptors:
        return
    predicates = " OR ".join(
        "(canonical_symbol = ? AND venue = ? AND venue_symbol = ?)"
        for _ in feed_descriptors
    )

    with duckdb.connect() as connection:
        current_symbol: str | None = None
        current_sample: datetime | None = None
        current_rows: dict[tuple[str, str], tuple[datetime, object, object]] = {}
        for market_files in market_partitions:
            market_paths = ", ".join(
                "'" + str(path).replace("'", "''") + "'" for path in market_files
            )
            query = f"""
                SELECT
                    sample_time,
                    observed_at,
                    venue,
                    venue_symbol,
                    canonical_symbol,
                    buy_10k_vwap,
                    sell_10k_vwap
                FROM read_parquet([{market_paths}])
                WHERE sample_time >= ?
                  AND sample_time < ?
                  AND ({predicates})
                ORDER BY canonical_symbol, sample_time, observed_at
            """
            parameters: list[object] = [start, end]
            for descriptor in feed_descriptors:
                parameters.extend(descriptor)
            result = connection.execute(query, parameters)
            reader = result.to_arrow_reader(batch_size=QUERY_BATCH_SIZE)
            for batch in reader:
                columns = batch.to_pydict()
                for index in range(batch.num_rows):
                    canonical_symbol = str(columns["canonical_symbol"][index])
                    sample_time = _as_utc(columns["sample_time"][index], "sample_time")
                    row = (
                        _as_utc(columns["observed_at"][index], "observed_at"),
                        columns["buy_10k_vwap"][index],
                        columns["sell_10k_vwap"][index],
                    )
                    feed_key = (
                        str(columns["venue"][index]),
                        str(columns["venue_symbol"][index]),
                    )
                    if current_symbol is None:
                        current_symbol = canonical_symbol
                    if current_sample is None:
                        current_sample = sample_time
                    if (
                        canonical_symbol != current_symbol
                        or sample_time != current_sample
                    ):
                        assert current_symbol is not None
                        assert current_sample is not None
                        yield current_symbol, current_sample, current_rows
                        current_symbol = canonical_symbol
                        current_sample = sample_time
                        current_rows = {}
                    # observed_at is ascending within a feed/sample group, so
                    # the final assignment preserves latest-observation dedup.
                    current_rows[feed_key] = row
        if current_symbol is not None and current_sample is not None:
            yield current_symbol, current_sample, current_rows


def _market_partitions_for_window(
    data_root: Path,
    *,
    start: datetime,
    end: datetime,
) -> tuple[tuple[Path, ...], ...]:
    first_date = start.date()
    last_date = (end - timedelta(microseconds=1)).date()
    partitions: list[tuple[Path, ...]] = []
    current_date = first_date
    while current_date <= last_date:
        partition = data_root / "market" / f"date={current_date.isoformat()}"
        files = tuple(
            path for path in sorted(partition.glob("part-*.parquet")) if path.is_file()
        )
        if files:
            partitions.append(files)
        current_date += timedelta(days=1)
    return tuple(partitions)


def _market_files_for_window(
    data_root: Path,
    *,
    start: datetime,
    end: datetime,
) -> tuple[Path, ...]:
    return tuple(
        path
        for partition in _market_partitions_for_window(data_root, start=start, end=end)
        for path in partition
    )


def _collect_replay_episodes(
    output: list[ReplayEpisode],
    states: dict[SpreadPairKey, _PairState],
    *,
    configuration: str,
    start: datetime,
    end: datetime,
) -> None:
    for state in states.values():
        tracker_state = state.trackers[configuration]
        tracker_state.tracker.finalize()
        for episode in tracker_state.tracker.confirmed_episodes:
            if not _intersects_window(episode, start, end):
                continue
            context = tracker_state.confirmation_contexts.get(episode.episode_id)
            if context is None:
                raise RuntimeError(
                    "confirmed episode is missing its confirmation context"
                )
            output.append(ReplayEpisode(episode, context))


def _process_sample(
    sample_time: datetime,
    rows: dict[tuple[str, str], tuple[datetime, object, object]],
    states: dict[SpreadPairKey, _PairState],
    *,
    stale_after_seconds: int,
) -> None:
    valid: dict[tuple[str, str], tuple[float, float]] = {}
    for feed_key, (observed_at, buy_value, sell_value) in rows.items():
        skew_seconds = abs((sample_time - observed_at).total_seconds())
        if skew_seconds > stale_after_seconds:
            continue
        buy_price = _positive_float(buy_value)
        sell_price = _positive_float(sell_value)
        if buy_price is None or sell_price is None:
            continue
        valid[feed_key] = (buy_price, sell_price)

    for state in states.values():
        long_values = valid.get((state.key.long_venue, state.key.long_venue_symbol))
        short_values = valid.get((state.key.short_venue, state.key.short_venue_symbol))
        if long_values is None or short_values is None:
            continue
        raw_spread_bps = (short_values[1] / long_values[0] - 1.0) * 10_000.0
        if not math.isfinite(raw_spread_bps):
            continue
        basis_stats = state.basis.observe(sample_time, raw_spread_bps)
        observation = AnomalyObservation(
            sample_time=sample_time,
            raw_spread_bps=raw_spread_bps,
            rolling_mean_bps=basis_stats.mean_bps,
            rolling_std_bps=basis_stats.std_bps,
            basis_eligible=basis_stats.eligible,
        )
        confirmation_context: ConfirmationContext | None = None
        for tracker_state in state.trackers.values():
            transitions = tracker_state.tracker.observe(observation)
            for transition in transitions:
                if transition.kind == "confirmed":
                    if confirmation_context is None:
                        confirmation_context = ConfirmationContext(
                            stats_12h=state.context_history.stats_before(
                                sample_time, CONTEXT_WINDOW_SECONDS["12h"]
                            ),
                            stats_24h=state.context_history.stats_before(
                                sample_time, CONTEXT_WINDOW_SECONDS["24h"]
                            ),
                            stats_48h=state.context_history.stats_before(
                                sample_time, CONTEXT_WINDOW_SECONDS["48h"]
                            ),
                        )
                    tracker_state.confirmation_contexts[transition.episode.episode_id] = (
                        confirmation_context
                    )
        state.context_history.append(sample_time, raw_spread_bps)


def _positive_float(value: object) -> float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    numeric = float(value)
    return numeric if math.isfinite(numeric) and numeric > 0 else None


def _intersects_window(
    episode: AnomalyEpisode,
    start: datetime,
    end: datetime,
) -> bool:
    if episode.confirmed_at is None:
        return False
    if episode.confirmed_at >= end:
        return False
    return episode.ended_at is None or episode.ended_at >= start


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _epoch_microseconds(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


def _time_string(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if __import__("sys").platform == "darwin" else value * 1024


def _research_summary(result: ReplayResult) -> dict[str, Any]:
    episodes = list(result.episodes)
    total_durations = [
        value
        for episode in episodes
        if (value := episode.episode.total_duration_seconds) is not None
    ]
    post_alive = [
        value
        for episode in episodes
        if (value := episode.episode.post_confirmation_alive_seconds) is not None
    ]
    expansions = [
        value
        for episode in episodes
        if (value := episode.episode.post_confirmation_expansion_bps) is not None
    ]
    confirmed_per_day = Counter(
        episode.episode.confirmed_at.date().isoformat()
        for episode in episodes
        if episode.episode.confirmed_at is not None
    )
    reasons = Counter(episode.episode.resolution_reason for episode in episodes)

    def percentile75(values: list[float]) -> float | None:
        if not values:
            return None
        return values[0] if len(values) == 1 else quantiles(values, n=4, method="inclusive")[2]

    def representative_row(replay_episode: ReplayEpisode) -> dict[str, object]:
        episode = replay_episode.episode
        return {
            "symbol": episode.pair_key.canonical_symbol,
            "direction": (
                f"{episode.pair_key.long_venue}:{episode.pair_key.long_venue_symbol}"
                f" -> {episode.pair_key.short_venue}:{episode.pair_key.short_venue_symbol}"
            ),
            "confirmation_time": _time_string(episode.confirmed_at),
            "reference_mean_bps": episode.reference_mean_bps,
            "confirmation_deviation_bps": episode.confirmation_deviation_bps,
            "post_confirmation_peak_deviation_bps": (
                episode.post_confirmation_peak_deviation_bps
            ),
            "post_confirmation_expansion_bps": episode.post_confirmation_expansion_bps,
            "post_confirmation_alive_seconds": episode.post_confirmation_alive_seconds,
            "resolution_reason": episode.resolution_reason,
        }

    longest = sorted(
        episodes,
        key=lambda item: (
            -(item.episode.total_duration_seconds or 0.0),
            item.episode.episode_id,
        ),
    )[:10]
    shortest = sorted(
        episodes,
        key=lambda item: (
            item.episode.total_duration_seconds or 0.0,
            item.episode.episode_id,
        ),
    )[:10]
    largest_expansion = sorted(
        episodes,
        key=lambda item: (
            -(item.episode.post_confirmation_expansion_bps or 0.0),
            item.episode.episode_id,
        ),
    )[:10]
    return {
        "confirmed_episode_count": len(episodes),
        "confirmed_episodes_per_day": dict(sorted(confirmed_per_day.items())),
        "median_total_episode_duration_seconds": median(total_durations)
        if total_durations
        else None,
        "median_post_confirmation_alive_seconds": median(post_alive) if post_alive else None,
        "post_confirmation_alive_at_least_5m_pct": (
            100.0 * sum(value >= 300 for value in post_alive) / len(post_alive)
            if post_alive
            else None
        ),
        "post_confirmation_alive_at_least_10m_pct": (
            100.0 * sum(value >= 600 for value in post_alive) / len(post_alive)
            if post_alive
            else None
        ),
        "median_post_confirmation_expansion_bps": median(expansions)
        if expansions
        else None,
        "p75_post_confirmation_expansion_bps": percentile75(expansions),
        "returned_to_mean_band_count": reasons.get("returned_to_mean_band", 0),
        "data_gap_count": reasons.get("data_gap", 0),
        "open_at_end_count": reasons.get("open_at_end", 0),
        "longest": [representative_row(item) for item in longest],
        "shortest": [representative_row(item) for item in shortest],
        "largest_expansion": [representative_row(item) for item in largest_expansion],
    }


def _print_research_summary(name: str, result: ReplayResult) -> None:
    summary = _research_summary(result)
    print(f"[{name}] {summary}")


def _parse_boundary(value: str, *, end: bool) -> datetime:
    if len(value) == 10:
        parsed_date = date.fromisoformat(value)
        parsed = datetime.combine(parsed_date, datetime.min.time(), tzinfo=UTC)
        return parsed + timedelta(days=1) if end else parsed
    return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")), "boundary")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay anomaly episodes from market Parquet")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--start", required=True, help="UTC date or ISO-8601 start")
    parser.add_argument("--end", required=True, help="UTC date (inclusive) or end-exclusive ISO-8601")
    parser.add_argument("--symbol")
    parser.add_argument("--long-venue")
    parser.add_argument("--long-venue-symbol")
    parser.add_argument("--short-venue")
    parser.add_argument("--short-venue-symbol")
    parser.add_argument("--deviation-bps", type=float, default=10.0)
    parser.add_argument("--confirmation-seconds", type=int, default=60)
    parser.add_argument("--return-band-bps", type=float, default=5.0)
    parser.add_argument("--max-gap-seconds", type=int, default=20)
    parser.add_argument("--output-csv", type=Path)
    parser.add_argument(
        "--research-abc",
        action="store_true",
        help="run A/B/C research configurations through one shared replay",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    config = load_config(args.config)
    start = _parse_boundary(args.start, end=False)
    end = _parse_boundary(args.end, end=True)
    if args.research_abc:
        if args.output_csv is not None:
            raise SystemExit("--output-csv cannot be combined with --research-abc")
        multi_result = replay_market_data_multi(
            data_root=args.data_root,
            config=config,
            start=start,
            end=end,
            parameter_sets=RESEARCH_PARAMETER_SETS,
            symbol=args.symbol,
            long_venue=args.long_venue,
            long_venue_symbol=args.long_venue_symbol,
            short_venue=args.short_venue,
            short_venue_symbol=args.short_venue_symbol,
        )
        print(f"symbols={len(multi_result.processed_symbols)}")
        print(f"directional_pairs={multi_result.processed_pairs}")
        print(f"elapsed_seconds={multi_result.elapsed_seconds:.3f}")
        print(f"peak_rss_mb={multi_result.peak_rss_bytes / 1024 / 1024:.1f}")
        for name in RESEARCH_PARAMETER_SETS:
            _print_research_summary(name, multi_result.results[name])
        return 0
    single_result = replay_market_data(
        data_root=args.data_root,
        config=config,
        start=start,
        end=end,
        parameters=ReplayParameters(
            anomaly_deviation_bps=args.deviation_bps,
            confirmation_seconds=args.confirmation_seconds,
            return_band_bps=args.return_band_bps,
            max_gap_seconds=args.max_gap_seconds,
        ),
        symbol=args.symbol,
        long_venue=args.long_venue,
        long_venue_symbol=args.long_venue_symbol,
        short_venue=args.short_venue,
        short_venue_symbol=args.short_venue_symbol,
    )
    if args.output_csv is not None:
        write_replay_csv(args.output_csv, single_result)
    print(f"symbols={len(single_result.processed_symbols)}")
    print(f"directional_pairs={single_result.processed_pairs}")
    print(f"episodes={len(single_result.episodes)}")
    print(f"elapsed_seconds={single_result.elapsed_seconds:.3f}")
    print(f"peak_rss_mb={single_result.peak_rss_bytes / 1024 / 1024:.1f}")
    for replay_episode in single_result.episodes:
        print(replay_episode.to_row())
    if args.output_csv is not None:
        print(f"csv={args.output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
