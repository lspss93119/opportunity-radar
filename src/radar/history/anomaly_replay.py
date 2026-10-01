from __future__ import annotations

import argparse
import csv
from collections import deque
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import math
from pathlib import Path
import resource
import time
from typing import Iterator

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
            "peak_at": episode.peak_at.isoformat(),
            "ended_at": _time_string(episode.ended_at),
            "resolution_reason": episode.resolution_reason,
            "reference_mean_bps": episode.reference_mean_bps,
            "reference_std_bps": episode.reference_std_bps,
            "confirmation_spread_bps": episode.confirmation_spread_bps,
            "confirmation_deviation_bps": episode.confirmation_deviation_bps,
            "peak_spread_bps": episode.peak_spread_bps,
            "peak_deviation_bps": episode.peak_deviation_bps,
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


class _PriorWindow:
    def __init__(self, window_seconds: int) -> None:
        self.window_seconds = window_seconds
        self.expected_count = window_seconds // EXPECTED_INTERVAL_SECONDS
        self.minimum_count = math.ceil(self.expected_count * 0.8)
        self._values: deque[tuple[datetime, float]] = deque()
        self._sum = 0.0
        self._sum_squares = 0.0

    def stats_before(self, sample_time: datetime) -> ReplayWindowStats:
        cutoff = sample_time - timedelta(seconds=self.window_seconds)
        while self._values and self._values[0][0] < cutoff:
            _timestamp, value = self._values.popleft()
            self._sum -= value
            self._sum_squares -= value * value

        values = [value for _timestamp, value in self._values]
        count = len(values)
        mean: float | None = None
        std: float | None = None
        minimum: float | None = None
        maximum: float | None = None
        if values:
            mean = self._sum / count
            std = math.sqrt(max(0.0, self._sum_squares / count - mean * mean))
            minimum = min(values)
            maximum = max(values)
        coverage = count / self.expected_count
        has_full_window_anchor = bool(values) and self._values[0][0] <= cutoff
        eligible = (
            count >= self.minimum_count
            and coverage >= 0.8
            and has_full_window_anchor
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

    def append(self, sample_time: datetime, value: float) -> None:
        self._values.append((sample_time, value))
        self._sum += value
        self._sum_squares += value * value


@dataclass
class _PairState:
    key: SpreadPairKey
    tracker: AnomalyTracker
    basis: RollingBasis
    windows: dict[str, _PriorWindow]
    confirmation_contexts: dict[str, ConfirmationContext]


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
    """Replay configured exact directional pairs over ``[start, end)``.

    One ordered streaming query supplies the selected feeds.  Replay keeps
    only one canonical symbol's pair trackers and bounded rolling windows at a
    time, rather than the full market cross-product.
    """
    started = time.perf_counter()
    start_utc = _as_utc(start, "start")
    end_utc = _as_utc(end, "end")
    if end_utc <= start_utc:
        raise ValueError("end must be after start")
    anomaly_parameters = parameters.as_anomaly_parameters()
    feeds_by_symbol = _configured_feeds(config, symbol=symbol)
    all_replay_episodes: list[ReplayEpisode] = []
    warm_start = start_utc - timedelta(seconds=CONTEXT_WINDOW_SECONDS["48h"])

    current_symbol: str | None = None
    pair_states: dict[SpreadPairKey, _PairState] = {}
    for canonical_symbol, sample_time, rows in _iter_market_rows(
        data_root=data_root,
        feeds_by_symbol=feeds_by_symbol,
        start=warm_start,
        end=end_utc,
    ):
        if canonical_symbol != current_symbol:
            _collect_replay_episodes(
                all_replay_episodes,
                pair_states,
                start=start_utc,
                end=end_utc,
            )
            current_symbol = canonical_symbol
            pair_states = _pair_states(
                canonical_symbol,
                feeds_by_symbol[canonical_symbol],
                anomaly_parameters,
                long_venue=long_venue,
                long_venue_symbol=long_venue_symbol,
                short_venue=short_venue,
                short_venue_symbol=short_venue_symbol,
            )
        if pair_states:
            _process_sample(
                sample_time,
                rows,
                pair_states,
                stale_after_seconds=config.monitors.spread.stale_after_seconds,
            )

    _collect_replay_episodes(
        all_replay_episodes,
        pair_states,
        start=start_utc,
        end=end_utc,
    )

    all_replay_episodes.sort(
        key=lambda item: (
            item.episode.confirmed_at or item.episode.candidate_started_at,
            item.episode.episode_id,
        )
    )
    return ReplayResult(
        episodes=tuple(all_replay_episodes),
        elapsed_seconds=time.perf_counter() - started,
        peak_rss_bytes=_peak_rss_bytes(),
        processed_symbols=tuple(sorted(feeds_by_symbol)),
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
) -> dict[str, tuple[tuple[str, str], ...]]:
    feeds: dict[str, set[tuple[str, str]]] = {}
    for market in config.markets:
        if not market.enabled or (symbol is not None and market.canonical_symbol != symbol):
            continue
        feeds.setdefault(market.canonical_symbol, set()).add(
            (market.venue, market.venue_symbol)
        )
    return {
        canonical_symbol: tuple(sorted(symbols))
        for canonical_symbol, symbols in feeds.items()
    }


def _pair_states(
    canonical_symbol: str,
    feeds: tuple[tuple[str, str], ...],
    parameters: AnomalyParameters,
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
                tracker=AnomalyTracker(key, parameters),
                basis=RollingBasis(),
                windows={
                    name: _PriorWindow(seconds)
                    for name, seconds in CONTEXT_WINDOW_SECONDS.items()
                },
                confirmation_contexts={},
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
    market_files = _market_files_for_window(data_root, start=start, end=end)
    if not market_files:
        return
    market_paths = ", ".join(
        "'" + str(path).replace("'", "''") + "'" for path in market_files
    )
    feed_descriptors = [
        (canonical_symbol, venue, venue_symbol)
        for canonical_symbol, feeds in sorted(feeds_by_symbol.items())
        for venue, venue_symbol in feeds
    ]
    predicates = " OR ".join(
        "(canonical_symbol = ? AND venue = ? AND venue_symbol = ?)"
        for _ in feed_descriptors
    )
    query = f"""
        WITH filtered AS (
            SELECT
                sample_time,
                observed_at,
                venue,
                venue_symbol,
                canonical_symbol,
                buy_10k_vwap,
                sell_10k_vwap,
                row_number() OVER (
                    PARTITION BY sample_time, venue, venue_symbol, canonical_symbol
                    ORDER BY observed_at DESC
                ) AS row_number
            FROM read_parquet([{market_paths}])
            WHERE sample_time >= ?
              AND sample_time < ?
              AND ({predicates})
        )
        SELECT
            sample_time,
            observed_at,
            venue,
            venue_symbol,
            canonical_symbol,
            buy_10k_vwap,
            sell_10k_vwap
        FROM filtered
        WHERE row_number = 1
        ORDER BY canonical_symbol, sample_time, observed_at
    """
    parameters: list[object] = [start, end]
    for descriptor in feed_descriptors:
        parameters.extend(descriptor)

    with duckdb.connect() as connection:
        result = connection.execute(query, parameters)
        reader = result.to_arrow_reader(batch_size=QUERY_BATCH_SIZE)
        current_symbol: str | None = None
        current_sample: datetime | None = None
        current_rows: dict[tuple[str, str], tuple[datetime, object, object]] = {}
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
                current_rows[feed_key] = row
        if current_symbol is not None and current_sample is not None:
            yield current_symbol, current_sample, current_rows


def _market_files_for_window(
    data_root: Path,
    *,
    start: datetime,
    end: datetime,
) -> tuple[Path, ...]:
    first_date = start.date()
    last_date = (end - timedelta(microseconds=1)).date()
    files: list[Path] = []
    current_date = first_date
    while current_date <= last_date:
        partition = data_root / "market" / f"date={current_date.isoformat()}"
        files.extend(
            path for path in sorted(partition.glob("part-*.parquet")) if path.is_file()
        )
        current_date += timedelta(days=1)
    return tuple(files)


def _collect_replay_episodes(
    output: list[ReplayEpisode],
    states: dict[SpreadPairKey, _PairState],
    *,
    start: datetime,
    end: datetime,
) -> None:
    for state in states.values():
        state.tracker.finalize()
        for episode in state.tracker.confirmed_episodes:
            if not _intersects_window(episode, start, end):
                continue
            context = state.confirmation_contexts.get(episode.episode_id)
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
        age_seconds = (sample_time - observed_at).total_seconds()
        if age_seconds < 0 or age_seconds > stale_after_seconds:
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
        context_stats = {
            name: window.stats_before(sample_time)
            for name, window in state.windows.items()
        }
        for window in state.windows.values():
            window.append(sample_time, raw_spread_bps)
        transitions = state.tracker.observe(
            AnomalyObservation(
                sample_time=sample_time,
                raw_spread_bps=raw_spread_bps,
                rolling_mean_bps=basis_stats.mean_bps,
                rolling_std_bps=basis_stats.std_bps,
                basis_eligible=basis_stats.eligible,
            )
        )
        for transition in transitions:
            if transition.kind == "confirmed":
                state.confirmation_contexts[transition.episode.episode_id] = (
                    ConfirmationContext(
                        stats_12h=context_stats["12h"],
                        stats_24h=context_stats["24h"],
                        stats_48h=context_stats["48h"],
                    )
                )


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


def _time_string(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if __import__("sys").platform == "darwin" else value * 1024


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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    config = load_config(args.config)
    result = replay_market_data(
        data_root=args.data_root,
        config=config,
        start=_parse_boundary(args.start, end=False),
        end=_parse_boundary(args.end, end=True),
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
        write_replay_csv(args.output_csv, result)
    print(f"symbols={len(result.processed_symbols)}")
    print(f"episodes={len(result.episodes)}")
    print(f"elapsed_seconds={result.elapsed_seconds:.3f}")
    print(f"peak_rss_mb={result.peak_rss_bytes / 1024 / 1024:.1f}")
    for replay_episode in result.episodes:
        print(replay_episode.to_row())
    if args.output_csv is not None:
        print(f"csv={args.output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
