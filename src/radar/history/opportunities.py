from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median

import duckdb  # type: ignore[import-untyped]

from radar.config import RadarConfig
from radar.monitors.spread.models import (
    SpreadPairKey,
    calculate_net_spread_bps,
    calculate_raw_spread_bps,
)

UTC_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S UTC"
VWAP_COLUMNS = {
    1_000: ("buy_1k_vwap", "sell_1k_vwap"),
    5_000: ("buy_5k_vwap", "sell_5k_vwap"),
    10_000: ("buy_10k_vwap", "sell_10k_vwap"),
}


@dataclass(frozen=True)
class HistoricalOpportunitySample:
    key: SpreadPairKey
    sample_time: datetime
    long_observed_at: datetime
    short_observed_at: datetime
    long_buy_vwap: float
    short_sell_vwap: float
    raw_spread_bps: float
    net_spread_bps: float

    @property
    def observed_skew_ms(self) -> float:
        return abs(
            (self.long_observed_at - self.short_observed_at).total_seconds() * 1000
        )


@dataclass(frozen=True)
class HistoricalOpportunityEpisode:
    key: SpreadPairKey
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    sample_count: int
    first_net_spread_bps: float
    max_net_spread_bps: float
    median_net_spread_bps: float
    last_net_spread_bps: float
    max_raw_spread_bps: float
    median_observed_skew_ms: float
    max_observed_skew_ms: float
    candidate_confirmed: bool
    alert_qualified: bool


@dataclass(frozen=True)
class HistoricalPairSummary:
    key: SpreadPairKey
    sample_count: int
    candidate_sample_count: int
    candidate_percent: float
    max_net_spread_bps: float
    median_net_spread_bps: float
    p95_net_spread_bps: float
    confirmed_candidate_episode_count: int
    alert_qualified_episode_count: int


@dataclass(frozen=True)
class HistoricalOpportunityReport:
    data_as_of: datetime | None
    window_start: datetime | None
    window_end: datetime | None
    primary_size_usd: int
    min_net_bps: float
    candidate_net_bps: float
    candidate_duration_seconds: int
    alert_net_bps: float
    alert_duration_seconds: int
    venue_count: int
    feed_count: int
    sample_count: int
    pair_samples: tuple[HistoricalOpportunitySample, ...]
    latest_spreads: tuple[HistoricalOpportunitySample, ...]
    episodes: tuple[HistoricalOpportunityEpisode, ...]
    unconfirmed_spikes: tuple[HistoricalOpportunityEpisode, ...]
    pair_summaries: tuple[HistoricalPairSummary, ...]


@dataclass(frozen=True)
class _MarketRow:
    sample_time: datetime
    observed_at: datetime
    venue: str
    venue_symbol: str
    canonical_symbol: str
    buy_vwap: float | None
    sell_vwap: float | None


def _as_utc(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _positive_float(value: object) -> float | None:
    if value is None or isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result) or result <= 0:
        return None
    return result


def _fee_for(venue: str, fees_bps: Mapping[str, float]) -> float | None:
    for configured_venue, value in fees_bps.items():
        if not isinstance(configured_venue, str):
            continue
        if configured_venue.lower() != venue.lower():
            continue
        if isinstance(value, bool):
            return None
        try:
            fee = float(value)
        except (TypeError, ValueError):
            return None
        return fee if math.isfinite(fee) and fee >= 0 else None
    return None


def _p95(values: list[float]) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("p95 requires at least one value")
    position = (len(ordered) - 1) * 0.95
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _market_files(data_root: Path) -> tuple[Path, ...]:
    return tuple(
        path
        for path in sorted(data_root.glob("market/date=*/part-*.parquet"))
        if path.is_file()
    )


def _read_market_rows(data_root: Path, primary_size_usd: int) -> tuple[_MarketRow, ...]:
    try:
        buy_column, sell_column = VWAP_COLUMNS[primary_size_usd]
    except KeyError as exc:
        raise ValueError("size_usd must be 1000, 5000, or 10000") from exc

    if not _market_files(data_root):
        return ()

    market_glob = str(data_root / "market" / "date=*" / "part-*.parquet").replace(
        "'", "''"
    )
    query = f"""
        WITH ranked AS (
            SELECT
                sample_time,
                observed_at,
                venue,
                venue_symbol,
                canonical_symbol,
                {buy_column} AS buy_vwap,
                {sell_column} AS sell_vwap,
                row_number() OVER (
                    PARTITION BY venue, venue_symbol, canonical_symbol, sample_time
                    ORDER BY observed_at DESC
                ) AS row_number
            FROM read_parquet('{market_glob}')
        )
        SELECT
            sample_time,
            observed_at,
            venue,
            venue_symbol,
            canonical_symbol,
            buy_vwap,
            sell_vwap
        FROM ranked
        WHERE row_number = 1
    """
    with duckdb.connect() as connection:
        raw_rows = connection.execute(query).fetchall()

    rows: list[_MarketRow] = []
    for raw_row in raw_rows:
        try:
            sample_time = _as_utc(raw_row[0], "sample_time")
            observed_at = _as_utc(raw_row[1], "observed_at")
        except (IndexError, ValueError):
            continue
        values = raw_row[2:5]
        if len(values) != 3 or any(
            not isinstance(value, str) or not value.strip() for value in values
        ):
            continue
        venue, venue_symbol, canonical_symbol = values
        rows.append(
            _MarketRow(
                sample_time=sample_time,
                observed_at=observed_at,
                venue=venue,
                venue_symbol=venue_symbol,
                canonical_symbol=canonical_symbol,
                buy_vwap=_positive_float(raw_row[5]),
                sell_vwap=_positive_float(raw_row[6]),
            )
        )
    return tuple(rows)


def _pair_sort_key(key: SpreadPairKey) -> tuple[str, str, str, str, str]:
    return (
        key.canonical_symbol,
        key.long_venue,
        key.long_venue_symbol,
        key.short_venue,
        key.short_venue_symbol,
    )


def _build_pair_samples(
    rows: tuple[_MarketRow, ...],
    fees_bps: Mapping[str, float],
) -> tuple[HistoricalOpportunitySample, ...]:
    grouped: dict[tuple[datetime, str], list[_MarketRow]] = defaultdict(list)
    for row in rows:
        grouped[(row.sample_time, row.canonical_symbol)].append(row)

    samples: list[HistoricalOpportunitySample] = []
    for (sample_time, canonical_symbol), grouped_rows in grouped.items():
        for long_row in grouped_rows:
            if long_row.buy_vwap is None:
                continue
            long_fee = _fee_for(long_row.venue, fees_bps)
            if long_fee is None:
                continue
            for short_row in grouped_rows:
                if long_row.venue.lower() == short_row.venue.lower():
                    continue
                if short_row.sell_vwap is None:
                    continue
                short_fee = _fee_for(short_row.venue, fees_bps)
                if short_fee is None:
                    continue
                try:
                    raw_spread = calculate_raw_spread_bps(
                        long_row.buy_vwap,
                        short_row.sell_vwap,
                    )
                    net_spread = calculate_net_spread_bps(
                        raw_spread,
                        long_fee,
                        short_fee,
                    )
                except (OverflowError, ValueError):
                    continue
                samples.append(
                    HistoricalOpportunitySample(
                        key=SpreadPairKey(
                            canonical_symbol=canonical_symbol,
                            long_venue=long_row.venue,
                            long_venue_symbol=long_row.venue_symbol,
                            short_venue=short_row.venue,
                            short_venue_symbol=short_row.venue_symbol,
                        ),
                        sample_time=sample_time,
                        long_observed_at=long_row.observed_at,
                        short_observed_at=short_row.observed_at,
                        long_buy_vwap=long_row.buy_vwap,
                        short_sell_vwap=short_row.sell_vwap,
                        raw_spread_bps=raw_spread,
                        net_spread_bps=net_spread,
                    )
                )
    return tuple(
        sorted(
            samples,
            key=lambda sample: (sample.sample_time, _pair_sort_key(sample.key)),
        )
    )


def _episode_from_points(
    points: list[HistoricalOpportunitySample],
    *,
    interval_seconds: int,
    candidate_duration_seconds: int,
    alert_net_bps: float,
    alert_duration_seconds: int,
) -> HistoricalOpportunityEpisode:
    first = points[0]
    last = points[-1]
    duration_seconds = (last.sample_time - first.sample_time).total_seconds()
    candidate_confirmed = duration_seconds >= candidate_duration_seconds

    high_segment: list[HistoricalOpportunitySample] = []
    alert_qualified = False

    def finish_high_segment() -> None:
        nonlocal alert_qualified
        if not high_segment:
            return
        segment_duration = (
            high_segment[-1].sample_time - high_segment[0].sample_time
        ).total_seconds()
        if segment_duration >= alert_duration_seconds:
            alert_qualified = True

    previous_high: HistoricalOpportunitySample | None = None
    for point in points:
        if point.net_spread_bps < alert_net_bps:
            finish_high_segment()
            high_segment.clear()
            previous_high = None
            continue
        if (
            previous_high is not None
            and (point.sample_time - previous_high.sample_time).total_seconds()
            > interval_seconds * 2
        ):
            finish_high_segment()
            high_segment.clear()
        high_segment.append(point)
        previous_high = point
    finish_high_segment()

    return HistoricalOpportunityEpisode(
        key=first.key,
        start_time=first.sample_time,
        end_time=last.sample_time,
        duration_seconds=duration_seconds,
        sample_count=len(points),
        first_net_spread_bps=first.net_spread_bps,
        max_net_spread_bps=max(point.net_spread_bps for point in points),
        median_net_spread_bps=float(
            median(point.net_spread_bps for point in points)
        ),
        last_net_spread_bps=last.net_spread_bps,
        max_raw_spread_bps=max(point.raw_spread_bps for point in points),
        median_observed_skew_ms=float(median(point.observed_skew_ms for point in points)),
        max_observed_skew_ms=max(point.observed_skew_ms for point in points),
        candidate_confirmed=candidate_confirmed,
        alert_qualified=candidate_confirmed and alert_qualified,
    )


def _build_episodes(
    samples: tuple[HistoricalOpportunitySample, ...],
    *,
    candidate_net_bps: float,
    interval_seconds: int,
    candidate_duration_seconds: int,
    alert_net_bps: float,
    alert_duration_seconds: int,
) -> tuple[HistoricalOpportunityEpisode, ...]:
    by_key: dict[SpreadPairKey, list[HistoricalOpportunitySample]] = defaultdict(list)
    for sample in samples:
        by_key[sample.key].append(sample)

    episodes: list[HistoricalOpportunityEpisode] = []
    for key in sorted(by_key, key=_pair_sort_key):
        current: list[HistoricalOpportunitySample] = []
        previous_qualifying: HistoricalOpportunitySample | None = None
        for sample in by_key[key]:
            if sample.net_spread_bps < candidate_net_bps:
                if current:
                    episodes.append(
                        _episode_from_points(
                            current,
                            interval_seconds=interval_seconds,
                            candidate_duration_seconds=candidate_duration_seconds,
                            alert_net_bps=alert_net_bps,
                            alert_duration_seconds=alert_duration_seconds,
                        )
                    )
                current = []
                previous_qualifying = None
                continue
            if (
                previous_qualifying is not None
                and (
                    sample.sample_time - previous_qualifying.sample_time
                ).total_seconds()
                > interval_seconds * 2
            ):
                if current:
                    episodes.append(
                        _episode_from_points(
                            current,
                            interval_seconds=interval_seconds,
                            candidate_duration_seconds=candidate_duration_seconds,
                            alert_net_bps=alert_net_bps,
                            alert_duration_seconds=alert_duration_seconds,
                        )
                    )
                current = []
            current.append(sample)
            previous_qualifying = sample
        if current:
            episodes.append(
                _episode_from_points(
                    current,
                    interval_seconds=interval_seconds,
                    candidate_duration_seconds=candidate_duration_seconds,
                    alert_net_bps=alert_net_bps,
                    alert_duration_seconds=alert_duration_seconds,
                )
            )

    return tuple(
        sorted(
            episodes,
            key=lambda episode: (
                not episode.alert_qualified,
                not episode.candidate_confirmed,
                -episode.max_net_spread_bps,
                episode.start_time,
                _pair_sort_key(episode.key),
            ),
        )
    )


def _build_pair_summaries(
    samples: tuple[HistoricalOpportunitySample, ...],
    episodes: tuple[HistoricalOpportunityEpisode, ...],
    *,
    candidate_net_bps: float,
    min_net_bps: float,
) -> tuple[HistoricalPairSummary, ...]:
    by_key: dict[SpreadPairKey, list[HistoricalOpportunitySample]] = defaultdict(list)
    for sample in samples:
        by_key[sample.key].append(sample)
    episodes_by_key: dict[SpreadPairKey, list[HistoricalOpportunityEpisode]] = defaultdict(
        list
    )
    for episode in episodes:
        episodes_by_key[episode.key].append(episode)

    summaries: list[HistoricalPairSummary] = []
    for key, pair_samples in by_key.items():
        candidate_samples = [
            sample
            for sample in pair_samples
            if sample.net_spread_bps >= candidate_net_bps
        ]
        pair_episodes = episodes_by_key[key]
        if not candidate_samples and not any(
            sample.net_spread_bps >= min_net_bps for sample in pair_samples
        ) and not any(episode.candidate_confirmed for episode in pair_episodes):
            continue
        values = [sample.net_spread_bps for sample in pair_samples]
        summaries.append(
            HistoricalPairSummary(
                key=key,
                sample_count=len(pair_samples),
                candidate_sample_count=len(candidate_samples),
                candidate_percent=100.0 * len(candidate_samples) / len(pair_samples),
                max_net_spread_bps=max(values),
                median_net_spread_bps=float(median(values)),
                p95_net_spread_bps=_p95(values),
                confirmed_candidate_episode_count=sum(
                    episode.candidate_confirmed for episode in pair_episodes
                ),
                alert_qualified_episode_count=sum(
                    episode.alert_qualified for episode in pair_episodes
                ),
            )
        )
    return tuple(
        sorted(
            summaries,
            key=lambda summary: (
                -summary.confirmed_candidate_episode_count,
                -summary.max_net_spread_bps,
                _pair_sort_key(summary.key),
            ),
        )
    )


def _empty_report(
    primary_size_usd: int,
    min_net_bps: float,
    *,
    candidate_net_bps: float,
    candidate_duration_seconds: int,
    alert_net_bps: float,
    alert_duration_seconds: int,
) -> HistoricalOpportunityReport:
    return HistoricalOpportunityReport(
        data_as_of=None,
        window_start=None,
        window_end=None,
        primary_size_usd=primary_size_usd,
        min_net_bps=min_net_bps,
        candidate_net_bps=candidate_net_bps,
        candidate_duration_seconds=candidate_duration_seconds,
        alert_net_bps=alert_net_bps,
        alert_duration_seconds=alert_duration_seconds,
        venue_count=0,
        feed_count=0,
        sample_count=0,
        pair_samples=(),
        latest_spreads=(),
        episodes=(),
        unconfirmed_spikes=(),
        pair_summaries=(),
    )


def build_opportunity_report(
    data_root: Path,
    config: RadarConfig,
    *,
    hours: float = 6.0,
    top: int = 20,
    size_usd: int | None = None,
    symbol: str | None = None,
    min_net_bps: float | None = None,
) -> HistoricalOpportunityReport:
    if not math.isfinite(hours) or hours <= 0:
        raise ValueError("hours must be a positive finite number")
    if top <= 0:
        raise ValueError("top must be positive")
    primary_size_usd = (
        config.monitors.spread.primary_size_usd if size_usd is None else size_usd
    )
    if primary_size_usd not in VWAP_COLUMNS:
        raise ValueError("size_usd must be 1000, 5000, or 10000")
    threshold = (
        config.monitors.spread.candidate_net_bps
        if min_net_bps is None
        else min_net_bps
    )
    if not math.isfinite(threshold):
        raise ValueError("min_net_bps must be finite")

    rows = _read_market_rows(Path(data_root), primary_size_usd)
    if not rows:
        return _empty_report(
            primary_size_usd,
            threshold,
            candidate_net_bps=config.monitors.spread.candidate_net_bps,
            candidate_duration_seconds=config.monitors.spread.candidate_duration_seconds,
            alert_net_bps=config.monitors.spread.alert_net_bps,
            alert_duration_seconds=config.monitors.spread.alert_duration_seconds,
        )

    data_as_of = max(row.sample_time for row in rows)
    window_start = data_as_of - timedelta(hours=hours)
    selected_symbol = symbol.strip().upper() if symbol is not None else None
    window_rows = tuple(
        row
        for row in rows
        if window_start <= row.sample_time <= data_as_of
        and (
            selected_symbol is None
            or row.canonical_symbol.upper() == selected_symbol
        )
    )
    pair_samples = _build_pair_samples(window_rows, config.fees_bps)
    episodes = _build_episodes(
        pair_samples,
        candidate_net_bps=config.monitors.spread.candidate_net_bps,
        interval_seconds=config.monitors.spread.interval_seconds,
        candidate_duration_seconds=config.monitors.spread.candidate_duration_seconds,
        alert_net_bps=config.monitors.spread.alert_net_bps,
        alert_duration_seconds=config.monitors.spread.alert_duration_seconds,
    )
    latest_sample_time = (
        max(row.sample_time for row in window_rows) if window_rows else None
    )
    latest_spreads: tuple[HistoricalOpportunitySample, ...] = ()
    if latest_sample_time is not None:
        latest_spreads = tuple(
            sorted(
                (
                    sample
                    for sample in pair_samples
                    if sample.sample_time == latest_sample_time
                    and sample.net_spread_bps >= threshold
                ),
                key=lambda sample: (
                    -sample.net_spread_bps,
                    _pair_sort_key(sample.key),
                ),
            )[:top]
        )
    pair_summaries = _build_pair_summaries(
        pair_samples,
        episodes,
        candidate_net_bps=config.monitors.spread.candidate_net_bps,
        min_net_bps=threshold,
    )
    return HistoricalOpportunityReport(
        data_as_of=data_as_of,
        window_start=window_start,
        window_end=data_as_of,
        primary_size_usd=primary_size_usd,
        min_net_bps=threshold,
        candidate_net_bps=config.monitors.spread.candidate_net_bps,
        candidate_duration_seconds=config.monitors.spread.candidate_duration_seconds,
        alert_net_bps=config.monitors.spread.alert_net_bps,
        alert_duration_seconds=config.monitors.spread.alert_duration_seconds,
        venue_count=len({row.venue for row in window_rows}),
        feed_count=len(
            {(row.venue, row.venue_symbol, row.canonical_symbol) for row in window_rows}
        ),
        sample_count=len({row.sample_time for row in window_rows}),
        pair_samples=pair_samples,
        latest_spreads=latest_spreads,
        episodes=episodes,
        unconfirmed_spikes=tuple(
            episode for episode in episodes if not episode.candidate_confirmed
        ),
        pair_summaries=pair_summaries,
    )


def _format_timestamp(value: datetime | None) -> str:
    return "—" if value is None else value.astimezone(UTC).strftime(UTC_TIMESTAMP_FORMAT)


def _format_bps(value: float) -> str:
    return f"{value:.2f}"


def _format_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    if not rows:
        return ["No rows."]
    widths = [len(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))
    lines = [
        "  ".join(value.ljust(widths[index]) for index, value in enumerate(headers))
    ]
    lines.extend(
        "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))
        for row in rows
    )
    return lines


def format_opportunity_report(report: HistoricalOpportunityReport, *, top: int = 20) -> str:
    lines = [
        "=" * 60,
        "OPPORTUNITY RADAR REPORT",
        "=" * 60,
    ]
    if report.data_as_of is None:
        lines.append("No market data found in data/market.")
        return "\n".join(lines)

    lines.extend(
        [
            f"Data as of: {_format_timestamp(report.data_as_of)}",
            f"Window:     {_format_timestamp(report.window_start)} -> {_format_timestamp(report.window_end)}",
            f"VWAP size:  ${report.primary_size_usd:,.0f}",
            f"Venues:     {report.venue_count}",
            f"Feeds:      {report.feed_count}",
            f"Samples:    {report.sample_count}",
            f"Candidate:  >= {_format_bps(report.candidate_net_bps)} bps "
            f"for {report.candidate_duration_seconds}s",
            f"Alert:      >= {_format_bps(report.alert_net_bps)} bps "
            f"for {report.alert_duration_seconds}s",
            f"Report filter: >= {_format_bps(report.min_net_bps)} bps",
            "",
            "-" * 60,
            "1. CURRENT / LATEST SPREADS",
            "-" * 60,
        ]
    )
    if not report.latest_spreads:
        lines.append(f"No current pair exceeds {_format_bps(report.min_net_bps)} bps.")
    else:
        latest_rows = [
            [
                sample.key.canonical_symbol,
                f"{sample.key.long_venue} -> {sample.key.short_venue}",
                _format_bps(sample.raw_spread_bps),
                _format_bps(sample.net_spread_bps),
                f"{sample.observed_skew_ms:.0f}ms",
            ]
            for sample in report.latest_spreads[:top]
        ]
        lines.extend(
            _format_table(
                ["Symbol", "Pair", "Raw bps", "Net bps", "Skew"],
                latest_rows,
            )
        )

    lines.extend(["", "-" * 60, "2. CONFIRMED OPPORTUNITY EPISODES", "-" * 60])
    confirmed = [episode for episode in report.episodes if episode.candidate_confirmed]
    if not confirmed:
        lines.append("No confirmed candidate episodes.")
    else:
        lines.extend(
            _format_table(
                ["Symbol", "Pair", "Start", "Dur", "N", "Max", "Med", "SkewMax", "Alert"],
                [
                    [
                        episode.key.canonical_symbol,
                        f"{episode.key.long_venue} -> {episode.key.short_venue}",
                        _format_timestamp(episode.start_time),
                        f"{episode.duration_seconds:.0f}s",
                        str(episode.sample_count),
                        _format_bps(episode.max_net_spread_bps),
                        _format_bps(episode.median_net_spread_bps),
                        f"{episode.max_observed_skew_ms:.0f}ms",
                        "yes" if episode.alert_qualified else "no",
                    ]
                    for episode in confirmed
                ],
            )
        )

    lines.extend(["", "-" * 60, "3. TOP UNCONFIRMED SPIKES", "-" * 60])
    if not report.unconfirmed_spikes:
        lines.append("No unconfirmed candidate spikes.")
    else:
        lines.extend(
            _format_table(
                ["Symbol", "Pair", "Start", "Max bps", "Dur", "N", "SkewMax"],
                [
                    [
                        episode.key.canonical_symbol,
                        f"{episode.key.long_venue} -> {episode.key.short_venue}",
                        _format_timestamp(episode.start_time),
                        _format_bps(episode.max_net_spread_bps),
                        f"{episode.duration_seconds:.0f}s",
                        str(episode.sample_count),
                        f"{episode.max_observed_skew_ms:.0f}ms",
                    ]
                    for episode in report.unconfirmed_spikes[:top]
                ],
            )
        )

    lines.extend(["", "-" * 60, "4. PAIR SUMMARY", "-" * 60])
    if not report.pair_summaries:
        lines.append("No useful pairs in the requested window.")
    else:
        lines.extend(
            _format_table(
                ["Symbol", "Pair", "N", ">=Cand", "%", "Max", "Med", "P95", "CandEp", "AlertEp"],
                [
                    [
                        summary.key.canonical_symbol,
                        f"{summary.key.long_venue} -> {summary.key.short_venue}",
                        str(summary.sample_count),
                        str(summary.candidate_sample_count),
                        f"{summary.candidate_percent:.1f}%",
                        _format_bps(summary.max_net_spread_bps),
                        _format_bps(summary.median_net_spread_bps),
                        _format_bps(summary.p95_net_spread_bps),
                        str(summary.confirmed_candidate_episode_count),
                        str(summary.alert_qualified_episode_count),
                    ]
                    for summary in report.pair_summaries
                ],
            )
        )

    lines.extend(["", "Summary:"])
    lines.append(
        f"- {sum(episode.candidate_confirmed for episode in report.episodes)} "
        "confirmed candidate episodes"
    )
    lines.append(
        f"- {sum(episode.alert_qualified for episode in report.episodes)} "
        "alert-qualified episodes"
    )
    if report.episodes:
        strongest = max(report.episodes, key=lambda episode: episode.max_net_spread_bps)
        longest = max(report.episodes, key=lambda episode: episode.duration_seconds)
        lines.append(
            f"- strongest: {strongest.key.canonical_symbol} "
            f"{strongest.key.long_venue} -> {strongest.key.short_venue}, "
            f"max {_format_bps(strongest.max_net_spread_bps)} bps"
        )
        lines.append(
            f"- longest: {longest.key.canonical_symbol} "
            f"{longest.key.long_venue} -> {longest.key.short_venue}, "
            f"{longest.duration_seconds:.0f}s"
        )
    lines.append(f"- {len(report.unconfirmed_spikes)} unconfirmed spikes")
    return "\n".join(lines)
