from __future__ import annotations

import math
from bisect import bisect_right
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median

import duckdb  # type: ignore[import-untyped]

from radar.config import MarketConfig, RadarConfig
from radar.monitors.spread.models import calculate_raw_spread_bps

MAX_MARKET_AGE_SECONDS = 20.0
PERSISTENCE_THRESHOLDS_BPS = (10.0, 20.0, 50.0)
VARIATIONAL_LONG = "variational_long"
OTHER_LONG = "other_long"


@dataclass(frozen=True)
class VariationalPairKey:
    canonical_symbol: str
    direction: str
    other_venue: str
    other_venue_symbol: str


@dataclass(frozen=True)
class _QuoteRow:
    quote_time: datetime
    fetched_at: datetime
    venue_symbol: str
    canonical_symbol: str
    bid_1k: float
    ask_1k: float
    funding_rate: float
    funding_interval_seconds: int


@dataclass(frozen=True)
class _MarketRow:
    sample_time: datetime
    observed_at: datetime
    venue: str
    venue_symbol: str
    canonical_symbol: str
    buy_vwap: float | None
    sell_vwap: float | None


@dataclass(frozen=True)
class _FundingRow:
    effective_time: datetime
    observed_at: datetime
    venue: str
    venue_symbol: str
    canonical_symbol: str
    funding_rate: float
    next_funding_time: datetime | None


@dataclass(frozen=True)
class VariationalOpportunityObservation:
    key: VariationalPairKey
    quote_time: datetime
    fetched_at: datetime
    market_sample_time: datetime
    market_observed_at: datetime
    variational_bid_1k: float
    variational_ask_1k: float
    other_buy_1k_vwap: float | None
    other_sell_1k_vwap: float | None
    long_entry_price: float
    short_entry_price: float
    raw_spread_bps: float
    other_venue_fee_bps: float
    indicative_net_bps: float
    market_age_at_quote_ms: float
    observed_skew_ms: float
    variational_funding_rate: float
    variational_funding_interval_seconds: int
    other_funding_rate: float | None
    other_funding_effective_time: datetime | None
    other_funding_interval_seconds: int | None = None

    @property
    def canonical_symbol(self) -> str:
        return self.key.canonical_symbol

    @property
    def direction(self) -> str:
        return self.key.direction

    @property
    def other_venue(self) -> str:
        return self.key.other_venue


@dataclass(frozen=True)
class VariationalPairSummary:
    key: VariationalPairKey
    sample_count: int
    median_net_bps: float
    p90_net_bps: float
    p95_net_bps: float
    max_net_bps: float
    positive_percent: float
    count_ge_10: int
    count_ge_20: int
    count_ge_50: int

    @property
    def canonical_symbol(self) -> str:
        return self.key.canonical_symbol

    @property
    def direction(self) -> str:
        return self.key.direction

    @property
    def other_venue(self) -> str:
        return self.key.other_venue


@dataclass(frozen=True)
class VariationalPersistenceEpisode:
    key: VariationalPairKey
    threshold_bps: float
    start_quote_time: datetime
    end_quote_time: datetime
    quote_update_count: int
    duration_seconds: float
    median_net_bps: float
    max_net_bps: float

    @property
    def canonical_symbol(self) -> str:
        return self.key.canonical_symbol

    @property
    def direction(self) -> str:
        return self.key.direction

    @property
    def other_venue(self) -> str:
        return self.key.other_venue


@dataclass(frozen=True)
class VariationalOpportunityReport:
    data_as_of: datetime | None
    window_start: datetime | None
    window_end: datetime | None
    display_min_net_bps: float
    total_quote_observations: int
    matched_observations: int
    unmatched_observations: int
    symbol_count: int
    pair_count: int
    count_ge_10: int
    count_ge_20: int
    count_ge_50: int
    largest_spread_bps: float | None
    most_persistent_episode: VariationalPersistenceEpisode | None
    observations: tuple[VariationalOpportunityObservation, ...]
    current_latest: tuple[VariationalOpportunityObservation, ...]
    top_opportunities: tuple[VariationalOpportunityObservation, ...]
    pair_summaries: tuple[VariationalPairSummary, ...]
    persistence_episodes: tuple[VariationalPersistenceEpisode, ...]


def _as_utc(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _finite_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _positive_float(value: object) -> float | None:
    result = _finite_float(value)
    return result if result is not None and result > 0 else None


def _fee_for(venue: str, fees_bps: Mapping[str, float]) -> float | None:
    for configured_venue, value in fees_bps.items():
        if not isinstance(configured_venue, str):
            continue
        if configured_venue.lower() != venue.lower():
            continue
        fee = _finite_float(value)
        return fee if fee is not None and fee >= 0 else None
    return None


def _dataset_files(data_root: Path, dataset: str) -> tuple[Path, ...]:
    return tuple(
        path
        for path in sorted(data_root.glob(f"{dataset}/date=*/part-*.parquet"))
        if path.is_file()
    )


def _parquet_glob(data_root: Path, dataset: str) -> str:
    return str(data_root / dataset / "date=*" / "part-*.parquet").replace("'", "''")


def _configured_quotes(
    config: RadarConfig,
    symbol: str | None,
) -> dict[str, MarketConfig]:
    selected_symbol = symbol.strip().upper() if symbol is not None else None
    configured: dict[str, MarketConfig] = {}
    for market in config.quoted_markets:
        if not market.enabled or market.venue.lower() != "variational":
            continue
        if selected_symbol is not None and market.canonical_symbol.upper() != selected_symbol:
            continue
        configured[market.venue_symbol] = market
    return configured


def _read_quote_rows(
    data_root: Path,
    config: RadarConfig,
    *,
    symbol: str | None,
) -> tuple[_QuoteRow, ...]:
    files = _dataset_files(data_root, "quoted_market")
    configured = _configured_quotes(config, symbol)
    if not files or not configured:
        return ()

    quote_glob = _parquet_glob(data_root, "quoted_market")
    query = f"""
        WITH ranked AS (
            SELECT
                quote_time,
                fetched_at,
                venue_symbol,
                canonical_symbol,
                bid_1k,
                ask_1k,
                funding_rate,
                funding_interval_seconds,
                row_number() OVER (
                    PARTITION BY venue_symbol, canonical_symbol, quote_time
                    ORDER BY fetched_at DESC
                ) AS row_number
            FROM read_parquet('{quote_glob}')
            WHERE lower(venue) = 'variational'
        )
        SELECT
            quote_time,
            fetched_at,
            venue_symbol,
            canonical_symbol,
            bid_1k,
            ask_1k,
            funding_rate,
            funding_interval_seconds
        FROM ranked
        WHERE row_number = 1
        ORDER BY quote_time, venue_symbol
    """
    with duckdb.connect() as connection:
        raw_rows = connection.execute(query).fetchall()

    rows: list[_QuoteRow] = []
    for raw_row in raw_rows:
        try:
            quote_time = _as_utc(raw_row[0], "quote_time")
            fetched_at = _as_utc(raw_row[1], "fetched_at")
            venue_symbol = raw_row[2]
            canonical_symbol = raw_row[3]
            if not isinstance(venue_symbol, str) or not isinstance(canonical_symbol, str):
                continue
            market = configured.get(venue_symbol)
            if market is None or market.canonical_symbol != canonical_symbol:
                continue
            bid_1k = _positive_float(raw_row[4])
            ask_1k = _positive_float(raw_row[5])
            funding_rate = _finite_float(raw_row[6])
            funding_interval = raw_row[7]
            if bid_1k is None or ask_1k is None or funding_rate is None:
                continue
            if isinstance(funding_interval, bool) or not isinstance(
                funding_interval, (int, float)
            ):
                continue
            if int(funding_interval) != funding_interval or funding_interval <= 0:
                continue
        except (IndexError, TypeError, ValueError, OverflowError):
            continue
        rows.append(
            _QuoteRow(
                quote_time=quote_time,
                fetched_at=fetched_at,
                venue_symbol=venue_symbol,
                canonical_symbol=canonical_symbol,
                bid_1k=bid_1k,
                ask_1k=ask_1k,
                funding_rate=funding_rate,
                funding_interval_seconds=int(funding_interval),
            )
        )
    return tuple(rows)


def _read_market_rows(data_root: Path) -> tuple[_MarketRow, ...]:
    if not _dataset_files(data_root, "market"):
        return ()
    market_glob = _parquet_glob(data_root, "market")
    query = f"""
        WITH ranked AS (
            SELECT
                sample_time,
                observed_at,
                venue,
                venue_symbol,
                canonical_symbol,
                buy_1k_vwap,
                sell_1k_vwap,
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
            buy_1k_vwap,
            sell_1k_vwap
        FROM ranked
        WHERE row_number = 1
        ORDER BY sample_time, venue, venue_symbol
    """
    with duckdb.connect() as connection:
        raw_rows = connection.execute(query).fetchall()

    rows: list[_MarketRow] = []
    for raw_row in raw_rows:
        try:
            sample_time = _as_utc(raw_row[0], "sample_time")
            observed_at = _as_utc(raw_row[1], "observed_at")
        except (IndexError, TypeError, ValueError):
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


def _read_funding_rows(data_root: Path) -> tuple[_FundingRow, ...]:
    if not _dataset_files(data_root, "funding"):
        return ()
    funding_glob = _parquet_glob(data_root, "funding")
    query = f"""
        SELECT
            effective_time,
            observed_at,
            venue,
            venue_symbol,
            canonical_symbol,
            funding_rate,
            next_funding_time
        FROM read_parquet('{funding_glob}')
        ORDER BY observed_at, venue, venue_symbol
    """
    with duckdb.connect() as connection:
        raw_rows = connection.execute(query).fetchall()

    rows: list[_FundingRow] = []
    for raw_row in raw_rows:
        try:
            effective_time = _as_utc(raw_row[0], "effective_time")
            observed_at = _as_utc(raw_row[1], "observed_at")
            venue, venue_symbol, canonical_symbol = raw_row[2:5]
            funding_rate = _finite_float(raw_row[5])
            next_funding_time = (
                None
                if raw_row[6] is None
                else _as_utc(raw_row[6], "next_funding_time")
            )
        except (IndexError, TypeError, ValueError):
            continue
        if (
            not isinstance(venue, str)
            or not isinstance(venue_symbol, str)
            or not isinstance(canonical_symbol, str)
            or funding_rate is None
        ):
            continue
        rows.append(
            _FundingRow(
                effective_time=effective_time,
                observed_at=observed_at,
                venue=venue,
                venue_symbol=venue_symbol,
                canonical_symbol=canonical_symbol,
                funding_rate=funding_rate,
                next_funding_time=next_funding_time,
            )
        )
    return tuple(rows)


def _p_percentile(values: Sequence[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("percentile requires at least one value")
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _market_groups(
    rows: Sequence[_MarketRow],
) -> dict[str, dict[tuple[str, str], tuple[_MarketRow, ...]]]:
    grouped: dict[str, dict[tuple[str, str], list[_MarketRow]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        grouped[row.canonical_symbol.upper()][(row.venue, row.venue_symbol)].append(row)
    return {
        canonical_symbol: {
            key: tuple(sorted(group, key=lambda row: row.sample_time))
            for key, group in venue_groups.items()
        }
        for canonical_symbol, venue_groups in grouped.items()
    }


def _funding_groups(
    rows: Sequence[_FundingRow],
) -> dict[tuple[str, str, str], tuple[_FundingRow, ...]]:
    grouped: dict[tuple[str, str, str], list[_FundingRow]] = defaultdict(list)
    for row in rows:
        grouped[
            (row.venue.lower(), row.venue_symbol, row.canonical_symbol.upper())
        ].append(row)
    return {
        key: tuple(sorted(group, key=lambda row: row.observed_at))
        for key, group in grouped.items()
    }


def _latest_funding(
    funding_groups: Mapping[tuple[str, str, str], tuple[_FundingRow, ...]],
    *,
    quote_time: datetime,
    venue: str,
    venue_symbol: str,
    canonical_symbol: str,
) -> _FundingRow | None:
    rows = funding_groups.get((venue.lower(), venue_symbol, canonical_symbol.upper()), ())
    if not rows:
        return None
    observed_times = [row.observed_at for row in rows]
    index = bisect_right(observed_times, quote_time) - 1
    return rows[index] if index >= 0 else None


def _make_observation(
    quote: _QuoteRow,
    market: _MarketRow,
    *,
    direction: str,
    fee_bps: float,
    funding: _FundingRow | None,
) -> VariationalOpportunityObservation | None:
    if direction == VARIATIONAL_LONG:
        if market.sell_vwap is None:
            return None
        long_entry_price = quote.ask_1k
        short_entry_price = market.sell_vwap
    else:
        if market.buy_vwap is None:
            return None
        long_entry_price = market.buy_vwap
        short_entry_price = quote.bid_1k
    try:
        raw_spread_bps = calculate_raw_spread_bps(
            long_entry_price,
            short_entry_price,
        )
    except (OverflowError, ValueError):
        return None
    net_spread_bps = raw_spread_bps - fee_bps
    key = VariationalPairKey(
        canonical_symbol=quote.canonical_symbol,
        direction=direction,
        other_venue=market.venue,
        other_venue_symbol=market.venue_symbol,
    )
    return VariationalOpportunityObservation(
        key=key,
        quote_time=quote.quote_time,
        fetched_at=quote.fetched_at,
        market_sample_time=market.sample_time,
        market_observed_at=market.observed_at,
        variational_bid_1k=quote.bid_1k,
        variational_ask_1k=quote.ask_1k,
        other_buy_1k_vwap=market.buy_vwap,
        other_sell_1k_vwap=market.sell_vwap,
        long_entry_price=long_entry_price,
        short_entry_price=short_entry_price,
        raw_spread_bps=raw_spread_bps,
        other_venue_fee_bps=fee_bps,
        indicative_net_bps=net_spread_bps,
        market_age_at_quote_ms=(quote.quote_time - market.sample_time).total_seconds()
        * 1000.0,
        observed_skew_ms=abs(
            (market.observed_at - quote.quote_time).total_seconds() * 1000.0
        ),
        variational_funding_rate=quote.funding_rate,
        variational_funding_interval_seconds=quote.funding_interval_seconds,
        other_funding_rate=None if funding is None else funding.funding_rate,
        other_funding_effective_time=None
        if funding is None
        else funding.effective_time,
    )


def _build_observations(
    quotes: Sequence[_QuoteRow],
    market_groups: Mapping[str, Mapping[tuple[str, str], tuple[_MarketRow, ...]]],
    funding_groups: Mapping[tuple[str, str, str], tuple[_FundingRow, ...]],
    *,
    fees_bps: Mapping[str, float],
    other_venue: str | None,
) -> tuple[tuple[VariationalOpportunityObservation, ...], int]:
    observations: list[VariationalOpportunityObservation] = []
    unmatched = 0
    selected_other_venue = (
        other_venue.strip().lower() if other_venue is not None else None
    )
    for quote in quotes:
        candidates = market_groups.get(quote.canonical_symbol.upper(), {})
        for (venue, venue_symbol), rows in candidates.items():
            if venue.lower() == "variational":
                continue
            if selected_other_venue is not None and venue.lower() != selected_other_venue:
                continue
            fee_bps = _fee_for(venue, fees_bps)
            if fee_bps is None:
                continue
            sample_times = [row.sample_time for row in rows]
            index = bisect_right(sample_times, quote.quote_time) - 1
            market = rows[index] if index >= 0 else None
            if (
                market is None
                or (quote.quote_time - market.sample_time).total_seconds()
                > MAX_MARKET_AGE_SECONDS
            ):
                unmatched += 1
                continue
            funding = _latest_funding(
                funding_groups,
                quote_time=quote.quote_time,
                venue=venue,
                venue_symbol=venue_symbol,
                canonical_symbol=quote.canonical_symbol,
            )
            for direction in (VARIATIONAL_LONG, OTHER_LONG):
                observation = _make_observation(
                    quote,
                    market,
                    direction=direction,
                    fee_bps=fee_bps,
                    funding=funding,
                )
                if observation is not None:
                    observations.append(observation)
    return tuple(
        sorted(
            observations,
            key=lambda observation: (
                observation.quote_time,
                observation.canonical_symbol,
                observation.direction,
                observation.other_venue,
                observation.key.other_venue_symbol,
            ),
        )
    ), unmatched


def _pair_sort_key(key: VariationalPairKey) -> tuple[str, str, str, str]:
    return (
        key.canonical_symbol,
        key.direction,
        key.other_venue,
        key.other_venue_symbol,
    )


def _build_pair_summaries(
    observations: Sequence[VariationalOpportunityObservation],
) -> tuple[VariationalPairSummary, ...]:
    grouped: dict[VariationalPairKey, list[VariationalOpportunityObservation]] = defaultdict(
        list
    )
    for observation in observations:
        grouped[observation.key].append(observation)
    summaries: list[VariationalPairSummary] = []
    for key, pair_observations in grouped.items():
        values = [observation.indicative_net_bps for observation in pair_observations]
        summaries.append(
            VariationalPairSummary(
                key=key,
                sample_count=len(values),
                median_net_bps=float(median(values)),
                p90_net_bps=_p_percentile(values, 0.90),
                p95_net_bps=_p_percentile(values, 0.95),
                max_net_bps=max(values),
                positive_percent=100.0
                * sum(value > 0 for value in values)
                / len(values),
                count_ge_10=sum(value >= 10.0 for value in values),
                count_ge_20=sum(value >= 20.0 for value in values),
                count_ge_50=sum(value >= 50.0 for value in values),
            )
        )
    return tuple(
        sorted(
            summaries,
            key=lambda summary: (
                -summary.max_net_bps,
                _pair_sort_key(summary.key),
            ),
        )
    )


def _make_episode(
    key: VariationalPairKey,
    threshold_bps: float,
    points: Sequence[VariationalOpportunityObservation],
) -> VariationalPersistenceEpisode:
    return VariationalPersistenceEpisode(
        key=key,
        threshold_bps=threshold_bps,
        start_quote_time=points[0].quote_time,
        end_quote_time=points[-1].quote_time,
        quote_update_count=len(points),
        duration_seconds=(points[-1].quote_time - points[0].quote_time).total_seconds(),
        median_net_bps=float(
            median(point.indicative_net_bps for point in points)
        ),
        max_net_bps=max(point.indicative_net_bps for point in points),
    )


def _build_persistence_episodes(
    quotes_by_symbol: Mapping[str, Sequence[_QuoteRow]],
    observations: Sequence[VariationalOpportunityObservation],
) -> tuple[VariationalPersistenceEpisode, ...]:
    by_pair_and_time = {
        (observation.key, observation.quote_time): observation
        for observation in observations
    }
    keys = sorted({observation.key for observation in observations}, key=_pair_sort_key)
    episodes: list[VariationalPersistenceEpisode] = []
    for key in keys:
        quote_times = quotes_by_symbol.get(key.canonical_symbol, ())
        for threshold_bps in PERSISTENCE_THRESHOLDS_BPS:
            current: list[VariationalOpportunityObservation] = []
            for quote in quote_times:
                observation = by_pair_and_time.get((key, quote.quote_time))
                if (
                    observation is None
                    or observation.indicative_net_bps < threshold_bps
                ):
                    if current:
                        episodes.append(_make_episode(key, threshold_bps, current))
                    current = []
                    continue
                current.append(observation)
            if current:
                episodes.append(_make_episode(key, threshold_bps, current))
    return tuple(
        sorted(
            episodes,
            key=lambda episode: (
                episode.threshold_bps,
                episode.start_quote_time,
                -episode.duration_seconds,
                -episode.max_net_bps,
                _pair_sort_key(episode.key),
            ),
        )
    )


def _empty_report(display_min_net_bps: float) -> VariationalOpportunityReport:
    return VariationalOpportunityReport(
        data_as_of=None,
        window_start=None,
        window_end=None,
        display_min_net_bps=display_min_net_bps,
        total_quote_observations=0,
        matched_observations=0,
        unmatched_observations=0,
        symbol_count=0,
        pair_count=0,
        count_ge_10=0,
        count_ge_20=0,
        count_ge_50=0,
        largest_spread_bps=None,
        most_persistent_episode=None,
        observations=(),
        current_latest=(),
        top_opportunities=(),
        pair_summaries=(),
        persistence_episodes=(),
    )


def build_variational_opportunity_report(
    data_root: Path,
    config: RadarConfig,
    *,
    hours: float = 24.0,
    top: int = 30,
    symbol: str | None = None,
    min_net_bps: float | None = None,
    other_venue: str | None = None,
) -> VariationalOpportunityReport:
    if not math.isfinite(hours) or hours <= 0:
        raise ValueError("hours must be a positive finite number")
    if top <= 0:
        raise ValueError("top must be positive")
    display_min_net_bps = 0.0 if min_net_bps is None else min_net_bps
    if not math.isfinite(display_min_net_bps):
        raise ValueError("min_net_bps must be finite")
    if other_venue is not None and not other_venue.strip():
        raise ValueError("other_venue must not be empty")

    all_quotes = _read_quote_rows(data_root, config, symbol=symbol)
    if not all_quotes:
        return _empty_report(display_min_net_bps)
    data_as_of = max(quote.quote_time for quote in all_quotes)
    window_start = data_as_of - timedelta(hours=hours)
    quotes = tuple(
        quote
        for quote in all_quotes
        if window_start <= quote.quote_time <= data_as_of
    )
    if not quotes:
        return _empty_report(display_min_net_bps)

    market_groups = _market_groups(_read_market_rows(data_root))
    funding_groups = _funding_groups(_read_funding_rows(data_root))
    observations, unmatched = _build_observations(
        quotes,
        market_groups,
        funding_groups,
        fees_bps=config.fees_bps,
        other_venue=other_venue,
    )
    pair_summaries = _build_pair_summaries(observations)
    quotes_by_symbol: dict[str, tuple[_QuoteRow, ...]] = {}
    grouped_quotes: dict[str, list[_QuoteRow]] = defaultdict(list)
    for quote in quotes:
        grouped_quotes[quote.canonical_symbol].append(quote)
    quotes_by_symbol = {
        canonical_symbol: tuple(sorted(group, key=lambda quote: quote.quote_time))
        for canonical_symbol, group in grouped_quotes.items()
    }
    persistence_episodes = _build_persistence_episodes(quotes_by_symbol, observations)

    latest_quotes = {
        canonical_symbol: max(group, key=lambda quote: quote.quote_time)
        for canonical_symbol, group in quotes_by_symbol.items()
    }
    current_latest = tuple(
        sorted(
            (
                observation
                for observation in observations
                if latest_quotes[observation.canonical_symbol].quote_time
                == observation.quote_time
                and observation.indicative_net_bps >= display_min_net_bps
            ),
            key=lambda observation: (
                -observation.indicative_net_bps,
                observation.canonical_symbol,
                observation.direction,
                observation.other_venue,
            ),
        )
    )
    top_opportunities = tuple(
        sorted(
            (
                observation
                for observation in observations
                if observation.indicative_net_bps >= display_min_net_bps
            ),
            key=lambda observation: (
                -observation.indicative_net_bps,
                observation.quote_time,
                _pair_sort_key(observation.key),
            ),
        )[:top]
    )
    most_persistent_episode = max(
        persistence_episodes,
        key=lambda episode: (
            episode.duration_seconds,
            episode.quote_update_count,
            episode.max_net_bps,
        ),
        default=None,
    )
    values = [observation.indicative_net_bps for observation in observations]
    return VariationalOpportunityReport(
        data_as_of=data_as_of,
        window_start=window_start,
        window_end=data_as_of,
        display_min_net_bps=display_min_net_bps,
        total_quote_observations=len(quotes),
        matched_observations=len(observations),
        unmatched_observations=unmatched,
        symbol_count=len(quotes_by_symbol),
        pair_count=len(pair_summaries),
        count_ge_10=sum(value >= 10.0 for value in values),
        count_ge_20=sum(value >= 20.0 for value in values),
        count_ge_50=sum(value >= 50.0 for value in values),
        largest_spread_bps=max(values, default=None),
        most_persistent_episode=most_persistent_episode,
        observations=observations,
        current_latest=current_latest,
        top_opportunities=top_opportunities,
        pair_summaries=pair_summaries,
        persistence_episodes=persistence_episodes,
    )


def _format_timestamp(value: datetime | None) -> str:
    if value is None:
        return "—"
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_bps(value: float) -> str:
    return f"{value:.2f}"


def _format_rate(rate: float | None, interval_seconds: int | None) -> str:
    if rate is None:
        return "—"
    if interval_seconds is None:
        return f"{rate:.6g}/stored"
    return f"{rate:.6g}/{interval_seconds}s"


def _direction_label(key: VariationalPairKey) -> str:
    if key.direction == VARIATIONAL_LONG:
        return f"Long variational / Short {key.other_venue}"
    return f"Long {key.other_venue} / Short variational"


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


def format_variational_opportunity_report(
    report: VariationalOpportunityReport,
    *,
    top: int = 30,
) -> str:
    if top <= 0:
        raise ValueError("top must be positive")
    lines = [
        "=" * 72,
        "VARIATIONAL $1K DISCOVERY REPORT",
        "=" * 72,
    ]
    if report.data_as_of is None:
        lines.append("No Variational quoted-market data found in data/quoted_market.")
        return "\n".join(lines)

    lines.extend(
        [
            f"Data as of: {_format_timestamp(report.data_as_of)}",
            f"Window:     {_format_timestamp(report.window_start)} -> {_format_timestamp(report.window_end)}",
            "Quote size: $1,000 Variational metadata quote vs $1,000 venue executable VWAP",
            f"Display filter: >= {_format_bps(report.display_min_net_bps)} bps",
            "INDICATIVE NET SPREAD only; no funding carry, slippage, or execution guarantee.",
            "Limitation: public metadata $1k quote != guaranteed firm execution quote.",
            "Funding: displayed as raw rate / stated interval; not converted to hourly.",
        ]
    )

    lines.extend(["", "-" * 72, "1. CURRENT / LATEST", "-" * 72])
    if not report.current_latest:
        lines.append(
            f"No current matched spread exceeds {_format_bps(report.display_min_net_bps)} bps."
        )
    else:
        lines.extend(
            _format_table(
                [
                    "Symbol",
                    "Direction",
                    "Long",
                    "Short",
                    "V quote",
                    "Other sample",
                    "Raw bps",
                    "Fee bps",
                    "INDICATIVE NET SPREAD",
                    "Skew",
                    "V funding",
                    "Other funding",
                ],
                [
                    [
                        observation.canonical_symbol,
                        _direction_label(observation.key),
                        "variational"
                        if observation.direction == VARIATIONAL_LONG
                        else observation.other_venue,
                        observation.other_venue
                        if observation.direction == VARIATIONAL_LONG
                        else "variational",
                        _format_timestamp(observation.quote_time),
                        _format_timestamp(observation.market_sample_time),
                        _format_bps(observation.raw_spread_bps),
                        _format_bps(observation.other_venue_fee_bps),
                        _format_bps(observation.indicative_net_bps),
                        f"{observation.observed_skew_ms:.0f}ms",
                        _format_rate(
                            observation.variational_funding_rate,
                            observation.variational_funding_interval_seconds,
                        ),
                        _format_rate(
                            observation.other_funding_rate,
                            observation.other_funding_interval_seconds,
                        ),
                    ]
                    for observation in report.current_latest[:top]
                ],
            )
        )

    lines.extend(["", "-" * 72, "2. TOP OPPORTUNITIES", "-" * 72])
    if not report.top_opportunities:
        lines.append("No matched opportunities in the selected window.")
    else:
        lines.extend(
            _format_table(
                ["Symbol", "Direction", "Other", "Quote time", "Raw bps", "Fee bps", "INDICATIVE NET SPREAD", "Age"],
                [
                    [
                        observation.canonical_symbol,
                        _direction_label(observation.key),
                        observation.other_venue,
                        _format_timestamp(observation.quote_time),
                        _format_bps(observation.raw_spread_bps),
                        _format_bps(observation.other_venue_fee_bps),
                        _format_bps(observation.indicative_net_bps),
                        f"{observation.market_age_at_quote_ms:.0f}ms",
                    ]
                    for observation in report.top_opportunities[:top]
                ],
            )
        )

    lines.extend(["", "-" * 72, "3. PAIR SUMMARY", "-" * 72])
    if not report.pair_summaries:
        lines.append("No valid matched pair observations.")
    else:
        lines.extend(
            _format_table(
                ["Symbol", "Direction", "Other", "N", "Median", "P90", "P95", "Max", ">0%", ">=10", ">=20", ">=50"],
                [
                    [
                        summary.canonical_symbol,
                        _direction_label(summary.key),
                        summary.other_venue,
                        str(summary.sample_count),
                        _format_bps(summary.median_net_bps),
                        _format_bps(summary.p90_net_bps),
                        _format_bps(summary.p95_net_bps),
                        _format_bps(summary.max_net_bps),
                        f"{summary.positive_percent:.1f}%",
                        str(summary.count_ge_10),
                        str(summary.count_ge_20),
                        str(summary.count_ge_50),
                    ]
                    for summary in report.pair_summaries
                ],
            )
        )

    lines.extend(["", "-" * 72, "4. PERSISTENCE", "-" * 72])
    if not report.persistence_episodes:
        lines.append("No threshold episodes.")
    else:
        lines.extend(
            _format_table(
                ["Threshold", "Symbol", "Direction", "Other", "Start", "End", "N", "Duration", "Median", "Max"],
                [
                    [
                        f">={episode.threshold_bps:.0f} bps",
                        episode.canonical_symbol,
                        _direction_label(episode.key),
                        episode.other_venue,
                        _format_timestamp(episode.start_quote_time),
                        _format_timestamp(episode.end_quote_time),
                        str(episode.quote_update_count),
                        f"{episode.duration_seconds:.0f}s",
                        _format_bps(episode.median_net_bps),
                        _format_bps(episode.max_net_bps),
                    ]
                    for episode in report.persistence_episodes
                ],
            )
        )

    lines.extend(["", "-" * 72, "5. SUMMARY", "-" * 72])
    lines.extend(
        [
            f"- Variational quote observations: {report.total_quote_observations}",
            f"- Matched directional observations: {report.matched_observations}",
            f"- Unmatched due to time alignment: {report.unmatched_observations}",
            f"- Symbols: {report.symbol_count}",
            f"- Directed venue pairs: {report.pair_count}",
            f"- Observations >=10 / >=20 / >=50 bps: {report.count_ge_10} / {report.count_ge_20} / {report.count_ge_50}",
            f"- Largest indicative net spread: {_format_bps(report.largest_spread_bps) if report.largest_spread_bps is not None else '—'} bps",
        ]
    )
    if report.most_persistent_episode is None:
        lines.append("- Most persistent pair: —")
    else:
        episode = report.most_persistent_episode
        lines.append(
            f"- Most persistent pair: {episode.canonical_symbol} "
            f"{_direction_label(episode.key)}, >= {episode.threshold_bps:.0f} bps "
            f"for {episode.duration_seconds:.0f}s"
        )
    return "\n".join(lines)
