"""V2 offline data-quality and convergence research primitives.

This module is intentionally separate from the V1 replay scanner.  It reads
historical observations, attributes why a pair slot was unusable, and provides
pure helpers for causal research.  It never places orders or touches live
Radar state.
"""
from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from statistics import median
from zoneinfo import ZoneInfo

from radar.history.stable_baseline import Pair, _as_datetime, _quantile


BASIS_TYPES = ("mid", "exec_buy_a_sell_b", "exec_buy_b_sell_a")


@dataclass(frozen=True)
class AnalysisParams:
    sample_seconds: int = 60
    raw_sample_seconds: int = 10
    max_quote_age_seconds: int = 12
    max_skew_seconds: int = 3
    min_daily_coverage: float = 0.60
    min_baseline_days: int = 5
    stable_range_bps: float = 12.0
    return_band_bps: float = 5.0

    def __post_init__(self) -> None:
        if self.sample_seconds <= 0 or self.raw_sample_seconds <= 0:
            raise ValueError("sampling intervals must be positive")
        if self.max_quote_age_seconds < 0 or self.max_skew_seconds < 0:
            raise ValueError("quote age and skew limits must be non-negative")
        if not 0 < self.min_daily_coverage <= 1:
            raise ValueError("min_daily_coverage must be in (0, 1]")
        if self.min_baseline_days < 1:
            raise ValueError("min_baseline_days must be positive")
        for name in ("stable_range_bps", "return_band_bps"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")


@dataclass(frozen=True)
class MarketObservation:
    sample_time: datetime
    observed_at: datetime
    venue: str
    venue_symbol: str
    symbol: str
    buy: float | None
    sell: float | None


@dataclass(frozen=True)
class ObservationAssessment:
    valid: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class PairPoint:
    sample_time: datetime
    pair: Pair
    a: MarketObservation | None
    b: MarketObservation | None


@dataclass(frozen=True)
class PairSlotAssessment:
    valid_sync: bool
    reasons: tuple[str, ...]
    skew_seconds: float | None


@dataclass(frozen=True)
class DailySeries:
    day: date
    values: dict[str, tuple[float, ...]]
    sample_count: int
    coverage: float


@dataclass(frozen=True)
class BaselineReference:
    baseline_bps: float
    q90_q10_bps: float
    days_used: int
    stable: bool
    drift_bps_per_day: float
    days: tuple[date, ...]


def _classify_observation(
    observation: MarketObservation,
    params: AnalysisParams = AnalysisParams(),
) -> ObservationAssessment:
    reasons: list[str] = []
    age = (observation.sample_time - observation.observed_at).total_seconds()
    if age < 0:
        reasons.append("observed_after_sample")
    elif age > params.max_quote_age_seconds:
        reasons.append("stale")
    prices_valid = (
        observation.buy is not None
        and observation.sell is not None
        and math.isfinite(observation.buy)
        and math.isfinite(observation.sell)
        and observation.buy > 0
        and observation.sell > 0
        and observation.buy >= observation.sell
    )
    if not prices_valid:
        reasons.append("invalid_vwap")
    return ObservationAssessment(not reasons, tuple(reasons))


def _attribute_pair_slot(
    point: PairPoint,
    params: AnalysisParams = AnalysisParams(),
) -> PairSlotAssessment:
    if point.a is None and point.b is None:
        return PairSlotAssessment(False, ("missing_a", "missing_b"), None)
    if point.a is None:
        return PairSlotAssessment(False, ("missing_a",), None)
    if point.b is None:
        return PairSlotAssessment(False, ("missing_b",), None)
    if point.a.sample_time != point.b.sample_time:
        return PairSlotAssessment(False, ("sample_time_mismatch",), None)
    a_status = _classify_observation(point.a, params)
    b_status = _classify_observation(point.b, params)
    reasons = [f"a_{reason}" for reason in a_status.reasons]
    reasons.extend(f"b_{reason}" for reason in b_status.reasons)
    skew = abs((point.a.observed_at - point.b.observed_at).total_seconds())
    if not reasons and skew > params.max_skew_seconds:
        reasons.append("observed_at_skew")
    return PairSlotAssessment(not reasons, tuple(reasons), skew)


def _sampling_loss_count(
    valid_raw_slots: set[datetime],
    valid_sampled_slots: set[datetime],
) -> int:
    """Count valid raw slots not represented by the coarser sample grid."""

    return len(valid_raw_slots - valid_sampled_slots)


def us_session_regime(timestamp: datetime) -> str:
    """Classify a timestamp using New York regular/extended/closed sessions."""

    local = timestamp.astimezone(ZoneInfo("America/New_York"))
    if local.weekday() >= 5:
        return "us_closed"
    regular_start = time(9, 30)
    regular_end = time(16, 0)
    if regular_start <= local.time() < regular_end:
        return "us_regular"
    return "us_extended"


def _basis_values(point: PairPoint) -> dict[str, float]:
    if point.a is None or point.b is None:
        raise ValueError("basis requires both observations")
    a = point.a
    b = point.b
    if any(value is None for value in (a.buy, a.sell, b.buy, b.sell)):
        raise ValueError("basis requires complete VWAP values")
    a_mid = (a.buy + a.sell) / 2
    b_mid = (b.buy + b.sell) / 2
    return {
        "mid": 10000 * (b_mid / a_mid - 1),
        "exec_buy_a_sell_b": 10000 * (b.sell / a.buy - 1),
        "exec_buy_b_sell_a": 10000 * (a.sell / b.buy - 1),
    }


def build_daily_series(
    points: Iterator[PairPoint],
    *,
    params: AnalysisParams = AnalysisParams(),
    regime: str = "all",
) -> dict[date, DailySeries]:
    values_by_day: dict[date, dict[str, list[float]]] = {}
    for point in points:
        if regime != "all" and us_session_regime(point.sample_time) != regime:
            continue
        if not _attribute_pair_slot(point, params).valid_sync:
            continue
        day_values = values_by_day.setdefault(
            point.sample_time.date(), {basis: [] for basis in BASIS_TYPES}
        )
        for basis, value in _basis_values(point).items():
            day_values[basis].append(value)
    expected = 86400 / params.sample_seconds
    return {
        day: DailySeries(
            day=day,
            values={basis: tuple(day_values[basis]) for basis in BASIS_TYPES},
            sample_count=len(day_values["mid"]),
            coverage=len(day_values["mid"]) / expected,
        )
        for day, day_values in values_by_day.items()
    }


def _linear_slope(days: list[date], values: list[float]) -> float:
    if len(days) < 2:
        return 0.0
    x = [(day - days[0]).days for day in days]
    x_mean = sum(x) / len(x)
    y_mean = sum(values) / len(values)
    denominator = sum((value - x_mean) ** 2 for value in x)
    if denominator == 0:
        return 0.0
    return sum((x_value - x_mean) * (value - y_mean)
               for x_value, value in zip(x, values, strict=True)) / denominator


def causal_baseline(
    history: dict[date, DailySeries],
    *,
    today: date,
    basis_type: str,
    lookback_days: int,
    min_days: int,
    use_daily_medians: bool,
    stable_range_bps: float,
    min_daily_coverage: float = 0.60,
) -> BaselineReference | None:
    if basis_type not in BASIS_TYPES:
        raise ValueError(f"unknown basis_type: {basis_type}")
    eligible = [
        series
        for day, series in history.items()
        if 0 < (today - day).days <= lookback_days
        and series.coverage >= min_daily_coverage
        and series.values[basis_type]
    ]
    eligible.sort(key=lambda series: series.day)
    if len(eligible) < min_days:
        return None
    if use_daily_medians:
        values = [median(series.values[basis_type]) for series in eligible]
    else:
        values = [value for series in eligible for value in series.values[basis_type]]
    baseline = median(values)
    spread = _quantile(values, 0.9) - _quantile(values, 0.1)
    daily_days = [series.day for series in eligible]
    daily_medians = [median(series.values[basis_type]) for series in eligible]
    return BaselineReference(
        baseline_bps=baseline,
        q90_q10_bps=spread,
        days_used=len(eligible),
        stable=spread <= stable_range_bps,
        drift_bps_per_day=_linear_slope(daily_days, daily_medians),
        days=tuple(daily_days),
    )


def build_baseline_rows(
    pair: Pair,
    history: dict[date, DailySeries],
    *,
    params: AnalysisParams = AnalysisParams(),
    regime: str = "all",
) -> list[dict[str, object]]:
    windows = (
        ("24h", 1, 1, False),
        ("7d", 7, params.min_baseline_days, True),
        ("30d", 30, params.min_baseline_days, True),
    )
    rows: list[dict[str, object]] = []
    for window, lookback_days, min_days, use_daily_medians in windows:
        for basis_type in BASIS_TYPES:
            references = [
                reference
                for today in sorted(history)
                if (reference := causal_baseline(
                    history,
                    today=today,
                    basis_type=basis_type,
                    lookback_days=lookback_days,
                    min_days=min_days,
                    use_daily_medians=use_daily_medians,
                    stable_range_bps=params.stable_range_bps,
                    min_daily_coverage=params.min_daily_coverage,
                )) is not None
            ]
            spreads = [reference.q90_q10_bps for reference in references]
            drifts = [reference.drift_bps_per_day for reference in references]
            daily_medians = {
                day: median(series.values[basis_type])
                for day, series in history.items()
                if series.coverage >= params.min_daily_coverage
                and series.values[basis_type]
            }
            changes = [
                abs(daily_medians[next_day] - daily_medians[day])
                for day, next_day in zip(
                    sorted(daily_medians), sorted(daily_medians)[1:], strict=False
                )
            ]
            status = (
                "insufficient_data" if not references
                else "stable" if all(reference.stable for reference in references)
                else "unstable"
            )
            rows.append({
                **{
                    "symbol": pair.symbol,
                    "venue_a": pair.venue_a,
                    "symbol_a": pair.symbol_a,
                    "venue_b": pair.venue_b,
                    "symbol_b": pair.symbol_b,
                },
                "regime": regime,
                "window": window,
                "basis_type": basis_type,
                "covered_days": len(daily_medians),
                "eligible_windows": len(references),
                "stable_windows": sum(reference.stable for reference in references),
                "unstable_windows": sum(not reference.stable for reference in references),
                "median_daily_coverage": median([series.coverage for series in history.values()]) if history else 0.0,
                "median_baseline_bps": median([reference.baseline_bps for reference in references]) if references else None,
                "median_q90_q10_bps": median(spreads) if spreads else None,
                "max_q90_q10_bps": max(spreads) if spreads else None,
                "median_abs_day_change_bps": median(changes) if changes else None,
                "p90_abs_day_change_bps": _quantile(changes, 0.9) if changes else None,
                "median_drift_bps_per_day": median(drifts) if drifts else None,
                "status": status,
            })
    return rows


def iter_pair_points(
    files: list[str],
    *,
    configured: set[tuple[str, str, str]],
    size_usd: int = 1000,
    sample_seconds: int = 60,
) -> Iterator[PairPoint]:
    """Yield exact-sample-time pair slots using the latest duplicate row.

    The query intentionally emits only slots where both configured markets have
    a row.  Missing-leg attribution is performed from the market slot counts
    and from ``PairPoint`` fixtures in the pure classifier; valid synchronized
    rows are the stream consumed by baseline and episode analysis.
    """

    import duckdb  # type: ignore[import-untyped]

    if not files:
        raise ValueError("files required")
    if size_usd not in (1000, 5000, 10000):
        raise ValueError("VWAP size must be 1000, 5000 or 10000")
    if sample_seconds <= 0 or 86400 % sample_seconds:
        raise ValueError("sample_seconds must divide one UTC day")
    if not configured:
        return

    buy = f"buy_{size_usd // 1000}k_vwap"
    sell = f"sell_{size_usd // 1000}k_vwap"
    placeholders = ", ".join("(?, ?, ?)" for _ in configured)
    config_values = [value for row in sorted(configured) for value in row]
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TEMP TABLE configured_markets "
            "(venue VARCHAR, venue_symbol VARCHAR, canonical_symbol VARCHAR)"
        )
        connection.execute(
            f"INSERT INTO configured_markets VALUES {placeholders}", config_values
        )
        query = f"""
            WITH latest AS (
                SELECT
                    CAST(r.sample_time AS TIMESTAMPTZ) AS sample_time,
                    CAST(r.observed_at AS TIMESTAMPTZ) AS observed_at,
                    r.venue,
                    r.venue_symbol,
                    r.canonical_symbol,
                    r.{buy} AS buy_price,
                    r.{sell} AS sell_price
                FROM read_parquet(?, union_by_name=true) AS r
                INNER JOIN configured_markets AS c
                    ON c.venue = r.venue
                   AND c.venue_symbol = r.venue_symbol
                   AND c.canonical_symbol = r.canonical_symbol
                WHERE CAST(EXTRACT(EPOCH FROM CAST(r.sample_time AS TIMESTAMPTZ)) AS BIGINT) % ? = 0
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY r.sample_time, r.venue, r.venue_symbol, r.canonical_symbol
                    ORDER BY r.observed_at DESC
                ) = 1
            )
            SELECT
                a.sample_time,
                a.observed_at, a.venue, a.venue_symbol, a.canonical_symbol,
                a.buy_price, a.sell_price,
                b.observed_at, b.venue, b.venue_symbol, b.canonical_symbol,
                b.buy_price, b.sell_price
            FROM latest AS a
            INNER JOIN latest AS b
                ON b.sample_time = a.sample_time
               AND b.canonical_symbol = a.canonical_symbol
               AND a.venue < b.venue
            ORDER BY a.canonical_symbol, a.venue, a.venue_symbol,
                     b.venue, b.venue_symbol, a.sample_time
        """
        cursor = connection.execute(query, [files, sample_seconds])
        while chunk := cursor.fetchmany(8192):
            for row in chunk:
                (
                    sample_time,
                    observed_a, venue_a, symbol_a, canonical_a, buy_a, sell_a,
                    observed_b, venue_b, symbol_b, canonical_b, buy_b, sell_b,
                ) = row
                timestamp = _as_datetime(sample_time)
                a = MarketObservation(
                    sample_time=timestamp,
                    observed_at=_as_datetime(observed_a),
                    venue=str(venue_a),
                    venue_symbol=str(symbol_a),
                    symbol=str(canonical_a),
                    buy=float(buy_a) if buy_a is not None else None,
                    sell=float(sell_a) if sell_a is not None else None,
                )
                b = MarketObservation(
                    sample_time=timestamp,
                    observed_at=_as_datetime(observed_b),
                    venue=str(venue_b),
                    venue_symbol=str(symbol_b),
                    symbol=str(canonical_b),
                    buy=float(buy_b) if buy_b is not None else None,
                    sell=float(sell_b) if sell_b is not None else None,
                )
                yield PairPoint(
                    sample_time=timestamp,
                    pair=Pair(
                        str(canonical_a), str(venue_a), str(symbol_a),
                        str(venue_b), str(symbol_b),
                    ),
                    a=a,
                    b=b,
                )
    finally:
        connection.close()
