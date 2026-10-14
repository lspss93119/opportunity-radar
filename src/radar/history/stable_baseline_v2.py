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
from datetime import UTC, datetime

from radar.history.stable_baseline import Pair, _as_datetime


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
