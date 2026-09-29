from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median

import duckdb  # type: ignore[import-untyped]

from radar.monitors.spread.models import SpreadPairKey

VWAP_COLUMNS = {
    1_000: ("buy_1k_vwap", "sell_1k_vwap"),
    5_000: ("buy_5k_vwap", "sell_5k_vwap"),
    10_000: ("buy_10k_vwap", "sell_10k_vwap"),
}


@dataclass(frozen=True)
class HistoricalSpreadPoint:
    sample_time: datetime
    raw_spread_bps: float


@dataclass(frozen=True)
class WindowStats:
    sample_count: int
    median_raw_spread_bps: float | None


@dataclass(frozen=True)
class HistoricalSpreadContext:
    points_7d: tuple[HistoricalSpreadPoint, ...]
    stats_7d: WindowStats
    stats_30d: WindowStats
    stats_90d: WindowStats

    @classmethod
    def empty(cls) -> HistoricalSpreadContext:
        empty_stats = WindowStats(0, None)
        return cls((), empty_stats, empty_stats, empty_stats)


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _stats_for_window(
    points: list[HistoricalSpreadPoint],
    *,
    start: datetime,
    end: datetime,
) -> WindowStats:
    values = [
        point.raw_spread_bps
        for point in points
        if start <= point.sample_time <= end
    ]
    return WindowStats(
        sample_count=len(values),
        median_raw_spread_bps=None if not values else float(median(values)),
    )


class SpreadHistory:
    def __init__(self, data_root: Path) -> None:
        self.data_root = Path(data_root)

    def query(
        self,
        *,
        canonical_symbol: str,
        long_venue: str,
        long_venue_symbol: str,
        short_venue: str,
        short_venue_symbol: str,
        primary_size_usd: int,
        as_of: datetime,
    ) -> HistoricalSpreadContext:
        try:
            buy_column, sell_column = VWAP_COLUMNS[primary_size_usd]
        except KeyError as exc:
            raise ValueError("primary_size_usd must be 1000, 5000, or 10000") from exc

        as_of_utc = _as_utc(as_of, "as_of")
        start_90d = as_of_utc - timedelta(days=90)
        market_files = tuple(
            path
            for path in self.data_root.glob("market/date=*/part-*.parquet")
            if path.is_file()
        )
        if not market_files:
            return HistoricalSpreadContext.empty()

        market_glob = str(
            self.data_root / "market" / "date=*" / "part-*.parquet"
        ).replace("'", "''")
        query = f"""
            WITH filtered AS (
                SELECT
                    sample_time,
                    observed_at,
                    venue,
                    venue_symbol,
                    canonical_symbol,
                    {buy_column} AS buy_vwap,
                    {sell_column} AS sell_vwap
                FROM read_parquet('{market_glob}')
                WHERE sample_time >= ?
                  AND sample_time <= ?
                  AND canonical_symbol = ?
                  AND (
                      (venue = ? AND venue_symbol = ?)
                      OR (venue = ? AND venue_symbol = ?)
                  )
            ),
            deduplicated AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY
                            venue,
                            venue_symbol,
                            canonical_symbol,
                            sample_time
                        ORDER BY observed_at DESC
                    ) AS row_number
                FROM filtered
            ),
            long_side AS (
                SELECT sample_time, buy_vwap
                FROM deduplicated
                WHERE row_number = 1
                  AND venue = ?
                  AND venue_symbol = ?
                  AND canonical_symbol = ?
            ),
            short_side AS (
                SELECT sample_time, sell_vwap
                FROM deduplicated
                WHERE row_number = 1
                  AND venue = ?
                  AND venue_symbol = ?
                  AND canonical_symbol = ?
            )
            SELECT
                long_side.sample_time,
                long_side.buy_vwap,
                short_side.sell_vwap
            FROM long_side
            INNER JOIN short_side USING (sample_time)
            ORDER BY long_side.sample_time
        """
        parameters = [
            start_90d,
            as_of_utc,
            canonical_symbol,
            long_venue,
            long_venue_symbol,
            short_venue,
            short_venue_symbol,
            long_venue,
            long_venue_symbol,
            canonical_symbol,
            short_venue,
            short_venue_symbol,
            canonical_symbol,
        ]
        with duckdb.connect() as connection:
            rows = connection.execute(query, parameters).fetchall()

        points: list[HistoricalSpreadPoint] = []
        for sample_time, long_buy_vwap, short_sell_vwap in rows:
            sample_time_utc = _as_utc(sample_time, "sample_time")
            if not isinstance(long_buy_vwap, (int, float)) or not isinstance(
                short_sell_vwap, (int, float)
            ):
                continue
            long_buy = float(long_buy_vwap)
            short_sell = float(short_sell_vwap)
            if (
                not math.isfinite(long_buy)
                or not math.isfinite(short_sell)
                or long_buy <= 0
                or short_sell <= 0
            ):
                continue
            raw_spread_bps = (short_sell / long_buy - 1.0) * 10_000
            if not math.isfinite(raw_spread_bps):
                continue
            points.append(
                HistoricalSpreadPoint(
                    sample_time=sample_time_utc,
                    raw_spread_bps=raw_spread_bps,
                )
            )

        points.sort(key=lambda point: point.sample_time)
        stats_7d = _stats_for_window(
            points,
            start=as_of_utc - timedelta(days=7),
            end=as_of_utc,
        )
        stats_30d = _stats_for_window(
            points,
            start=as_of_utc - timedelta(days=30),
            end=as_of_utc,
        )
        stats_90d = _stats_for_window(
            points,
            start=start_90d,
            end=as_of_utc,
        )
        points_7d = tuple(
            point
            for point in points
            if as_of_utc - timedelta(days=7) <= point.sample_time <= as_of_utc
        )
        return HistoricalSpreadContext(
            points_7d=points_7d,
            stats_7d=stats_7d,
            stats_30d=stats_30d,
            stats_90d=stats_90d,
        )

    def load_recent_pair_points(
        self,
        *,
        primary_size_usd: int,
        as_of: datetime,
        window: timedelta = timedelta(days=1),
    ) -> dict[SpreadPairKey, tuple[tuple[datetime, float], ...]]:
        """Load one bounded prior window for stable-basis monitor hydration."""
        try:
            buy_column, sell_column = VWAP_COLUMNS[primary_size_usd]
        except KeyError as exc:
            raise ValueError("primary_size_usd must be 1000, 5000, or 10000") from exc

        as_of_utc = _as_utc(as_of, "as_of")
        market_files = tuple(
            path
            for path in self.data_root.glob("market/date=*/part-*.parquet")
            if path.is_file()
        )
        if not market_files:
            return {}

        market_glob = str(
            self.data_root / "market" / "date=*" / "part-*.parquet"
        ).replace("'", "''")
        query = f"""
            SELECT
                sample_time,
                observed_at,
                venue,
                venue_symbol,
                canonical_symbol,
                {buy_column} AS buy_vwap,
                {sell_column} AS sell_vwap
            FROM read_parquet('{market_glob}')
            WHERE sample_time >= ?
              AND sample_time < ?
        """
        with duckdb.connect() as connection:
            rows = connection.execute(
                query,
                [as_of_utc - window, as_of_utc],
            ).fetchall()

        latest: dict[tuple[str, str, str, datetime], tuple[datetime, object, object]] = {}
        for (
            sample_time,
            observed_at,
            venue,
            venue_symbol,
            canonical_symbol,
            buy_vwap,
            sell_vwap,
        ) in rows:
            if not all(
                isinstance(value, str) and value.strip()
                for value in (venue, venue_symbol, canonical_symbol)
            ):
                continue
            try:
                sample_time_utc = _as_utc(sample_time, "sample_time")
                observed_at_utc = _as_utc(observed_at, "observed_at")
            except (AttributeError, TypeError, ValueError):
                continue
            feed_key = (venue, venue_symbol, canonical_symbol, sample_time_utc)
            previous = latest.get(feed_key)
            if previous is None or observed_at_utc > previous[0]:
                latest[feed_key] = (observed_at_utc, buy_vwap, sell_vwap)

        by_sample: dict[tuple[str, datetime], dict[tuple[str, str], tuple[object, object]]] = {}
        for (venue, venue_symbol, canonical_symbol, sample_time), (
            _observed_at,
            buy_vwap,
            sell_vwap,
        ) in latest.items():
            by_sample.setdefault((canonical_symbol, sample_time), {})[
                (venue, venue_symbol)
            ] = (buy_vwap, sell_vwap)

        hydrated: dict[SpreadPairKey, list[tuple[datetime, float]]] = {}
        for (canonical_symbol, sample_time), feeds in by_sample.items():
            for (long_venue, long_symbol), (long_buy, _long_sell) in feeds.items():
                if not isinstance(long_buy, (int, float)) or not math.isfinite(float(long_buy)) or float(long_buy) <= 0:
                    continue
                for (short_venue, short_symbol), (_short_buy, short_sell) in feeds.items():
                    if long_venue.lower() == short_venue.lower():
                        continue
                    if not isinstance(short_sell, (int, float)) or not math.isfinite(float(short_sell)) or float(short_sell) <= 0:
                        continue
                    raw_spread_bps = (float(short_sell) / float(long_buy) - 1.0) * 10_000.0
                    if not math.isfinite(raw_spread_bps):
                        continue
                    pair_key = SpreadPairKey(
                        canonical_symbol=canonical_symbol,
                        long_venue=long_venue,
                        long_venue_symbol=long_symbol,
                        short_venue=short_venue,
                        short_venue_symbol=short_symbol,
                    )
                    hydrated.setdefault(pair_key, []).append((sample_time, raw_spread_bps))

        return {
            key: tuple(sorted(points, key=lambda point: point[0]))
            for key, points in hydrated.items()
        }
