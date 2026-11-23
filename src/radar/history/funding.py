"""Startup-only funding history for the funding monitor's rolling windows."""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

FeedKey = tuple[str, str, str]
HOURS_PER_YEAR = 365 * 24


def load_recent_funding_points(
    data_root: Path,
    *,
    as_of: datetime,
    window: timedelta,
) -> dict[FeedKey, tuple[tuple[datetime, float], ...]]:
    """Return (observed_at, APR) per feed for ``[as_of - window, as_of)``.

    Rows written before funding carried ``interval_seconds`` cannot be
    converted to APR and are skipped rather than guessed.
    """
    files = [path for path in data_root.glob("funding/date=*/part-*.parquet") if path.is_file()]
    if not files:
        return {}
    glob = str(data_root / "funding" / "date=*" / "part-*.parquet").replace("'", "''")
    as_of_utc = as_of.astimezone(UTC)
    with duckdb.connect() as connection:
        columns = {
            row[0]
            for row in connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{glob}', union_by_name=true)"
            ).fetchall()
        }
        if "interval_seconds" not in columns:
            return {}
        rows = connection.execute(
            f"""
            SELECT observed_at, venue, venue_symbol, canonical_symbol,
                   funding_rate, interval_seconds
            FROM read_parquet('{glob}', union_by_name=true)
            WHERE observed_at >= ? AND observed_at < ?
              AND interval_seconds IS NOT NULL AND interval_seconds > 0
            ORDER BY observed_at
            """,
            [as_of_utc - window, as_of_utc],
        ).fetchall()
    points: dict[FeedKey, list[tuple[datetime, float]]] = defaultdict(list)
    for observed_at, venue, venue_symbol, canonical_symbol, rate, interval in rows:
        if not isinstance(observed_at, datetime) or observed_at.tzinfo is None:
            continue
        if not all(isinstance(value, str) and value for value in (venue, venue_symbol, canonical_symbol)):
            continue
        apr = float(rate) * 3600 / int(interval) * HOURS_PER_YEAR
        if math.isfinite(apr):
            points[(venue, venue_symbol, canonical_symbol)].append(
                (observed_at.astimezone(UTC), apr)
            )
    return {key: tuple(feed_points) for key, feed_points in points.items()}
