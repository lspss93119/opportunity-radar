"""One-shot offline replay comparing spread anchors (mean_24h vs median_2h).

Read-only research over the market Parquet dataset; it never places orders,
touches live Radar state or keeps running. For each directional pair it
rebuilds the $1k taker spread (short-leg sell VWAP / long-leg buy VWAP - 1),
computes prior-only anchors with DuckDB window functions, then simulates one
position at a time:

* entry when spread - anchor >= threshold (anchor available and covered);
* exit when the spread is back within ``return_band_bps`` of the anchor
  frozen at entry, or censored ("not returned") after ``max_hold_hours``;
* equal-quantity P&L at taker VWAPs minus four taker fees;
* re-entry only after the deviation has dropped below the threshold.

Run: python -m radar.history.anchor_replay --config CONFIG --data-root DATA
     --output-csv OUT.csv --output-json OUT.json
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import numpy as np

from radar.config import RadarConfig, load_config

ANCHORS = {
    # name: (window seconds, min fraction of expected samples)
    "mean_24h": (24 * 60 * 60, 0.8),
    "median_2h": (2 * 60 * 60, 0.8),
}
DEFAULT_THRESHOLDS = (15.0, 25.0, 40.0)


@dataclass(frozen=True)
class ReplayParams:
    size_usd: int = 1_000
    thresholds_bps: tuple[float, ...] = DEFAULT_THRESHOLDS
    return_band_bps: float = 5.0
    max_hold_hours: float = 24.0
    stale_after_seconds: float = 30.0
    max_skew_seconds: float = 3.0
    sample_seconds: int = 10
    max_gap_seconds: float = 20.0


@dataclass
class TradeStats:
    trades: int = 0
    returned: int = 0
    censored: int = 0  # hold window ran past the end of the data
    pnl_bps: list[float] = field(default_factory=list)
    return_seconds: list[float] = field(default_factory=list)
    not_returned_adverse_bps: list[float] = field(default_factory=list)
    adverse_bps: list[float] = field(default_factory=list)


@dataclass(frozen=True)
class PairArrays:
    """Aligned samples for one directional pair (A long, B short)."""

    times: np.ndarray  # epoch seconds
    long_buy: np.ndarray  # A ask VWAP (entry)
    long_sell: np.ndarray  # A bid VWAP (exit)
    short_sell: np.ndarray  # B bid VWAP (entry)
    short_buy: np.ndarray  # B ask VWAP (exit)
    anchors: Mapping[str, np.ndarray]  # NaN where unavailable

    @property
    def spread_bps(self) -> np.ndarray:
        return (self.short_sell / self.long_buy - 1.0) * 10_000.0


def simulate(
    arrays: PairArrays,
    anchor: str,
    threshold_bps: float,
    *,
    fee_bps_total: float,
    return_band_bps: float,
    max_hold_seconds: float,
    max_gap_seconds: float = 20.0,
) -> TradeStats:
    """Simulate one position at a time for one anchor and threshold."""
    stats = TradeStats()
    spread = arrays.spread_bps
    reference = arrays.anchors[anchor]
    deviation = spread - reference  # NaN where the anchor is unavailable
    n = len(spread)
    gap_ok = np.empty(n, dtype=bool)
    if n:
        gap_ok[0] = False
        gap_ok[1:] = np.diff(arrays.times) <= max_gap_seconds
    with np.errstate(invalid="ignore"):
        above = np.flatnonzero((deviation >= threshold_bps) & gap_ok)
        below = np.flatnonzero(deviation < threshold_bps)
    position = 0
    while True:
        cursor = np.searchsorted(above, position)
        if cursor >= len(above):
            break
        entry = int(above[cursor])
        entry_anchor = reference[entry]
        target = entry_anchor + return_band_bps
        deadline = arrays.times[entry] + max_hold_seconds
        last = int(np.searchsorted(arrays.times, deadline, side="right")) - 1
        exit_index = _first_at_or_below(spread, target, entry + 1, last)
        returned = exit_index is not None
        if not returned and deadline > arrays.times[-1]:
            stats.censored += 1
            break
        if exit_index is None:
            exit_index = last if last > entry else entry
        window = spread[entry : exit_index + 1]
        adverse = float(np.max(window) - spread[entry]) if len(window) else 0.0
        long_leg = arrays.long_sell[exit_index] / arrays.long_buy[entry] - 1.0
        short_leg = 1.0 - arrays.short_buy[exit_index] / arrays.short_sell[entry]
        pnl = (long_leg + short_leg) * 10_000.0 - fee_bps_total
        stats.trades += 1
        stats.pnl_bps.append(pnl)
        stats.adverse_bps.append(adverse)
        if returned:
            stats.returned += 1
            stats.return_seconds.append(
                float(arrays.times[exit_index] - arrays.times[entry])
            )
        else:
            stats.not_returned_adverse_bps.append(adverse)
        # Re-arm only after the deviation drops below the threshold again.
        rearm = np.searchsorted(below, exit_index + (0 if returned else 1))
        if rearm >= len(below) or exit_index >= n - 1:
            break
        position = int(below[rearm]) + 1
    return stats


def _first_at_or_below(
    values: np.ndarray, target: float, start: int, stop: int
) -> int | None:
    """First index in [start, stop] with value <= target, scanning in chunks."""
    chunk = 64
    index = start
    while index <= stop:
        end = min(stop + 1, index + chunk)
        hits = np.flatnonzero(values[index:end] <= target)
        if len(hits):
            return index + int(hits[0])
        index = end
        chunk = min(chunk * 4, 65_536)
    return None


ANCHOR_SQL = """
    SELECT
        epoch(sample_time) AS t,
        long_buy, long_sell, short_sell, short_buy,
        spread_bps,
        avg(spread_bps) OVER w24 AS mean_24h,
        count(*) OVER w24 AS n_24h,
        median(spread_bps) OVER w2 AS median_2h,
        count(*) OVER w2 AS n_2h
    FROM pair
    WINDOW
        w24 AS (ORDER BY sample_time RANGE BETWEEN INTERVAL 24 HOURS PRECEDING
                AND INTERVAL 1 MICROSECOND PRECEDING),
        w2 AS (ORDER BY sample_time RANGE BETWEEN INTERVAL 2 HOURS PRECEDING
               AND INTERVAL 1 MICROSECOND PRECEDING)
    ORDER BY sample_time
"""


def pair_arrays(
    connection: duckdb.DuckDBPyConnection,
    long_feed: tuple[str, str],
    short_feed: tuple[str, str],
    params: ReplayParams,
) -> PairArrays | None:
    """Join two feeds of the loaded symbol table and compute prior-only anchors."""
    connection.execute("DROP TABLE IF EXISTS pair")
    connection.execute(
        """
        CREATE TEMP TABLE pair AS
        SELECT a.sample_time,
               a.buy AS long_buy, a.sell AS long_sell,
               b.sell AS short_sell, b.buy AS short_buy,
               (b.sell / a.buy - 1.0) * 10000.0 AS spread_bps
        FROM feeds a JOIN feeds b USING (sample_time)
        WHERE a.venue = ? AND a.venue_symbol = ?
          AND b.venue = ? AND b.venue_symbol = ?
          AND abs(epoch(a.observed_at) - epoch(b.observed_at)) <= ?
        """,
        [*long_feed, *short_feed, params.max_skew_seconds],
    )
    data = connection.execute(ANCHOR_SQL).fetchnumpy()
    if len(data["t"]) == 0:
        return None
    anchors: dict[str, np.ndarray] = {}
    for name, column, count_column in (
        ("mean_24h", "mean_24h", "n_24h"),
        ("median_2h", "median_2h", "n_2h"),
    ):
        window, coverage = ANCHORS[name]
        required = coverage * window / params.sample_seconds
        values = np.asarray(data[column], dtype=float)
        counts = np.asarray(data[count_column], dtype=float)
        anchors[name] = np.where(counts >= required, values, np.nan)
    return PairArrays(
        times=np.asarray(data["t"], dtype=float),
        long_buy=np.asarray(data["long_buy"], dtype=float),
        long_sell=np.asarray(data["long_sell"], dtype=float),
        short_sell=np.asarray(data["short_sell"], dtype=float),
        short_buy=np.asarray(data["short_buy"], dtype=float),
        anchors=anchors,
    )


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def consolidate(
    connection: duckdb.DuckDBPyConnection,
    data_root: Path,
    work_dir: Path,
    identities: Iterable[tuple[str, str, str]],
    params: ReplayParams,
) -> Path:
    """One streaming pass: keep only needed rows/columns, sorted per symbol.

    The live dataset holds many small part files per day; re-reading all of
    them once per symbol would dominate the runtime.
    """
    size = {1_000: "1k", 5_000: "5k", 10_000: "10k"}[params.size_usd]
    source = _sql_path(data_root / "market" / "date=*" / "part-*.parquet")
    target = work_dir / f"replay_{size}.parquet"
    work_dir.mkdir(parents=True, exist_ok=True)
    connection.execute("DROP TABLE IF EXISTS wanted")
    connection.execute(
        "CREATE TEMP TABLE wanted (venue VARCHAR, venue_symbol VARCHAR, canonical_symbol VARCHAR)"
    )
    connection.executemany("INSERT INTO wanted VALUES (?, ?, ?)", sorted(identities))
    connection.execute(
        f"""
        COPY (
            SELECT m.canonical_symbol, m.venue, m.venue_symbol, m.sample_time, m.observed_at,
                   m.buy_{size}_vwap AS buy, m.sell_{size}_vwap AS sell
            FROM read_parquet('{source}', union_by_name=true) m
            JOIN wanted USING (venue, venue_symbol, canonical_symbol)
            WHERE m.buy_{size}_vwap > 0 AND m.sell_{size}_vwap > 0
            ORDER BY m.canonical_symbol, m.venue, m.venue_symbol, m.sample_time
        ) TO '{_sql_path(target)}' (FORMAT parquet, ROW_GROUP_SIZE 200000)
        """
    )
    return target


def load_symbol(
    connection: duckdb.DuckDBPyConnection,
    source: Path,
    symbol: str,
    params: ReplayParams,
) -> None:
    """Load fresh, deduplicated VWAPs for one canonical symbol."""
    connection.execute("DROP TABLE IF EXISTS feeds")
    connection.execute(
        f"""
        CREATE TEMP TABLE feeds AS
        SELECT sample_time, observed_at, venue, venue_symbol, buy, sell
        FROM (
            SELECT *, row_number() OVER (
                PARTITION BY venue, venue_symbol, sample_time ORDER BY observed_at DESC
            ) AS rn
            FROM read_parquet('{_sql_path(source)}')
            WHERE canonical_symbol = ?
        )
        WHERE rn = 1
          AND epoch(sample_time) - epoch(observed_at) BETWEEN -1 AND ?
        """,
        [symbol, params.stale_after_seconds],
    )


def _percentile(values: Sequence[float], q: float) -> float | None:
    return float(np.percentile(values, q)) if values else None


def summarize(stats: TradeStats) -> dict[str, object]:
    return {
        "trades": stats.trades,
        "returned": stats.returned,
        "censored": stats.censored,
        "return_rate": stats.returned / stats.trades if stats.trades else None,
        "mean_pnl_bps": float(np.mean(stats.pnl_bps)) if stats.pnl_bps else None,
        "median_pnl_bps": float(np.median(stats.pnl_bps)) if stats.pnl_bps else None,
        "return_p50_s": _percentile(stats.return_seconds, 50),
        "return_p90_s": _percentile(stats.return_seconds, 90),
        "return_max_s": max(stats.return_seconds) if stats.return_seconds else None,
        "not_returned_max_adverse_bps": (
            max(stats.not_returned_adverse_bps)
            if stats.not_returned_adverse_bps
            else None
        ),
        "not_returned_mean_adverse_bps": (
            float(np.mean(stats.not_returned_adverse_bps))
            if stats.not_returned_adverse_bps
            else None
        ),
    }


def _fee(fees: Mapping[str, float], venue: str) -> float | None:
    for name, value in fees.items():
        if name.lower() == venue.lower():
            return float(value)
    return None


def run(
    config: RadarConfig, data_root: Path, work_dir: Path, params: ReplayParams
) -> dict[str, object]:
    identities = config.monitor_market_identities()
    by_symbol: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for venue, venue_symbol, canonical in identities:
        by_symbol[canonical].add((venue, venue_symbol))
    rows: list[dict[str, object]] = []
    pooled: dict[tuple[str, float], TradeStats] = defaultdict(TradeStats)
    started = time.monotonic()
    with duckdb.connect() as connection:
        connection.execute(
            f"SET temp_directory = '{_sql_path(work_dir / 'duckdb_tmp')}'"
        )
        source = consolidate(connection, data_root, work_dir, identities, params)
        print(
            f"consolidated ({time.monotonic() - started:.0f}s)",
            file=sys.stderr,
            flush=True,
        )
        for symbol in sorted(by_symbol):
            feeds = by_symbol[symbol]
            if len(feeds) < 2:
                continue
            load_symbol(connection, source, symbol, params)
            for long_feed in sorted(feeds):
                for short_feed in sorted(feeds):
                    if long_feed[0] == short_feed[0]:
                        continue
                    long_fee = _fee(config.fees_bps, long_feed[0])
                    short_fee = _fee(config.fees_bps, short_feed[0])
                    if long_fee is None or short_fee is None:
                        continue
                    arrays = pair_arrays(connection, long_feed, short_feed, params)
                    if arrays is None:
                        continue
                    for anchor in ANCHORS:
                        for threshold in params.thresholds_bps:
                            stats = simulate(
                                arrays,
                                anchor,
                                threshold,
                                fee_bps_total=2.0 * (long_fee + short_fee),
                                return_band_bps=params.return_band_bps,
                                max_hold_seconds=params.max_hold_hours * 3600,
                                max_gap_seconds=params.max_gap_seconds,
                            )
                            pool = pooled[(anchor, threshold)]
                            pool.trades += stats.trades
                            pool.returned += stats.returned
                            pool.censored += stats.censored
                            pool.pnl_bps.extend(stats.pnl_bps)
                            pool.return_seconds.extend(stats.return_seconds)
                            pool.not_returned_adverse_bps.extend(
                                stats.not_returned_adverse_bps
                            )
                            rows.append(
                                {
                                    "symbol": symbol,
                                    "long_venue": long_feed[0],
                                    "long_venue_symbol": long_feed[1],
                                    "short_venue": short_feed[0],
                                    "short_venue_symbol": short_feed[1],
                                    "anchor": anchor,
                                    "threshold_bps": threshold,
                                    "samples": int(len(arrays.times)),
                                    "round_trip_fee_bps": 2.0 * (long_fee + short_fee),
                                    **summarize(stats),
                                }
                            )
            print(
                f"{symbol}: done ({time.monotonic() - started:.0f}s elapsed)",
                file=sys.stderr,
                flush=True,
            )
        span = connection.execute(
            f"SELECT min(sample_time), max(sample_time) FROM read_parquet('{_sql_path(source)}')"
        ).fetchone()
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "params": asdict(params),
        "data_start": span[0].isoformat() if span and span[0] else None,
        "data_end": span[1].isoformat() if span and span[1] else None,
        "pairs": len({(r["symbol"], r["long_venue"], r["short_venue"]) for r in rows}),
        "pooled": [
            {"anchor": anchor, "threshold_bps": threshold, **summarize(stats)}
            for (anchor, threshold), stats in sorted(pooled.items())
        ],
        "rows": rows,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument(
        "--size-usd", type=int, default=1_000, choices=(1_000, 5_000, 10_000)
    )
    parser.add_argument("--thresholds", default="15,25,40")
    parser.add_argument("--return-band-bps", type=float, default=5.0)
    parser.add_argument("--max-hold-hours", type=float, default=24.0)
    args = parser.parse_args(argv)
    params = ReplayParams(
        size_usd=args.size_usd,
        thresholds_bps=tuple(float(value) for value in args.thresholds.split(",")),
        return_band_bps=args.return_band_bps,
        max_hold_hours=args.max_hold_hours,
    )
    result = run(load_config(args.config), args.data_root, args.work_dir, params)
    rows = result["rows"]
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", newline="", encoding="utf-8") as handle:
        if rows:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    summary = {key: value for key, value in result.items() if key != "rows"}
    args.output_json.write_text(
        json.dumps(summary, indent=2, default=lambda v: None if v != v else v),
        encoding="utf-8",
    )
    print(json.dumps(summary["pooled"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
