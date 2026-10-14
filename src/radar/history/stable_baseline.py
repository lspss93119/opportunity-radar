"""Offline, read-only multi-market stable-baseline research scanner.

All signal baselines use *previous completed UTC days only*. Trades execute at
subsequent valid observations. This is research, not a fill/execution guarantee.
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import combinations, groupby
from pathlib import Path
from statistics import median


@dataclass(frozen=True, order=True)
class Pair:
    symbol: str
    venue_a: str
    symbol_a: str
    venue_b: str
    symbol_b: str


@dataclass(frozen=True)
class Quote:
    sample_time: datetime
    observed_at: datetime
    venue: str
    venue_symbol: str
    symbol: str
    buy: float  # actual size-specific buy VWAP
    sell: float  # actual size-specific sell VWAP

    @property
    def mid(self) -> float:
        return (self.buy + self.sell) / 2


@dataclass(frozen=True)
class Params:
    sample_seconds: int = 60
    max_quote_age_seconds: int = 12
    max_skew_seconds: int = 3
    max_gap_seconds: int = 120
    baseline_days: int = 7
    min_baseline_days: int = 5
    min_daily_coverage: float = 0.60
    stable_range_bps: float = 12.0
    deviation_bps: float = 20.0
    confirmation_samples: int = 2
    return_band_bps: float = 5.0
    stop_expansion_bps: float = 50.0
    max_hold_seconds: int = 14400
    min_expected_net_bps: float = 5.0
    extra_slippage_bps_per_order: float = 0.0
    notional_per_leg: float = 1000.0

    def __post_init__(self) -> None:
        if (self.sample_seconds <= 0 or 86400 % self.sample_seconds
                or self.max_quote_age_seconds < 0 or self.max_skew_seconds < 0
                or self.max_gap_seconds < self.sample_seconds
                or self.baseline_days < self.min_baseline_days or self.min_baseline_days < 1
                or not 0 < self.min_daily_coverage <= 1 or self.confirmation_samples < 1
                or self.max_hold_seconds <= 0 or self.notional_per_leg <= 0):
            raise ValueError("invalid scanner time/coverage/size parameters")
        for name in ("stable_range_bps", "deviation_bps", "return_band_bps",
                     "stop_expansion_bps", "min_expected_net_bps",
                     "extra_slippage_bps_per_order"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")


@dataclass
class Entry:
    timestamp: datetime
    direction: int       # +1: buy A/sell B; -1: buy B/sell A
    baseline_bps: float
    deviation_bps: float
    long_buy: float
    short_sell: float
    long_venue: str
    short_venue: str
    pending_exit: str | None = None


@dataclass
class PairState:
    last_time: datetime | None = None
    candidate_side: int = 0
    candidate_count: int = 0
    pending_entry: int = 0
    position: Entry | None = None


def _is_valid(q: Quote, params: Params) -> bool:
    age = (q.sample_time - q.observed_at).total_seconds()
    return (0 <= age <= params.max_quote_age_seconds
            and all(math.isfinite(p) and p > 0 for p in (q.buy, q.sell))
            and q.buy >= q.sell and q.sample_time.tzinfo is not None
            and q.observed_at.tzinfo is not None)


def _sample_pairs(quotes: Iterator[Quote], params: Params,
                  configured: set[tuple[str, str, str]]) -> Iterator[tuple[datetime, Pair, Quote, Quote]]:
    # Input must be sample_time sorted. A duplicated feed uses its latest valid
    # observation deterministically; cross-venue observed_at skew is mandatory.
    for ts, block in groupby(quotes, key=lambda q: q.sample_time):
        by_symbol: dict[str, dict[tuple[str, str], Quote]] = defaultdict(dict)
        for quote in block:
            if ((quote.venue, quote.venue_symbol, quote.symbol) not in configured
                    or not _is_valid(quote, params)):
                continue
            market = (quote.venue, quote.venue_symbol)
            old = by_symbol[quote.symbol].get(market)
            if old is None or old.observed_at < quote.observed_at:
                by_symbol[quote.symbol][market] = quote
        for sym in sorted(by_symbol):
            for a, b in combinations(sorted(by_symbol[sym].values(), key=lambda q: (q.venue, q.venue_symbol)), 2):
                if a.venue == b.venue:
                    continue
                if abs((a.observed_at - b.observed_at).total_seconds()) > params.max_skew_seconds:
                    continue
                yield ts, Pair(sym, a.venue, a.venue_symbol, b.venue, b.venue_symbol), a, b


def _quantile(values: list[float], fraction: float) -> float:
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    offset = (len(values) - 1) * fraction
    low = int(offset)
    high = min(low + 1, len(values) - 1)
    return values[low] + (values[high] - values[low]) * (offset - low)


def _basis(a: Quote, b: Quote) -> float:
    return 10000 * (b.mid / a.mid - 1)


def _daily_basis(factory: Callable[[], Iterator[Quote]], params: Params,
                 configured: set[tuple[str, str, str]]) -> tuple[dict[Pair, dict[date, float]], Counter]:
    samples: dict[tuple[Pair, date], list[float]] = defaultdict(list)
    seen: Counter = Counter()
    for ts, pair, a, b in _sample_pairs(factory(), params, configured):
        samples[(pair, ts.date())].append(_basis(a, b))
        seen[pair] += 1
    min_samples = math.ceil(86400 / params.sample_seconds * params.min_daily_coverage)
    daily: dict[Pair, dict[date, float]] = defaultdict(dict)
    for (pair, day), values in samples.items():
        if len(values) >= min_samples:
            daily[pair][day] = median(values)
    return daily, seen


def _reference(history: Mapping[date, float], today: date, params: Params) -> tuple[float, float] | None:
    values = [value for day, value in history.items()
              if 0 < (today - day).days <= params.baseline_days]
    if len(values) < params.min_baseline_days:
        return None
    spread = _quantile(values, .9) - _quantile(values, .1)
    if spread > params.stable_range_bps:
        return None
    return median(values), spread


def _estimate_cost_bps(a: Quote, b: Quote, fees: Mapping[str, float], params: Params) -> float:
    bidask = 10000 * ((a.buy - a.sell) / a.mid + (b.buy - b.sell) / b.mid)
    return bidask + 2 * (fees[a.venue] + fees[b.venue]) + 4 * params.extra_slippage_bps_per_order


def _close_trade(pair: Pair, entry: Entry, ts: datetime, a: Quote, b: Quote,
                 reason: str, fees: Mapping[str, float], params: Params) -> dict:
    long_exit_sell, short_exit_buy = (a.sell, b.buy) if entry.direction == 1 else (b.sell, a.buy)
    qty_long = params.notional_per_leg / entry.long_buy
    qty_short = params.notional_per_leg / entry.short_sell
    gross = ((long_exit_sell - entry.long_buy) * qty_long
             + (entry.short_sell - short_exit_buy) * qty_short)
    fee = (params.notional_per_leg * (fees[entry.long_venue] + fees[entry.short_venue])
           + long_exit_sell * qty_long * fees[entry.long_venue]
           + short_exit_buy * qty_short * fees[entry.short_venue]) / 10000
    slippage = 4 * params.notional_per_leg * params.extra_slippage_bps_per_order / 10000
    pnl = gross - fee - slippage
    return {
        **asdict(pair), "entry_utc": entry.timestamp.isoformat(), "exit_utc": ts.isoformat(),
        "direction": "buy_a_sell_b" if entry.direction == 1 else "buy_b_sell_a",
        "entry_deviation_bps": entry.deviation_bps,
        "exit_deviation_bps": (_basis(a, b) - entry.baseline_bps) * entry.direction,
        "reference_bps": entry.baseline_bps, "duration_seconds": (ts - entry.timestamp).total_seconds(),
        "exit_reason": reason, "gross_usd": gross, "fee_usd": fee,
        "slippage_usd": slippage, "net_usd": pnl,
        "net_bps_per_leg_notional": pnl / params.notional_per_leg * 10000,
    }


def scan(factory: Callable[[], Iterator[Quote]], *,
         configured: set[tuple[str, str, str]], fees: Mapping[str, float],
         params: Params = Params()) -> dict:
    """Replay independent pairs with causal daily baselines and next-tick fills.

    Returns every configured cross-venue pair, including those with insufficient
    data. Quoted/indicative feeds should never be passed as executable markets.
    """
    if any(v not in fees or not math.isfinite(fees[v]) or fees[v] < 0
           for v, _, _ in configured):
        raise ValueError("missing or invalid venue fee; fail closed")
    days, seen = _daily_basis(factory, params, configured)
    pairs = set(days) | set(seen)
    # Include routes without even one valid synchronized quote.
    symbol_markets: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for venue, venue_symbol, symbol in configured:
        symbol_markets[symbol].append((venue, venue_symbol))
    for symbol, markets in symbol_markets.items():
        for (va, sa), (vb, sb) in combinations(sorted(markets), 2):
            if va != vb:
                pairs.add(Pair(symbol, va, sa, vb, sb))
    states = defaultdict(PairState)
    trades: list[dict] = []
    eligible_days: dict[Pair, set[date]] = defaultdict(set)
    for ts, pair, a, b in _sample_pairs(factory(), params, configured):
        state = states[pair]
        gap = ((ts - state.last_time).total_seconds()
               if state.last_time is not None else None)
        state.last_time = ts
        if gap is not None and gap > params.sample_seconds:
            # A missing bar cannot count toward consecutive confirmation.
            state.candidate_count = state.candidate_side = state.pending_entry = 0
            if gap > params.max_gap_seconds and state.position is not None:
                state.position.pending_exit = "data_gap"
        if state.position is not None:
            entry = state.position
            if entry.pending_exit is not None:
                trades.append(_close_trade(pair, entry, ts, a, b, entry.pending_exit, fees, params))
                state.position = None
                continue
            deviation = (_basis(a, b) - entry.baseline_bps) * entry.direction
            if deviation <= params.return_band_bps:
                entry.pending_exit = "converged"
            elif deviation >= entry.deviation_bps + params.stop_expansion_bps:
                entry.pending_exit = "stop_expansion"
            elif (ts - entry.timestamp).total_seconds() >= params.max_hold_seconds:
                entry.pending_exit = "timeout"
            continue
        ref = _reference(days.get(pair, {}), ts.date(), params)
        if ref is None:
            state.candidate_count = state.candidate_side = state.pending_entry = 0
            continue
        baseline, _stability = ref
        eligible_days[pair].add(ts.date())
        deviation = _basis(a, b) - baseline
        sign = 1 if deviation > 0 else -1
        if state.pending_entry:
            pending = state.pending_entry
            state.pending_entry = 0
            if (sign == pending and abs(deviation) >= params.deviation_bps
                    and abs(deviation) - params.return_band_bps
                    - _estimate_cost_bps(a, b, fees, params) >= params.min_expected_net_bps):
                long_buy, short_sell = (a.buy, b.sell) if sign == 1 else (b.buy, a.sell)
                state.position = Entry(ts, sign, baseline, abs(deviation), long_buy, short_sell,
                                       a.venue if sign == 1 else b.venue,
                                       b.venue if sign == 1 else a.venue)
                state.candidate_count = state.candidate_side = 0
                continue
        if abs(deviation) < params.deviation_bps:
            state.candidate_count = state.candidate_side = 0
        else:
            if state.candidate_side == sign and gap == params.sample_seconds:
                state.candidate_count += 1
            else:
                state.candidate_side, state.candidate_count = sign, 1
            if state.candidate_count >= params.confirmation_samples:
                state.pending_entry = sign
                state.candidate_count = state.candidate_side = 0
    grouped: dict[Pair, list[dict]] = defaultdict(list)
    for trade in trades:
        key = Pair(*(trade[k] for k in ("symbol", "venue_a", "symbol_a", "venue_b", "symbol_b")))
        grouped[key].append(trade)
    rows = []
    for pair in sorted(pairs):
        own = grouped[pair]
        # After a feed outage the first restored quote cannot prove that an
        # earlier exit was executable. Keep it in the audit log, not PnL ranking.
        scored = [x for x in own if x["exit_reason"] != "data_gap"]
        net = sum(x["net_usd"] for x in scored)
        stable_days = len(eligible_days[pair])
        rows.append({**asdict(pair), "valid_sample_count": seen[pair],
                     "eligible_signal_days": stable_days, "complete_baseline_days": len(days.get(pair, {})),
                     "closed_trades": len(own), "scored_trades": len(scored),
                     "excluded_gap_exits": len(own) - len(scored),
                     "open_at_end": int(states[pair].position is not None),
                     "converged_trades": sum(t["exit_reason"] == "converged" for t in own),
                     "win_rate": sum(t["net_usd"] > 0 for t in scored) / len(scored) if scored else None,
                     "total_net_usd": net, "mean_net_bps": (sum(t["net_bps_per_leg_notional"] for t in scored)/len(scored) if scored else None),
                     "median_hold_seconds": median(t["duration_seconds"] for t in scored) if scored else None,
                     "status": ("insufficient_history" if not stable_days else
                                "open_at_end" if states[pair].position is not None else
                                "insufficient_trades" if len(scored) < 5 else "research_only")})
    rows.sort(key=lambda row: (row["status"] != "research_only", -row["total_net_usd"], -row["closed_trades"], row["symbol"]))
    return {"parameters": asdict(params), "pairs": rows,
            "trades": sorted(trades, key=lambda t: (t["entry_utc"], t["symbol"], t["venue_a"])),
            "summary": {"configured_pairs": len(pairs), "observed_pairs": len(seen),
                        "pairs_with_eligible_days": sum(bool(eligible_days[p]) for p in pairs),
                        "closed_trades": len(trades),
                        "excluded_gap_exits": sum(t["exit_reason"] == "data_gap" for t in trades),
                        "open_positions_at_end": sum(s.position is not None for s in states.values()),
                        "funding_included": False, "fees_are_configured_assumptions": True,
                        "execution_model": "next eligible sample, fixed per-leg VWAP, no partial fills"}}


def _as_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def csv_or_parquet_quotes(files: list[str], *, size_usd: int, sample_seconds: int) -> Iterator[Quote]:
    import duckdb  # type: ignore[import-untyped]

    if not files or size_usd not in (1000, 5000, 10000):
        raise ValueError("files required; VWAP size must be 1000, 5000 or 10000")
    kind = "parquet" if all(f.endswith(".parquet") for f in files) else "csv" if all(f.endswith(".csv.gz") for f in files) else None
    if kind is None:
        raise ValueError("do not mix parquet and csv.gz inputs")
    source = ("read_parquet(?, union_by_name=true)" if kind == "parquet"
              else "read_csv_auto(?, union_by_name=true, header=true)")
    buy = f"buy_{size_usd//1000}k_vwap"
    sell = f"sell_{size_usd//1000}k_vwap"
    sql = f"""SELECT CAST(sample_time AS TIMESTAMPTZ), CAST(observed_at AS TIMESTAMPTZ),
                venue, venue_symbol, canonical_symbol, {buy}, {sell}
              FROM {source}
              WHERE CAST(EXTRACT(EPOCH FROM CAST(sample_time AS TIMESTAMPTZ)) AS BIGINT) % ? = 0
              ORDER BY sample_time, canonical_symbol, venue, venue_symbol, observed_at"""
    db = duckdb.connect()
    try:
        cursor = db.execute(sql, [files, sample_seconds])
        while chunk := cursor.fetchmany(8192):
            for ts, observed, venue, vsym, sym, buy_px, sell_px in chunk:
                try:
                    yield Quote(_as_datetime(ts), _as_datetime(observed), str(venue), str(vsym),
                                str(sym), float(buy_px), float(sell_px))
                except (ValueError, TypeError, OverflowError):
                    continue
    finally:
        db.close()


def write_results(result: dict, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    for name, rows in (("pair_ranking.csv", result["pairs"]), ("completed_trades.csv", result["trades"])):
        if not rows:
            (output / name).write_text("", encoding="utf-8")
            continue
        with (output / name).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    (output / "summary.json").write_text(json.dumps(
        {"summary": result["summary"], "parameters": result["parameters"]},
        ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    from radar.config import load_config

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--data-root", type=Path, help="local market/date=*/part-*.parquet")
    source.add_argument("--csv-glob", help="historical market CSV.GZ glob")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--size-usd", type=int, choices=(1000, 5000, 10000), default=1000)
    parser.add_argument("--sample-seconds", type=int, default=60)
    parser.add_argument("--extra-slippage-bps", type=float, default=0.0)
    args = parser.parse_args(argv)
    params = Params(sample_seconds=args.sample_seconds,
                    extra_slippage_bps_per_order=args.extra_slippage_bps,
                    notional_per_leg=float(args.size_usd))
    config = load_config(args.config)
    markets = {(m.venue, m.venue_symbol, m.canonical_symbol)
               for m in config.markets if m.enabled}
    paths = (sorted(str(p) for p in args.data_root.glob("market/date=*/part-*.parquet"))
             if args.data_root else sorted(glob.glob(args.csv_glob)))
    if not paths:
        parser.error("no input market files matched")
    factory = lambda: csv_or_parquet_quotes(paths, size_usd=args.size_usd,
                                            sample_seconds=params.sample_seconds)
    result = scan(factory, configured=markets, fees=config.fees_bps, params=params)
    write_results(result, args.output)
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
