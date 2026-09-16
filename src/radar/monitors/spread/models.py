from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from radar.models import MarketSnapshot

UTC = timezone.utc
VwapSide = Literal["buy", "sell"]

_VWAP_FIELDS: dict[int, dict[VwapSide, str]] = {
    1_000: {"buy": "buy_1k_vwap", "sell": "sell_1k_vwap"},
    5_000: {"buy": "buy_5k_vwap", "sell": "sell_5k_vwap"},
    10_000: {"buy": "buy_10k_vwap", "sell": "sell_10k_vwap"},
}


@dataclass(frozen=True)
class SpreadPairKey:
    canonical_symbol: str
    long_venue: str
    long_venue_symbol: str
    short_venue: str
    short_venue_symbol: str


@dataclass(frozen=True)
class SpreadCandidate:
    key: SpreadPairKey
    sample_time: datetime
    long_buy_vwap: float
    short_sell_vwap: float
    long_fee_bps: float
    short_fee_bps: float
    raw_spread_bps: float
    net_spread_bps: float


def _finite_number(value: float, field_name: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


def _positive_number(value: float, field_name: str) -> float:
    result = _finite_number(value, field_name)
    if result <= 0:
        raise ValueError(f"{field_name} must be positive")
    return result


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def selected_vwap(
    snapshot: MarketSnapshot,
    side: VwapSide,
    size_usd: int,
) -> float | None:
    try:
        field_name = _VWAP_FIELDS[size_usd][side]
    except KeyError as exc:
        raise ValueError("size_usd and side must be supported executable VWAP values") from exc
    value = getattr(snapshot, field_name)
    if value is None:
        return None
    try:
        return _positive_number(value, field_name)
    except (TypeError, ValueError):
        return None


def is_valid_market_snapshot(
    snapshot: MarketSnapshot,
    now: datetime,
    *,
    stale_after_seconds: int,
) -> bool:
    current_time = _as_utc(now, "now")
    if stale_after_seconds <= 0:
        raise ValueError("stale_after_seconds must be positive")
    if any(
        not isinstance(value, str) or not value.strip()
        for value in (snapshot.venue, snapshot.venue_symbol, snapshot.canonical_symbol)
    ):
        return False
    try:
        observed_at = _as_utc(snapshot.observed_at, "observed_at")
    except ValueError:
        return False
    age_seconds = (current_time - observed_at).total_seconds()
    return 0 <= age_seconds <= stale_after_seconds


def _fee_for(venue: str, fees_bps: Mapping[str, float]) -> float | None:
    for configured_venue, value in fees_bps.items():
        if not isinstance(configured_venue, str) or configured_venue.lower() != venue.lower():
            continue
        if isinstance(value, bool):
            return None
        try:
            fee = float(value)
        except (TypeError, ValueError):
            return None
        return fee if math.isfinite(fee) and fee >= 0 else None
    return None


def build_spread_candidates(
    snapshots: Iterable[MarketSnapshot],
    now: datetime,
    *,
    primary_size_usd: int,
    top_n: int,
    stale_after_seconds: int,
    fees_bps: Mapping[str, float],
) -> tuple[SpreadCandidate, ...]:
    current_time = _as_utc(now, "now")
    if top_n <= 0:
        raise ValueError("top_n must be positive")
    if stale_after_seconds <= 0:
        raise ValueError("stale_after_seconds must be positive")

    grouped: dict[str, list[MarketSnapshot]] = {}
    for snapshot in snapshots:
        if not is_valid_market_snapshot(
            snapshot,
            current_time,
            stale_after_seconds=stale_after_seconds,
        ):
            continue
        grouped.setdefault(snapshot.canonical_symbol, []).append(snapshot)

    candidates: list[SpreadCandidate] = []
    for canonical_symbol in sorted(grouped):
        valid_snapshots = grouped[canonical_symbol]
        buys = [
            (snapshot, price)
            for snapshot in valid_snapshots
            if (price := selected_vwap(snapshot, "buy", primary_size_usd)) is not None
        ]
        sells = [
            (snapshot, price)
            for snapshot in valid_snapshots
            if (price := selected_vwap(snapshot, "sell", primary_size_usd)) is not None
        ]
        buys.sort(key=lambda item: (item[1], item[0].venue, item[0].venue_symbol))
        sells.sort(key=lambda item: (-item[1], item[0].venue, item[0].venue_symbol))

        for long_snapshot, long_price in buys[:top_n]:
            long_fee = _fee_for(long_snapshot.venue, fees_bps)
            if long_fee is None:
                continue
            for short_snapshot, short_price in sells[:top_n]:
                if long_snapshot.venue.lower() == short_snapshot.venue.lower():
                    continue
                try:
                    long_sample_time = _as_utc(long_snapshot.sample_time, "sample_time")
                    short_sample_time = _as_utc(
                        short_snapshot.sample_time, "sample_time"
                    )
                except ValueError:
                    continue
                if long_sample_time != short_sample_time:
                    continue
                short_fee = _fee_for(short_snapshot.venue, fees_bps)
                if short_fee is None:
                    continue
                raw_spread = calculate_raw_spread_bps(long_price, short_price)
                net_spread = calculate_net_spread_bps(
                    raw_spread,
                    long_fee,
                    short_fee,
                )
                candidates.append(
                    SpreadCandidate(
                        key=SpreadPairKey(
                            canonical_symbol=canonical_symbol,
                            long_venue=long_snapshot.venue,
                            long_venue_symbol=long_snapshot.venue_symbol,
                            short_venue=short_snapshot.venue,
                            short_venue_symbol=short_snapshot.venue_symbol,
                        ),
                        sample_time=long_sample_time,
                        long_buy_vwap=long_price,
                        short_sell_vwap=short_price,
                        long_fee_bps=long_fee,
                        short_fee_bps=short_fee,
                        raw_spread_bps=raw_spread,
                        net_spread_bps=net_spread,
                    )
                )
    return tuple(candidates)


def calculate_raw_spread_bps(long_buy_price: float, short_sell_price: float) -> float:
    long_price = _positive_number(long_buy_price, "long_buy_price")
    short_price = _positive_number(short_sell_price, "short_sell_price")
    result = (short_price / long_price - 1.0) * 10_000.0
    if not math.isfinite(result):
        raise ValueError("raw spread must be finite")
    return result


def calculate_net_spread_bps(
    raw_spread_bps: float,
    long_fee_bps: float,
    short_fee_bps: float,
) -> float:
    raw = _finite_number(raw_spread_bps, "raw_spread_bps")
    long_fee = _finite_number(long_fee_bps, "long_fee_bps")
    short_fee = _finite_number(short_fee_bps, "short_fee_bps")
    if long_fee < 0 or short_fee < 0:
        raise ValueError("fees must be non-negative")
    result = raw - long_fee - short_fee
    if not math.isfinite(result):
        raise ValueError("net spread must be finite")
    return result
