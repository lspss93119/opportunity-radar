from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BookLevel:
    price: float
    base_size: float

    def __post_init__(self) -> None:
        if self.price <= 0:
            raise ValueError("price must be positive")
        if self.base_size <= 0:
            raise ValueError("base_size must be positive")


def _vwap(levels: list[BookLevel], notional_usd: float) -> float | None:
    if notional_usd <= 0:
        raise ValueError("notional_usd must be positive")
    if not levels:
        return None

    remaining = float(notional_usd)
    total_base = 0.0

    for level in levels:
        available_notional = level.price * level.base_size
        consumed_notional = min(remaining, available_notional)
        total_base += consumed_notional / level.price
        remaining -= consumed_notional
        if remaining <= 1e-12:
            return notional_usd / total_base

    return None


def buy_vwap(asks: list[BookLevel], notional_usd: float) -> float | None:
    ordered = sorted(asks, key=lambda level: level.price)
    return _vwap(ordered, notional_usd)


def sell_vwap(bids: list[BookLevel], notional_usd: float) -> float | None:
    ordered = sorted(bids, key=lambda level: level.price, reverse=True)
    return _vwap(ordered, notional_usd)
