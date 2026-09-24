from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from collections.abc import Sequence

from radar.collectors.base import CollectorBatch
from radar.config import MarketConfig
from radar.models import MarketSnapshot
from radar.vwap import BookLevel, buy_vwap, sell_vwap

UTC = timezone.utc
_EMPTY_OBSERVATION = datetime.min.replace(tzinfo=UTC)


def _require_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    return value


def _require_non_empty_identity(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be non-empty")
    return value


def _validate_book_level(level: BookLevel, side: str) -> None:
    if not isinstance(level, BookLevel):
        raise ValueError(f"{side} levels must contain BookLevel values")
    if not math.isfinite(level.price) or level.price <= 0:
        raise ValueError(f"{side} price must be positive and finite")
    if not math.isfinite(level.base_size) or level.base_size < 0:
        raise ValueError(f"{side} size must be non-negative and finite")


def _optional_positive(value: float | None, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be positive and finite")
    try:
        numeric_value = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be positive and finite") from exc
    if not math.isfinite(numeric_value) or numeric_value <= 0:
        raise ValueError(f"{field_name} must be positive and finite")
    return numeric_value


@dataclass(frozen=True)
class LatestMarketView:
    venue: str
    venue_symbol: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    observed_at: datetime
    ready: bool
    mark_price: float | None = None
    index_price: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "bids",
            tuple(sorted(self.bids, key=lambda level: level.price, reverse=True)),
        )
        object.__setattr__(
            self,
            "asks",
            tuple(sorted(self.asks, key=lambda level: level.price)),
        )


class LatestMarketData:
    """Keep the newest complete normalized book for each configured feed."""

    def __init__(self) -> None:
        self._views: dict[tuple[str, str], LatestMarketView] = {}

    def update_book(
        self,
        *,
        venue: str,
        venue_symbol: str,
        bids: Sequence[BookLevel],
        asks: Sequence[BookLevel],
        observed_at: datetime,
    ) -> None:
        venue = _require_non_empty_identity(venue, "venue")
        venue_symbol = _require_non_empty_identity(venue_symbol, "venue_symbol")
        observed_at = _require_utc(observed_at, "observed_at")
        bid_levels = tuple(bids)
        ask_levels = tuple(asks)
        if not bid_levels or not ask_levels:
            raise ValueError("bids and asks must be non-empty")
        for level in bid_levels:
            _validate_book_level(level, "bid")
        for level in ask_levels:
            _validate_book_level(level, "ask")

        sorted_bids = tuple(
            sorted(bid_levels, key=lambda level: level.price, reverse=True)
        )
        sorted_asks = tuple(sorted(ask_levels, key=lambda level: level.price))
        if sorted_bids[0].price >= sorted_asks[0].price:
            raise ValueError("book must not be crossed or locked")

        key = (venue, venue_symbol)
        previous = self._views.get(key)
        self._views[key] = LatestMarketView(
            venue=venue,
            venue_symbol=venue_symbol,
            bids=sorted_bids,
            asks=sorted_asks,
            observed_at=observed_at,
            ready=True,
            mark_price=None if previous is None else previous.mark_price,
            index_price=None if previous is None else previous.index_price,
        )

    def invalidate(self, *, venue: str, venue_symbol: str) -> None:
        venue = _require_non_empty_identity(venue, "venue")
        venue_symbol = _require_non_empty_identity(venue_symbol, "venue_symbol")
        key = (venue, venue_symbol)
        previous = self._views.get(key)
        self._views[key] = LatestMarketView(
            venue=venue,
            venue_symbol=venue_symbol,
            bids=(),
            asks=(),
            observed_at=(
                _EMPTY_OBSERVATION if previous is None else previous.observed_at
            ),
            ready=False,
            mark_price=None if previous is None else previous.mark_price,
            index_price=None if previous is None else previous.index_price,
        )

    def update_metadata(
        self,
        *,
        venue: str,
        venue_symbol: str,
        mark_price: float | None,
        index_price: float | None,
    ) -> None:
        venue = _require_non_empty_identity(venue, "venue")
        venue_symbol = _require_non_empty_identity(venue_symbol, "venue_symbol")
        mark_price = _optional_positive(mark_price, "mark_price")
        index_price = _optional_positive(index_price, "index_price")
        key = (venue, venue_symbol)
        previous = self._views.get(key)
        self._views[key] = LatestMarketView(
            venue=venue,
            venue_symbol=venue_symbol,
            bids=() if previous is None else previous.bids,
            asks=() if previous is None else previous.asks,
            observed_at=(
                _EMPTY_OBSERVATION if previous is None else previous.observed_at
            ),
            ready=False if previous is None else previous.ready,
            mark_price=mark_price,
            index_price=index_price,
        )

    def build_batch(
        self,
        markets: Sequence[MarketConfig],
        *,
        sample_time: datetime,
        now: datetime,
        stale_after_seconds: int,
    ) -> CollectorBatch:
        sample_time = _require_utc(sample_time, "sample_time")
        now = _require_utc(now, "now")
        self._validate_stale_after(stale_after_seconds)

        snapshots: list[MarketSnapshot] = []
        for market in markets:
            if not market.enabled:
                continue
            view = self._views.get((market.venue, market.venue_symbol))
            if view is None or not self._is_fresh(view, now, stale_after_seconds):
                continue

            snapshots.append(
                MarketSnapshot(
                    sample_time=sample_time,
                    observed_at=view.observed_at,
                    venue=view.venue,
                    venue_symbol=view.venue_symbol,
                    canonical_symbol=market.canonical_symbol,
                    best_bid=view.bids[0].price,
                    best_bid_size=view.bids[0].base_size,
                    best_ask=view.asks[0].price,
                    best_ask_size=view.asks[0].base_size,
                    mark_price=view.mark_price,
                    index_price=view.index_price,
                    buy_1k_vwap=buy_vwap(list(view.asks), 1_000),
                    sell_1k_vwap=sell_vwap(list(view.bids), 1_000),
                    buy_5k_vwap=buy_vwap(list(view.asks), 5_000),
                    sell_5k_vwap=sell_vwap(list(view.bids), 5_000),
                    buy_10k_vwap=buy_vwap(list(view.asks), 10_000),
                    sell_10k_vwap=sell_vwap(list(view.bids), 10_000),
                )
            )
        return CollectorBatch(market_snapshots=tuple(snapshots))

    def ready_venues(
        self,
        *,
        markets: Sequence[MarketConfig],
        canonical_symbol: str,
        now: datetime,
        stale_after_seconds: int,
    ) -> frozenset[str]:
        now = _require_utc(now, "now")
        self._validate_stale_after(stale_after_seconds)
        venues: set[str] = set()
        for market in markets:
            if not market.enabled or market.canonical_symbol != canonical_symbol:
                continue
            view = self._views.get((market.venue, market.venue_symbol))
            if view is None or not self._is_fresh(view, now, stale_after_seconds):
                continue
            if (
                buy_vwap(list(view.asks), 10_000) is not None
                and sell_vwap(list(view.bids), 10_000) is not None
            ):
                venues.add(market.venue)
        return frozenset(venues)

    @staticmethod
    def _validate_stale_after(stale_after_seconds: int) -> None:
        if isinstance(stale_after_seconds, bool) or stale_after_seconds < 0:
            raise ValueError("stale_after_seconds must be non-negative")

    @staticmethod
    def _is_fresh(
        view: LatestMarketView,
        now: datetime,
        stale_after_seconds: int,
    ) -> bool:
        if not view.ready or not view.bids or not view.asks:
            return False
        age_seconds = (now - view.observed_at).total_seconds()
        return 0 <= age_seconds <= stale_after_seconds
