from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
import math
from typing import Callable, Protocol, Sequence, TypeAlias, runtime_checkable

from radar.config import MarketConfig
from radar.models import FundingSnapshot, HourlyContext, MarketSnapshot
from radar.vwap import BookLevel

LOGGER = logging.getLogger(__name__)
CollectorErrorHandler: TypeAlias = Callable[[str, Exception], None]


@dataclass(frozen=True)
class CollectorBatch:
    """Normalized data returned by one collector for one sample."""

    market_snapshots: tuple[MarketSnapshot, ...] = ()
    funding_snapshots: tuple[FundingSnapshot, ...] = ()
    hourly_contexts: tuple[HourlyContext, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "market_snapshots", tuple(self.market_snapshots))
        object.__setattr__(self, "funding_snapshots", tuple(self.funding_snapshots))
        object.__setattr__(self, "hourly_contexts", tuple(self.hourly_contexts))


@runtime_checkable
class Collector(Protocol):
    venue: str

    async def collect(
        self, *, sample_time: datetime, include_hourly_context: bool
    ) -> CollectorBatch:
        """Collect one normalized sample without placing or signing orders."""


@runtime_checkable
class ManagedCollector(Protocol):
    venue: str

    async def start(self) -> None:
        """Start background market-data ingestion."""

    async def stop(self) -> None:
        """Stop background market-data ingestion."""

    async def collect_hourly(self, *, sample_time: datetime) -> CollectorBatch:
        """Collect the independent hourly funding/context batch."""


CollectorLike: TypeAlias = Collector | ManagedCollector


def report_collector_error(
    error_handler: CollectorErrorHandler | None,
    venue: str,
    error: Exception,
) -> None:
    if error_handler is None:
        LOGGER.error(
            "collector %s failed: %s: %s",
            venue,
            type(error).__name__,
            error,
        )
        return
    try:
        error_handler(venue, error)
    except Exception as handler_error:  # noqa: BLE001
        LOGGER.error(
            "collector error handler failed for %s",
            venue,
            exc_info=handler_error,
        )


def markets_for_venue(
    markets: Sequence[MarketConfig], venue: str
) -> tuple[MarketConfig, ...]:
    normalized_venue = venue.lower()
    return tuple(
        market
        for market in markets
        if market.enabled and market.venue.lower() == normalized_venue
    )


def finite_float(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError(f"{field_name} must be numeric")
    try:
        result = float(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


def positive_float(value: object, field_name: str) -> float:
    result = finite_float(value, field_name)
    if result <= 0:
        raise ValueError(f"{field_name} must be positive")
    return result


def non_negative_float(value: object, field_name: str) -> float:
    result = finite_float(value, field_name)
    if result < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return result


def parse_book_levels(raw_levels: object, side: str) -> list[BookLevel]:
    if not isinstance(raw_levels, list) or not raw_levels:
        raise ValueError(f"{side} levels must be a non-empty list")

    levels: list[BookLevel] = []
    for raw_level in raw_levels:
        if not isinstance(raw_level, dict):
            raise ValueError(f"{side} level must be an object")
        levels.append(
            BookLevel(
                price=positive_float(raw_level.get("px"), f"{side} price"),
                base_size=positive_float(raw_level.get("sz"), f"{side} size"),
            )
        )
    return levels
