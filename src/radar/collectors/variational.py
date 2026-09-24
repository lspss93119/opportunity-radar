from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime

from radar.collectors.base import (
    CollectorErrorHandler,
    finite_float,
    markets_for_venue,
    non_negative_float,
    positive_float,
    report_collector_error,
)
from radar.collectors.http import request_json as default_request_json
from radar.config import MarketConfig
from radar.models import QuotedMarketSnapshot

VARIATIONAL_VENUE = "variational"
VARIATIONAL_STATS_URL = (
    "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats"
)
VARIATIONAL_POLL_INTERVAL_SECONDS = 30.0
VARIATIONAL_STALE_AFTER_SECONDS = 120.0
US500_SPY_NAME = "State Street SPDR S&P 500 ETF Trust"


def _require_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field_name} must be an ISO timestamp") from error
    return _require_utc(parsed, field_name)


def _parse_interval(value: object) -> int:
    numeric = finite_float(value, "funding_interval_s")
    interval = int(numeric)
    if numeric != interval or interval < 0:
        raise ValueError("funding_interval_s must be a non-negative integer")
    return interval


def _parse_quote_side(raw_quotes: object, size: str, side: str) -> float:
    if not isinstance(raw_quotes, dict):
        raise ValueError("quotes must be an object")
    raw_size = raw_quotes.get(size)
    if not isinstance(raw_size, dict):
        raise ValueError(f"quotes.{size} must be an object")
    return positive_float(raw_size.get(side), f"quotes.{size}.{side}")


def _parse_optional_quote_side(
    raw_quotes: object, size: str, side: str
) -> float | None:
    if not isinstance(raw_quotes, dict):
        raise ValueError("quotes must be an object")
    if size not in raw_quotes:
        return None
    return _parse_quote_side(raw_quotes, size, side)


def _parse_optional_non_negative(value: object, field_name: str) -> float | None:
    return None if value is None else non_negative_float(value, field_name)


def _parse_listing(
    listing: dict[str, object],
    market: MarketConfig,
    *,
    fetched_at: datetime,
    stale_after_seconds: float,
) -> QuotedMarketSnapshot | None:
    ticker = listing.get("ticker")
    if ticker != market.venue_symbol:
        raise ValueError("listing ticker does not match configured venue symbol")

    if ticker == "US500":
        if market.canonical_symbol != "SPY":
            raise ValueError("US500 must map to canonical SPY")
        if listing.get("name") != US500_SPY_NAME:
            raise ValueError("US500 identity does not match the SPY ETF")

    raw_quotes = listing.get("quotes")
    if not isinstance(raw_quotes, dict):
        raise ValueError("listing quotes must be an object")
    quote_time = _parse_timestamp(raw_quotes.get("updated_at"), "quotes.updated_at")
    age_seconds = (fetched_at - quote_time).total_seconds()
    if age_seconds < 0 or age_seconds > stale_after_seconds:
        return None

    funding_interval_seconds = _parse_interval(listing.get("funding_interval_s"))
    if funding_interval_seconds == 0:
        return None

    raw_open_interest = listing.get("open_interest")
    if not isinstance(raw_open_interest, dict):
        raise ValueError("open_interest must be an object")

    bid_1m = _parse_optional_quote_side(raw_quotes, "size_1m", "bid")
    ask_1m = _parse_optional_quote_side(raw_quotes, "size_1m", "ask")
    if (bid_1m is None) != (ask_1m is None):
        raise ValueError("size_1m must contain both bid and ask")

    return QuotedMarketSnapshot(
        quote_time=quote_time,
        fetched_at=fetched_at,
        venue=VARIATIONAL_VENUE,
        venue_symbol=market.venue_symbol,
        canonical_symbol=market.canonical_symbol,
        mark_price=positive_float(listing.get("mark_price"), "mark_price"),
        bid_1k=_parse_quote_side(raw_quotes, "size_1k", "bid"),
        ask_1k=_parse_quote_side(raw_quotes, "size_1k", "ask"),
        bid_100k=_parse_quote_side(raw_quotes, "size_100k", "bid"),
        ask_100k=_parse_quote_side(raw_quotes, "size_100k", "ask"),
        bid_1m=bid_1m,
        ask_1m=ask_1m,
        funding_rate=finite_float(listing.get("funding_rate"), "funding_rate"),
        funding_interval_seconds=funding_interval_seconds,
        volume_24h=_parse_optional_non_negative(
            listing.get("volume_24h"), "volume_24h"
        ),
        long_open_interest=_parse_optional_non_negative(
            raw_open_interest.get("long_open_interest"),
            "long_open_interest",
        ),
        short_open_interest=_parse_optional_non_negative(
            raw_open_interest.get("short_open_interest"),
            "short_open_interest",
        ),
    )


def parse_variational_stats(
    payload: object,
    configured_markets: Sequence[MarketConfig],
    *,
    fetched_at: datetime,
    stale_after_seconds: float = VARIATIONAL_STALE_AFTER_SECONDS,
) -> tuple[QuotedMarketSnapshot, ...]:
    """Parse fresh configured Variational perp quotes from one stats payload."""
    if not isinstance(payload, dict):
        raise ValueError("Variational stats response must be an object")
    raw_listings = payload.get("listings")
    if not isinstance(raw_listings, list):
        raise ValueError("Variational stats response has invalid listings")
    if stale_after_seconds < 0:
        raise ValueError("stale_after_seconds must be non-negative")

    fetched_at = _require_utc(fetched_at, "fetched_at")
    configured = {
        market.venue_symbol: market
        for market in markets_for_venue(configured_markets, VARIATIONAL_VENUE)
    }
    snapshots: list[QuotedMarketSnapshot] = []
    seen_tickers: set[str] = set()
    for raw_listing in raw_listings:
        if not isinstance(raw_listing, dict):
            continue
        ticker = raw_listing.get("ticker")
        if not isinstance(ticker, str) or ticker not in configured:
            continue
        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)
        try:
            snapshot = _parse_listing(
                raw_listing,
                configured[ticker],
                fetched_at=fetched_at,
                stale_after_seconds=stale_after_seconds,
            )
        except (TypeError, ValueError):
            continue
        if snapshot is not None:
            snapshots.append(snapshot)
    return tuple(snapshots)


class VariationalCollector:
    """Read-only background source for size-specific Variational quotes."""

    venue = VARIATIONAL_VENUE

    def __init__(
        self,
        markets: Sequence[MarketConfig],
        *,
        request_json=default_request_json,
        clock=lambda: datetime.now(UTC),
        error_handler: CollectorErrorHandler | None = None,
        stale_after_seconds: float = VARIATIONAL_STALE_AFTER_SECONDS,
        poll_interval_seconds: float = VARIATIONAL_POLL_INTERVAL_SECONDS,
    ) -> None:
        if stale_after_seconds < 0:
            raise ValueError("stale_after_seconds must be non-negative")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        self._markets = markets_for_venue(markets, self.venue)
        self._request_json = request_json
        self._clock = clock
        self._error_handler = error_handler
        self.stale_after_seconds = stale_after_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self._last_quote_times: dict[str, datetime] = {}

    async def poll_once(self) -> tuple[QuotedMarketSnapshot, ...]:
        try:
            payload = await self._request_json(
                VARIATIONAL_STATS_URL,
                method="GET",
            )
            fetched_at = _require_utc(self._clock(), "fetched_at")
            parsed = parse_variational_stats(
                payload,
                self._markets,
                fetched_at=fetched_at,
                stale_after_seconds=self.stale_after_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return ()

        published: list[QuotedMarketSnapshot] = []
        for snapshot in parsed:
            previous_quote_time = self._last_quote_times.get(snapshot.venue_symbol)
            if previous_quote_time is not None and snapshot.quote_time <= previous_quote_time:
                continue
            self._last_quote_times[snapshot.venue_symbol] = snapshot.quote_time
            published.append(snapshot)
        return tuple(published)
