from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from radar.collectors.base import (
    CollectorBatch,
    CollectorErrorHandler,
    finite_float,
    markets_for_venue,
    non_negative_float,
    positive_float,
    report_collector_error,
)
from radar.collectors.http import request_json as default_request_json
from radar.config import MarketConfig
from radar.models import FundingSnapshot, HourlyContext, MarketSnapshot
from radar.vwap import BookLevel, buy_vwap, sell_vwap

UTC = timezone.utc


@dataclass(frozen=True)
class BackpackMarketDetail:
    symbol: str
    base_symbol: str


@dataclass(frozen=True)
class BackpackMarkPrice:
    mark_price: float | None
    index_price: float | None
    funding_rate: float | None
    next_funding_time: datetime | None


@dataclass(frozen=True)
class BackpackFundingPoint:
    effective_time: datetime
    funding_rate: float


def _optional_positive_float(value: object, field_name: str) -> float | None:
    return None if value is None else positive_float(value, field_name)


def _timestamp_milliseconds(value: object, field_name: str) -> datetime:
    timestamp = finite_float(value, field_name)
    return datetime.fromtimestamp(timestamp / 1_000, tz=UTC)


def _timestamp_iso_utc(value: object, field_name: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_backpack_markets(payload: object) -> dict[str, BackpackMarketDetail]:
    if not isinstance(payload, list):
        raise ValueError("markets response must be a list")

    markets: dict[str, BackpackMarketDetail] = {}
    for raw_market in payload:
        if not isinstance(raw_market, dict):
            raise ValueError("markets entries must be objects")
        if (
            raw_market.get("marketType") != "PERP"
            or raw_market.get("orderBookState") != "Open"
            or raw_market.get("visible") is not True
            or raw_market.get("rwaMarketType") != "STOCK"
        ):
            continue
        symbol = raw_market.get("symbol")
        base_symbol = raw_market.get("baseSymbol")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("markets symbol is invalid")
        if not isinstance(base_symbol, str) or not base_symbol:
            raise ValueError("markets baseSymbol is invalid")
        if symbol in markets:
            raise ValueError("markets contains duplicate symbols")
        markets[symbol] = BackpackMarketDetail(
            symbol=symbol,
            base_symbol=base_symbol,
        )
    return markets


def _parse_backpack_side(raw_levels: object, side: str) -> list[BookLevel]:
    if not isinstance(raw_levels, list) or not raw_levels:
        raise ValueError(f"{side} levels must be a non-empty list")

    levels: list[BookLevel] = []
    for raw_level in raw_levels:
        if not isinstance(raw_level, list) or len(raw_level) != 2:
            raise ValueError(f"{side} level must contain price and size")
        levels.append(
            BookLevel(
                price=positive_float(raw_level[0], f"{side} price"),
                base_size=positive_float(raw_level[1], f"{side} size"),
            )
        )
    levels.sort(key=lambda level: level.price, reverse=side == "bid")
    return levels


def parse_backpack_depth(
    payload: object,
) -> tuple[list[BookLevel], list[BookLevel]]:
    if not isinstance(payload, dict):
        raise ValueError("depth response must be an object")
    return (
        _parse_backpack_side(payload.get("bids"), "bid"),
        _parse_backpack_side(payload.get("asks"), "ask"),
    )


def parse_backpack_mark_prices(payload: object) -> dict[str, BackpackMarkPrice]:
    if not isinstance(payload, list):
        raise ValueError("markPrices response must be a list")

    prices: dict[str, BackpackMarkPrice] = {}
    for raw_price in payload:
        if not isinstance(raw_price, dict):
            raise ValueError("markPrices entries must be objects")
        symbol = raw_price.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("markPrices symbol is invalid")
        if symbol in prices:
            raise ValueError("markPrices contains duplicate symbols")
        next_funding_raw = raw_price.get("nextFundingTimestamp")
        prices[symbol] = BackpackMarkPrice(
            mark_price=_optional_positive_float(
                raw_price.get("markPrice"), "markPrice"
            ),
            index_price=_optional_positive_float(
                raw_price.get("indexPrice"), "indexPrice"
            ),
            funding_rate=(
                None
                if raw_price.get("fundingRate") is None
                else finite_float(raw_price.get("fundingRate"), "fundingRate")
            ),
            next_funding_time=(
                None
                if next_funding_raw is None
                else _timestamp_milliseconds(
                    next_funding_raw, "nextFundingTimestamp"
                )
            ),
        )
    return prices


def parse_backpack_open_interest(payload: object) -> dict[str, float]:
    if not isinstance(payload, list):
        raise ValueError("openInterest response must be a list")

    open_interest: dict[str, float] = {}
    for raw_point in payload:
        if not isinstance(raw_point, dict):
            raise ValueError("openInterest entries must be objects")
        symbol = raw_point.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("openInterest symbol is invalid")
        if symbol in open_interest:
            raise ValueError("openInterest contains duplicate symbols")
        open_interest[symbol] = non_negative_float(
            raw_point.get("openInterest"), "openInterest"
        )
    return open_interest


def parse_backpack_tickers(payload: object) -> dict[str, float | None]:
    if not isinstance(payload, list):
        raise ValueError("tickers response must be a list")

    tickers: dict[str, float | None] = {}
    for raw_ticker in payload:
        if not isinstance(raw_ticker, dict):
            raise ValueError("tickers entries must be objects")
        symbol = raw_ticker.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("tickers symbol is invalid")
        if symbol in tickers:
            raise ValueError("tickers contains duplicate symbols")
        quote_volume = raw_ticker.get("quoteVolume")
        tickers[symbol] = (
            None
            if quote_volume is None
            else non_negative_float(quote_volume, "quoteVolume")
        )
    return tickers


def parse_backpack_funding_rates(
    payload: object, *, expected_symbol: str
) -> BackpackFundingPoint | None:
    if not isinstance(payload, list):
        raise ValueError("fundingRates response must be a list")

    points: list[BackpackFundingPoint] = []
    for raw_point in payload:
        if not isinstance(raw_point, dict):
            raise ValueError("fundingRates entries must be objects")
        if raw_point.get("symbol") != expected_symbol:
            raise ValueError("fundingRates symbol does not match request")
        points.append(
            BackpackFundingPoint(
                effective_time=_timestamp_iso_utc(
                    raw_point.get("intervalEndTimestamp"),
                    "intervalEndTimestamp",
                ),
                funding_rate=finite_float(
                    raw_point.get("fundingRate"), "fundingRate"
                ),
            )
        )
    return max(points, key=lambda point: point.effective_time) if points else None


class BackpackCollector:
    venue = "backpack"
    BASE_URL = "https://api.backpack.exchange"
    MARKETS_URL = f"{BASE_URL}/api/v1/markets"
    DEPTH_URL = f"{BASE_URL}/api/v1/depth"
    MARK_PRICES_URL = f"{BASE_URL}/api/v1/markPrices"
    OPEN_INTEREST_URL = f"{BASE_URL}/api/v1/openInterest"
    FUNDING_RATES_URL = f"{BASE_URL}/api/v1/fundingRates"
    TICKERS_URL = f"{BASE_URL}/api/v1/tickers"
    DEPTH_LIMIT = 1000

    def __init__(
        self,
        markets: list[MarketConfig] | tuple[MarketConfig, ...],
        *,
        request_json=default_request_json,
        clock=lambda: datetime.now(UTC),
        error_handler: CollectorErrorHandler | None = None,
    ) -> None:
        self._markets = markets_for_venue(markets, self.venue)
        self._request_json = request_json
        self._clock = clock
        self._error_handler = error_handler

    async def collect(
        self, *, sample_time: datetime, include_hourly_context: bool
    ) -> CollectorBatch:
        try:
            markets_payload = await self._request_json(
                self.MARKETS_URL,
                method="GET",
                params={"marketType": "PERP"},
            )
            details = parse_backpack_markets(markets_payload)
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return CollectorBatch()

        mark_prices: dict[str, BackpackMarkPrice] = {}
        try:
            mark_prices_payload = await self._request_json(
                self.MARK_PRICES_URL,
                method="GET",
                params={"marketType": "PERP"},
            )
            mark_prices = parse_backpack_mark_prices(mark_prices_payload)
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)

        open_interest: dict[str, float] = {}
        tickers: dict[str, float | None] = {}
        context_observed_at = self._clock()
        if include_hourly_context:
            try:
                open_interest_payload = await self._request_json(
                    self.OPEN_INTEREST_URL,
                    method="GET",
                )
                open_interest = parse_backpack_open_interest(open_interest_payload)
            except Exception as error:  # noqa: BLE001
                report_collector_error(self._error_handler, self.venue, error)
            try:
                tickers_payload = await self._request_json(
                    self.TICKERS_URL,
                    method="GET",
                    params={"interval": "1d"},
                )
                tickers = parse_backpack_tickers(tickers_payload)
            except Exception as error:  # noqa: BLE001
                report_collector_error(self._error_handler, self.venue, error)
            context_observed_at = self._clock()

        configured_details = tuple(
            (market, details[market.venue_symbol])
            for market in self._markets
            if market.venue_symbol in details
        )
        market_results = await asyncio.gather(
            *(
                self._collect_market(
                    market,
                    detail,
                    mark_prices,
                    sample_time,
                )
                for market, detail in configured_details
            )
        )

        funding_snapshots: tuple[FundingSnapshot, ...] = ()
        hourly_contexts: tuple[HourlyContext, ...] = ()
        if include_hourly_context:
            funding_results = await asyncio.gather(
                *(
                    self._collect_funding(
                        market,
                        mark_prices.get(market.venue_symbol),
                    )
                    for market, _ in configured_details
                )
            )
            funding_snapshots = tuple(
                funding for funding in funding_results if funding is not None
            )
            hourly_sample = sample_time.astimezone(UTC).replace(
                minute=0, second=0, microsecond=0
            )
            hourly_contexts = tuple(
                HourlyContext(
                    sample_time=hourly_sample,
                    observed_at=context_observed_at,
                    venue=self.venue,
                    venue_symbol=market.venue_symbol,
                    canonical_symbol=market.canonical_symbol,
                    open_interest=open_interest.get(market.venue_symbol),
                    volume_24h=tickers.get(market.venue_symbol),
                )
                for market, _ in configured_details
            )

        return CollectorBatch(
            market_snapshots=tuple(
                snapshot for snapshot in market_results if snapshot is not None
            ),
            funding_snapshots=funding_snapshots,
            hourly_contexts=hourly_contexts,
        )

    async def _collect_market(
        self,
        market: MarketConfig,
        detail: BackpackMarketDetail,
        mark_prices: dict[str, BackpackMarkPrice],
        sample_time: datetime,
    ) -> MarketSnapshot | None:
        try:
            payload = await self._request_json(
                self.DEPTH_URL,
                method="GET",
                params={
                    "symbol": detail.symbol,
                    "limit": self.DEPTH_LIMIT,
                },
            )
            bids, asks = parse_backpack_depth(payload)
            mark_price = mark_prices.get(detail.symbol)
            observed_at = self._clock()
            return MarketSnapshot(
                sample_time=sample_time,
                observed_at=observed_at,
                venue=self.venue,
                venue_symbol=market.venue_symbol,
                canonical_symbol=market.canonical_symbol,
                best_bid=bids[0].price,
                best_bid_size=bids[0].base_size,
                best_ask=asks[0].price,
                best_ask_size=asks[0].base_size,
                mark_price=None if mark_price is None else mark_price.mark_price,
                index_price=None if mark_price is None else mark_price.index_price,
                buy_1k_vwap=buy_vwap(asks, 1_000),
                sell_1k_vwap=sell_vwap(bids, 1_000),
                buy_5k_vwap=buy_vwap(asks, 5_000),
                sell_5k_vwap=sell_vwap(bids, 5_000),
                buy_10k_vwap=buy_vwap(asks, 10_000),
                sell_10k_vwap=sell_vwap(bids, 10_000),
            )
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return None

    async def _collect_funding(
        self,
        market: MarketConfig,
        mark_price: BackpackMarkPrice | None,
    ) -> FundingSnapshot | None:
        try:
            payload = await self._request_json(
                self.FUNDING_RATES_URL,
                method="GET",
                params={"symbol": market.venue_symbol, "limit": 1},
            )
            point = parse_backpack_funding_rates(
                payload,
                expected_symbol=market.venue_symbol,
            )
            if point is None:
                return None
            return FundingSnapshot(
                effective_time=point.effective_time,
                observed_at=self._clock(),
                venue=self.venue,
                venue_symbol=market.venue_symbol,
                canonical_symbol=market.canonical_symbol,
                funding_rate=point.funding_rate,
                next_funding_time=(
                    None if mark_price is None else mark_price.next_funding_time
                ),
            )
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return None
