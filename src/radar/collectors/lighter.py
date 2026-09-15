from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from radar.collectors.base import (
    CollectorBatch,
    finite_float,
    markets_for_venue,
    non_negative_float,
    positive_float,
)
from radar.collectors.http import request_json as default_request_json
from radar.config import MarketConfig
from radar.models import FundingSnapshot, HourlyContext, MarketSnapshot
from radar.vwap import BookLevel, buy_vwap, sell_vwap

UTC = timezone.utc


@dataclass(frozen=True)
class LighterMarketDetail:
    symbol: str
    market_id: int
    mark_price: float
    index_price: float
    open_interest: float
    volume_24h: float


@dataclass(frozen=True)
class LighterFundingPoint:
    effective_time: datetime
    funding_rate: float


def _require_success(payload: object, endpoint: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError(f"{endpoint} response must be an object")
    if payload.get("code") != 200:
        raise ValueError(f"{endpoint} response code must be 200")
    return payload


def parse_lighter_order_book_details(
    payload: object,
) -> dict[str, LighterMarketDetail]:
    response = _require_success(payload, "orderBookDetails")
    raw_markets = response.get("order_book_details")
    raw_spot_markets = response.get("spot_order_book_details")
    if not isinstance(raw_markets, list) or (
        raw_spot_markets is not None and not isinstance(raw_spot_markets, list)
    ):
        raise ValueError("orderBookDetails response has invalid market lists")

    markets: dict[str, LighterMarketDetail] = {}
    for raw_market in raw_markets:
        if not isinstance(raw_market, dict):
            raise ValueError("orderBookDetails entries must be objects")
        if raw_market.get("market_type") != "perp" or raw_market.get("status") != "active":
            continue
        symbol = raw_market.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("orderBookDetails market symbol is invalid")
        if symbol in markets:
            raise ValueError("orderBookDetails contains duplicate symbols")
        markets[symbol] = LighterMarketDetail(
            symbol=symbol,
            market_id=int(finite_float(raw_market.get("market_id"), "market_id")),
            mark_price=positive_float(raw_market.get("mark_price"), "mark_price"),
            index_price=positive_float(raw_market.get("index_price"), "index_price"),
            open_interest=non_negative_float(
                raw_market.get("open_interest"), "open_interest"
            ),
            volume_24h=non_negative_float(
                raw_market.get("daily_quote_token_volume"),
                "daily_quote_token_volume",
            ),
        )
    return markets


def _parse_lighter_side(raw_orders: object, side: str) -> list[BookLevel]:
    if not isinstance(raw_orders, list) or not raw_orders:
        raise ValueError(f"{side} orders must be a non-empty list")
    aggregated: dict[float, float] = {}
    for raw_order in raw_orders:
        if not isinstance(raw_order, dict):
            raise ValueError(f"{side} order must be an object")
        price = positive_float(raw_order.get("price"), f"{side} price")
        base_size = positive_float(
            raw_order.get("remaining_base_amount"), f"{side} remaining_base_amount"
        )
        aggregated[price] = aggregated.get(price, 0.0) + base_size
    levels = [BookLevel(price=price, base_size=size) for price, size in aggregated.items()]
    levels.sort(
        key=lambda level: level.price,
        reverse=side == "bid",
    )
    return levels


def parse_lighter_order_book_orders(
    payload: object,
) -> tuple[list[BookLevel], list[BookLevel]]:
    response = _require_success(payload, "orderBookOrders")
    return (
        _parse_lighter_side(response.get("bids"), "bid"),
        _parse_lighter_side(response.get("asks"), "ask"),
    )


def parse_lighter_fundings(payload: object) -> LighterFundingPoint | None:
    response = _require_success(payload, "fundings")
    raw_fundings = response.get("fundings")
    if not isinstance(raw_fundings, list):
        raise ValueError("fundings response has invalid funding list")

    points: list[LighterFundingPoint] = []
    for raw_funding in raw_fundings:
        if not isinstance(raw_funding, dict):
            raise ValueError("fundings entries must be objects")
        timestamp = finite_float(raw_funding.get("timestamp"), "funding timestamp")
        points.append(
            LighterFundingPoint(
                effective_time=datetime.fromtimestamp(timestamp, tz=UTC),
                funding_rate=finite_float(raw_funding.get("rate"), "funding rate"),
            )
        )
    return max(points, key=lambda point: point.effective_time) if points else None


class LighterCollector:
    venue = "lighter"
    BASE_URL = "https://mainnet.zklighter.elliot.ai"
    ORDER_BOOK_DETAILS_URL = f"{BASE_URL}/api/v1/orderBookDetails"
    ORDER_BOOK_ORDERS_URL = f"{BASE_URL}/api/v1/orderBookOrders"
    FUNDINGS_URL = f"{BASE_URL}/api/v1/fundings"
    ORDER_BOOK_LIMIT = 250

    def __init__(
        self,
        markets: list[MarketConfig] | tuple[MarketConfig, ...],
        *,
        request_json=default_request_json,
        clock=lambda: datetime.now(UTC),
    ) -> None:
        self._markets = markets_for_venue(markets, self.venue)
        self._request_json = request_json
        self._clock = clock

    async def collect(
        self, *, sample_time: datetime, include_hourly_context: bool
    ) -> CollectorBatch:
        try:
            details_payload = await self._request_json(
                self.ORDER_BOOK_DETAILS_URL,
                method="GET",
                params={"filter": "perp"},
            )
            details = parse_lighter_order_book_details(details_payload)
            details_observed_at = self._clock()
        except Exception:
            return CollectorBatch()

        market_snapshots = await asyncio.gather(
            *(
                self._collect_market(market, details, sample_time)
                for market in self._markets
            )
        )
        funding_snapshots: tuple[FundingSnapshot, ...] = ()
        hourly_contexts: tuple[HourlyContext, ...] = ()
        configured_details = tuple(
            (market, details[market.venue_symbol])
            for market in self._markets
            if market.venue_symbol in details
        )
        if include_hourly_context:
            funding_results = await asyncio.gather(
                *(
                    self._collect_funding(market, detail)
                    for market, detail in configured_details
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
                    observed_at=details_observed_at,
                    venue=self.venue,
                    venue_symbol=market.venue_symbol,
                    canonical_symbol=market.canonical_symbol,
                    open_interest=detail.open_interest,
                    volume_24h=detail.volume_24h,
                )
                for market, detail in configured_details
            )

        return CollectorBatch(
            market_snapshots=tuple(
                snapshot for snapshot in market_snapshots if snapshot is not None
            ),
            funding_snapshots=funding_snapshots,
            hourly_contexts=hourly_contexts,
        )

    async def _collect_market(
        self,
        market: MarketConfig,
        details: dict[str, LighterMarketDetail],
        sample_time: datetime,
    ) -> MarketSnapshot | None:
        detail = details.get(market.venue_symbol)
        if detail is None:
            return None
        try:
            payload = await self._request_json(
                self.ORDER_BOOK_ORDERS_URL,
                method="GET",
                params={
                    "market_id": detail.market_id,
                    "limit": self.ORDER_BOOK_LIMIT,
                },
            )
            bids, asks = parse_lighter_order_book_orders(payload)
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
                mark_price=detail.mark_price,
                index_price=detail.index_price,
                buy_1k_vwap=buy_vwap(asks, 1_000),
                sell_1k_vwap=sell_vwap(bids, 1_000),
                buy_5k_vwap=buy_vwap(asks, 5_000),
                sell_5k_vwap=sell_vwap(bids, 5_000),
                buy_10k_vwap=buy_vwap(asks, 10_000),
                sell_10k_vwap=sell_vwap(bids, 10_000),
            )
        except Exception:
            return None

    async def _collect_funding(
        self,
        market: MarketConfig,
        detail: LighterMarketDetail,
    ) -> FundingSnapshot | None:
        end_timestamp = int(self._clock().timestamp())
        try:
            payload = await self._request_json(
                self.FUNDINGS_URL,
                method="GET",
                params={
                    "market_id": detail.market_id,
                    "resolution": "1h",
                    "start_timestamp": max(0, end_timestamp - 2 * 60 * 60),
                    "end_timestamp": end_timestamp,
                    "count_back": 1,
                },
            )
            point = parse_lighter_fundings(payload)
            if point is None:
                return None
            return FundingSnapshot(
                effective_time=point.effective_time,
                observed_at=self._clock(),
                venue=self.venue,
                venue_symbol=market.venue_symbol,
                canonical_symbol=market.canonical_symbol,
                funding_rate=point.funding_rate,
                next_funding_time=None,
            )
        except Exception:
            return None
