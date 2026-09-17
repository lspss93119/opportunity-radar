from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

from radar.collectors.base import (
    CollectorBatch,
    CollectorErrorHandler,
    finite_float,
    markets_for_venue,
    non_negative_float,
    parse_book_levels,
    positive_float,
    report_collector_error,
)
from radar.collectors.http import request_json as default_request_json
from radar.config import MarketConfig
from radar.models import FundingSnapshot, HourlyContext, MarketSnapshot
from radar.vwap import BookLevel, buy_vwap, sell_vwap

UTC = timezone.utc


@dataclass(frozen=True)
class HyperliquidAssetContext:
    mark_price: float
    index_price: float
    funding_rate: float
    open_interest: float
    volume_24h: float


@dataclass(frozen=True)
class HyperliquidFundingPoint:
    effective_time: datetime
    funding_rate: float


def parse_hyperliquid_meta_and_asset_ctxs(
    payload: object,
) -> dict[str, HyperliquidAssetContext]:
    if not isinstance(payload, list) or len(payload) != 2:
        raise ValueError("metaAndAssetCtxs response must contain metadata and contexts")
    metadata, raw_contexts = payload
    if not isinstance(metadata, dict) or not isinstance(raw_contexts, list):
        raise ValueError("metaAndAssetCtxs response has invalid structure")
    universe = metadata.get("universe")
    if not isinstance(universe, list) or len(universe) != len(raw_contexts):
        raise ValueError("metaAndAssetCtxs universe and contexts must align")

    contexts: dict[str, HyperliquidAssetContext] = {}
    for raw_market, raw_context in zip(universe, raw_contexts, strict=True):
        if not isinstance(raw_market, dict) or not isinstance(raw_context, dict):
            raise ValueError("metaAndAssetCtxs entries must be objects")
        symbol = raw_market.get("name")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("metaAndAssetCtxs market name is invalid")
        if symbol in contexts:
            raise ValueError("metaAndAssetCtxs contains duplicate market names")
        contexts[symbol] = HyperliquidAssetContext(
            mark_price=positive_float(raw_context.get("markPx"), "markPx"),
            index_price=positive_float(raw_context.get("oraclePx"), "oraclePx"),
            funding_rate=finite_float(raw_context.get("funding"), "funding"),
            open_interest=non_negative_float(
                raw_context.get("openInterest"), "openInterest"
            ),
            volume_24h=non_negative_float(
                raw_context.get("dayNtlVlm"), "dayNtlVlm"
            ),
        )
    return contexts


def parse_hyperliquid_l2_book(
    payload: object,
    *,
    expected_coin: str | None = None,
) -> tuple[list[BookLevel], list[BookLevel]]:
    if not isinstance(payload, dict):
        raise ValueError("l2Book response must be an object")
    if expected_coin is not None and payload.get("coin") != expected_coin:
        raise ValueError("l2Book coin does not match request")
    raw_levels = payload.get("levels")
    if not isinstance(raw_levels, list) or len(raw_levels) != 2:
        raise ValueError("l2Book levels must contain bids and asks")
    bids = parse_book_levels(raw_levels[0], "bid")
    asks = parse_book_levels(raw_levels[1], "ask")
    bids.sort(key=lambda level: level.price, reverse=True)
    asks.sort(key=lambda level: level.price)
    return bids, asks


def parse_hyperliquid_funding_history(
    payload: object, *, expected_coin: str | None = None
) -> HyperliquidFundingPoint | None:
    if not isinstance(payload, list):
        raise ValueError("fundingHistory response must be a list")
    points: list[HyperliquidFundingPoint] = []
    for raw_point in payload:
        if not isinstance(raw_point, dict):
            raise ValueError("fundingHistory entries must be objects")
        if expected_coin is not None and raw_point.get("coin") != expected_coin:
            raise ValueError("fundingHistory coin does not match request")
        timestamp_ms = int(finite_float(raw_point.get("time"), "funding time"))
        points.append(
            HyperliquidFundingPoint(
                effective_time=datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC),
                funding_rate=finite_float(
                    raw_point.get("fundingRate"), "fundingRate"
                ),
            )
        )
    return max(points, key=lambda point: point.effective_time) if points else None


class HyperliquidCollector:
    venue = "hyperliquid"
    INFO_URL = "https://api.hyperliquid.xyz/info"

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
            context_payload = await self._request_json(
                self.INFO_URL,
                method="POST",
                json_body={"type": "metaAndAssetCtxs"},
            )
            contexts = parse_hyperliquid_meta_and_asset_ctxs(context_payload)
            metadata_observed_at = self._clock()
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return CollectorBatch()

        market_snapshots = await asyncio.gather(
            *(self._collect_market(market, contexts, sample_time) for market in self._markets)
        )
        funding_snapshots: tuple[FundingSnapshot, ...] = ()
        hourly_contexts: tuple[HourlyContext, ...] = ()
        if include_hourly_context:
            funding_results = await asyncio.gather(
                *(
                    self._collect_funding(market)
                    for market in self._markets
                    if market.venue_symbol in contexts
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
                    observed_at=metadata_observed_at,
                    venue=self.venue,
                    venue_symbol=market.venue_symbol,
                    canonical_symbol=market.canonical_symbol,
                    open_interest=contexts[market.venue_symbol].open_interest,
                    volume_24h=contexts[market.venue_symbol].volume_24h,
                )
                for market in self._markets
                if market.venue_symbol in contexts
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
        contexts: dict[str, HyperliquidAssetContext],
        sample_time: datetime,
    ) -> MarketSnapshot | None:
        context = contexts.get(market.venue_symbol)
        if context is None:
            return None
        try:
            payload = await self._request_json(
                self.INFO_URL,
                method="POST",
                json_body={"type": "l2Book", "coin": market.venue_symbol},
            )
            bids, asks = parse_hyperliquid_l2_book(
                payload, expected_coin=market.venue_symbol
            )
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
                mark_price=context.mark_price,
                index_price=context.index_price,
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
    ) -> FundingSnapshot | None:
        end_time = int(self._clock().timestamp() * 1000)
        try:
            payload = await self._request_json(
                self.INFO_URL,
                method="POST",
                json_body={
                    "type": "fundingHistory",
                    "coin": market.venue_symbol,
                    "startTime": end_time - 2 * 60 * 60 * 1000,
                    "endTime": end_time,
                },
            )
            point = parse_hyperliquid_funding_history(
                payload, expected_coin=market.venue_symbol
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
                next_funding_time=None,
            )
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return None
