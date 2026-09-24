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
    positive_float,
    report_collector_error,
)
from radar.collectors.http import request_json as default_request_json
from radar.config import MarketConfig
from radar.market_data import LatestMarketData
from radar.models import FundingSnapshot, HourlyContext, MarketSnapshot
from radar.vwap import BookLevel, buy_vwap, sell_vwap

UTC = timezone.utc


@dataclass(frozen=True)
class ArcusMarketDetail:
    market_display_name: str
    market_id: int
    mark_price: float
    oracle_price: float
    funding_rate: float
    next_funding_time: datetime | None
    open_interest: float | None
    volume_24h: float | None


@dataclass(frozen=True)
class ArcusFundingPoint:
    effective_time: datetime
    funding_rate: float


def _timestamp_seconds(value: object, field_name: str) -> datetime:
    timestamp = finite_float(value, field_name)
    return datetime.fromtimestamp(timestamp, tz=UTC)


def _optional_non_negative_float(value: object, field_name: str) -> float | None:
    return None if value is None else non_negative_float(value, field_name)


def parse_arcus_markets(payload: object) -> dict[str, ArcusMarketDetail]:
    if not isinstance(payload, dict):
        raise ValueError("markets response must be an object")
    raw_markets = payload.get("markets")
    if not isinstance(raw_markets, list):
        raise ValueError("markets response has invalid market list")

    markets: dict[str, ArcusMarketDetail] = {}
    for raw_market in raw_markets:
        if not isinstance(raw_market, dict):
            raise ValueError("markets entries must be objects")
        if raw_market.get("status") != "ONLINE" or raw_market.get("type") != "PERPETUAL":
            continue
        market_display_name = raw_market.get("marketDisplayName")
        if not isinstance(market_display_name, str) or not market_display_name:
            raise ValueError("markets marketDisplayName is invalid")
        if market_display_name in markets:
            raise ValueError("markets contains duplicate marketDisplayName values")
        next_funding_raw = raw_market.get("nextFundingAt")
        markets[market_display_name] = ArcusMarketDetail(
            market_display_name=market_display_name,
            market_id=int(finite_float(raw_market.get("marketId"), "marketId")),
            mark_price=positive_float(raw_market.get("markPrice"), "markPrice"),
            oracle_price=positive_float(raw_market.get("oraclePrice"), "oraclePrice"),
            funding_rate=finite_float(raw_market.get("fundingRate"), "fundingRate"),
            next_funding_time=(
                None
                if next_funding_raw is None
                else _timestamp_seconds(next_funding_raw, "nextFundingAt")
            ),
            open_interest=_optional_non_negative_float(
                raw_market.get("openInterest"), "openInterest"
            ),
            volume_24h=_optional_non_negative_float(
                raw_market.get("volume24hNotional"), "volume24hNotional"
            ),
        )
    return markets


def _parse_arcus_side(raw_levels: object, side: str) -> list[BookLevel]:
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


def parse_arcus_l2_order_book(
    payload: object,
) -> tuple[list[BookLevel], list[BookLevel]]:
    if not isinstance(payload, dict):
        raise ValueError("l2OrderBook response must be an object")
    return (
        _parse_arcus_side(payload.get("bids"), "bid"),
        _parse_arcus_side(payload.get("asks"), "ask"),
    )


def parse_arcus_funding_rates(
    payload: object, *, expected_market_id: int
) -> ArcusFundingPoint | None:
    if not isinstance(payload, dict):
        raise ValueError("fundingRates response must be an object")
    raw_rates = payload.get("fundingRates")
    if not isinstance(raw_rates, list):
        raise ValueError("fundingRates response has invalid funding list")

    points: list[ArcusFundingPoint] = []
    for raw_rate in raw_rates:
        if not isinstance(raw_rate, dict):
            raise ValueError("fundingRates entries must be objects")
        market_id = int(finite_float(raw_rate.get("marketId"), "marketId"))
        if market_id != expected_market_id:
            raise ValueError("fundingRates marketId does not match request")
        timestamp_micros = int(finite_float(raw_rate.get("time"), "funding time"))
        points.append(
            ArcusFundingPoint(
                effective_time=datetime.fromtimestamp(
                    timestamp_micros / 1_000_000, tz=UTC
                ),
                funding_rate=finite_float(
                    raw_rate.get("fundingRate"), "fundingRate"
                ),
            )
        )
    return max(points, key=lambda point: point.effective_time) if points else None


class ArcusCollector:
    venue = "arcus"
    BASE_URL = "https://api.arcus.xyz/v1"
    MARKETS_URL = f"{BASE_URL}/markets"
    L2_ORDER_BOOK_URL = f"{BASE_URL}/l2OrderBook"
    FUNDING_RATES_URL = f"{BASE_URL}/fundingRates"
    REFRESH_INTERVAL_SECONDS = 10.0

    def __init__(
        self,
        markets: list[MarketConfig] | tuple[MarketConfig, ...],
        *,
        request_json=default_request_json,
        clock=lambda: datetime.now(UTC),
        error_handler: CollectorErrorHandler | None = None,
        latest_market_data: LatestMarketData | None = None,
        refresh_interval_seconds: float = REFRESH_INTERVAL_SECONDS,
    ) -> None:
        if refresh_interval_seconds < 0:
            raise ValueError("refresh_interval_seconds must be non-negative")
        self._markets = markets_for_venue(markets, self.venue)
        self._request_json = request_json
        self._clock = clock
        self._error_handler = error_handler
        self._latest_market_data = latest_market_data
        self._refresh_interval_seconds = refresh_interval_seconds
        self._details: dict[str, ArcusMarketDetail] = {}
        self._metadata_observed_at: datetime | None = None
        self._refresh_lock = asyncio.Lock()
        self._refresh_task: asyncio.Task[None] | None = None
        self._started = False

    async def start(self) -> None:
        if self._refresh_task is not None and not self._refresh_task.done():
            return
        self._started = True
        self._refresh_task = asyncio.create_task(
            self._run_refresh(),
            name=f"{self.venue}-market-refresh",
        )

    async def stop(self) -> None:
        self._started = False
        refresh_task = self._refresh_task
        if refresh_task is not None:
            refresh_task.cancel()
            await asyncio.gather(refresh_task, return_exceptions=True)
        self._refresh_task = None

    async def refresh_once(self) -> None:
        async with self._refresh_lock:
            try:
                markets_payload = await self._request_json(
                    self.MARKETS_URL,
                    method="GET",
                )
                details = parse_arcus_markets(markets_payload)
                metadata_observed_at = self._clock()
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001
                report_collector_error(self._error_handler, self.venue, error)
                return

            self._details = details
            self._metadata_observed_at = metadata_observed_at
            self._refresh_cache_metadata(details)
            await asyncio.gather(
                *(
                    self._refresh_market(market, details)
                    for market in self._markets
                )
            )

    async def _run_refresh(self) -> None:
        while self._started:
            await self.refresh_once()
            if not self._started:
                return
            await asyncio.sleep(self._refresh_interval_seconds)

    def _refresh_cache_metadata(
        self, details: dict[str, ArcusMarketDetail]
    ) -> None:
        if self._latest_market_data is None:
            return
        for market in self._markets:
            detail = details.get(market.venue_symbol)
            try:
                if detail is None:
                    self._latest_market_data.invalidate(
                        venue=self.venue,
                        venue_symbol=market.venue_symbol,
                    )
                    continue
                self._latest_market_data.update_metadata(
                    venue=self.venue,
                    venue_symbol=market.venue_symbol,
                    mark_price=detail.mark_price,
                    index_price=detail.oracle_price,
                )
            except Exception as error:  # noqa: BLE001
                report_collector_error(self._error_handler, self.venue, error)

    async def _refresh_market(
        self,
        market: MarketConfig,
        details: dict[str, ArcusMarketDetail],
    ) -> None:
        if market.venue_symbol not in details:
            return
        try:
            payload = await self._request_json(
                f"{self.L2_ORDER_BOOK_URL}/{market.venue_symbol}",
                method="GET",
            )
            observed_at = self._clock()
            bids, asks = parse_arcus_l2_order_book(payload)
            if self._latest_market_data is not None:
                self._latest_market_data.update_book(
                    venue=self.venue,
                    venue_symbol=market.venue_symbol,
                    bids=bids,
                    asks=asks,
                    observed_at=observed_at,
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)

    async def collect(
        self, *, sample_time: datetime, include_hourly_context: bool
    ) -> CollectorBatch:
        try:
            markets_payload = await self._request_json(
                self.MARKETS_URL,
                method="GET",
            )
            details = parse_arcus_markets(markets_payload)
            metadata_observed_at = self._clock()
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return CollectorBatch()

        self._details = details
        self._metadata_observed_at = metadata_observed_at
        self._refresh_cache_metadata(details)

        market_snapshots = await asyncio.gather(
            *(
                self._collect_market(market, details, sample_time)
                for market in self._markets
            )
        )
        configured_details = tuple(
            (market, details[market.venue_symbol])
            for market in self._markets
            if market.venue_symbol in details
        )

        hourly_batch = (
            await self._collect_hourly_from_details(
                sample_time=sample_time,
                configured_details=configured_details,
                metadata_observed_at=metadata_observed_at,
            )
            if include_hourly_context
            else CollectorBatch()
        )

        return CollectorBatch(
            market_snapshots=tuple(
                snapshot for snapshot in market_snapshots if snapshot is not None
            ),
            funding_snapshots=hourly_batch.funding_snapshots,
            hourly_contexts=hourly_batch.hourly_contexts,
        )

    async def collect_hourly(self, *, sample_time: datetime) -> CollectorBatch:
        metadata_observed_at = self._metadata_observed_at
        if metadata_observed_at is None:
            return CollectorBatch()
        configured_details = tuple(
            (market, self._details[market.venue_symbol])
            for market in self._markets
            if market.venue_symbol in self._details
        )
        return await self._collect_hourly_from_details(
            sample_time=sample_time,
            configured_details=configured_details,
            metadata_observed_at=metadata_observed_at,
        )

    async def _collect_hourly_from_details(
        self,
        *,
        sample_time: datetime,
        configured_details: tuple[tuple[MarketConfig, ArcusMarketDetail], ...],
        metadata_observed_at: datetime,
    ) -> CollectorBatch:
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
                observed_at=metadata_observed_at,
                venue=self.venue,
                venue_symbol=market.venue_symbol,
                canonical_symbol=market.canonical_symbol,
                open_interest=detail.open_interest,
                volume_24h=detail.volume_24h,
            )
            for market, detail in configured_details
        )
        return CollectorBatch(
            funding_snapshots=funding_snapshots,
            hourly_contexts=hourly_contexts,
        )

    async def _collect_market(
        self,
        market: MarketConfig,
        details: dict[str, ArcusMarketDetail],
        sample_time: datetime,
    ) -> MarketSnapshot | None:
        detail = details.get(market.venue_symbol)
        if detail is None:
            return None
        try:
            payload = await self._request_json(
                f"{self.L2_ORDER_BOOK_URL}/{market.venue_symbol}",
                method="GET",
            )
            bids, asks = parse_arcus_l2_order_book(payload)
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
                index_price=detail.oracle_price,
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
        detail: ArcusMarketDetail,
    ) -> FundingSnapshot | None:
        try:
            payload = await self._request_json(
                self.FUNDING_RATES_URL,
                method="GET",
                params={"market": market.venue_symbol},
            )
            point = parse_arcus_funding_rates(
                payload, expected_market_id=detail.market_id
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
                next_funding_time=detail.next_funding_time,
            )
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return None
