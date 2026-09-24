from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

import websockets

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
LIGHTER_ROBINHOOD_BASE_URL = "https://api.rh.lighter.xyz"
LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_ROBINHOOD_WS_URL = "wss://api.rh.lighter.xyz/stream"


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


@dataclass(frozen=True)
class LighterOrderBookSnapshot:
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    observed_at: datetime


def _parse_lighter_ws_level(raw_level: object, side: str) -> tuple[float, float]:
    if not isinstance(raw_level, dict):
        raise ValueError(f"{side} websocket level must be an object")
    price = positive_float(raw_level.get("price"), f"{side} price")
    size = non_negative_float(raw_level.get("size"), f"{side} size")
    return price, size


def _parse_lighter_ws_levels(raw_levels: object, side: str) -> dict[float, float]:
    if not isinstance(raw_levels, list):
        raise ValueError(f"{side} websocket levels must be a list")
    levels: dict[float, float] = {}
    for raw_level in raw_levels:
        price, size = _parse_lighter_ws_level(raw_level, side)
        if size > 0:
            levels[price] = levels.get(price, 0.0) + size
    return levels


def _parse_lighter_ws_nonce(raw_value: object, field_name: str) -> int:
    if isinstance(raw_value, bool) or not isinstance(raw_value, (str, int)):
        raise ValueError(f"{field_name} must be an integer")
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


class LighterOrderBookState:
    """Mutable state for one Lighter order-book channel."""

    def __init__(self, market_id: int) -> None:
        self.market_id = market_id
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._last_nonce: int | None = None
        self._observed_at: datetime | None = None

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self._last_nonce = None
        self._observed_at = None

    def apply_snapshot(self, raw_order_book: object, observed_at: datetime) -> None:
        order_book = _validate_lighter_ws_order_book(raw_order_book)
        self._asks = _parse_lighter_ws_levels(order_book.get("asks"), "ask")
        self._bids = _parse_lighter_ws_levels(order_book.get("bids"), "bid")
        self._last_nonce = _parse_lighter_ws_nonce(
            order_book.get("nonce"), "snapshot nonce"
        )
        self._observed_at = observed_at

    def apply_delta(self, raw_order_book: object, observed_at: datetime) -> None:
        if self._last_nonce is None:
            raise ValueError("order-book delta arrived before snapshot")
        order_book = _validate_lighter_ws_order_book(raw_order_book)
        begin_nonce = _parse_lighter_ws_nonce(
            order_book.get("begin_nonce"), "delta begin_nonce"
        )
        nonce = _parse_lighter_ws_nonce(order_book.get("nonce"), "delta nonce")
        if begin_nonce != self._last_nonce:
            raise ValueError(
                "order-book delta nonce gap: "
                f"expected {self._last_nonce}, got {begin_nonce}"
            )
        if nonce < begin_nonce:
            raise ValueError("delta nonce must not precede begin_nonce")
        ask_updates = _parse_lighter_ws_levels_with_zero(
            order_book.get("asks"), "ask"
        )
        bid_updates = _parse_lighter_ws_levels_with_zero(
            order_book.get("bids"), "bid"
        )
        self._apply_updates(self._asks, ask_updates)
        self._apply_updates(self._bids, bid_updates)
        self._last_nonce = nonce
        self._observed_at = observed_at

    @staticmethod
    def _apply_updates(
        levels: dict[float, float], updates: list[tuple[float, float]]
    ) -> None:
        for price, size in updates:
            if size == 0:
                levels.pop(price, None)
            else:
                levels[price] = size

    def snapshot(self) -> LighterOrderBookSnapshot | None:
        if self._last_nonce is None or self._observed_at is None:
            return None
        bids = tuple(
            BookLevel(price=price, base_size=size)
            for price, size in sorted(self._bids.items(), reverse=True)
        )
        asks = tuple(
            BookLevel(price=price, base_size=size)
            for price, size in sorted(self._asks.items())
        )
        return LighterOrderBookSnapshot(
            bids=bids,
            asks=asks,
            observed_at=self._observed_at,
        )


def _validate_lighter_ws_order_book(raw_order_book: object) -> dict[str, Any]:
    if not isinstance(raw_order_book, dict):
        raise ValueError("websocket order_book must be an object")
    if raw_order_book.get("code") != 0:
        raise ValueError("websocket order_book code must be 0")
    return raw_order_book


def _parse_lighter_ws_levels_with_zero(
    raw_levels: object, side: str
) -> list[tuple[float, float]]:
    if not isinstance(raw_levels, list):
        raise ValueError(f"{side} websocket levels must be a list")
    return [_parse_lighter_ws_level(raw_level, side) for raw_level in raw_levels]


class LighterOrderBookFeed:
    """One persistent public WebSocket feed for a Lighter deployment."""

    def __init__(
        self,
        ws_url: str,
        market_ids: Sequence[int],
        *,
        on_book: Callable[[int, LighterOrderBookSnapshot], None] | None = None,
        on_invalidate: Callable[[int], None] | None = None,
        connect: Callable[[str], Any] = websockets.connect,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        error_handler: CollectorErrorHandler | None = None,
        venue: str = "lighter",
        reconnect_delay_seconds: float = 1.0,
    ) -> None:
        if reconnect_delay_seconds < 0:
            raise ValueError("reconnect_delay_seconds must be non-negative")
        self.ws_url = ws_url
        self._market_ids = tuple(sorted(set(market_ids)))
        self._connect = connect
        self._clock = clock
        self._error_handler = error_handler
        self._venue = venue
        self._on_book = on_book
        self._on_invalidate = on_invalidate
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._states: dict[int, LighterOrderBookState] = {
            market_id: LighterOrderBookState(market_id)
            for market_id in self._market_ids
        }
        self._task: asyncio.Task[None] | None = None
        self._websocket: Any | None = None
        self._stopping = False
        self.reconnect_count = 0

    @property
    def market_ids(self) -> tuple[int, ...]:
        return self._market_ids

    async def set_market_ids(self, market_ids: Sequence[int]) -> None:
        new_market_ids = tuple(sorted(set(market_ids)))
        if new_market_ids == self._market_ids:
            return
        self._clear_states()
        self._market_ids = new_market_ids
        self._states = {
            market_id: self._states.get(market_id, LighterOrderBookState(market_id))
            for market_id in new_market_ids
        }
        if self._websocket is not None:
            await self._websocket.close()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._clear_states()
        if not self._market_ids:
            return
        self._task = asyncio.create_task(self._run(), name=f"{self._venue}-order-book")

    async def stop(self) -> None:
        self._stopping = True
        if self._websocket is not None:
            await self._websocket.close()
        task = self._task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._task = None
        self._websocket = None
        self._clear_states()

    def snapshot(self, market_id: int) -> LighterOrderBookSnapshot | None:
        state = self._states.get(market_id)
        return None if state is None else state.snapshot()

    def _clear_states(self) -> None:
        for state in self._states.values():
            state.clear()
            if self._on_invalidate is not None:
                self._on_invalidate(state.market_id)

    async def _run(self) -> None:
        while not self._stopping:
            self._clear_states()
            try:
                async with self._connect(self.ws_url) as websocket:
                    self._websocket = websocket
                    async for raw_message in websocket:
                        await self._handle_message(websocket, raw_message)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001
                report_collector_error(self._error_handler, self._venue, error)
            finally:
                self._websocket = None
                self._clear_states()
            if self._stopping:
                return
            self.reconnect_count += 1
            await asyncio.sleep(self._reconnect_delay_seconds)

    async def _handle_message(self, websocket: Any, raw_message: object) -> None:
        received_at = self._clock()
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode()
        if not isinstance(raw_message, str):
            raise ValueError("websocket message must be text")
        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            raise ValueError("websocket message must be valid JSON") from exc
        if not isinstance(message, dict):
            raise ValueError("websocket message must be an object")

        message_type = message.get("type")
        if message_type == "connected":
            for market_id in self._market_ids:
                await websocket.send(
                    json.dumps(
                        {"type": "subscribe", "channel": f"order_book/{market_id}"}
                    )
                )
            return
        if message_type == "ping":
            await websocket.send(json.dumps({"type": "pong"}))
            return
        if message_type not in {"subscribed/order_book", "update/order_book"}:
            raise ValueError(f"unexpected websocket message type: {message_type}")

        market_id = self._market_id_from_channel(message.get("channel"))
        state = self._states.get(market_id)
        if state is None:
            raise ValueError(f"websocket market {market_id} is not configured")
        order_book = message.get("order_book")
        if message_type == "subscribed/order_book":
            state.apply_snapshot(order_book, received_at)
        else:
            state.apply_delta(order_book, received_at)
        snapshot = state.snapshot()
        if snapshot is None:
            raise ValueError("valid websocket order_book did not produce a snapshot")
        if self._on_book is not None:
            self._on_book(market_id, snapshot)

    @staticmethod
    def _market_id_from_channel(channel: object) -> int:
        if not isinstance(channel, str) or not channel.startswith("order_book:"):
            raise ValueError("websocket order-book channel is invalid")
        return _parse_lighter_ws_nonce(channel.removeprefix("order_book:"), "market_id")


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
        direction = raw_funding.get("direction")
        if direction not in ("long", "short"):
            raise ValueError("funding direction must be long or short")
        raw_rate = finite_float(raw_funding.get("rate"), "funding rate")
        if raw_rate < 0:
            raise ValueError("funding rate must be non-negative")
        funding_rate = raw_rate / 100.0
        if direction == "short":
            funding_rate = -funding_rate
        points.append(
            LighterFundingPoint(
                effective_time=datetime.fromtimestamp(timestamp, tz=UTC),
                funding_rate=funding_rate,
            )
        )
    return max(points, key=lambda point: point.effective_time) if points else None


class LighterCollector:
    venue = "lighter"
    BASE_URL = "https://mainnet.zklighter.elliot.ai"
    WS_URL = LIGHTER_WS_URL
    ORDER_BOOK_DETAILS_URL = f"{BASE_URL}/api/v1/orderBookDetails"
    ORDER_BOOK_ORDERS_URL = f"{BASE_URL}/api/v1/orderBookOrders"
    FUNDINGS_URL = f"{BASE_URL}/api/v1/fundings"
    ORDER_BOOK_LIMIT = 250
    METADATA_REFRESH_SECONDS = 60.0

    def __init__(
        self,
        markets: list[MarketConfig] | tuple[MarketConfig, ...],
        *,
        venue: str = "lighter",
        base_url: str | None = None,
        request_json=default_request_json,
        clock=lambda: datetime.now(UTC),
        error_handler: CollectorErrorHandler | None = None,
        websocket_connect: Callable[[str], Any] = websockets.connect,
        ws_url: str | None = None,
        latest_market_data: LatestMarketData | None = None,
        metadata_refresh_seconds: float = METADATA_REFRESH_SECONDS,
    ) -> None:
        if metadata_refresh_seconds < 0:
            raise ValueError("metadata_refresh_seconds must be non-negative")
        self.venue = venue
        self.base_url = (self.BASE_URL if base_url is None else base_url).rstrip("/")
        if ws_url is None:
            ws_url = (
                LIGHTER_ROBINHOOD_WS_URL
                if self.venue == "lighter_robinhood"
                else LIGHTER_WS_URL
            )
        self.ws_url = ws_url
        self.order_book_details_url = f"{self.base_url}/api/v1/orderBookDetails"
        self.order_book_orders_url = f"{self.base_url}/api/v1/orderBookOrders"
        self.fundings_url = f"{self.base_url}/api/v1/fundings"
        self._markets = markets_for_venue(markets, self.venue)
        self._request_json = request_json
        self._clock = clock
        self._error_handler = error_handler
        self._latest_market_data = latest_market_data
        self._metadata_refresh_seconds = metadata_refresh_seconds
        self._details: dict[str, LighterMarketDetail] = {}
        self._details_observed_at: datetime | None = None
        self._market_id_to_markets: dict[int, tuple[MarketConfig, ...]] = {}
        self._metadata_task: asyncio.Task[None] | None = None
        self._feed_started = False
        self._order_book_feed = LighterOrderBookFeed(
            self.ws_url,
            (),
            connect=websocket_connect,
            clock=clock,
            error_handler=error_handler,
            venue=self.venue,
            on_book=self._publish_book,
            on_invalidate=self._invalidate_book,
        )
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self._refresh_metadata_once()
        self._metadata_task = asyncio.create_task(
            self._run_metadata_refresh(),
            name=f"{self.venue}-metadata",
        )

    async def stop(self) -> None:
        self._started = False
        metadata_task = self._metadata_task
        if metadata_task is not None:
            metadata_task.cancel()
            await asyncio.gather(metadata_task, return_exceptions=True)
        self._metadata_task = None
        await self._order_book_feed.stop()
        self._feed_started = False

    async def _fetch_market_details(
        self,
    ) -> tuple[dict[str, LighterMarketDetail], datetime]:
        details_payload = await self._request_json(
            self.order_book_details_url,
            method="GET",
            params={"filter": "perp"},
        )
        return parse_lighter_order_book_details(details_payload), self._clock()

    def _configured_market_map(
        self, details: dict[str, LighterMarketDetail]
    ) -> dict[int, tuple[MarketConfig, ...]]:
        market_map: dict[int, list[MarketConfig]] = {}
        for market in self._markets:
            detail = details.get(market.venue_symbol)
            if detail is not None:
                market_map.setdefault(detail.market_id, []).append(market)
        return {
            market_id: tuple(markets)
            for market_id, markets in market_map.items()
        }

    async def _ensure_order_book_feed(
        self, details: dict[str, LighterMarketDetail]
    ) -> None:
        market_map = self._configured_market_map(details)
        market_ids = tuple(sorted(market_map))
        await self._order_book_feed.set_market_ids(market_ids)
        self._market_id_to_markets = market_map
        if market_ids and not self._feed_started:
            await self._order_book_feed.start()
            self._feed_started = True
        elif not market_ids and self._feed_started:
            await self._order_book_feed.stop()
            self._feed_started = False

    async def _refresh_metadata_once(self) -> bool:
        try:
            details, details_observed_at = await self._fetch_market_details()
            await self._ensure_order_book_feed(details)
            self._details = details
            self._details_observed_at = details_observed_at
            if self._latest_market_data is not None:
                for market in self._markets:
                    detail = details.get(market.venue_symbol)
                    if detail is None:
                        continue
                    self._latest_market_data.update_metadata(
                        venue=self.venue,
                        venue_symbol=market.venue_symbol,
                        mark_price=detail.mark_price,
                        index_price=detail.index_price,
                    )
            return True
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return False

    async def _run_metadata_refresh(self) -> None:
        while self._started:
            await asyncio.sleep(self._metadata_refresh_seconds)
            if not self._started:
                return
            await self._refresh_metadata_once()

    def _publish_book(
        self, market_id: int, snapshot: LighterOrderBookSnapshot
    ) -> None:
        if self._latest_market_data is None:
            return
        for market in self._market_id_to_markets.get(market_id, ()):
            try:
                self._latest_market_data.update_book(
                    venue=self.venue,
                    venue_symbol=market.venue_symbol,
                    bids=snapshot.bids,
                    asks=snapshot.asks,
                    observed_at=snapshot.observed_at,
                )
            except Exception as error:  # noqa: BLE001
                report_collector_error(self._error_handler, self.venue, error)

    def _invalidate_book(self, market_id: int) -> None:
        if self._latest_market_data is None:
            return
        for market in self._market_id_to_markets.get(market_id, ()):
            self._latest_market_data.invalidate(
                venue=self.venue,
                venue_symbol=market.venue_symbol,
            )

    async def collect(
        self, *, sample_time: datetime, include_hourly_context: bool
    ) -> CollectorBatch:
        details = self._details

        market_snapshots = await asyncio.gather(
            *(
                self._collect_market(market, details, sample_time)
                for market in self._markets
            )
        )
        funding_snapshots: tuple[FundingSnapshot, ...] = ()
        hourly_contexts: tuple[HourlyContext, ...] = ()
        if include_hourly_context:
            hourly_batch = await self.collect_hourly(sample_time=sample_time)
            funding_snapshots = hourly_batch.funding_snapshots
            hourly_contexts = hourly_batch.hourly_contexts

        return CollectorBatch(
            market_snapshots=tuple(
                snapshot for snapshot in market_snapshots if snapshot is not None
            ),
            funding_snapshots=funding_snapshots,
            hourly_contexts=hourly_contexts,
        )

    async def collect_hourly(self, *, sample_time: datetime) -> CollectorBatch:
        details_observed_at = self._details_observed_at
        if details_observed_at is None:
            return CollectorBatch()
        configured_details = tuple(
            (market, self._details[market.venue_symbol])
            for market in self._markets
            if market.venue_symbol in self._details
        )
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
            book = self._order_book_feed.snapshot(detail.market_id)
            if book is None or not book.bids or not book.asks:
                return None
            bids, asks = list(book.bids), list(book.asks)
            observed_at = book.observed_at
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
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return None

    async def _collect_funding(
        self,
        market: MarketConfig,
        detail: LighterMarketDetail,
    ) -> FundingSnapshot | None:
        end_timestamp = int(self._clock().timestamp())
        try:
            payload = await self._request_json(
                self.fundings_url,
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
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return None
