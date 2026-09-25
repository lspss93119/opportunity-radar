from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import websockets

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
from radar.market_data import LatestMarketData
from radar.models import FundingSnapshot, HourlyContext, MarketSnapshot
from radar.vwap import BookLevel, buy_vwap, sell_vwap

UTC = timezone.utc
HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_HEARTBEAT_INTERVAL_SECONDS = 50.0
HYPERLIQUID_RECONNECT_DELAYS_SECONDS = (1.0, 2.0, 4.0, 8.0, 16.0, 30.0)
L2_SILENCE_TIMEOUT_SECONDS = 20.0
LOGGER = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class HyperliquidOrderBookSnapshot:
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    observed_at: datetime


@dataclass
class _HyperliquidSession:
    generation: int
    websocket: Any | None
    started_at: datetime
    started_at_monotonic: float = 0.0
    last_l2_activity_monotonic: float = 0.0
    l2_activity_event: asyncio.Event | None = None
    last_message_at: datetime | None = None
    last_l2book_at: datetime | None = None
    first_l2book_at: datetime | None = None
    all_l2books_at: datetime | None = None
    last_pong_at: datetime | None = None
    pings_sent: int = 0
    pongs_received: int = 0
    healthy: bool = False
    disconnect_reason: str = "unknown"


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


class HyperliquidOrderBookFeed:
    """One persistent complete-snapshot feed for one Hyperliquid domain."""

    def __init__(
        self,
        ws_url: str,
        coins: Sequence[str],
        *,
        connect: Callable[..., Any] = websockets.connect,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        venue: str = "hyperliquid",
        on_book: Callable[[str, HyperliquidOrderBookSnapshot], None] | None = None,
        on_invalidate: Callable[[str], None] | None = None,
        reconnect_delay_seconds: float | None = None,
        error_handler: CollectorErrorHandler | None = None,
        heartbeat_interval_seconds: float = HYPERLIQUID_HEARTBEAT_INTERVAL_SECONDS,
        reconnect_delays: Sequence[float] = HYPERLIQUID_RECONNECT_DELAYS_SECONDS,
        reconnect_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        l2_silence_timeout_seconds: float = L2_SILENCE_TIMEOUT_SECONDS,
    ) -> None:
        if reconnect_delay_seconds is not None and reconnect_delay_seconds < 0:
            raise ValueError("reconnect_delay_seconds must be non-negative")
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        if l2_silence_timeout_seconds <= 0:
            raise ValueError("l2_silence_timeout_seconds must be positive")
        normalized_reconnect_delays = tuple(reconnect_delays)
        if not normalized_reconnect_delays or any(
            delay < 0 for delay in normalized_reconnect_delays
        ):
            raise ValueError("reconnect_delays must contain non-negative values")
        normalized_coins = tuple(coins)
        if any(not isinstance(coin, str) or not coin for coin in normalized_coins):
            raise ValueError("coins must contain non-empty strings")
        self.ws_url = ws_url
        self._coins = tuple(dict.fromkeys(normalized_coins))
        self._coin_set = frozenset(self._coins)
        self._connect = connect
        self._clock = clock
        self._venue = venue
        self._on_book = on_book
        self._on_invalidate = on_invalidate
        self._reconnect_delays = (
            (reconnect_delay_seconds,)
            if reconnect_delay_seconds is not None
            else normalized_reconnect_delays
        )
        self._reconnect_sleep = reconnect_sleep
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._monotonic = monotonic
        self._l2_silence_timeout_seconds = l2_silence_timeout_seconds
        self._error_handler = error_handler
        self._snapshots: dict[str, HyperliquidOrderBookSnapshot] = {}
        self._task: asyncio.Task[None] | None = None
        self._websocket: Any | None = None
        self._stopping = False
        self._session_generation = 0
        self.reconnect_count = 0

    @property
    def coins(self) -> tuple[str, ...]:
        return self._coins

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._clear_snapshots()
        if not self._coins:
            return
        self._task = asyncio.create_task(
            self._run(), name=f"{self._venue}-order-book"
        )

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
        self._clear_snapshots()

    def snapshot(self, coin: str) -> HyperliquidOrderBookSnapshot | None:
        if coin not in self._coin_set:
            return None
        return self._snapshots.get(coin)

    def _notify_invalidate(self, coin: str) -> None:
        if self._on_invalidate is None:
            return
        try:
            self._on_invalidate(coin)
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self._venue, error)

    def _clear_snapshots(self) -> None:
        self._snapshots.clear()
        for coin in self._coins:
            self._notify_invalidate(coin)

    def _invalidate_coin(self, coin: str) -> None:
        self._snapshots.pop(coin, None)
        self._notify_invalidate(coin)

    async def _run(self) -> None:
        backoff_index = 0
        while not self._stopping:
            self._clear_snapshots()
            session_healthy = await self._run_connection()
            self._clear_snapshots()
            if self._stopping:
                return
            delay = self._reconnect_delays[
                min(backoff_index, len(self._reconnect_delays) - 1)
            ]
            self.reconnect_count += 1
            LOGGER.info(
                "hyperliquid websocket reconnect scheduled venue=%s "
                "reconnect_count=%d delay_seconds=%.1f healthy=%s",
                self._venue,
                self.reconnect_count,
                delay,
                session_healthy,
            )
            await self._reconnect_sleep(delay)
            if session_healthy:
                backoff_index = 0
            else:
                backoff_index = min(backoff_index + 1, len(self._reconnect_delays) - 1)

    async def _run_connection(self) -> bool:
        self._session_generation += 1
        session = _HyperliquidSession(
            generation=self._session_generation,
            websocket=None,
            started_at=self._clock(),
        )
        receiver_task: asyncio.Task[None] | None = None
        heartbeat_task: asyncio.Task[None] | None = None
        l2_watchdog_task: asyncio.Task[None] | None = None
        try:
            async with self._connect(self.ws_url, ping_interval=None) as websocket:
                session.websocket = websocket
                session.started_at_monotonic = self._monotonic()
                session.last_l2_activity_monotonic = session.started_at_monotonic
                session.l2_activity_event = asyncio.Event()
                self._websocket = websocket
                LOGGER.info(
                    "hyperliquid websocket connected venue=%s session=%d",
                    self._venue,
                    session.generation,
                )
                for coin in self._coins:
                    await websocket.send(
                        json.dumps(
                            {
                                "method": "subscribe",
                                "subscription": {"type": "l2Book", "coin": coin},
                            }
                        )
                    )

                receiver_task = asyncio.create_task(
                    self._receive_messages(session),
                    name=f"{self._venue}-order-book-receiver",
                )
                heartbeat_task = asyncio.create_task(
                    self._send_heartbeats(session),
                    name=f"{self._venue}-websocket-heartbeat",
                )
                l2_watchdog_task = asyncio.create_task(
                    self._watch_l2_silence(session),
                    name=f"{self._venue}-l2-silence-watchdog",
                )
                done, _ = await asyncio.wait(
                    (receiver_task, heartbeat_task, l2_watchdog_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if receiver_task in done:
                    session.disconnect_reason = "receive_loop_ended"
                if heartbeat_task in done:
                    session.disconnect_reason = "heartbeat_task_ended"
                if l2_watchdog_task in done:
                    session.disconnect_reason = "l2_silence_timeout"
                for task in done:
                    task.result()
        except asyncio.CancelledError:
            session.disconnect_reason = "shutdown"
            raise
        except Exception as error:  # noqa: BLE001
            session.disconnect_reason = f"{type(error).__name__}: {error}"
            report_collector_error(self._error_handler, self._venue, error)
        finally:
            tasks = tuple(
                task
                for task in (receiver_task, heartbeat_task, l2_watchdog_task)
                if task is not None
            )
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if self._websocket is session.websocket:
                self._websocket = None
            now = self._clock()
            session_seconds = max(
                0.0, (now - session.started_at).total_seconds()
            )
            last_message_age = None
            if session.last_message_at is not None:
                last_message_age = max(
                    0.0, (now - session.last_message_at).total_seconds()
                )
            last_l2book_age = None
            if session.last_l2book_at is not None:
                last_l2book_age = max(
                    0.0, (now - session.last_l2book_at).total_seconds()
                )
            LOGGER.info(
                "hyperliquid websocket disconnected venue=%s session=%d "
                "reason=%s session_seconds=%.3f last_message_age_seconds=%s "
                "last_l2book_age_seconds=%s pings_sent=%d pongs_received=%d "
                "healthy=%s",
                self._venue,
                session.generation,
                session.disconnect_reason,
                session_seconds,
                last_message_age,
                last_l2book_age,
                session.pings_sent,
                session.pongs_received,
                session.healthy,
            )
        return session.healthy

    async def _receive_messages(self, session: _HyperliquidSession) -> None:
        websocket = session.websocket
        if websocket is None:
            raise RuntimeError("Hyperliquid websocket session is not connected")
        async for raw_message in websocket:
            received_at = self._clock()
            session.last_message_at = received_at
            await self._handle_message(session, raw_message, received_at)

    async def _send_heartbeats(self, session: _HyperliquidSession) -> None:
        websocket = session.websocket
        if websocket is None:
            raise RuntimeError("Hyperliquid websocket session is not connected")
        while not self._stopping:
            await asyncio.sleep(self._heartbeat_interval_seconds)
            if self._stopping:
                return
            await websocket.send(json.dumps({"method": "ping"}))
            session.pings_sent += 1
            LOGGER.debug(
                "hyperliquid websocket heartbeat sent venue=%s session=%d",
                self._venue,
                session.generation,
            )

    async def _watch_l2_silence(self, session: _HyperliquidSession) -> None:
        websocket = session.websocket
        activity_event = session.l2_activity_event
        if websocket is None:
            raise RuntimeError("Hyperliquid l2 watchdog session is not initialized")
        if activity_event is None:
            raise RuntimeError("Hyperliquid l2 watchdog event is not initialized")
        while not self._stopping:
            remaining = self._l2_silence_timeout_seconds - (
                self._monotonic() - session.last_l2_activity_monotonic
            )
            if remaining <= 0:
                if (
                    self._websocket is not websocket
                    or self._session_generation != session.generation
                ):
                    return
                LOGGER.warning(
                    "hyperliquid websocket l2 silence timeout venue=%s "
                    "session=%d elapsed_seconds=%.3f",
                    self._venue,
                    session.generation,
                    self._monotonic() - session.last_l2_activity_monotonic,
                )
                assert websocket is not None
                await websocket.close()
                return
            activity_event.clear()
            remaining = self._l2_silence_timeout_seconds - (
                self._monotonic() - session.last_l2_activity_monotonic
            )
            if remaining <= 0:
                continue
            try:
                await asyncio.wait_for(activity_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                continue

    def _mark_session_healthy(
        self, session: _HyperliquidSession, *, source: str
    ) -> None:
        if session.healthy:
            return
        session.healthy = True
        LOGGER.info(
            "hyperliquid websocket session healthy venue=%s session=%d source=%s",
            self._venue,
            session.generation,
            source,
        )

    async def _handle_message(
        self,
        session: _HyperliquidSession,
        raw_message: object,
        received_at: datetime,
    ) -> None:
        websocket = session.websocket
        if websocket is None:
            raise RuntimeError("Hyperliquid websocket session is not connected")
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

        channel = message.get("channel")
        if channel == "subscriptionResponse":
            return
        if message.get("method") == "ping":
            await websocket.send(json.dumps({"method": "pong"}))
            return
        if channel == "pong":
            session.pongs_received += 1
            session.last_pong_at = received_at
            self._mark_session_healthy(session, source="pong")
            LOGGER.debug(
                "hyperliquid websocket heartbeat pong received venue=%s session=%d",
                self._venue,
                session.generation,
            )
            return
        if channel == "heartbeat":
            return
        if channel != "l2Book":
            return

        data = message.get("data")
        if not isinstance(data, dict):
            raise ValueError("l2Book websocket data must be an object")
        coin = data.get("coin")
        if not isinstance(coin, str) or coin not in self._coin_set:
            report_collector_error(
                self._error_handler,
                self._venue,
                ValueError(f"websocket coin {coin!r} is not configured"),
            )
            return
        try:
            bids, asks = parse_hyperliquid_l2_book(data, expected_coin=coin)
        except Exception as error:  # noqa: BLE001
            self._invalidate_coin(coin)
            report_collector_error(self._error_handler, self._venue, error)
            return

        snapshot = HyperliquidOrderBookSnapshot(
            bids=tuple(bids),
            asks=tuple(asks),
            observed_at=received_at,
        )
        self._snapshots[coin] = snapshot
        session.last_l2_activity_monotonic = self._monotonic()
        if session.l2_activity_event is not None:
            session.l2_activity_event.set()
        session.last_l2book_at = received_at
        if session.first_l2book_at is None:
            session.first_l2book_at = received_at
            first_l2book_seconds = max(
                0.0, (received_at - session.started_at).total_seconds()
            )
            LOGGER.info(
                "hyperliquid websocket first l2Book venue=%s session=%d "
                "after_seconds=%.3f",
                self._venue,
                session.generation,
                first_l2book_seconds,
            )
        if session.all_l2books_at is None and all(
            configured_coin in self._snapshots
            for configured_coin in self._coins
        ):
            session.all_l2books_at = received_at
            all_l2books_seconds = max(
                0.0, (received_at - session.started_at).total_seconds()
            )
            LOGGER.info(
                "hyperliquid websocket all l2Books fresh venue=%s session=%d "
                "after_seconds=%.3f count=%d",
                self._venue,
                session.generation,
                all_l2books_seconds,
                len(self._coins),
            )
        self._mark_session_healthy(session, source="l2Book")
        if self._on_book is None:
            return
        try:
            self._on_book(coin, snapshot)
        except Exception as error:  # noqa: BLE001
            self._invalidate_coin(coin)
            report_collector_error(self._error_handler, self._venue, error)


class HyperliquidCollector:
    venue = "hyperliquid"
    INFO_URL = "https://api.hyperliquid.xyz/info"
    WS_URL = HYPERLIQUID_WS_URL
    METADATA_REFRESH_SECONDS = 60.0

    def __init__(
        self,
        markets: list[MarketConfig] | tuple[MarketConfig, ...],
        *,
        venue: str = "hyperliquid",
        dex: str | None = None,
        request_json=default_request_json,
        clock=lambda: datetime.now(UTC),
        error_handler: CollectorErrorHandler | None = None,
        websocket_connect: Callable[..., Any] = websockets.connect,
        ws_url: str | None = None,
        latest_market_data: LatestMarketData | None = None,
        metadata_refresh_seconds: float = METADATA_REFRESH_SECONDS,
    ) -> None:
        if metadata_refresh_seconds < 0:
            raise ValueError("metadata_refresh_seconds must be non-negative")
        self.venue = venue
        self.dex = dex
        self._markets = markets_for_venue(markets, self.venue)
        self._request_json = request_json
        self._clock = clock
        self._error_handler = error_handler
        self.ws_url = self.WS_URL if ws_url is None else ws_url
        self._latest_market_data = latest_market_data
        self._metadata_refresh_seconds = metadata_refresh_seconds
        self._contexts: dict[str, HyperliquidAssetContext] = {}
        self._metadata_observed_at: datetime | None = None
        self._metadata_task: asyncio.Task[None] | None = None
        self._started = False
        self._order_book_feed = HyperliquidOrderBookFeed(
            self.ws_url,
            tuple(market.venue_symbol for market in self._markets),
            connect=websocket_connect,
            clock=clock,
            venue=self.venue,
            on_book=self._publish_book,
            on_invalidate=self._invalidate_book,
            error_handler=error_handler,
        )

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self._order_book_feed.start()
        await self._refresh_metadata_once()
        self._metadata_task = asyncio.create_task(
            self._run_metadata_refresh(), name=f"{self.venue}-metadata"
        )

    async def stop(self) -> None:
        self._started = False
        metadata_task = self._metadata_task
        if metadata_task is not None:
            metadata_task.cancel()
            await asyncio.gather(metadata_task, return_exceptions=True)
        self._metadata_task = None
        await self._order_book_feed.stop()

    async def _refresh_metadata_once(self) -> bool:
        try:
            metadata_request: dict[str, object] = {"type": "metaAndAssetCtxs"}
            if self.dex is not None:
                metadata_request["dex"] = self.dex
            context_payload = await self._request_json(
                self.INFO_URL,
                method="POST",
                json_body=metadata_request,
            )
            contexts = parse_hyperliquid_meta_and_asset_ctxs(context_payload)
            metadata_observed_at = self._clock()
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return False

        self._contexts = contexts
        self._metadata_observed_at = metadata_observed_at
        if self._latest_market_data is not None:
            for market in self._markets:
                context = contexts.get(market.venue_symbol)
                try:
                    self._latest_market_data.update_metadata(
                        venue=self.venue,
                        venue_symbol=market.venue_symbol,
                        mark_price=None if context is None else context.mark_price,
                        index_price=None if context is None else context.index_price,
                    )
                except Exception as error:  # noqa: BLE001
                    report_collector_error(self._error_handler, self.venue, error)
        return True

    async def _run_metadata_refresh(self) -> None:
        while self._started:
            await asyncio.sleep(self._metadata_refresh_seconds)
            if not self._started:
                return
            await self._refresh_metadata_once()

    def _publish_book(
        self, coin: str, snapshot: HyperliquidOrderBookSnapshot
    ) -> None:
        if self._latest_market_data is None:
            return
        for market in self._markets:
            if market.venue_symbol != coin:
                continue
            try:
                self._latest_market_data.update_book(
                    venue=self.venue,
                    venue_symbol=market.venue_symbol,
                    bids=snapshot.bids,
                    asks=snapshot.asks,
                    observed_at=snapshot.observed_at,
                )
            except Exception:  # noqa: BLE001
                self._latest_market_data.invalidate(
                    venue=self.venue,
                    venue_symbol=market.venue_symbol,
                )
                raise

    def _invalidate_book(self, coin: str) -> None:
        if self._latest_market_data is None:
            return
        for market in self._markets:
            if market.venue_symbol == coin:
                self._latest_market_data.invalidate(
                    venue=self.venue,
                    venue_symbol=market.venue_symbol,
                )

    async def collect(
        self, *, sample_time: datetime, include_hourly_context: bool
    ) -> CollectorBatch:
        contexts = self._contexts

        market_snapshots = await asyncio.gather(
            *(self._collect_market(market, contexts, sample_time) for market in self._markets)
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

    async def _collect_market(
        self,
        market: MarketConfig,
        contexts: dict[str, HyperliquidAssetContext],
        sample_time: datetime,
    ) -> MarketSnapshot | None:
        context = contexts.get(market.venue_symbol)
        try:
            snapshot = self._order_book_feed.snapshot(market.venue_symbol)
            if snapshot is None or not snapshot.bids or not snapshot.asks:
                return None
            bids, asks = snapshot.bids, snapshot.asks
            return MarketSnapshot(
                sample_time=sample_time,
                observed_at=snapshot.observed_at,
                venue=self.venue,
                venue_symbol=market.venue_symbol,
                canonical_symbol=market.canonical_symbol,
                best_bid=bids[0].price,
                best_bid_size=bids[0].base_size,
                best_ask=asks[0].price,
                best_ask_size=asks[0].base_size,
                mark_price=None if context is None else context.mark_price,
                index_price=None if context is None else context.index_price,
                buy_1k_vwap=buy_vwap(list(asks), 1_000),
                sell_1k_vwap=sell_vwap(list(bids), 1_000),
                buy_5k_vwap=buy_vwap(list(asks), 5_000),
                sell_5k_vwap=sell_vwap(list(bids), 5_000),
                buy_10k_vwap=buy_vwap(list(asks), 10_000),
                sell_10k_vwap=sell_vwap(list(bids), 10_000),
            )
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)
            return None

    async def collect_hourly(self, *, sample_time: datetime) -> CollectorBatch:
        metadata_observed_at = self._metadata_observed_at
        if metadata_observed_at is None:
            return CollectorBatch()
        contexts = self._contexts
        configured_contexts = tuple(
            (market, contexts[market.venue_symbol])
            for market in self._markets
            if market.venue_symbol in contexts
        )
        funding_results = await asyncio.gather(
            *(self._collect_funding(market) for market, _ in configured_contexts)
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
                open_interest=context.open_interest,
                volume_24h=context.volume_24h,
            )
            for market, context in configured_contexts
        )
        return CollectorBatch(
            funding_snapshots=funding_snapshots,
            hourly_contexts=hourly_contexts,
        )

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
