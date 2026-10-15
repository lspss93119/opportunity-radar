from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Sequence

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
ARCUS_L2_CONCURRENCY = 4
ARCUS_WS_URL = "wss://api.arcus.xyz/v1/ws"
ARCUS_WS_CHANNEL = "l2Orderbook"
ARCUS_WS_DEFAULT_LEVELS = 20
ARCUS_WS_DEFAULT_SILENCE_SECONDS = 5.0
ARCUS_WS_DEFAULT_RECONNECT_DELAY_SECONDS = 1.0
LOGGER = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class ArcusWsSnapshot:
    market: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    observed_at: datetime
    venue_timestamp_raw: object | None
    last_sequence_id: int | None
    global_sequence_id: int | None


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
    bids = _parse_arcus_side(payload.get("bids"), "bid")
    asks = _parse_arcus_side(payload.get("asks"), "ask")
    if bids[0].price >= asks[0].price:
        raise ValueError("book must not be crossed or locked")
    return bids, asks


def _optional_sequence(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer")
    if isinstance(value, int):
        sequence = value
    elif isinstance(value, str) and value.isdigit():
        sequence = int(value)
    else:
        raise ValueError(f"{field_name} must be an integer")
    if sequence < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return sequence


def parse_arcus_ws_message(
    payload: object,
    configured_markets: frozenset[str],
    *,
    observed_at: datetime,
) -> ArcusWsSnapshot | None:
    """Parse one Arcus full-book message without changing local book state."""
    if not isinstance(payload, dict):
        raise ValueError("Arcus websocket message must be an object")
    if payload.get("channel") != ARCUS_WS_CHANNEL:
        return None
    message_type = payload.get("type")
    if message_type not in {"subscribed", "channel_data"}:
        return None
    market = payload.get("id")
    if not isinstance(market, str) or market not in configured_markets:
        return None
    contents = payload.get("contents")
    if contents is None:
        return None
    if not isinstance(contents, dict):
        raise ValueError("Arcus websocket contents must be an object")
    bids, asks = parse_arcus_l2_order_book(contents)
    return ArcusWsSnapshot(
        market=market,
        bids=tuple(bids),
        asks=tuple(asks),
        observed_at=observed_at,
        venue_timestamp_raw=contents.get("timestamp"),
        last_sequence_id=_optional_sequence(
            contents.get("lastSequenceId"), "lastSequenceId"
        ),
        global_sequence_id=_optional_sequence(
            contents.get("globalSequenceId"), "globalSequenceId"
        ),
    )


class ArcusWebSocketFeed:
    """Maintain complete Arcus `l2Orderbook` snapshots in memory."""

    def __init__(
        self,
        ws_url: str,
        markets: Sequence[str],
        *,
        n_levels: int = ARCUS_WS_DEFAULT_LEVELS,
        connect: Callable[..., Any] = websockets.connect,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        on_book: Callable[[str, ArcusWsSnapshot], None] | None = None,
        on_invalidate: Callable[[str], None] | None = None,
        error_handler: CollectorErrorHandler | None = None,
        venue: str = "arcus",
        silence_timeout_seconds: float = ARCUS_WS_DEFAULT_SILENCE_SECONDS,
        reconnect_delay_seconds: float = ARCUS_WS_DEFAULT_RECONNECT_DELAY_SECONDS,
    ) -> None:
        if not isinstance(ws_url, str) or not ws_url:
            raise ValueError("ws_url must be non-empty")
        if isinstance(n_levels, bool) or n_levels <= 0 or n_levels > 100:
            raise ValueError("n_levels must be between 1 and 100")
        if silence_timeout_seconds <= 0:
            raise ValueError("silence_timeout_seconds must be positive")
        if reconnect_delay_seconds < 0:
            raise ValueError("reconnect_delay_seconds must be non-negative")
        normalized_markets = tuple(dict.fromkeys(markets))
        if not normalized_markets or any(
            not isinstance(market, str) or not market for market in normalized_markets
        ):
            raise ValueError("markets must contain non-empty symbols")
        self.ws_url = ws_url
        self.markets = normalized_markets
        self._configured_markets = frozenset(normalized_markets)
        self.n_levels = n_levels
        self._connect = connect
        self._clock = clock
        self._monotonic = monotonic
        self._on_book = on_book
        self._on_invalidate = on_invalidate
        self._error_handler = error_handler
        self._venue = venue
        self._silence_timeout_seconds = silence_timeout_seconds
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._snapshots: dict[str, ArcusWsSnapshot] = {}
        self._last_updates: dict[str, float] = {}
        self._last_sequences: dict[str, tuple[int | None, int | None]] = {}
        self._task: asyncio.Task[None] | None = None
        self._socket: Any | None = None
        self._started = False
        self.reconnect_count = 0
        self.ignored_sequence_count = 0
        self.message_count = 0
        self.accepted_snapshot_count = 0
        self.last_published_snapshot: ArcusWsSnapshot | None = None

    def snapshot(self, market: str) -> ArcusWsSnapshot | None:
        return self._snapshots.get(market)

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._started = True
        self._task = asyncio.create_task(
            self._run(), name=f"{self._venue}-arcus-websocket"
        )

    async def stop(self) -> None:
        self._started = False
        socket = self._socket
        if socket is not None:
            try:
                await socket.close()
            except Exception:  # noqa: BLE001
                LOGGER.debug("Arcus websocket close failed", exc_info=True)
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._task = None
        self._socket = None
        self._invalidate_all()

    async def _run(self) -> None:
        while self._started:
            self._invalidate_all()
            try:
                async with self._connect(
                    self.ws_url,
                    open_timeout=10,
                    ping_interval=30,
                    max_size=8 * 1024 * 1024,
                ) as socket:
                    self._socket = socket
                    await self._subscribe(socket)
                    await self._receive(socket)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001
                report_collector_error(self._error_handler, self._venue, error)
            finally:
                self._socket = None
                self._invalidate_all()
            if not self._started:
                return
            self.reconnect_count += 1
            await asyncio.sleep(self._reconnect_delay_seconds)

    async def _subscribe(self, socket: Any) -> None:
        for market in self.markets:
            await socket.send(
                json.dumps(
                    {
                        "type": "subscribe",
                        "channel": ARCUS_WS_CHANNEL,
                        "id": market,
                        "nLevels": self.n_levels,
                    }
                )
            )

    async def _receive(self, socket: Any) -> None:
        poll_timeout = min(0.25, self._silence_timeout_seconds / 2)
        while self._started:
            self._invalidate_silent()
            try:
                raw_message = await asyncio.wait_for(
                    socket.recv(), timeout=poll_timeout
                )
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            self.message_count += 1
            payload = json.loads(raw_message)
            self._handle_message(payload)

    def _handle_message(self, payload: object) -> None:
        if not isinstance(payload, dict):
            raise ValueError("Arcus websocket message must be an object")
        if (
            payload.get("channel") == ARCUS_WS_CHANNEL
            and payload.get("type") == "degraded"
        ):
            market = payload.get("id")
            if isinstance(market, str) and market in self._configured_markets:
                self._invalidate_market(market)
            return
        try:
            snapshot = parse_arcus_ws_message(
                payload,
                self._configured_markets,
                observed_at=self._clock(),
            )
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self._venue, error)
            return
        if snapshot is None or not self._accept_sequence(snapshot):
            if snapshot is not None:
                self.ignored_sequence_count += 1
            return
        self._snapshots[snapshot.market] = snapshot
        self._last_updates[snapshot.market] = self._monotonic()
        self._last_sequences[snapshot.market] = (
            snapshot.last_sequence_id,
            snapshot.global_sequence_id,
        )
        self.accepted_snapshot_count += 1
        self.last_published_snapshot = snapshot
        if self._on_book is not None:
            self._on_book(snapshot.market, snapshot)

    def _accept_sequence(self, snapshot: ArcusWsSnapshot) -> bool:
        previous = self._last_sequences.get(snapshot.market)
        if previous is None:
            return True
        previous_last, previous_global = previous
        current_last = snapshot.last_sequence_id
        current_global = snapshot.global_sequence_id
        if (
            previous_last is not None
            and current_last is not None
            and current_last < previous_last
        ) or (
            previous_global is not None
            and current_global is not None
            and current_global < previous_global
        ):
            return False
        if (
            previous_last is not None
            and current_last is not None
            and previous_global is not None
            and current_global is not None
            and current_last == previous_last
            and current_global == previous_global
        ):
            return False
        return True

    def _invalidate_silent(self) -> None:
        now = self._monotonic()
        for market, last_update in tuple(self._last_updates.items()):
            if now - last_update >= self._silence_timeout_seconds:
                self._invalidate_market(market)

    def _invalidate_market(self, market: str) -> None:
        existed = market in self._snapshots
        self._snapshots.pop(market, None)
        self._last_updates.pop(market, None)
        self._last_sequences.pop(market, None)
        if existed and self._on_invalidate is not None:
            self._on_invalidate(market)

    def _invalidate_all(self) -> None:
        for market in tuple(self._snapshots):
            self._invalidate_market(market)
        self._last_updates.clear()
        self._last_sequences.clear()


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
        source: str = "rest",
        ws_url: str = ARCUS_WS_URL,
        ws_levels: int = ARCUS_WS_DEFAULT_LEVELS,
        ws_silence_seconds: float = ARCUS_WS_DEFAULT_SILENCE_SECONDS,
        ws_reconnect_delay_seconds: float = ARCUS_WS_DEFAULT_RECONNECT_DELAY_SECONDS,
        rest_fallback: bool = True,
        ws_connect: Callable[..., Any] = websockets.connect,
    ) -> None:
        if refresh_interval_seconds < 0:
            raise ValueError("refresh_interval_seconds must be non-negative")
        if source not in {"rest", "ws"}:
            raise ValueError("source must be rest or ws")
        self._markets = markets_for_venue(markets, self.venue)
        self._request_json = request_json
        self._clock = clock
        self._error_handler = error_handler
        self._latest_market_data = latest_market_data
        self._refresh_interval_seconds = refresh_interval_seconds
        self.source = source
        self.ws_url = ws_url
        self.ws_levels = ws_levels
        self.ws_silence_seconds = ws_silence_seconds
        self.ws_reconnect_delay_seconds = ws_reconnect_delay_seconds
        self.rest_fallback = rest_fallback
        self._ws_connect = ws_connect
        self._details: dict[str, ArcusMarketDetail] = {}
        self._metadata_observed_at: datetime | None = None
        self._refresh_lock = asyncio.Lock()
        self._refresh_task: asyncio.Task[None] | None = None
        self._ws_feed: ArcusWebSocketFeed | None = None
        self._ws_last_updates: dict[str, datetime] = {}
        self._started = False

    async def start(self) -> None:
        if self._refresh_task is not None and not self._refresh_task.done():
            return
        self._started = True
        if self.source == "ws":
            self._ws_feed = ArcusWebSocketFeed(
                self.ws_url,
                [market.venue_symbol for market in self._markets],
                n_levels=self.ws_levels,
                connect=self._ws_connect,
                clock=self._clock,
                on_book=self._on_ws_book,
                on_invalidate=self._on_ws_invalidate,
                error_handler=self._error_handler,
                silence_timeout_seconds=self.ws_silence_seconds,
                reconnect_delay_seconds=self.ws_reconnect_delay_seconds,
            )
            await self._ws_feed.start()
        self._refresh_task = asyncio.create_task(
            self._run_refresh(),
            name=f"{self.venue}-market-refresh",
        )

    async def stop(self) -> None:
        self._started = False
        ws_feed = self._ws_feed
        self._ws_feed = None
        if ws_feed is not None:
            await ws_feed.stop()
        self._ws_last_updates.clear()
        refresh_task = self._refresh_task
        if refresh_task is not None:
            refresh_task.cancel()
            await asyncio.gather(refresh_task, return_exceptions=True)
        self._refresh_task = None

    def _on_ws_book(self, venue_symbol: str, snapshot: ArcusWsSnapshot) -> None:
        self._ws_last_updates[venue_symbol] = snapshot.observed_at
        if self._latest_market_data is None:
            return
        try:
            self._latest_market_data.update_book(
                venue=self.venue,
                venue_symbol=venue_symbol,
                bids=snapshot.bids,
                asks=snapshot.asks,
                observed_at=snapshot.observed_at,
                live_signal_eligible=True,
            )
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)

    def _on_ws_invalidate(self, venue_symbol: str) -> None:
        self._ws_last_updates.pop(venue_symbol, None)
        if self._latest_market_data is None:
            return
        try:
            self._latest_market_data.invalidate(
                venue=self.venue,
                venue_symbol=venue_symbol,
            )
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)

    def _ws_book_is_fresh(self, venue_symbol: str) -> bool:
        observed_at = self._ws_last_updates.get(venue_symbol)
        if observed_at is None:
            return False
        age_seconds = (self._clock() - observed_at).total_seconds()
        return 0 <= age_seconds <= self.ws_silence_seconds

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
            l2_semaphore = asyncio.Semaphore(ARCUS_L2_CONCURRENCY)
            await asyncio.gather(
                *(
                    self._refresh_market(market, details, l2_semaphore)
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
        l2_semaphore: asyncio.Semaphore,
    ) -> None:
        if market.venue_symbol not in details:
            return
        if self.source == "ws":
            if self._ws_book_is_fresh(market.venue_symbol):
                return
            if not self.rest_fallback:
                return
        try:
            async with l2_semaphore:
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
                    live_signal_eligible=self.source == "rest",
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            contextual_error = RuntimeError(
                f"{self.venue} {market.venue_symbol} L2 request failed: {error}"
            )
            report_collector_error(
                self._error_handler,
                self.venue,
                contextual_error,
            )

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

        if self.source == "ws":
            market_snapshots = self._collect_ws_market_snapshots(
                sample_time=sample_time,
            )
        else:
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

    def _collect_ws_market_snapshots(
        self, *, sample_time: datetime
    ) -> tuple[MarketSnapshot, ...]:
        if self._latest_market_data is None:
            return ()
        batch, diagnostics = self._latest_market_data.build_batch_with_diagnostics(
            self._markets,
            sample_time=sample_time,
            now=self._clock(),
            stale_after_seconds=math.ceil(self.ws_silence_seconds),
        )
        eligible_keys = {
            (diagnostic.venue, diagnostic.venue_symbol, diagnostic.canonical_symbol)
            for diagnostic in diagnostics
            if diagnostic.live_signal_eligible
        }
        return tuple(
            snapshot
            for snapshot in batch.market_snapshots
            if (
                snapshot.venue,
                snapshot.venue_symbol,
                snapshot.canonical_symbol,
            )
            in eligible_keys
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
                params={"market": market.venue_symbol, "limit": 1},
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
