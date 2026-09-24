from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

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
BACKPACK_WS_URL = "wss://ws.backpack.exchange"


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


@dataclass(frozen=True)
class BackpackRestSnapshot:
    last_update_id: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass(frozen=True)
class BackpackOrderBookSnapshot:
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    observed_at: datetime


@dataclass(frozen=True)
class _BackpackDepthUpdate:
    first_update_id: int
    final_update_id: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]
    observed_at: datetime


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


def _non_negative_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{field_name} must be a non-negative integer")
    try:
        result = int(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a non-negative integer") from exc
    if result < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return result


def parse_backpack_snapshot(payload: object) -> BackpackRestSnapshot:
    if not isinstance(payload, dict):
        raise ValueError("depth response must be an object")
    bids, asks = parse_backpack_depth(payload)
    return BackpackRestSnapshot(
        last_update_id=_non_negative_integer(
            payload.get("lastUpdateId"), "lastUpdateId"
        ),
        bids=tuple(bids),
        asks=tuple(asks),
    )


def _parse_backpack_update_levels(
    raw_levels: object, side: str
) -> tuple[tuple[float, float], ...]:
    if not isinstance(raw_levels, list):
        raise ValueError(f"{side} websocket levels must be a list")

    levels: list[tuple[float, float]] = []
    for raw_level in raw_levels:
        if isinstance(raw_level, BookLevel):
            levels.append((raw_level.price, raw_level.base_size))
            continue
        if not isinstance(raw_level, (list, tuple)) or len(raw_level) != 2:
            raise ValueError(f"{side} websocket level must contain price and size")
        price = positive_float(raw_level[0], f"{side} price")
        size = non_negative_float(raw_level[1], f"{side} size")
        levels.append((price, size))
    return tuple(levels)


def _normalise_backpack_updates(
    levels: Sequence[object], side: str
) -> tuple[tuple[float, float], ...]:
    return _parse_backpack_update_levels(list(levels), side)


def _require_observed_at(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware UTC")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError("observed_at must be timezone-aware UTC")
    return value


class BackpackOrderBookState:
    """Mutable snapshot-plus-incremental state for one Backpack symbol."""

    def __init__(self) -> None:
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._last_update_id: int | None = None
        self._observed_at: datetime | None = None
        self._ready = False

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def last_update_id(self) -> int | None:
        return self._last_update_id

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self._last_update_id = None
        self._observed_at = None
        self._ready = False

    def seed(
        self,
        *,
        last_update_id: int,
        bids: Sequence[BookLevel],
        asks: Sequence[BookLevel],
        observed_at: datetime,
    ) -> None:
        self._last_update_id = _non_negative_integer(last_update_id, "last_update_id")
        self._bids = self._seed_levels(bids, "bid")
        self._asks = self._seed_levels(asks, "ask")
        self._observed_at = _require_observed_at(observed_at)
        self._ready = False

    def apply_update(
        self,
        *,
        first_update_id: int,
        final_update_id: int,
        bids: Sequence[object],
        asks: Sequence[object],
        observed_at: datetime,
    ) -> Literal["buffered", "ready", "updated", "gap"]:
        first_update_id = _non_negative_integer(first_update_id, "first_update_id")
        final_update_id = _non_negative_integer(final_update_id, "final_update_id")
        if final_update_id < first_update_id:
            raise ValueError("final_update_id must not precede first_update_id")
        parsed_bids = _normalise_backpack_updates(bids, "bid")
        parsed_asks = _normalise_backpack_updates(asks, "ask")
        observed_at = _require_observed_at(observed_at)

        if self._last_update_id is None:
            return "buffered"
        if final_update_id <= self._last_update_id:
            return "buffered"

        expected = self._last_update_id + 1
        if not self._ready:
            if first_update_id > expected or final_update_id < expected:
                return "buffered"
            self._apply_levels(parsed_bids, parsed_asks)
            self._ready = True
            self._last_update_id = final_update_id
            self._observed_at = observed_at
            return "ready"

        if first_update_id != expected:
            self.clear()
            return "gap"

        self._apply_levels(parsed_bids, parsed_asks)
        self._last_update_id = final_update_id
        self._observed_at = observed_at
        return "updated"

    def snapshot(self) -> BackpackOrderBookSnapshot | None:
        if not self._ready or self._observed_at is None:
            return None
        return BackpackOrderBookSnapshot(
            bids=tuple(
                BookLevel(price=price, base_size=size)
                for price, size in sorted(self._bids.items(), reverse=True)
            ),
            asks=tuple(
                BookLevel(price=price, base_size=size)
                for price, size in sorted(self._asks.items())
            ),
            observed_at=self._observed_at,
        )

    @staticmethod
    def _seed_levels(
        levels: Sequence[BookLevel], side: str
    ) -> dict[float, float]:
        parsed = _normalise_backpack_updates(list(levels), side)
        return {price: size for price, size in parsed if size > 0}

    def _apply_levels(
        self,
        bids: Sequence[tuple[float, float]],
        asks: Sequence[tuple[float, float]],
    ) -> None:
        for levels, updates in ((self._bids, bids), (self._asks, asks)):
            for price, size in updates:
                if size == 0:
                    levels.pop(price, None)
                else:
                    levels[price] = size


class BackpackOrderBookFeed:
    """One persistent public WebSocket feed for configured Backpack symbols."""

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        connect: Callable[[str], Any],
        snapshot_loader: Callable[[str], Any],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        on_book: Callable[[str, BackpackOrderBookSnapshot], None] | None = None,
        on_invalidate: Callable[[str], None] | None = None,
        error_handler: CollectorErrorHandler | None = None,
        ws_url: str = BACKPACK_WS_URL,
        reconnect_delay_seconds: float = 1.0,
    ) -> None:
        if reconnect_delay_seconds < 0:
            raise ValueError("reconnect_delay_seconds must be non-negative")
        normalized_symbols = tuple(symbols)
        if any(not isinstance(symbol, str) or not symbol for symbol in normalized_symbols):
            raise ValueError("symbols must contain non-empty strings")
        self.ws_url = ws_url
        self._symbols = tuple(dict.fromkeys(normalized_symbols))
        self._symbol_set = frozenset(self._symbols)
        self._connect = connect
        self._snapshot_loader = snapshot_loader
        self._clock = clock
        self._on_book = on_book
        self._on_invalidate = on_invalidate
        self._error_handler = error_handler
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._states = {symbol: BackpackOrderBookState() for symbol in self._symbols}
        self._pending: dict[str, list[_BackpackDepthUpdate]] = {
            symbol: [] for symbol in self._symbols
        }
        self._rebuild_tasks: dict[str, asyncio.Task[None]] = {}
        self._rebuild_again: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._websocket: Any | None = None
        self._stopping = False
        self.reconnect_count = 0

    @property
    def symbols(self) -> tuple[str, ...]:
        return self._symbols

    @property
    def rebuild_tasks(self) -> dict[str, asyncio.Task[None]]:
        return dict(self._rebuild_tasks)

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._clear_all()
        if not self._symbols:
            return
        self._task = asyncio.create_task(
            self._run(), name="backpack-order-book"
        )

    async def stop(self) -> None:
        self._stopping = True
        if self._websocket is not None:
            await self._websocket.close()
        task = self._task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._cancel_rebuild_tasks()
        self._task = None
        self._websocket = None
        self._clear_all()

    def snapshot(self, symbol: str) -> BackpackOrderBookSnapshot | None:
        state = self._states.get(symbol)
        return None if state is None else state.snapshot()

    def _notify_invalidate(self, symbol: str) -> None:
        if self._on_invalidate is None:
            return
        try:
            self._on_invalidate(symbol)
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, "backpack", error)

    def _clear_all(self) -> None:
        for symbol, state in self._states.items():
            state.clear()
            self._pending[symbol].clear()
            self._notify_invalidate(symbol)

    def _invalidate_symbol(self, symbol: str) -> None:
        self._states[symbol].clear()
        self._pending[symbol].clear()
        self._notify_invalidate(symbol)

    async def _cancel_rebuild_tasks(self) -> None:
        self._rebuild_again.clear()
        tasks = tuple(self._rebuild_tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._rebuild_tasks.clear()

    def _schedule_rebuild(self, symbol: str) -> None:
        existing = self._rebuild_tasks.get(symbol)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(
            self._rebuild(symbol), name=f"backpack-rebuild-{symbol}"
        )
        self._rebuild_tasks[symbol] = task

        def forget(done: asyncio.Task[None]) -> None:
            if self._rebuild_tasks.get(symbol) is done:
                self._rebuild_tasks.pop(symbol, None)
            if symbol in self._rebuild_again:
                self._rebuild_again.discard(symbol)
                if not self._stopping:
                    self._schedule_rebuild(symbol)

        task.add_done_callback(forget)

    async def _run(self) -> None:
        while not self._stopping:
            self._clear_all()
            try:
                async with self._connect(self.ws_url) as websocket:
                    self._websocket = websocket
                    await websocket.send(
                        json.dumps(
                            {
                                "method": "SUBSCRIBE",
                                "params": [f"depth.{symbol}" for symbol in self._symbols],
                            }
                        )
                    )
                    for symbol in self._symbols:
                        self._schedule_rebuild(symbol)
                    async for raw_message in websocket:
                        await self._handle_message(raw_message)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001
                report_collector_error(self._error_handler, "backpack", error)
            finally:
                self._websocket = None
                await self._cancel_rebuild_tasks()
                self._clear_all()
            if self._stopping:
                return
            self.reconnect_count += 1
            await asyncio.sleep(self._reconnect_delay_seconds)

    async def _handle_message(self, raw_message: object) -> None:
        if isinstance(raw_message, bytes):
            try:
                raw_message = raw_message.decode()
            except UnicodeDecodeError as exc:
                report_collector_error(self._error_handler, "backpack", exc)
                return
        if not isinstance(raw_message, str):
            report_collector_error(
                self._error_handler,
                "backpack",
                ValueError("websocket message must be text"),
            )
            return
        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            report_collector_error(self._error_handler, "backpack", exc)
            return
        if not isinstance(message, dict):
            report_collector_error(
                self._error_handler,
                "backpack",
                ValueError("websocket message must be an object"),
            )
            return

        stream = message.get("stream")
        if not isinstance(stream, str) or not stream.startswith("depth."):
            return
        symbol = stream.removeprefix("depth.")
        if symbol not in self._symbol_set:
            report_collector_error(
                self._error_handler,
                "backpack",
                ValueError(f"websocket symbol {symbol!r} is not configured"),
            )
            return
        try:
            update = self._parse_update(message, symbol)
        except Exception as error:  # noqa: BLE001
            self._fail_symbol(symbol, error)
            return
        self._receive_update(symbol, update)

    @staticmethod
    def _parse_update(message: dict[str, object], symbol: str) -> _BackpackDepthUpdate:
        raw_data = message.get("data")
        if not isinstance(raw_data, dict):
            raise ValueError("depth websocket data must be an object")
        if raw_data.get("e") != "depth":
            raise ValueError("depth websocket event type is invalid")
        if raw_data.get("s") != symbol:
            raise ValueError("depth websocket symbol does not match stream")
        return _BackpackDepthUpdate(
            first_update_id=_non_negative_integer(raw_data.get("U"), "U"),
            final_update_id=_non_negative_integer(raw_data.get("u"), "u"),
            bids=_parse_backpack_update_levels(raw_data.get("b"), "bid"),
            asks=_parse_backpack_update_levels(raw_data.get("a"), "ask"),
            observed_at=datetime.now(UTC),
        )

    def _receive_update(self, symbol: str, update: _BackpackDepthUpdate) -> None:
        update = _BackpackDepthUpdate(
            first_update_id=update.first_update_id,
            final_update_id=update.final_update_id,
            bids=update.bids,
            asks=update.asks,
            observed_at=self._clock(),
        )
        rebuild = self._rebuild_tasks.get(symbol)
        state = self._states[symbol]
        if (rebuild is not None and not rebuild.done()) or state.last_update_id is None:
            self._pending[symbol].append(update)
            if rebuild is None or rebuild.done():
                self._schedule_rebuild(symbol)
            return
        try:
            result = state.apply_update(
                first_update_id=update.first_update_id,
                final_update_id=update.final_update_id,
                bids=update.bids,
                asks=update.asks,
                observed_at=update.observed_at,
            )
        except Exception as error:  # noqa: BLE001
            self._fail_symbol(symbol, error)
            return
        if result == "gap":
            self._fail_symbol(
                symbol, ValueError(f"depth sequence gap for {symbol}")
            )
            return
        if result in {"ready", "updated"}:
            self._publish(symbol)

    async def _rebuild(self, symbol: str) -> None:
        while not self._stopping:
            try:
                raw_snapshot = self._snapshot_loader(symbol)
                if inspect.isawaitable(raw_snapshot):
                    raw_snapshot = await raw_snapshot
                snapshot = self._coerce_snapshot(raw_snapshot)
                self._states[symbol].seed(
                    last_update_id=snapshot.last_update_id,
                    bids=snapshot.bids,
                    asks=snapshot.asks,
                    observed_at=self._clock(),
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001
                self._invalidate_symbol(symbol)
                report_collector_error(self._error_handler, "backpack", error)
                return

            pending = self._pending[symbol]
            self._pending[symbol] = []
            gap = False
            for update in pending:
                try:
                    result = self._states[symbol].apply_update(
                        first_update_id=update.first_update_id,
                        final_update_id=update.final_update_id,
                        bids=update.bids,
                        asks=update.asks,
                        observed_at=update.observed_at,
                    )
                except Exception as error:  # noqa: BLE001
                    self._fail_symbol(symbol, error)
                    return
                if result == "gap":
                    gap = True
                    break
                if result in {"ready", "updated"}:
                    self._publish(symbol)
            if not gap:
                return
            self._invalidate_symbol(symbol)
            report_collector_error(
                self._error_handler,
                "backpack",
                ValueError(f"depth sequence gap for {symbol}"),
            )

    @staticmethod
    def _coerce_snapshot(raw_snapshot: object) -> BackpackRestSnapshot:
        if isinstance(raw_snapshot, BackpackRestSnapshot):
            return raw_snapshot
        if isinstance(raw_snapshot, dict):
            return parse_backpack_snapshot(raw_snapshot)
        if isinstance(raw_snapshot, tuple) and len(raw_snapshot) == 3:
            last_update_id, bids, asks = raw_snapshot
            if not isinstance(bids, Sequence) or isinstance(bids, (str, bytes)):
                raise ValueError("snapshot bids must be a sequence")
            if not isinstance(asks, Sequence) or isinstance(asks, (str, bytes)):
                raise ValueError("snapshot asks must be a sequence")
            return BackpackRestSnapshot(
                last_update_id=_non_negative_integer(
                    last_update_id, "last_update_id"
                ),
                bids=tuple(
                    BookLevel(price=price, base_size=size)
                    for price, size in _normalise_backpack_updates(bids, "bid")
                    if size > 0
                ),
                asks=tuple(
                    BookLevel(price=price, base_size=size)
                    for price, size in _normalise_backpack_updates(asks, "ask")
                    if size > 0
                ),
            )
        raise ValueError("snapshot loader returned an invalid snapshot")

    def _publish(self, symbol: str) -> None:
        snapshot = self._states[symbol].snapshot()
        if snapshot is None or self._on_book is None:
            return
        try:
            self._on_book(symbol, snapshot)
        except Exception as error:  # noqa: BLE001
            self._fail_symbol(symbol, error)

    def _fail_symbol(self, symbol: str, error: Exception) -> None:
        self._invalidate_symbol(symbol)
        report_collector_error(self._error_handler, "backpack", error)
        existing = self._rebuild_tasks.get(symbol)
        if existing is not None and not existing.done():
            self._rebuild_again.add(symbol)
        else:
            self._schedule_rebuild(symbol)


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
    WS_URL = BACKPACK_WS_URL
    MARKETS_URL = f"{BASE_URL}/api/v1/markets"
    DEPTH_URL = f"{BASE_URL}/api/v1/depth"
    MARK_PRICES_URL = f"{BASE_URL}/api/v1/markPrices"
    OPEN_INTEREST_URL = f"{BASE_URL}/api/v1/openInterest"
    FUNDING_RATES_URL = f"{BASE_URL}/api/v1/fundingRates"
    TICKERS_URL = f"{BASE_URL}/api/v1/tickers"
    DEPTH_LIMIT = 1000
    METADATA_REFRESH_SECONDS = 60.0

    def __init__(
        self,
        markets: list[MarketConfig] | tuple[MarketConfig, ...],
        *,
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
        self._markets = markets_for_venue(markets, self.venue)
        self._request_json = request_json
        self._clock = clock
        self._error_handler = error_handler
        self.ws_url = self.WS_URL if ws_url is None else ws_url
        self._latest_market_data = latest_market_data
        self._metadata_refresh_seconds = metadata_refresh_seconds
        self._details: dict[str, BackpackMarketDetail] = {}
        self._metadata_observed_at: datetime | None = None
        self._mark_prices: dict[str, BackpackMarkPrice] = {}
        self._open_interest: dict[str, float] = {}
        self._tickers: dict[str, float | None] = {}
        self._metadata_task: asyncio.Task[None] | None = None
        self._started = False
        self._order_book_feed = BackpackOrderBookFeed(
            tuple(market.venue_symbol for market in self._markets),
            connect=websocket_connect,
            snapshot_loader=self._load_depth_snapshot,
            clock=clock,
            error_handler=error_handler,
            ws_url=self.ws_url,
            on_book=self._publish_book,
            on_invalidate=self._invalidate_book,
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

    async def _load_depth_snapshot(self, symbol: str) -> BackpackRestSnapshot:
        payload = await self._request_json(
            self.DEPTH_URL,
            method="GET",
            params={"symbol": symbol, "limit": self.DEPTH_LIMIT},
        )
        return parse_backpack_snapshot(payload)

    async def _refresh_metadata_once(self) -> bool:
        markets_succeeded = False
        try:
            markets_payload = await self._request_json(
                self.MARKETS_URL,
                method="GET",
                params={"marketType": "PERP"},
            )
            self._details = parse_backpack_markets(markets_payload)
            self._metadata_observed_at = self._clock()
            markets_succeeded = True
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)

        try:
            mark_prices_payload = await self._request_json(
                self.MARK_PRICES_URL,
                method="GET",
                params={"marketType": "PERP"},
            )
            self._mark_prices = parse_backpack_mark_prices(mark_prices_payload)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)

        try:
            open_interest_payload = await self._request_json(
                self.OPEN_INTEREST_URL,
                method="GET",
            )
            self._open_interest = parse_backpack_open_interest(open_interest_payload)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)

        try:
            tickers_payload = await self._request_json(
                self.TICKERS_URL,
                method="GET",
                params={"interval": "1d"},
            )
            self._tickers = parse_backpack_tickers(tickers_payload)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            report_collector_error(self._error_handler, self.venue, error)

        self._refresh_cache_metadata()
        self._publish_cached_books()
        return markets_succeeded

    def _refresh_cache_metadata(self) -> None:
        if self._latest_market_data is None:
            return
        for market in self._markets:
            if market.venue_symbol not in self._details:
                self._invalidate_book(market.venue_symbol)
                continue
            mark_price = self._mark_prices.get(market.venue_symbol)
            try:
                self._latest_market_data.update_metadata(
                    venue=self.venue,
                    venue_symbol=market.venue_symbol,
                    mark_price=None if mark_price is None else mark_price.mark_price,
                    index_price=None if mark_price is None else mark_price.index_price,
                )
            except Exception as error:  # noqa: BLE001
                report_collector_error(self._error_handler, self.venue, error)
                self._invalidate_book(market.venue_symbol)

    def _publish_cached_books(self) -> None:
        for market in self._markets:
            if market.venue_symbol not in self._details:
                continue
            snapshot = self._order_book_feed.snapshot(market.venue_symbol)
            if snapshot is not None:
                self._publish_book(market.venue_symbol, snapshot)

    async def _run_metadata_refresh(self) -> None:
        while self._started:
            await asyncio.sleep(self._metadata_refresh_seconds)
            if not self._started:
                return
            await self._refresh_metadata_once()

    async def collect(
        self, *, sample_time: datetime, include_hourly_context: bool
    ) -> CollectorBatch:
        market_results = await asyncio.gather(
            *(self._collect_market(market, sample_time) for market in self._markets)
        )
        funding_snapshots: tuple[FundingSnapshot, ...] = ()
        hourly_contexts: tuple[HourlyContext, ...] = ()
        if include_hourly_context:
            hourly_batch = await self.collect_hourly(sample_time=sample_time)
            funding_snapshots = hourly_batch.funding_snapshots
            hourly_contexts = hourly_batch.hourly_contexts

        return CollectorBatch(
            market_snapshots=tuple(
                snapshot for snapshot in market_results if snapshot is not None
            ),
            funding_snapshots=funding_snapshots,
            hourly_contexts=hourly_contexts,
        )

    async def collect_hourly(self, *, sample_time: datetime) -> CollectorBatch:
        metadata_observed_at = self._metadata_observed_at
        if metadata_observed_at is None:
            return CollectorBatch()
        configured_markets = tuple(
            market
            for market in self._markets
            if market.venue_symbol in self._details
        )
        funding_results = await asyncio.gather(
            *(
                self._collect_funding(
                    market,
                    self._mark_prices.get(market.venue_symbol),
                )
                for market in configured_markets
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
                open_interest=self._open_interest.get(market.venue_symbol),
                volume_24h=self._tickers.get(market.venue_symbol),
            )
            for market in configured_markets
        )
        return CollectorBatch(
            funding_snapshots=funding_snapshots,
            hourly_contexts=hourly_contexts,
        )

    async def _collect_market(
        self,
        market: MarketConfig,
        sample_time: datetime,
    ) -> MarketSnapshot | None:
        if market.venue_symbol not in self._details:
            return None
        try:
            snapshot = self._order_book_feed.snapshot(market.venue_symbol)
            if snapshot is None or not snapshot.bids or not snapshot.asks:
                return None
            bids, asks = snapshot.bids, snapshot.asks
            mark_price = self._mark_prices.get(market.venue_symbol)
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
                mark_price=None if mark_price is None else mark_price.mark_price,
                index_price=None if mark_price is None else mark_price.index_price,
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

    def _publish_book(
        self, symbol: str, snapshot: BackpackOrderBookSnapshot
    ) -> None:
        if self._latest_market_data is None:
            return
        if symbol not in self._details:
            return
        for market in self._markets:
            if market.venue_symbol != symbol:
                continue
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
                self._invalidate_book(symbol)
                raise

    def _invalidate_book(self, symbol: str) -> None:
        if self._latest_market_data is None:
            return
        for market in self._markets:
            if market.venue_symbol == symbol:
                self._latest_market_data.invalidate(
                    venue=self.venue,
                    venue_symbol=market.venue_symbol,
                )

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
