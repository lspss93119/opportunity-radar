from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from radar.collectors.backpack import (
    BackpackOrderBookFeed,
    BackpackOrderBookState,
)
from radar.config import MarketConfig
from radar.market_data import LatestMarketData
from radar.vwap import BookLevel

OBSERVED_AT = datetime(2026, 9, 15, 10, 0, 8, tzinfo=UTC)


def snapshot_payload(
    last_update_id: int,
    *,
    bids: Sequence[tuple[float, float]] = ((99.0, 1.0),),
    asks: Sequence[tuple[float, float]] = ((101.0, 1.0),),
) -> dict:
    return {
        "lastUpdateId": str(last_update_id),
        "bids": [[str(price), str(size)] for price, size in bids],
        "asks": [[str(price), str(size)] for price, size in asks],
    }


def depth_update(
    symbol: str,
    *,
    first_update_id: int,
    final_update_id: int,
    bids: Sequence[tuple[float, float]] = (),
    asks: Sequence[tuple[float, float]] = (),
) -> dict:
    return {
        "stream": f"depth.{symbol}",
        "data": {
            "e": "depth",
            "s": symbol,
            "U": first_update_id,
            "u": final_update_id,
            "b": [[str(price), str(size)] for price, size in bids],
            "a": [[str(price), str(size)] for price, size in asks],
        },
    }


class FixtureWebSocket:
    def __init__(self, messages: Sequence[dict] = ()) -> None:
        self.sent: list[dict] = []
        self.closed = False
        self._queue: asyncio.Queue[str | None] = asyncio.Queue()
        for message in messages:
            self.push(message)

    def push(self, message: dict) -> None:
        self._queue.put_nowait(json.dumps(message))

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._queue.put_nowait(None)

    async def __aenter__(self) -> "FixtureWebSocket":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    def __aiter__(self) -> "FixtureWebSocket":
        return self

    async def __anext__(self) -> str:
        message = await self._queue.get()
        if message is None:
            raise StopAsyncIteration
        return message


async def wait_until(predicate) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("predicate was not satisfied")


def test_backpack_update_requires_bridge_then_contiguous_ids():
    snapshot_bids = (BookLevel(99.0, 1.0),)
    snapshot_asks = (BookLevel(101.0, 1.0),)
    observed_at = OBSERVED_AT
    state = BackpackOrderBookState()

    state.seed(
        last_update_id=10,
        bids=snapshot_bids,
        asks=snapshot_asks,
        observed_at=observed_at,
    )

    assert state.ready is False
    assert (
        state.apply_update(
            first_update_id=9,
            final_update_id=10,
            bids=(),
            asks=(),
            observed_at=observed_at,
        )
        == "buffered"
    )
    assert (
        state.apply_update(
            first_update_id=10,
            final_update_id=11,
            bids=((99.0, 2.0),),
            asks=((101.0, 0.0),),
            observed_at=observed_at,
        )
        == "ready"
    )
    assert state.snapshot() is not None
    assert state.snapshot().bids == (BookLevel(99.0, 2.0),)
    assert state.snapshot().asks == ()
    assert (
        state.apply_update(
            first_update_id=12,
            final_update_id=12,
            bids=((98.0, 3.0),),
            asks=((102.0, 4.0),),
            observed_at=observed_at,
        )
        == "updated"
    )
    assert state.snapshot().bids == (
        BookLevel(99.0, 2.0),
        BookLevel(98.0, 3.0),
    )
    assert (
        state.apply_update(
            first_update_id=14,
            final_update_id=14,
            bids=(),
            asks=(),
            observed_at=observed_at,
        )
        == "gap"
    )
    assert state.ready is False
    assert state.snapshot() is None


def test_backpack_order_book_state_keeps_symbols_isolated():
    first = BackpackOrderBookState()
    second = BackpackOrderBookState()
    first.seed(
        last_update_id=10,
        bids=(BookLevel(99.0, 1.0),),
        asks=(BookLevel(101.0, 1.0),),
        observed_at=OBSERVED_AT,
    )
    second.seed(
        last_update_id=20,
        bids=(BookLevel(199.0, 1.0),),
        asks=(BookLevel(201.0, 1.0),),
        observed_at=OBSERVED_AT,
    )

    assert first.apply_update(
        first_update_id=10,
        final_update_id=11,
        bids=((99.0, 2.0),),
        asks=(),
        observed_at=OBSERVED_AT,
    ) == "ready"
    assert second.apply_update(
        first_update_id=20,
        final_update_id=21,
        bids=((199.0, 3.0),),
        asks=(),
        observed_at=OBSERVED_AT,
    ) == "ready"

    assert first.snapshot().bids == (BookLevel(99.0, 2.0),)
    assert second.snapshot().bids == (BookLevel(199.0, 3.0),)


@pytest.mark.asyncio
async def test_backpack_feed_subscribes_once_buffers_updates_and_publishes_cache():
    symbol = "SNDK.US_USDC_PERP"
    websocket = FixtureWebSocket(
        [
            {"id": 1, "result": None},
            depth_update(
                symbol,
                first_update_id=9,
                final_update_id=10,
            ),
            depth_update(
                symbol,
                first_update_id=10,
                final_update_id=11,
                bids=((99.0, 2.0),),
                asks=((101.0, 0.0), (102.0, 1.0)),
            ),
            depth_update(
                symbol,
                first_update_id=12,
                final_update_id=12,
                bids=((98.0, 3.0),),
                asks=((102.0, 4.0),),
            ),
        ]
    )
    latest = LatestMarketData()
    published: list[str] = []
    invalidated: list[str] = []

    async def snapshot_loader(requested_symbol: str):
        assert requested_symbol == symbol
        return snapshot_payload(10)

    feed = BackpackOrderBookFeed(
        [symbol],
        connect=lambda _url: websocket,
        snapshot_loader=snapshot_loader,
        clock=lambda: OBSERVED_AT,
        on_book=lambda received_symbol, snapshot: (
            published.append(received_symbol),
            latest.update_book(
                venue="backpack",
                venue_symbol=received_symbol,
                bids=snapshot.bids,
                asks=snapshot.asks,
                observed_at=snapshot.observed_at,
            ),
        ),
        on_invalidate=lambda received_symbol: (
            invalidated.append(received_symbol),
            latest.invalidate(venue="backpack", venue_symbol=received_symbol),
        ),
        reconnect_delay_seconds=1.0,
    )

    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot(symbol) is not None)
        snapshot = feed.snapshot(symbol)
        assert snapshot is not None
        assert snapshot.bids == (
            BookLevel(99.0, 2.0),
            BookLevel(98.0, 3.0),
        )
        assert snapshot.asks == (BookLevel(102.0, 4.0),)
        assert snapshot.observed_at == OBSERVED_AT
        assert websocket.sent == [
            {"method": "SUBSCRIBE", "params": [f"depth.{symbol}"]}
        ]
        assert published
        assert invalidated
        batch = latest.build_batch(
            [MarketConfig(venue="backpack", venue_symbol=symbol, canonical_symbol="SNDK")],
            sample_time=OBSERVED_AT,
            now=OBSERVED_AT,
            stale_after_seconds=30,
        )
        assert len(batch.market_snapshots) == 1
        assert batch.market_snapshots[0].best_bid == 99.0
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_backpack_feed_gap_rebuilds_only_affected_symbol():
    btc = "BTC_USDC_PERP"
    eth = "ETH_USDC_PERP"
    websocket = FixtureWebSocket(
        [
            {"id": 1, "result": None},
            depth_update(btc, first_update_id=10, final_update_id=11),
            depth_update(eth, first_update_id=20, final_update_id=21),
            depth_update(btc, first_update_id=13, final_update_id=13),
        ]
    )
    loader_calls: list[str] = []

    async def snapshot_loader(symbol: str):
        loader_calls.append(symbol)
        if symbol == btc and loader_calls.count(symbol) == 1:
            return snapshot_payload(10)
        if symbol == btc:
            return snapshot_payload(12, bids=((98.0, 2.0),), asks=((102.0, 2.0),))
        return snapshot_payload(20, bids=((199.0, 1.0),), asks=((201.0, 1.0),))

    latest = LatestMarketData()
    feed = BackpackOrderBookFeed(
        [btc, eth],
        connect=lambda _url: websocket,
        snapshot_loader=snapshot_loader,
        clock=lambda: OBSERVED_AT,
        on_book=lambda symbol, snapshot: latest.update_book(
            venue="backpack",
            venue_symbol=symbol,
            bids=snapshot.bids,
            asks=snapshot.asks,
            observed_at=snapshot.observed_at,
        ),
        on_invalidate=lambda symbol: latest.invalidate(
            venue="backpack", venue_symbol=symbol
        ),
        reconnect_delay_seconds=1.0,
    )
    markets = [
        MarketConfig(venue="backpack", venue_symbol=btc, canonical_symbol="BTC"),
        MarketConfig(venue="backpack", venue_symbol=eth, canonical_symbol="ETH"),
    ]

    await feed.start()
    try:
        await wait_until(lambda: loader_calls.count(btc) >= 2)
        before_recovery = latest.build_batch(
            markets,
            sample_time=OBSERVED_AT,
            now=OBSERVED_AT,
            stale_after_seconds=30,
        )
        assert {snapshot.canonical_symbol for snapshot in before_recovery.market_snapshots} == {
            "ETH"
        }
        websocket.push(
            depth_update(
                btc,
                first_update_id=12,
                final_update_id=13,
                bids=((98.0, 2.0),),
                asks=((102.0, 2.0),),
            )
        )
        await wait_until(lambda: feed.snapshot(btc) is not None)
        assert feed.reconnect_count == 0
        assert feed.snapshot(eth) is not None
        assert latest.build_batch(
            markets,
            sample_time=OBSERVED_AT,
            now=OBSERVED_AT,
            stale_after_seconds=30,
        ).market_snapshots[0].canonical_symbol == "BTC"
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_backpack_feed_recovers_from_forward_gap_after_seed():
    symbol = "BTC_USDC_PERP"
    websocket = FixtureWebSocket(
        [
            {"id": 1, "result": None},
            depth_update(symbol, first_update_id=12, final_update_id=12),
        ]
    )
    loader_calls: list[int] = []
    errors: list[Exception] = []

    async def snapshot_loader(_symbol: str):
        loader_calls.append(len(loader_calls) + 1)
        return snapshot_payload(10 if len(loader_calls) == 1 else 12)

    feed = BackpackOrderBookFeed(
        [symbol],
        connect=lambda _url: websocket,
        snapshot_loader=snapshot_loader,
        clock=lambda: OBSERVED_AT,
        on_book=lambda _symbol, _snapshot: None,
        error_handler=lambda _venue, error: errors.append(error),
        reconnect_delay_seconds=1.0,
    )

    await feed.start()
    try:
        await wait_until(lambda: len(loader_calls) >= 2)
        assert feed.snapshot(symbol) is None

        websocket.push(
            depth_update(
                symbol,
                first_update_id=12,
                final_update_id=13,
                bids=((99.0, 2.0),),
                asks=((101.0, 2.0),),
            )
        )
        await wait_until(lambda: feed.snapshot(symbol) is not None)
        assert feed.snapshot(symbol).bids == (BookLevel(99.0, 2.0),)
        assert any("gap" in str(error) for error in errors)
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_backpack_feed_reconnect_clears_cache_and_resubscribes():
    symbol = "BTC_USDC_PERP"
    first = FixtureWebSocket(
        [
            {"id": 1, "result": None},
            depth_update(symbol, first_update_id=10, final_update_id=11),
        ]
    )
    second = FixtureWebSocket([{ "id": 2, "result": None }])
    connections = [first, second]

    def connect(_url: str):
        return connections.pop(0) if connections else second

    async def snapshot_loader(_symbol: str):
        return snapshot_payload(10)

    latest = LatestMarketData()
    market = MarketConfig(venue="backpack", venue_symbol=symbol, canonical_symbol="BTC")
    feed = BackpackOrderBookFeed(
        [symbol],
        connect=connect,
        snapshot_loader=snapshot_loader,
        clock=lambda: OBSERVED_AT,
        on_book=lambda received_symbol, snapshot: latest.update_book(
            venue="backpack",
            venue_symbol=received_symbol,
            bids=snapshot.bids,
            asks=snapshot.asks,
            observed_at=snapshot.observed_at,
        ),
        on_invalidate=lambda received_symbol: latest.invalidate(
            venue="backpack", venue_symbol=received_symbol
        ),
        reconnect_delay_seconds=0,
    )

    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot(symbol) is not None)
        assert len(
            latest.build_batch(
                [market],
                sample_time=OBSERVED_AT,
                now=OBSERVED_AT,
                stale_after_seconds=30,
            ).market_snapshots
        ) == 1

        await first.close()
        await wait_until(lambda: len(second.sent) == 1)
        assert feed.snapshot(symbol) is None
        assert latest.build_batch(
            [market],
            sample_time=OBSERVED_AT,
            now=OBSERVED_AT,
            stale_after_seconds=30,
        ).market_snapshots == ()

        second.push(
            depth_update(symbol, first_update_id=10, final_update_id=11)
        )
        await wait_until(lambda: feed.snapshot(symbol) is not None)
        assert feed.reconnect_count >= 1
    finally:
        await feed.stop()

    assert second.closed
    assert feed.snapshot(symbol) is None
    assert not feed.rebuild_tasks
