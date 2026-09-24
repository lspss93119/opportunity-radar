from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

from radar.collectors.hyperliquid import (
    HyperliquidOrderBookFeed,
)
from radar.config import MarketConfig
from radar.market_data import LatestMarketData
from radar.vwap import BookLevel

OBSERVED_AT = datetime(2026, 9, 15, 10, 0, 8, tzinfo=UTC)


class FixtureWebSocket:
    def __init__(self, messages: list[dict]) -> None:
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


def subscription_ack(coin: str) -> dict:
    return {
        "channel": "subscriptionResponse",
        "data": {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": coin},
        },
    }


def l2_book_message(
    coin: str,
    *,
    bids: tuple[tuple[float, float], ...],
    asks: tuple[tuple[float, float], ...],
) -> dict:
    return {
        "channel": "l2Book",
        "data": {
            "coin": coin,
            "levels": [
                [{"px": str(price), "sz": str(size), "n": 1} for price, size in bids],
                [{"px": str(price), "sz": str(size), "n": 1} for price, size in asks],
            ],
        },
    }


async def wait_until(predicate) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("predicate was not satisfied")


@pytest.mark.parametrize(
    ("venue", "coin"),
    [("hyperliquid", "BTC"), ("trade_xyz", "xyz:TSLA"), ("entropy", "io:SNDK")],
)
@pytest.mark.asyncio
async def test_hyperliquid_feed_subscribes_and_preserves_exact_coin_identity(
    venue: str, coin: str
):
    websocket = FixtureWebSocket(
        [
            subscription_ack(coin),
            l2_book_message(
                coin,
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            ),
        ]
    )

    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        [coin],
        connect=lambda _url: websocket,
        clock=lambda: OBSERVED_AT,
        venue=venue,
    )
    try:
        await feed.start()
        await wait_until(lambda: feed.snapshot(coin) is not None)
        snapshot = feed.snapshot(coin)
    finally:
        await feed.stop()

    assert websocket.sent == [
        {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": coin},
        }
    ]
    assert snapshot is not None
    assert snapshot.bids == (BookLevel(price=99.0, base_size=1.0),)
    assert snapshot.asks == (BookLevel(price=101.0, base_size=1.0),)
    assert snapshot.observed_at == OBSERVED_AT


@pytest.mark.asyncio
async def test_hyperliquid_complete_snapshot_replaces_book_without_delta_logic():
    websocket = FixtureWebSocket(
        [
            subscription_ack("BTC"),
            subscription_ack("xyz:TSLA"),
            l2_book_message(
                "BTC",
                bids=((98.0, 2.0), (97.0, 3.0)),
                asks=((102.0, 2.0), (103.0, 3.0)),
            ),
            l2_book_message(
                "xyz:TSLA",
                bids=((98.0, 2.0), (97.0, 3.0)),
                asks=((102.0, 2.0), (103.0, 3.0)),
            ),
            l2_book_message(
                "BTC",
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            ),
            l2_book_message(
                "xyz:TSLA",
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            ),
        ]
    )
    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC", "xyz:TSLA"],
        connect=lambda _url: websocket,
        clock=lambda: OBSERVED_AT,
        venue="trade_xyz",
    )
    try:
        await feed.start()
        await wait_until(lambda: feed.snapshot("xyz:TSLA") is not None)

        snapshot = feed.snapshot("xyz:TSLA")
        assert snapshot is not None
        assert snapshot.bids == (BookLevel(price=99.0, base_size=1.0),)
        assert snapshot.asks == (BookLevel(price=101.0, base_size=1.0),)
        assert snapshot.observed_at == OBSERVED_AT
        assert feed.snapshot("BTC") is not None
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_hyperliquid_feed_ignores_unconfigured_coin_without_reconnect_or_invalidation():
    websocket = FixtureWebSocket(
        [
            subscription_ack("BTC"),
            l2_book_message(
                "BTC",
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            ),
        ]
    )
    failures: list[tuple[str, Exception]] = []
    invalidated: list[str] = []
    latest = LatestMarketData()

    def on_book(coin: str, snapshot) -> None:
        latest.update_book(
            venue="hyperliquid",
            venue_symbol=coin,
            bids=snapshot.bids,
            asks=snapshot.asks,
            observed_at=snapshot.observed_at,
        )

    def on_invalidate(coin: str) -> None:
        invalidated.append(coin)
        latest.invalidate(venue="hyperliquid", venue_symbol=coin)

    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=lambda _url: websocket,
        clock=lambda: OBSERVED_AT,
        venue="hyperliquid",
        on_book=on_book,
        on_invalidate=on_invalidate,
        error_handler=lambda venue, error: failures.append((venue, error)),
        reconnect_delay_seconds=1.0,
    )

    market = MarketConfig(
        venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
    )
    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot("BTC") is not None)
        invalidation_count = len(invalidated)
        websocket.push(
            l2_book_message(
                "xyz:TSLA",
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            )
        )
        await wait_until(lambda: bool(failures))
        for _ in range(3):
            await asyncio.sleep(0)

        batch = latest.build_batch(
            [market],
            sample_time=OBSERVED_AT,
            now=OBSERVED_AT,
            stale_after_seconds=30,
        )
        assert [(venue, str(error)) for venue, error in failures] == [
            ("hyperliquid", "websocket coin 'xyz:TSLA' is not configured")
        ]
        assert feed.reconnect_count == 0
        assert len(invalidated) == invalidation_count
        assert feed.snapshot("BTC") is not None
        assert len(batch.market_snapshots) == 1
        assert batch.market_snapshots[0].best_bid == 99.0
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_hyperliquid_reconnect_clears_books_until_fresh_snapshots_arrive():
    first = FixtureWebSocket(
        [
            subscription_ack("BTC"),
            l2_book_message(
                "BTC",
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            ),
        ]
    )
    second = FixtureWebSocket([subscription_ack("BTC")])
    connections = [first, second]

    def connect(_url: str):
        return connections.pop(0) if connections else second

    invalidated: list[str] = []
    latest = LatestMarketData()

    def on_book(coin: str, snapshot) -> None:
        latest.update_book(
            venue="hyperliquid",
            venue_symbol=coin,
            bids=snapshot.bids,
            asks=snapshot.asks,
            observed_at=snapshot.observed_at,
        )

    def on_invalidate(coin: str) -> None:
        invalidated.append(coin)
        latest.invalidate(venue="hyperliquid", venue_symbol=coin)

    market = MarketConfig(
        venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
    )
    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=connect,
        clock=lambda: OBSERVED_AT,
        venue="hyperliquid",
        on_book=on_book,
        on_invalidate=on_invalidate,
        reconnect_delay_seconds=0,
    )
    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot("BTC") is not None)
        assert len(
            latest.build_batch(
                [market],
                sample_time=OBSERVED_AT,
                now=OBSERVED_AT,
                stale_after_seconds=30,
            ).market_snapshots
        ) == 1

        await first.close()
        await wait_until(lambda: feed.snapshot("BTC") is None and len(second.sent) == 1)
        assert latest.build_batch(
            [market],
            sample_time=OBSERVED_AT,
            now=OBSERVED_AT,
            stale_after_seconds=30,
        ).market_snapshots == ()

        second.push(
            l2_book_message(
                "BTC",
                bids=((98.0, 2.0),),
                asks=((102.0, 2.0),),
            )
        )
        await wait_until(lambda: feed.snapshot("BTC") is not None)
        assert feed.snapshot("BTC").bids == (BookLevel(price=98.0, base_size=2.0),)
        republished = latest.build_batch(
            [market],
            sample_time=OBSERVED_AT,
            now=OBSERVED_AT,
            stale_after_seconds=30,
        )
        assert len(republished.market_snapshots) == 1
        assert republished.market_snapshots[0].best_bid == 98.0

        await feed.stop()
        assert latest.build_batch(
            [market],
            sample_time=OBSERVED_AT,
            now=OBSERVED_AT,
            stale_after_seconds=30,
        ).market_snapshots == ()
        assert invalidated
    finally:
        await feed.stop()
