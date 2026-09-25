from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

from radar.collectors.hyperliquid import (
    HyperliquidOrderBookFeed,
    HyperliquidOrderBookSnapshot,
)
from radar.config import MarketConfig
from radar.market_data import LatestMarketData
from radar.vwap import BookLevel

OBSERVED_AT = datetime(2026, 9, 15, 10, 0, 8, tzinfo=UTC)


class FixtureWebSocket:
    def __init__(self, messages: list[dict]) -> None:
        self.sent: list[dict] = []
        self.closed = False
        self.read_calls = 0
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
        self.read_calls += 1
        message = await self._queue.get()
        if message is None:
            raise StopAsyncIteration
        return message


class ClosingWebSocket(FixtureWebSocket):
    async def __anext__(self) -> str:
        self.read_calls += 1
        try:
            message = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            raise StopAsyncIteration
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
    for _ in range(1_000):
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
        connect=lambda _url, **_kwargs: websocket,
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
        connect=lambda _url, **_kwargs: websocket,
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
        connect=lambda _url, **_kwargs: websocket,
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

    def connect(_url: str, **_kwargs: object):
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


@pytest.mark.asyncio
async def test_hyperliquid_feed_disables_rfc_keepalive_on_connect():
    websocket = FixtureWebSocket([])
    connect_calls: list[tuple[str, dict[str, object]]] = []

    def connect(url: str, **kwargs: object):
        connect_calls.append((url, kwargs))
        return websocket

    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=connect,
        heartbeat_interval_seconds=60,
    )
    await feed.start()
    try:
        await wait_until(lambda: bool(connect_calls))
    finally:
        await feed.stop()

    assert connect_calls == [("wss://test.invalid/ws", {"ping_interval": None})]


@pytest.mark.asyncio
async def test_hyperliquid_feed_sends_application_ping_without_a_second_reader():
    websocket = FixtureWebSocket([])
    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=lambda _url, **_kwargs: websocket,
        heartbeat_interval_seconds=0.001,
    )
    await feed.start()
    try:
        await wait_until(
            lambda: websocket.sent.count({"method": "ping"}) >= 2
        )
        await asyncio.sleep(0)
        assert websocket.read_calls == 1
    finally:
        await feed.stop()

    assert websocket.sent.count({"method": "ping"}) >= 2


@pytest.mark.asyncio
async def test_hyperliquid_pong_acknowledges_heartbeat_without_updating_market_time():
    websocket = FixtureWebSocket(
        [
            l2_book_message(
                "BTC",
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            )
        ]
    )
    published: list[HyperliquidOrderBookSnapshot] = []
    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=lambda _url, **_kwargs: websocket,
        clock=lambda: OBSERVED_AT,
        on_book=lambda _coin, snapshot: published.append(snapshot),
        heartbeat_interval_seconds=0.001,
    )
    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot("BTC") is not None)
        await wait_until(lambda: {"method": "ping"} in websocket.sent)
        websocket.push({"channel": "pong"})
        await asyncio.sleep(0)
        snapshot = feed.snapshot("BTC")
        assert snapshot is not None
        assert snapshot.observed_at == OBSERVED_AT
        assert len(published) == 1
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_hyperliquid_missing_pong_does_not_close_invalidate_or_reconnect():
    first = FixtureWebSocket(
        [
            l2_book_message(
                "BTC",
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            )
        ]
    )
    connect_calls: list[dict[str, object]] = []
    invalidated: list[str] = []

    def connect(_url: str, **kwargs: object):
        connect_calls.append(kwargs)
        return first

    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=connect,
        heartbeat_interval_seconds=0.001,
        reconnect_delay_seconds=0,
        on_invalidate=invalidated.append,
    )
    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot("BTC") is not None)
        invalidated.clear()
        await wait_until(
            lambda: first.sent.count({"method": "ping"}) >= 2
        )
        await asyncio.sleep(0.01)
        assert not first.closed
        assert len(connect_calls) == 1
        assert feed.snapshot("BTC") is not None
        assert invalidated == []
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_hyperliquid_l2_silence_watchdog_reconnects_after_timeout():
    first = FixtureWebSocket(
        [
            l2_book_message(
                "BTC",
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            )
        ]
    )
    second = FixtureWebSocket([])
    connections = [first, second]
    second_connected = asyncio.Event()

    def connect(_url: str, **_kwargs: object):
        websocket = connections.pop(0)
        if websocket is second:
            second_connected.set()
        return websocket

    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=connect,
        l2_silence_timeout_seconds=0.01,
        reconnect_delay_seconds=0,
    )
    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot("BTC") is not None)
        await asyncio.wait_for(second_connected.wait(), timeout=1)
        assert first.closed
        assert feed.snapshot("BTC") is None
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_hyperliquid_valid_l2_book_resets_silence_watchdog():
    websocket = FixtureWebSocket(
        [
            l2_book_message(
                "BTC",
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            )
        ]
    )
    published: list[HyperliquidOrderBookSnapshot] = []
    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=lambda _url, **_kwargs: websocket,
        on_book=lambda _coin, snapshot: published.append(snapshot),
        l2_silence_timeout_seconds=0.1,
    )
    await feed.start()
    try:
        await wait_until(lambda: len(published) == 1)
        await asyncio.sleep(0.02)
        websocket.push(
            l2_book_message(
                "BTC",
                bids=((98.0, 2.0),),
                asks=((102.0, 2.0),),
            )
        )
        await wait_until(lambda: len(published) == 2)
        await asyncio.sleep(0.02)
        assert not websocket.closed
    finally:
        await feed.stop()


@pytest.mark.parametrize(
    "message",
    [
        subscription_ack("BTC"),
        {"channel": "pong"},
        l2_book_message(
            "xyz:TSLA",
            bids=((99.0, 1.0),),
            asks=((101.0, 1.0),),
        ),
        {
            "channel": "l2Book",
            "data": {"coin": "BTC", "levels": [[], []]},
        },
    ],
)
@pytest.mark.asyncio
async def test_hyperliquid_non_l2_messages_do_not_reset_silence_watchdog(
    message: dict,
):
    websocket = FixtureWebSocket(
        [
            l2_book_message(
                "BTC",
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            )
        ]
    )
    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=lambda _url, **_kwargs: websocket,
        l2_silence_timeout_seconds=0.01,
    )
    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot("BTC") is not None)
        websocket.push(message)
        await asyncio.wait_for(
            wait_until(lambda: websocket.closed),
            timeout=1,
        )
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_hyperliquid_reconnect_backoff_progresses_and_caps_at_30_seconds():
    connections = [ClosingWebSocket([]) for _ in range(8)]
    delays: list[float] = []

    async def reconnect_sleep(delay: float) -> None:
        delays.append(delay)
        await asyncio.sleep(0)

    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=lambda _url, **_kwargs: connections.pop(0),
        reconnect_sleep=reconnect_sleep,
        heartbeat_interval_seconds=60,
    )
    await feed.start()
    try:
        await wait_until(lambda: len(delays) >= 7)
    finally:
        await feed.stop()

    assert delays[:7] == [1, 2, 4, 8, 16, 30, 30]


@pytest.mark.asyncio
async def test_hyperliquid_backoff_resets_only_after_healthy_market_traffic():
    connections = [
        ClosingWebSocket([]),
        ClosingWebSocket([]),
        ClosingWebSocket(
            [
                l2_book_message(
                    "BTC",
                    bids=((99.0, 1.0),),
                    asks=((101.0, 1.0),),
                )
            ]
        ),
        ClosingWebSocket([]),
    ]
    delays: list[float] = []

    async def reconnect_sleep(delay: float) -> None:
        delays.append(delay)
        await asyncio.sleep(0)

    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=lambda _url, **_kwargs: connections.pop(0),
        reconnect_sleep=reconnect_sleep,
        heartbeat_interval_seconds=60,
    )
    await feed.start()
    try:
        await wait_until(lambda: len(delays) >= 4)
    finally:
        await feed.stop()

    assert delays[:4] == [1, 2, 4, 1]


@pytest.mark.asyncio
async def test_hyperliquid_feed_shutdown_cancels_heartbeat_and_reconnect_waits():
    websocket = FixtureWebSocket([])
    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=lambda _url, **_kwargs: websocket,
        heartbeat_interval_seconds=0.001,
    )
    await feed.start()
    await wait_until(lambda: {"method": "ping"} in websocket.sent)
    await feed.stop()
    assert feed._task is None
    assert websocket.closed


@pytest.mark.asyncio
async def test_hyperliquid_feed_shutdown_cancels_reconnect_backoff():
    websocket = ClosingWebSocket([])
    backoff_started = asyncio.Event()

    async def reconnect_sleep(_delay: float) -> None:
        backoff_started.set()
        await asyncio.Event().wait()

    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=lambda _url, **_kwargs: websocket,
        reconnect_sleep=reconnect_sleep,
        heartbeat_interval_seconds=60,
    )
    await feed.start()
    await asyncio.wait_for(backoff_started.wait(), timeout=1)
    await feed.stop()
    assert feed._task is None


@pytest.mark.asyncio
async def test_hyperliquid_old_session_cannot_close_new_session():
    first = FixtureWebSocket(
        [
            l2_book_message(
                "BTC",
                bids=((99.0, 1.0),),
                asks=((101.0, 1.0),),
            )
        ]
    )
    second = FixtureWebSocket([])
    connections = [first, second]
    connect_calls: list[int] = []
    second_connected = asyncio.Event()

    def connect(_url: str, **_kwargs: object):
        connect_calls.append(1)
        if len(connect_calls) == 2:
            second_connected.set()
        return connections.pop(0)

    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC"],
        connect=connect,
        heartbeat_interval_seconds=0.001,
        reconnect_delay_seconds=0,
    )
    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot("BTC") is not None)
        await first.close()
        await asyncio.wait_for(second_connected.wait(), timeout=1)
        await asyncio.sleep(0)
        assert not second.closed
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_hyperliquid_family_domains_do_not_invalidate_each_other():
    native_websocket = FixtureWebSocket(
        [l2_book_message("BTC", bids=((99.0, 1.0),), asks=((101.0, 1.0),))]
    )
    xyz_websocket = FixtureWebSocket(
        [
            l2_book_message(
                "xyz:TSLA", bids=((199.0, 1.0),), asks=((201.0, 1.0),)
            )
        ]
    )
    entropy_websocket = FixtureWebSocket(
        [
            l2_book_message(
                "io:SNDK", bids=((299.0, 1.0),), asks=((301.0, 1.0),)
            )
        ]
    )
    feeds = [
        HyperliquidOrderBookFeed(
            "wss://test.invalid/ws",
            ["BTC"],
            connect=lambda _url, **_kwargs: native_websocket,
            venue="hyperliquid",
        ),
        HyperliquidOrderBookFeed(
            "wss://test.invalid/ws",
            ["xyz:TSLA"],
            connect=lambda _url, **_kwargs: xyz_websocket,
            venue="trade_xyz",
        ),
        HyperliquidOrderBookFeed(
            "wss://test.invalid/ws",
            ["io:SNDK"],
            connect=lambda _url, **_kwargs: entropy_websocket,
            venue="entropy",
        ),
    ]
    await asyncio.gather(*(feed.start() for feed in feeds))
    try:
        await wait_until(
            lambda: all(
                feed.snapshot(coin) is not None
                for feed, coin in zip(feeds, ("BTC", "xyz:TSLA", "io:SNDK"), strict=True)
            )
        )
        await native_websocket.close()
        await wait_until(lambda: feeds[0].snapshot("BTC") is None)
        assert feeds[1].snapshot("xyz:TSLA") is not None
        assert feeds[2].snapshot("io:SNDK") is not None
    finally:
        await asyncio.gather(*(feed.stop() for feed in feeds))
