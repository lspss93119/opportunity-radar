from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from radar.collectors.lighter import (
    LighterOrderBookFeed,
    LighterOrderBookSnapshot,
    LighterOrderBookState,
    LighterCollector,
)
from radar.config import MarketConfig
from radar.market_data import LatestMarketData
from radar.pipeline import MarketDataPipeline
from radar.state import RadarState
from radar.vwap import buy_vwap, sell_vwap

OBSERVED_AT = datetime(2026, 9, 24, 10, 0, 1, 123000, tzinfo=UTC)


def snapshot_message(market_id: int, *, nonce: int = 10) -> dict:
    return {
        "type": "subscribed/order_book",
        "channel": f"order_book:{market_id}",
        "order_book": {
            "code": 0,
            "asks": [
                {"price": "101", "size": "2"},
                {"price": "100", "size": "1"},
            ],
            "bids": [
                {"price": "99", "size": "3"},
                {"price": "98", "size": "2"},
            ],
            "begin_nonce": 1,
            "nonce": nonce,
        },
    }


def update_message(
    market_id: int, *, begin_nonce: int = 10, nonce: int = 11
) -> dict:
    return {
        "type": "update/order_book",
        "channel": f"order_book:{market_id}",
        "order_book": {
            "code": 0,
            "asks": [
                {"price": "100", "size": "0"},
                {"price": "102", "size": "4"},
            ],
            "bids": [{"price": "99", "size": "1"}],
            "begin_nonce": begin_nonce,
            "nonce": nonce,
        },
    }


def snapshot_metrics(snapshot: LighterOrderBookSnapshot) -> tuple[object, ...]:
    bids = list(snapshot.bids)
    asks = list(snapshot.asks)
    return (
        bids[0].price,
        bids[0].base_size,
        asks[0].price,
        asks[0].base_size,
        buy_vwap(asks, 1_000),
        sell_vwap(bids, 1_000),
        buy_vwap(asks, 5_000),
        sell_vwap(bids, 5_000),
        buy_vwap(asks, 10_000),
        sell_vwap(bids, 10_000),
        snapshot.observed_at,
    )


def test_lighter_order_book_state_applies_snapshot_delta_and_deletes_levels():
    state = LighterOrderBookState(market_id=1)

    state.apply_snapshot(snapshot_message(1)["order_book"], OBSERVED_AT)
    state.apply_delta(update_message(1)["order_book"], OBSERVED_AT)

    view = state.snapshot()
    assert view is not None
    assert [(level.price, level.base_size) for level in view.bids] == [
        (99.0, 1.0),
        (98.0, 2.0),
    ]
    assert [(level.price, level.base_size) for level in view.asks] == [
        (101.0, 2.0),
        (102.0, 4.0),
    ]
    assert view.observed_at == OBSERVED_AT


def test_lighter_order_book_state_rejects_delta_nonce_gap_and_stays_unready():
    state = LighterOrderBookState(market_id=1)
    state.apply_snapshot(snapshot_message(1)["order_book"], OBSERVED_AT)

    with pytest.raises(ValueError, match="nonce"):
        state.apply_delta(
            update_message(1, begin_nonce=9, nonce=11)["order_book"],
            OBSERVED_AT,
        )

    state.clear()
    assert state.snapshot() is None


@pytest.mark.parametrize("venue", ["lighter", "lighter_robinhood"])
def test_lighter_boundary_model_matches_eager_reference(venue: str):
    del venue  # The shared state machine is used by both logical venues.
    raw_snapshot = snapshot_message(1)
    for level in raw_snapshot["order_book"]["asks"] + raw_snapshot["order_book"]["bids"]:
        level["size"] = "200"
    raw_deltas = [
        update_message(1, begin_nonce=10, nonce=11),
        update_message(1, begin_nonce=11, nonce=12),
        update_message(1, begin_nonce=12, nonce=13),
    ]
    eager = LighterOrderBookState(market_id=1)
    boundary = LighterOrderBookState(market_id=1)
    eager.apply_snapshot(raw_snapshot["order_book"], OBSERVED_AT)
    boundary.apply_snapshot(raw_snapshot["order_book"], OBSERVED_AT)

    for index, message in enumerate(raw_deltas):
        observed_at = OBSERVED_AT + timedelta(seconds=index + 1)
        eager.apply_delta(message["order_book"], observed_at)
        eager_view = eager.snapshot()
        assert eager_view is not None
        boundary.apply_delta(message["order_book"], observed_at)
        if index in {0, 2}:
            boundary_view = boundary.snapshot()
            assert boundary_view is not None
            assert snapshot_metrics(boundary_view) == snapshot_metrics(eager_view)


class FakeWebSocket:
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

    async def __aenter__(self) -> "FakeWebSocket":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    def __aiter__(self) -> "FakeWebSocket":
        return self

    async def __anext__(self) -> str:
        message = await self._queue.get()
        if message is None:
            raise StopAsyncIteration
        return message


async def wait_for_book(feed: LighterOrderBookFeed, market_id: int):
    await wait_until(lambda: feed.snapshot(market_id) is not None)
    view = feed.snapshot(market_id)
    assert view is not None
    return view


async def wait_until(predicate) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("predicate was not satisfied")


def connect_snapshot_only_fixture(_url: str):
    return FakeWebSocket([{"type": "connected"}, snapshot_message(1)])


@pytest.mark.asyncio
async def test_full_lighter_snapshot_is_ready_without_followup_delta():
    published: list[tuple[int, LighterOrderBookSnapshot]] = []
    feed = LighterOrderBookFeed(
        "wss://test.invalid/stream",
        [1],
        connect=connect_snapshot_only_fixture,
        on_book=lambda market_id, snapshot: published.append((market_id, snapshot)),
    )

    try:
        await feed.start()
        await wait_until(lambda: feed.snapshot(1) is not None)
        assert published == []

        feed.publish_current_books()

        assert len(published) == 1
        assert published[0][0] == 1
        assert published[0][1].bids[0].price == 99.0
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_lighter_deltas_wait_for_boundary_and_publish_latest_book_once():
    websocket = FakeWebSocket([{"type": "connected"}, snapshot_message(1)])
    published: list[LighterOrderBookSnapshot] = []
    feed = LighterOrderBookFeed(
        "wss://test.invalid/stream",
        [1],
        connect=lambda _url: websocket,
        on_book=lambda _market_id, snapshot: published.append(snapshot),
    )

    try:
        await feed.start()
        await wait_until(lambda: feed.snapshot(1) is not None)
        websocket.push(update_message(1, begin_nonce=10, nonce=11))
        websocket.push(update_message(1, begin_nonce=11, nonce=12))
        websocket.push(update_message(1, begin_nonce=12, nonce=13))
        await wait_until(lambda: feed.snapshot(1) is not None and feed.snapshot(1).bids[0].base_size == 1.0)

        assert published == []

        feed.publish_current_books()
        assert len(published) == 1
        assert published[0].bids[0].base_size == 1.0

        feed.publish_current_books()
        assert len(published) == 2
        assert published[1] == published[0]
    finally:
        await feed.stop()


def test_lighter_boundary_publication_materializes_each_ready_market_once(monkeypatch):
    feed = LighterOrderBookFeed("wss://test.invalid/stream", [1, 2])
    snapshots = {
        1: snapshot_message(1)["order_book"],
        2: snapshot_message(2)["order_book"],
    }
    for market_id, raw_order_book in snapshots.items():
        feed._states[market_id].apply_snapshot(raw_order_book, OBSERVED_AT)

    calls: dict[int, int] = {1: 0, 2: 0}
    for market_id, state in feed._states.items():
        original_snapshot = state.snapshot

        def counted_snapshot(original_snapshot=original_snapshot, market_id=market_id):
            calls[market_id] += 1
            return original_snapshot()

        monkeypatch.setattr(state, "snapshot", counted_snapshot)

    feed.publish_current_books()
    assert calls == {1: 1, 2: 1}


@pytest.mark.asyncio
async def test_lighter_order_book_feed_isolates_markets_and_resubscribes_after_reconnect():
    first = FakeWebSocket(
        [
            {"type": "connected"},
            snapshot_message(1),
            snapshot_message(2, nonce=20),
        ]
    )
    second = FakeWebSocket([{"type": "connected"}])
    connections = [first, second]

    def connect(_url: str):
        return connections.pop(0)

    invalidated: list[int] = []
    latest = LatestMarketData()
    symbols = {1: "BTC", 2: "ETH"}

    def on_book(market_id: int, snapshot: LighterOrderBookSnapshot) -> None:
        latest.update_book(
            venue="lighter",
            venue_symbol=symbols[market_id],
            bids=snapshot.bids,
            asks=snapshot.asks,
            observed_at=snapshot.observed_at,
        )

    def on_invalidate(market_id: int) -> None:
        invalidated.append(market_id)
        latest.invalidate(venue="lighter", venue_symbol=symbols[market_id])

    feed = LighterOrderBookFeed(
        "wss://example.test/stream",
        (1, 2),
        connect=connect,
        clock=lambda: OBSERVED_AT,
        on_book=on_book,
        on_invalidate=on_invalidate,
        reconnect_delay_seconds=0,
    )
    await feed.start()
    try:
        first_market = await wait_for_book(feed, 1)
        second_market = await wait_for_book(feed, 2)
        assert first_market.bids[0].price == 99.0
        assert second_market.bids[0].price == 99.0
        assert {message["channel"] for message in first.sent} == {
            "order_book/1",
            "order_book/2",
        }
        feed.publish_current_books()
        assert len(
            latest.build_batch(
                [
                    MarketConfig(
                        venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"
                    ),
                    MarketConfig(
                        venue="lighter", venue_symbol="ETH", canonical_symbol="ETH"
                    ),
                ],
                sample_time=OBSERVED_AT,
                now=OBSERVED_AT,
                stale_after_seconds=30,
            ).market_snapshots
        ) == 2
        invalidations_before_disconnect = len(invalidated)

        await first.close()
        await wait_until(lambda: len(invalidated) > invalidations_before_disconnect)
        assert feed.snapshot(1) is None
        assert feed.snapshot(2) is None
        assert set(invalidated[invalidations_before_disconnect:]) == {1, 2}
        assert latest.build_batch(
            [
                MarketConfig(
                    venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"
                ),
                MarketConfig(
                    venue="lighter", venue_symbol="ETH", canonical_symbol="ETH"
                ),
            ],
            sample_time=OBSERVED_AT,
            now=OBSERVED_AT,
            stale_after_seconds=30,
        ).market_snapshots == ()

        second.push(snapshot_message(1, nonce=30))
        await wait_for_book(feed, 1)
        feed.publish_current_books()
        assert {message["channel"] for message in second.sent} == {
            "order_book/1",
            "order_book/2",
        }
        assert feed.reconnect_count >= 1
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_lighter_order_book_feed_invalidates_only_market_with_nonce_gap():
    first = FakeWebSocket(
        [
            {"type": "connected"},
            snapshot_message(1),
            snapshot_message(2, nonce=20),
        ]
    )
    second = FakeWebSocket([{"type": "connected"}])
    connections = [first, second]

    def connect(_url: str):
        return connections.pop(0)

    invalidated: list[int] = []
    latest = LatestMarketData()
    symbols = {1: "BTC", 2: "ETH"}

    def on_book(market_id: int, snapshot: LighterOrderBookSnapshot) -> None:
        latest.update_book(
            venue="lighter",
            venue_symbol=symbols[market_id],
            bids=snapshot.bids,
            asks=snapshot.asks,
            observed_at=snapshot.observed_at,
        )

    def on_invalidate(market_id: int) -> None:
        invalidated.append(market_id)
        latest.invalidate(venue="lighter", venue_symbol=symbols[market_id])

    feed = LighterOrderBookFeed(
        "wss://example.test/stream",
        (1, 2),
        connect=connect,
        clock=lambda: OBSERVED_AT,
        on_book=on_book,
        on_invalidate=on_invalidate,
        reconnect_delay_seconds=1.0,
    )
    markets = [
        MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
        MarketConfig(venue="lighter", venue_symbol="ETH", canonical_symbol="ETH"),
    ]
    await feed.start()
    try:
        await wait_for_book(feed, 1)
        await wait_for_book(feed, 2)
        feed.publish_current_books()
        invalidations_before_gap = len(invalidated)

        first.push(update_message(1, begin_nonce=9, nonce=11))
        await wait_until(lambda: len(invalidated) > invalidations_before_gap)

        assert invalidated[invalidations_before_gap:] == [1]
        assert feed.snapshot(1) is None
        assert feed.snapshot(2) is not None
        assert feed.reconnect_count == 0
        assert [
            snapshot.canonical_symbol
            for snapshot in latest.build_batch(
                markets,
                sample_time=OBSERVED_AT,
                now=OBSERVED_AT,
                stale_after_seconds=30,
            ).market_snapshots
        ] == ["ETH"]
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_lighter_gap_recovery_waits_for_replacement_snapshot_without_reconnect():
    websocket = FakeWebSocket([
        {"type": "connected"},
        snapshot_message(1),
        snapshot_message(2, nonce=20),
    ])
    feed = LighterOrderBookFeed(
        "wss://example.test/stream",
        (1, 2),
        connect=lambda _url: websocket,
        clock=lambda: OBSERVED_AT,
    )
    await feed.start()
    try:
        await wait_for_book(feed, 1)
        await wait_for_book(feed, 2)

        websocket.push(update_message(1, begin_nonce=999, nonce=1000))
        await wait_until(lambda: feed.snapshot(1) is None)
        await wait_until(lambda: any(
            msg.get("type") == "unsubscribe" and msg.get("channel") == "order_book/1"
            for msg in websocket.sent
        ))
        assert feed.snapshot(2) is not None
        assert feed.reconnect_count == 0
        assert websocket.sent[-2:] == [
            {"type": "unsubscribe", "channel": "order_book/1"},
            {"type": "subscribe", "channel": "order_book/1"},
        ]
        websocket.push(update_message(1, begin_nonce=1000, nonce=1001))
        await asyncio.sleep(0)
        assert feed.snapshot(1) is None

        websocket.push(snapshot_message(1, nonce=50))
        recovered = await wait_for_book(feed, 1)
        assert recovered.bids[0].price == 99.0
        websocket.push(update_message(1, begin_nonce=50, nonce=51))
        await wait_until(lambda: feed.snapshot(1) is not None
                         and feed.snapshot(1).bids[0].base_size == 1.0)
        assert feed.snapshot(2) is not None
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_lighter_missing_initial_snapshot_retries_subscription():
    websocket = FakeWebSocket([{"type": "connected"}])
    feed = LighterOrderBookFeed(
        "wss://example.test/stream",
        (1,),
        connect=lambda _url: websocket,
        snapshot_timeout_seconds=0.001,
    )
    await feed.start()
    try:
        async def replacement_sent():
            for _ in range(200):
                if len(websocket.sent) >= 3:
                    return True
                await asyncio.sleep(0.001)
            return False

        assert await replacement_sent()
        assert websocket.sent[1:3] == [
            {"type": "unsubscribe", "channel": "order_book/1"},
            {"type": "subscribe", "channel": "order_book/1"},
        ]
        assert feed.snapshot(1) is None
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_lighter_reconnects_when_server_rejects_gap_resubscription():
    first = FakeWebSocket([{"type": "connected"}, snapshot_message(1)])
    second = FakeWebSocket([{"type": "connected"}])
    subscribe_count = 0

    async def first_send(message: str) -> None:
        nonlocal subscribe_count
        payload = json.loads(message)
        first.sent.append(payload)
        if payload.get("type") == "subscribe":
            subscribe_count += 1
            if subscribe_count == 2:
                first.push({"type": "error", "message": "subscription rejected"})

    first.send = first_send  # type: ignore[method-assign]
    connections = [first, second]

    def connect(_url: str):
        return connections.pop(0)

    feed = LighterOrderBookFeed(
        "wss://example.test/stream",
        (1,),
        connect=connect,
        reconnect_delay_seconds=0,
        snapshot_timeout_seconds=1.0,
    )
    await feed.start()
    try:
        await wait_for_book(feed, 1)
        first.push(update_message(1, begin_nonce=999, nonce=1000))
        await wait_until(lambda: feed.reconnect_count >= 1)
        second.push(snapshot_message(1, nonce=20))
        recovered = await wait_for_book(feed, 1)

        assert recovered.bids[0].price == 99.0
        assert feed.reconnect_count == 1
    finally:
        await feed.stop()


@pytest.mark.asyncio
async def test_lighter_stop_cleans_snapshot_watchdogs():
    websocket = FakeWebSocket([{"type": "connected"}])
    feed = LighterOrderBookFeed(
        "wss://example.test/stream",
        (1,),
        connect=lambda _url: websocket,
        snapshot_timeout_seconds=60.0,
    )

    await feed.start()
    await wait_until(
        lambda: any(
            task.get_name() == "lighter-book-snapshot-1"
            and not task.done()
            for task in asyncio.all_tasks()
        )
    )
    await feed.stop()
    await asyncio.sleep(0)

    assert not any(
        task.get_name() == "lighter-book-snapshot-1" and not task.done()
        for task in asyncio.all_tasks()
    )


@pytest.mark.asyncio
async def test_lighter_order_book_feed_handles_application_ping():
    websocket = FakeWebSocket([{"type": "connected"}])

    def connect(_url: str):
        return websocket

    feed = LighterOrderBookFeed(
        "wss://example.test/stream",
        (1,),
        connect=connect,
        reconnect_delay_seconds=0,
    )
    await feed.start()
    for _ in range(50):
        if websocket.sent:
            break
        await asyncio.sleep(0)
    websocket.push({"type": "ping"})
    for _ in range(50):
        if len(websocket.sent) >= 2:
            break
        await asyncio.sleep(0)
    assert websocket.sent[-1] == {"type": "pong"}
    await feed.stop()


@pytest.mark.asyncio
async def test_lighter_collector_reads_ws_book_without_rest_order_book_requests():
    calls: list[str] = []

    async def request_json(url: str, *, method: str, json_body=None, params=None):
        calls.append(url)
        if url.endswith("/orderBookDetails"):
            return {
                "code": 200,
                "order_book_details": [
                    {
                        "symbol": "BTC",
                        "market_id": 1,
                        "market_type": "perp",
                        "status": "active",
                        "mark_price": "100",
                        "index_price": "100",
                        "open_interest": "1",
                        "daily_quote_token_volume": "2",
                    }
                ],
                "spot_order_book_details": [],
            }
        raise AssertionError(f"unexpected REST request: {url}")

    rich_snapshot = snapshot_message(1)
    for level in rich_snapshot["order_book"]["asks"] + rich_snapshot["order_book"]["bids"]:
        level["size"] = "200"
    websocket = FakeWebSocket([{"type": "connected"}, rich_snapshot])

    def connect(_url: str):
        return websocket

    market = MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC")
    latest = LatestMarketData()
    collector = LighterCollector(
        [market],
        request_json=request_json,
        websocket_connect=connect,
        clock=lambda: OBSERVED_AT,
        latest_market_data=latest,
    )
    pipeline = MarketDataPipeline(
        [collector],
        RadarState(),
        markets=[market],
        latest_market_data=latest,
        clock=lambda: OBSERVED_AT,
    )
    try:
        await collector.start()
        await wait_for_book(collector._order_book_feed, 1)
        batch = await pipeline.collect_once(now=OBSERVED_AT)

        assert len(batch.market_snapshots) == 1
        assert batch.market_snapshots[0].buy_10k_vwap is not None
        assert batch.market_snapshots[0].observed_at == OBSERVED_AT
        assert all(not url.endswith("/orderBookOrders") for url in calls)
    finally:
        await collector.stop()
