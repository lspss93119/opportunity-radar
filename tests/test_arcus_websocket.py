from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from radar.collectors.arcus import (
    ArcusWebSocketFeed,
    parse_arcus_ws_message,
)
from radar.vwap import BookLevel

OBSERVED_AT = datetime(2026, 9, 15, 10, 0, 8, tzinfo=UTC)


def ws_snapshot(
    market: str,
    *,
    last_sequence_id: int = 10,
    global_sequence_id: int = 20,
    bids: Sequence[tuple[float, float]] = ((99.0, 20.0),),
    asks: Sequence[tuple[float, float]] = ((101.0, 20.0),),
    message_type: str = "channel_data",
) -> dict:
    return {
        "type": message_type,
        "channel": "l2Orderbook",
        "id": market,
        "contents": {
            "timestamp": 1770000000000,
            "lastSequenceId": last_sequence_id,
            "globalSequenceId": global_sequence_id,
            "bids": [[str(price), str(size)] for price, size in bids],
            "asks": [[str(price), str(size)] for price, size in asks],
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

    async def recv(self) -> str:
        message = await self._queue.get()
        if message is None:
            raise ConnectionError("fixture websocket closed")
        return message

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._queue.put_nowait(None)

    async def __aenter__(self) -> "FixtureWebSocket":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()


async def wait_until(predicate) -> None:
    for _ in range(1_000):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("predicate was not satisfied")


def test_parse_arcus_ws_snapshot_normalizes_levels_and_preserves_metadata():
    snapshot = parse_arcus_ws_message(
        ws_snapshot(
            "SNDK-USD",
            bids=((98.0, 2.0), (99.0, 1.0)),
            asks=((102.0, 2.0), (101.0, 1.0)),
        ),
        frozenset({"SNDK-USD"}),
        observed_at=OBSERVED_AT,
    )

    assert snapshot is not None
    assert snapshot.market == "SNDK-USD"
    assert snapshot.bids == (
        BookLevel(price=99.0, base_size=1.0),
        BookLevel(price=98.0, base_size=2.0),
    )
    assert snapshot.asks == (
        BookLevel(price=101.0, base_size=1.0),
        BookLevel(price=102.0, base_size=2.0),
    )
    assert snapshot.observed_at == OBSERVED_AT
    assert snapshot.venue_timestamp_raw == 1770000000000
    assert snapshot.last_sequence_id == 10
    assert snapshot.global_sequence_id == 20


@pytest.mark.parametrize(
    "payload",
    [
        ws_snapshot("SNDK-USD", bids=(), asks=((101.0, 1.0),)),
        ws_snapshot("SNDK-USD", bids=((101.0, 1.0),), asks=((101.0, 1.0),)),
        ws_snapshot("SNDK-USD", bids=((102.0, 1.0),), asks=((101.0, 1.0),)),
    ],
)
def test_parse_arcus_ws_snapshot_rejects_invalid_books(payload: dict):
    with pytest.raises(ValueError):
        parse_arcus_ws_message(
            payload,
            frozenset({"SNDK-USD"}),
            observed_at=OBSERVED_AT,
        )


def test_parse_arcus_ws_ignores_unconfigured_or_non_book_messages():
    assert (
        parse_arcus_ws_message(
            ws_snapshot("TSLA-USD"),
            frozenset({"SNDK-USD"}),
            observed_at=OBSERVED_AT,
        )
        is None
    )
    assert (
        parse_arcus_ws_message(
            {"type": "subscribed", "channel": "l2Orderbook", "id": "SNDK-USD"},
            frozenset({"SNDK-USD"}),
            observed_at=OBSERVED_AT,
        )
        is None
    )


@pytest.mark.asyncio
async def test_arcus_feed_subscribes_and_publishes_complete_snapshot():
    websocket = FixtureWebSocket(
        [
            {"type": "subscribed", "channel": "l2Orderbook", "id": "SNDK-USD"},
            ws_snapshot("SNDK-USD"),
        ]
    )
    published: list[object] = []
    feed = ArcusWebSocketFeed(
        "wss://test.invalid/v1/ws",
        ["SNDK-USD"],
        connect=lambda _url, **_kwargs: websocket,
        clock=lambda: OBSERVED_AT,
        on_book=lambda market, snapshot: published.append((market, snapshot)),
    )

    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot("SNDK-USD") is not None)
    finally:
        await feed.stop()

    assert websocket.sent == [
        {
            "type": "subscribe",
            "channel": "l2Orderbook",
            "id": "SNDK-USD",
            "nLevels": 20,
        }
    ]
    assert len(published) == 1
    snapshot = feed.last_published_snapshot
    assert snapshot is not None
    assert snapshot.bids == (BookLevel(price=99.0, base_size=20.0),)


@pytest.mark.asyncio
async def test_arcus_feed_ignores_duplicate_and_out_of_order_sequences():
    websocket = FixtureWebSocket(
        [
            ws_snapshot("SNDK-USD", last_sequence_id=10, global_sequence_id=20),
            ws_snapshot(
                "SNDK-USD",
                last_sequence_id=9,
                global_sequence_id=19,
                bids=((98.0, 20.0),),
            ),
            ws_snapshot(
                "SNDK-USD",
                last_sequence_id=10,
                global_sequence_id=20,
                bids=((97.0, 20.0),),
            ),
            ws_snapshot(
                "SNDK-USD",
                last_sequence_id=11,
                global_sequence_id=21,
                bids=((98.0, 20.0),),
            ),
        ]
    )
    feed = ArcusWebSocketFeed(
        "wss://test.invalid/v1/ws",
        ["SNDK-USD"],
        connect=lambda _url, **_kwargs: websocket,
        clock=lambda: OBSERVED_AT,
    )

    await feed.start()
    try:
        await wait_until(
            lambda: (
                feed.snapshot("SNDK-USD") is not None
                and feed.snapshot("SNDK-USD").last_sequence_id == 11
            )
        )
    finally:
        await feed.stop()

    snapshot = feed.last_published_snapshot
    assert snapshot is not None
    assert snapshot.bids == (BookLevel(price=98.0, base_size=20.0),)
    assert feed.ignored_sequence_count == 2


@pytest.mark.asyncio
async def test_arcus_feed_degraded_notice_and_silence_invalidate_book():
    websocket = FixtureWebSocket([ws_snapshot("SNDK-USD")])
    invalidated: list[str] = []
    feed = ArcusWebSocketFeed(
        "wss://test.invalid/v1/ws",
        ["SNDK-USD"],
        connect=lambda _url, **_kwargs: websocket,
        clock=lambda: OBSERVED_AT,
        silence_timeout_seconds=0.01,
        on_invalidate=invalidated.append,
    )

    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot("SNDK-USD") is not None)
        websocket.push(
            {
                "type": "degraded",
                "channel": "l2Orderbook",
                "id": "SNDK-USD",
            }
        )
        await wait_until(lambda: feed.snapshot("SNDK-USD") is None)
        websocket.push(ws_snapshot("SNDK-USD", last_sequence_id=11, global_sequence_id=21))
        await wait_until(lambda: feed.snapshot("SNDK-USD") is not None)
        await asyncio.sleep(0.03)
        await wait_until(lambda: feed.snapshot("SNDK-USD") is None)
    finally:
        await feed.stop()

    assert invalidated.count("SNDK-USD") >= 2


@pytest.mark.asyncio
async def test_arcus_feed_reconnect_clears_book_and_resubscribes():
    first = FixtureWebSocket([ws_snapshot("SNDK-USD")])
    second = FixtureWebSocket([])
    connections = [first, second]

    def connect(_url: str, **_kwargs: object):
        return connections.pop(0) if connections else second

    invalidated: list[str] = []
    feed = ArcusWebSocketFeed(
        "wss://test.invalid/v1/ws",
        ["SNDK-USD"],
        connect=connect,
        clock=lambda: OBSERVED_AT,
        reconnect_delay_seconds=0,
        on_invalidate=invalidated.append,
    )

    await feed.start()
    try:
        await wait_until(lambda: feed.snapshot("SNDK-USD") is not None)
        await first.close()
        await wait_until(
            lambda: feed.snapshot("SNDK-USD") is None and len(second.sent) == 1
        )
        second.push(ws_snapshot("SNDK-USD", last_sequence_id=11, global_sequence_id=21))
        await wait_until(lambda: feed.snapshot("SNDK-USD") is not None)
    finally:
        await feed.stop()

    assert first.sent == second.sent == [
        {
            "type": "subscribe",
            "channel": "l2Orderbook",
            "id": "SNDK-USD",
            "nLevels": 20,
        }
    ]
    assert feed.reconnect_count >= 1
    assert invalidated
    assert second.closed
