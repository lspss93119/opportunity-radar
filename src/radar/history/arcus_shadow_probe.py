"""Read-only Arcus REST / WebSocket snapshot comparison.

This is an isolated observer. It neither publishes books to the Radar cache
nor stores raw depth, places orders, or modifies Production files.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import websockets

from radar.collectors.arcus import parse_arcus_l2_order_book
from radar.vwap import buy_vwap, sell_vwap

WS_URL = "wss://api.arcus.xyz/v1/ws"
REST_URL = "https://api.arcus.xyz/v1/l2OrderBook"
FIELDS = (
    "source", "market", "received_at_utc", "request_started_at_utc",
    "request_duration_ms", "venue_timestamp_raw", "last_sequence_id",
    "global_sequence_id", "best_bid", "best_ask", "buy_1k_vwap",
    "sell_1k_vwap", "buy_5k_vwap", "sell_5k_vwap", "buy_10k_vwap",
    "sell_10k_vwap", "status", "detail",
)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def summarize_book(payload: object) -> dict[str, object]:
    """Normalize one *full* L2 snapshot, preserving raw venue time metadata."""
    bids, asks = parse_arcus_l2_order_book(payload)
    assert isinstance(payload, dict)
    best_bid = bids[0].price
    best_ask = asks[0].price
    if best_bid >= best_ask:
        raise ValueError("book must not be crossed or locked")
    return {
        "venue_timestamp_raw": payload.get("timestamp"),
        "last_sequence_id": payload.get("lastSequenceId"),
        "global_sequence_id": payload.get("globalSequenceId"),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "buy_1k_vwap": buy_vwap(asks, 1_000),
        "sell_1k_vwap": sell_vwap(bids, 1_000),
        "buy_5k_vwap": buy_vwap(asks, 5_000),
        "sell_5k_vwap": sell_vwap(bids, 5_000),
        "buy_10k_vwap": buy_vwap(asks, 10_000),
        "sell_10k_vwap": sell_vwap(bids, 10_000),
    }


def _fetch_book(url: str) -> object:
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "opportunity-radar-shadow/readonly"})
    with urlopen(request, timeout=10) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        return json.loads(response.read().decode("utf-8"))


def _event(
    source: str,
    market: str,
    payload: object,
    received_at: str,
    *,
    started_at: str = "",
    duration_ms: float | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "source": source, "market": market, "received_at_utc": received_at,
        "request_started_at_utc": started_at, "request_duration_ms": duration_ms,
        "status": "ok", "detail": "",
    }
    try:
        row.update(summarize_book(payload))
    except (ValueError, TypeError, KeyError, IndexError) as error:
        row["status"] = "invalid_book"
        row["detail"] = type(error).__name__
    return row


async def _rest_loop(
    queue: asyncio.Queue[dict[str, object] | None],
    market: str,
    url: str,
    interval: float,
    deadline: float,
) -> None:
    # Each REST request is timed independently; no guessed venue event time.
    while time.monotonic() < deadline:
        began = time.monotonic()
        started_at = utc_now()
        try:
            payload = await asyncio.to_thread(_fetch_book, f"{url}/{market}")
            row = _event(
                "rest", market, payload, utc_now(),
                started_at=started_at, duration_ms=round((time.monotonic() - began) * 1000, 3),
            )
        except Exception as error:  # noqa: BLE001
            row = {
                "source": "rest", "market": market, "received_at_utc": utc_now(),
                "request_started_at_utc": started_at,
                "request_duration_ms": round((time.monotonic() - began) * 1000, 3),
                "status": "request_error", "detail": type(error).__name__,
            }
        await queue.put(row)
        await asyncio.sleep(min(interval, max(0.0, deadline - time.monotonic())))


async def _ws_loop(
    queue: asyncio.Queue[dict[str, object] | None],
    market: str,
    url: str,
    deadline: float,
) -> None:
    while time.monotonic() < deadline:
        try:
            async with websockets.connect(url, open_timeout=10, ping_interval=30, max_size=8 * 1024 * 1024) as socket:
                await socket.send(json.dumps({
                    "type": "subscribe", "channel": "l2Orderbook",
                    "id": market, "nLevels": 20,
                }))
                while time.monotonic() < deadline:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(socket.recv(), timeout=remaining)
                    except TimeoutError:
                        break
                    received_at = utc_now()
                    try:
                        message = json.loads(raw)
                        if not isinstance(message, dict):
                            raise ValueError("invalid websocket envelope")
                        if message.get("channel") != "l2Orderbook" or message.get("type") not in ("subscribed", "channel_data"):
                            continue
                        if message.get("id") not in (None, market):
                            continue
                        contents = message.get("contents")
                        if not isinstance(contents, dict):
                            raise ValueError("snapshot has no book contents")
                        row = _event("ws", market, contents, received_at)
                    except (ValueError, TypeError, KeyError, IndexError) as error:
                        row = {
                            "source": "ws", "market": market, "received_at_utc": received_at,
                            "status": "invalid_message", "detail": type(error).__name__,
                        }
                    await queue.put(row)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            await queue.put({
                "source": "ws", "market": market, "received_at_utc": utc_now(),
                "status": "connection_error", "detail": type(error).__name__,
            })
            await asyncio.sleep(min(5.0, max(0.0, deadline - time.monotonic())))


async def _writer(queue: asyncio.Queue[dict[str, object] | None], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        while (row := await queue.get()) is not None:
            writer.writerow(row)
            handle.flush()


async def collect(
    market: str, output: Path, *, duration_seconds: float,
    rest_interval_seconds: float = 10.0, ws_url: str = WS_URL,
    rest_url: str = REST_URL,
) -> None:
    if duration_seconds <= 0 or rest_interval_seconds <= 0:
        raise ValueError("duration and interval must be positive")
    if not market or not all(c.isascii() and (c.isalnum() or c in "-_") for c in market):
        raise ValueError("market must be a configured Arcus display symbol")
    queue: asyncio.Queue[dict[str, object] | None] = asyncio.Queue()
    deadline = time.monotonic() + duration_seconds
    writer_task = asyncio.create_task(_writer(queue, output))
    try:
        await asyncio.gather(
            _rest_loop(queue, market, rest_url, rest_interval_seconds, deadline),
            _ws_loop(queue, market, ws_url, deadline),
        )
    finally:
        await queue.put(None)
        await writer_task


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market", required=True, help="e.g. BTC-USD; use Arcus market display name")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=float, default=3600.0)
    parser.add_argument("--rest-interval-seconds", type=float, default=10.0)
    args = parser.parse_args(argv)
    asyncio.run(collect(
        args.market, args.output,
        duration_seconds=args.duration_seconds,
        rest_interval_seconds=args.rest_interval_seconds,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
