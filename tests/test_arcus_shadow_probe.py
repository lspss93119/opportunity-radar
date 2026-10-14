from __future__ import annotations

import pytest

from radar.history.arcus_shadow_probe import _event, summarize_book, collect
from pathlib import Path


def test_shadow_book_preserves_venue_metadata_and_vwap_tiers():
    book = {
        "bids": [["99", "60"]],
        "asks": [["101", "60"]],
        "timestamp": 1770000000000,
        "lastSequenceId": 42,
        "globalSequenceId": 50,
    }
    values = summarize_book(book)
    assert values["venue_timestamp_raw"] == 1770000000000
    assert values["last_sequence_id"] == 42
    assert values["best_bid"] == 99.0
    assert values["best_ask"] == 101.0
    assert values["buy_1k_vwap"] == pytest.approx(101.0)
    assert values["sell_1k_vwap"] == pytest.approx(99.0)
    assert values["buy_10k_vwap"] is None
    assert values["sell_10k_vwap"] is None


def test_shadow_invalid_book_is_reported_not_misclassified_as_valid():
    row = _event("ws", "BTC-USD", {"bids": [], "asks": []}, "2026-10-14T00:00:00Z")
    assert row["status"] == "invalid_book"
    assert row["source"] == "ws"


@pytest.mark.parametrize(
    ("bid_price", "ask_price"),
    [("101", "101"), ("102", "101")],
)
def test_shadow_locked_or_crossed_book_is_not_reported_as_valid(
    bid_price: str, ask_price: str
):
    row = _event(
        "ws",
        "BTC-USD",
        {"bids": [[bid_price, "1"]], "asks": [[ask_price, "1"]]},
        "2026-10-14T00:00:00Z",
    )

    assert row["status"] == "invalid_book"
    assert row["detail"] == "ValueError"


@pytest.mark.asyncio
async def test_shadow_probe_rejects_invalid_market_without_network(tmp_path: Path):
    with pytest.raises(ValueError, match="market"):
        await collect("../escape", tmp_path / "shadow.csv", duration_seconds=1)
