import pytest

from radar.vwap import BookLevel, buy_vwap, sell_vwap


def test_buy_vwap_walks_asks_to_exact_notional():
    asks = [BookLevel(price=100, base_size=5), BookLevel(price=101, base_size=10)]
    result = buy_vwap(asks, 1000)
    expected_base = 5 + 500 / 101
    assert result == pytest.approx(1000 / expected_base)


def test_sell_vwap_walks_bids_to_exact_notional():
    bids = [BookLevel(price=100, base_size=5), BookLevel(price=99, base_size=10)]
    result = sell_vwap(bids, 1000)
    expected_base = 5 + 500 / 99
    assert result == pytest.approx(1000 / expected_base)


def test_vwap_returns_none_when_depth_is_insufficient():
    asks = [BookLevel(price=100, base_size=1)]
    bids = [BookLevel(price=100, base_size=1)]
    assert buy_vwap(asks, 1000) is None
    assert sell_vwap(bids, 1000) is None


def test_vwap_returns_none_for_empty_book():
    assert buy_vwap([], 1000) is None
    assert sell_vwap([], 1000) is None


def test_book_level_rejects_invalid_values():
    with pytest.raises(ValueError):
        BookLevel(price=0, base_size=1)
    with pytest.raises(ValueError):
        BookLevel(price=100, base_size=0)


def test_vwap_rejects_non_positive_notional():
    with pytest.raises(ValueError):
        buy_vwap([BookLevel(price=100, base_size=1)], 0)
