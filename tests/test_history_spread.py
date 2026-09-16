from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta, timezone

import pytest

from radar.history.spread import HistoricalSpreadContext, SpreadHistory
from radar.models import MarketSnapshot
from radar.storage.parquet import ParquetStorage

AS_OF = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


def make_market(
    venue: str,
    sample_time: datetime,
    *,
    observed_at: datetime | None = None,
    canonical_symbol: str = "BTC",
    venue_symbol: str = "BTC",
    buy_1k_vwap: float | None = 100.0,
    sell_1k_vwap: float | None = 101.0,
    buy_5k_vwap: float | None = 200.0,
    sell_5k_vwap: float | None = 202.0,
    buy_10k_vwap: float | None = 100.0,
    sell_10k_vwap: float | None = 101.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=sample_time if observed_at is None else observed_at,
        venue=venue,
        venue_symbol=venue_symbol,
        canonical_symbol=canonical_symbol,
        best_bid=99.0,
        best_bid_size=2.0,
        best_ask=100.0,
        best_ask_size=3.0,
        buy_1k_vwap=buy_1k_vwap,
        sell_1k_vwap=sell_1k_vwap,
        buy_5k_vwap=buy_5k_vwap,
        sell_5k_vwap=sell_5k_vwap,
        buy_10k_vwap=buy_10k_vwap,
        sell_10k_vwap=sell_10k_vwap,
    )


def make_pair(
    sample_time: datetime,
    *,
    long_buy: float | None = 100.0,
    short_sell: float | None = 101.0,
    observed_at: datetime | None = None,
) -> tuple[MarketSnapshot, MarketSnapshot]:
    return (
        make_market(
            "lighter",
            sample_time,
            observed_at=observed_at,
            buy_10k_vwap=long_buy,
        ),
        make_market(
            "hyperliquid",
            sample_time,
            observed_at=observed_at,
            sell_10k_vwap=short_sell,
        ),
    )


def write_markets(tmp_path, snapshots: list[MarketSnapshot]) -> None:
    store = ParquetStorage(tmp_path / "data")
    for snapshot in snapshots:
        store.append(snapshot)
    store.flush(now=AS_OF)


def query_context(tmp_path, *, as_of: datetime = AS_OF, size: int = 10_000):
    return SpreadHistory(tmp_path / "data").query(
        canonical_symbol="BTC",
        long_venue="lighter",
        long_venue_symbol="BTC",
        short_venue="hyperliquid",
        short_venue_symbol="BTC",
        primary_size_usd=size,
        as_of=as_of,
    )


def test_exact_sample_time_join_uses_short_sell_over_long_buy(tmp_path):
    long_snapshot, short_snapshot = make_pair(AS_OF, long_buy=100.0, short_sell=101.0)
    write_markets(tmp_path, [long_snapshot, short_snapshot])

    context = query_context(tmp_path)

    assert len(context.points_7d) == 1
    assert context.points_7d[0].sample_time == AS_OF
    assert context.points_7d[0].raw_spread_bps == pytest.approx(100.0)


def test_offset_sample_times_do_not_form_asof_point(tmp_path):
    long_snapshot, _ = make_pair(AS_OF)
    _, short_snapshot = make_pair(AS_OF - timedelta(seconds=1))
    write_markets(tmp_path, [long_snapshot, short_snapshot])

    context = query_context(tmp_path)

    assert context.points_7d == ()


def test_latest_observed_duplicate_is_selected_once(tmp_path):
    old_long, old_short = make_pair(
        AS_OF,
        long_buy=90.0,
        short_sell=100.0,
        observed_at=AS_OF + timedelta(seconds=1),
    )
    new_long, new_short = make_pair(
        AS_OF,
        long_buy=100.0,
        short_sell=102.0,
        observed_at=AS_OF + timedelta(seconds=2),
    )
    write_markets(tmp_path, [old_long, old_short, new_long, new_short])

    context = query_context(tmp_path)

    assert len(context.points_7d) == 1
    assert context.points_7d[0].raw_spread_bps == pytest.approx(200.0)


def test_rows_after_as_of_are_excluded(tmp_path):
    current_long, current_short = make_pair(AS_OF, long_buy=100.0, short_sell=101.0)
    future_long, future_short = make_pair(
        AS_OF + timedelta(seconds=10),
        long_buy=100.0,
        short_sell=103.0,
    )
    write_markets(
        tmp_path,
        [current_long, current_short, future_long, future_short],
    )

    context = query_context(tmp_path)

    assert context.stats_90d.sample_count == 1


@pytest.mark.parametrize(
    ("size", "long_buy", "short_sell"),
    [
        (1_000, 10.0, 10.1),
        (5_000, 20.0, 20.2),
        (10_000, 30.0, 30.3),
    ],
)
def test_fixed_size_selects_only_its_executable_vwap_columns(
    tmp_path,
    size: int,
    long_buy: float,
    short_sell: float,
):
    long_snapshot, short_snapshot = make_pair(AS_OF)
    long_snapshot = long_snapshot.model_copy(
        update={
            "buy_1k_vwap": 10.0,
            "buy_5k_vwap": 20.0,
            "buy_10k_vwap": 30.0,
        }
    )
    short_snapshot = short_snapshot.model_copy(
        update={
            "sell_1k_vwap": 10.1,
            "sell_5k_vwap": 20.2,
            "sell_10k_vwap": 30.3,
        }
    )
    write_markets(tmp_path, [long_snapshot, short_snapshot])

    context = query_context(tmp_path, size=size)

    assert context.points_7d[0].raw_spread_bps == pytest.approx(
        (short_sell / long_buy - 1.0) * 10_000
    )


def test_unsupported_size_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="primary_size_usd"):
        query_context(tmp_path, size=2_000)


def test_complete_identity_filter_excludes_unrelated_rows(tmp_path):
    expected_long, expected_short = make_pair(AS_OF, long_buy=100.0, short_sell=101.0)
    wrong_long_symbol = make_market(
        "lighter",
        AS_OF,
        venue_symbol="BTC-PERP",
        buy_10k_vwap=1.0,
    )
    wrong_long_canonical = make_market(
        "lighter",
        AS_OF,
        canonical_symbol="ETH",
        buy_10k_vwap=2.0,
    )
    wrong_short_venue = make_market(
        "other",
        AS_OF,
        sell_10k_vwap=3.0,
    )
    wrong_short_symbol = make_market(
        "hyperliquid",
        AS_OF,
        venue_symbol="BTC-PERP",
        sell_10k_vwap=4.0,
    )
    write_markets(
        tmp_path,
        [
            expected_long,
            expected_short,
            wrong_long_symbol,
            wrong_long_canonical,
            wrong_short_venue,
            wrong_short_symbol,
        ],
    )

    context = query_context(tmp_path)

    assert len(context.points_7d) == 1
    assert context.points_7d[0].raw_spread_bps == pytest.approx(100.0)


@pytest.mark.parametrize("invalid_value", [None, 0.0, -1.0, math.nan, math.inf])
@pytest.mark.parametrize("side", ["long", "short"])
def test_invalid_executable_depth_is_dropped(tmp_path, invalid_value, side):
    long_snapshot, short_snapshot = make_pair(AS_OF)
    if side == "long":
        long_snapshot = long_snapshot.model_copy(
            update={"buy_10k_vwap": invalid_value}
        )
    else:
        short_snapshot = short_snapshot.model_copy(
            update={"sell_10k_vwap": invalid_value}
        )
    write_markets(tmp_path, [long_snapshot, short_snapshot])

    context = query_context(tmp_path)

    assert context.points_7d == ()
    assert context.stats_90d.sample_count == 0


def test_window_counts_and_medians_are_inclusive(tmp_path):
    points = [
        (AS_OF, 100.0),
        (AS_OF - timedelta(days=7), 200.0),
        (AS_OF - timedelta(days=30), 300.0),
        (AS_OF - timedelta(days=90), 400.0),
        (AS_OF - timedelta(days=91), 500.0),
    ]
    snapshots = [
        snapshot
        for sample_time, raw_spread in points
        for snapshot in make_pair(
            sample_time,
            long_buy=100.0,
            short_sell=100.0 + raw_spread / 100.0,
        )
    ]
    write_markets(tmp_path, snapshots)

    context = query_context(tmp_path)

    assert context.stats_7d.sample_count == 2
    assert context.stats_7d.median_raw_spread_bps == pytest.approx(150.0)
    assert context.stats_30d.sample_count == 3
    assert context.stats_30d.median_raw_spread_bps == pytest.approx(200.0)
    assert context.stats_90d.sample_count == 4
    assert context.stats_90d.median_raw_spread_bps == pytest.approx(250.0)
    assert [point.raw_spread_bps for point in context.points_7d] == pytest.approx(
        [200.0, 100.0]
    )


def test_empty_data_returns_empty_context_without_matching_files(tmp_path):
    context = SpreadHistory(tmp_path / "data").query(
        canonical_symbol="BTC",
        long_venue="lighter",
        long_venue_symbol="BTC",
        short_venue="hyperliquid",
        short_venue_symbol="BTC",
        primary_size_usd=10_000,
        as_of=AS_OF,
    )

    assert context == HistoricalSpreadContext.empty()


def test_as_of_is_normalized_to_utc(tmp_path):
    long_snapshot, short_snapshot = make_pair(AS_OF)
    write_markets(tmp_path, [long_snapshot, short_snapshot])

    context = query_context(
        tmp_path,
        as_of=AS_OF.astimezone(timezone(timedelta(hours=8))),
    )

    assert context.points_7d[0].sample_time == AS_OF


def test_repeated_query_has_no_current_fee_input(tmp_path):
    long_snapshot, short_snapshot = make_pair(AS_OF)
    write_markets(tmp_path, [long_snapshot, short_snapshot])

    history = SpreadHistory(tmp_path / "data")
    first = history.query(
        canonical_symbol="BTC",
        long_venue="lighter",
        long_venue_symbol="BTC",
        short_venue="hyperliquid",
        short_venue_symbol="BTC",
        primary_size_usd=10_000,
        as_of=AS_OF,
    )
    second = history.query(
        canonical_symbol="BTC",
        long_venue="lighter",
        long_venue_symbol="BTC",
        short_venue="hyperliquid",
        short_venue_symbol="BTC",
        primary_size_usd=10_000,
        as_of=AS_OF,
    )

    assert first == second
