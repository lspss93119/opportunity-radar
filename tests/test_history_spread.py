from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from radar.history.spread import HistoricalSpreadContext, SpreadHistory
from radar.config import MarketConfig, RadarConfig
from radar.models import MarketSnapshot
from radar.monitors.spread.models import SpreadPairKey
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


def write_legacy_markets(data_root: Path, snapshots: list[MarketSnapshot]) -> None:
    partition = data_root / "market" / f"date={snapshots[0].sample_time.date()}"
    partition.mkdir(parents=True)
    rows = []
    for snapshot in snapshots:
        row = snapshot.model_dump(mode="python")
        row.pop("price_source")
        rows.append(row)
    pq.write_table(pa.Table.from_pylist(rows), partition / "part-legacy.parquet")


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


def legacy_hydrate(
    snapshots: list[MarketSnapshot],
    *,
    as_of: datetime,
    window: timedelta,
    stale_after_seconds: int | None,
    rfq_stale_after_seconds: int | None,
) -> dict[SpreadPairKey, tuple[tuple[datetime, float], ...]]:
    """Reference implementation kept as an oracle for SQL hydration."""
    latest: dict[tuple[str, str, str, datetime], tuple[datetime, object, object]] = {}
    start = as_of - window
    for snapshot in snapshots:
        if not start <= snapshot.sample_time < as_of:
            continue
        if stale_after_seconds is not None:
            stale_limit = (
                rfq_stale_after_seconds
                if snapshot.price_source == "rfq"
                and rfq_stale_after_seconds is not None
                else stale_after_seconds
            )
            if (
                abs((snapshot.sample_time - snapshot.observed_at).total_seconds())
                > stale_limit
            ):
                continue
        key = (
            snapshot.venue,
            snapshot.venue_symbol,
            snapshot.canonical_symbol,
            snapshot.sample_time,
        )
        previous = latest.get(key)
        if previous is None or snapshot.observed_at > previous[0]:
            latest[key] = (
                snapshot.observed_at,
                snapshot.buy_10k_vwap,
                snapshot.sell_10k_vwap,
            )

    by_sample: dict[
        tuple[str, datetime], dict[tuple[str, str], tuple[object, object]]
    ] = {}
    for (venue, venue_symbol, symbol, sample_time), (
        _observed_at,
        buy_vwap,
        sell_vwap,
    ) in latest.items():
        by_sample.setdefault((symbol, sample_time), {})[(venue, venue_symbol)] = (
            buy_vwap,
            sell_vwap,
        )

    result: dict[SpreadPairKey, list[tuple[datetime, float]]] = {}
    for (symbol, sample_time), feeds in by_sample.items():
        for (long_venue, long_symbol), (long_buy, _long_sell) in feeds.items():
            if not isinstance(long_buy, (int, float)) or not math.isfinite(long_buy) or long_buy <= 0:
                continue
            for (short_venue, short_symbol), (_short_buy, short_sell) in feeds.items():
                if long_venue.lower() == short_venue.lower():
                    continue
                if not isinstance(short_sell, (int, float)) or not math.isfinite(short_sell) or short_sell <= 0:
                    continue
                spread = (short_sell / long_buy - 1.0) * 10_000.0
                if not math.isfinite(spread):
                    continue
                key = SpreadPairKey(
                    canonical_symbol=symbol,
                    long_venue=long_venue,
                    long_venue_symbol=long_symbol,
                    short_venue=short_venue,
                    short_venue_symbol=short_symbol,
                )
                result.setdefault(key, []).append((sample_time, spread))
    return {key: tuple(sorted(points)) for key, points in result.items()}


def test_exact_sample_time_join_uses_short_sell_over_long_buy(tmp_path):
    long_snapshot, short_snapshot = make_pair(AS_OF, long_buy=100.0, short_sell=101.0)
    write_markets(tmp_path, [long_snapshot, short_snapshot])

    context = query_context(tmp_path)

    assert len(context.points_7d) == 1
    assert context.points_7d[0].sample_time == AS_OF
    assert context.points_7d[0].raw_spread_bps == pytest.approx(100.0)


def test_query_handles_old_and_new_market_schema_partitions(tmp_path):
    old_time = AS_OF - timedelta(days=1)
    old_long, old_short = make_pair(old_time, long_buy=100.0, short_sell=101.0)
    new_long, new_short = make_pair(AS_OF, long_buy=100.0, short_sell=102.0)
    write_legacy_markets(tmp_path / "data", [old_long, old_short])
    write_markets(tmp_path, [new_long, new_short])

    context = query_context(tmp_path)

    assert [point.sample_time for point in context.points_7d] == [old_time, AS_OF]


@pytest.mark.parametrize("schema", ["old", "new", "mixed"])
def test_load_recent_pair_points_handles_all_schema_layouts(tmp_path, schema):
    old_time = AS_OF - timedelta(days=1)
    new_time = AS_OF - timedelta(seconds=10)
    old_pair = make_pair(old_time, long_buy=100.0, short_sell=101.0)
    new_pair = make_pair(new_time, long_buy=100.0, short_sell=102.0)
    data_root = tmp_path / schema

    if schema == "old":
        write_legacy_markets(data_root, [*old_pair, *new_pair])
    elif schema == "new":
        storage = ParquetStorage(data_root)
        for snapshot in (*old_pair, *new_pair):
            storage.append(snapshot)
        storage.flush(now=AS_OF)
    else:
        write_legacy_markets(data_root, list(old_pair))
        partition = data_root / "market" / f"date={new_time.date()}"
        partition.mkdir(parents=True, exist_ok=True)
        new_rows = [snapshot.model_dump(mode="python") for snapshot in new_pair]
        pq.write_table(pa.Table.from_pylist(new_rows), partition / "part-new.parquet")

    points = SpreadHistory(data_root).load_recent_pair_points(
        primary_size_usd=10_000,
        as_of=AS_OF,
    )
    key = SpreadPairKey("BTC", "lighter", "BTC", "hyperliquid", "BTC")
    reverse = SpreadPairKey("BTC", "hyperliquid", "BTC", "lighter", "BTC")
    assert [sample for sample, _ in points[key]] == [old_time, new_time]
    assert [sample for sample, _ in points[reverse]] == [old_time, new_time]
    assert points[key][0][1] == pytest.approx(100.0)
    assert points[key][1][1] == pytest.approx(200.0)


def test_load_recent_pair_points_matches_legacy_reference_for_fixture(tmp_path):
    start = AS_OF - timedelta(minutes=5)
    samples = [
        MarketSnapshot(
            **make_market(
                "lighter", start, buy_10k_vwap=100.0
            ).model_dump(mode="python")
        ),
    ]
    long_old, short_old = make_pair(
        AS_OF - timedelta(seconds=40), long_buy=100.0, short_sell=101.0
    )
    long_newer = long_old.model_copy(
        update={
            "observed_at": long_old.sample_time + timedelta(seconds=2),
            "buy_10k_vwap": 99.0,
        }
    )
    rfq_long = long_old.model_copy(
        update={
            "venue": "variational",
            "venue_symbol": "BTC-USD",
            "price_source": "rfq",
            "sample_time": AS_OF - timedelta(seconds=30),
            "observed_at": AS_OF - timedelta(seconds=120),
            "buy_10k_vwap": 98.0,
        }
    )
    rfq_short = short_old.model_copy(
        update={
            "venue": "arcus",
            "venue_symbol": "BTC-USD",
            "price_source": "rfq",
            "sample_time": rfq_long.sample_time,
            "observed_at": rfq_long.observed_at,
            "sell_10k_vwap": 103.0,
        }
    )
    stale_rfq = rfq_short.model_copy(
        update={
            "sample_time": AS_OF - timedelta(seconds=20),
            "observed_at": AS_OF - timedelta(seconds=141),
        }
    )
    same_venue = short_old.model_copy(
        update={
            "venue": "LIGHTER",
            "sample_time": AS_OF - timedelta(seconds=20),
            "sell_10k_vwap": 110.0,
        }
    )
    at_as_of = short_old.model_copy(update={"sample_time": AS_OF})
    samples.extend(
        [
            long_old,
            short_old,
            long_newer,
            rfq_long,
            rfq_short,
            stale_rfq,
            same_venue,
            at_as_of,
        ]
    )
    write_markets(tmp_path, samples)

    expected = legacy_hydrate(
        samples,
        as_of=AS_OF,
        window=timedelta(minutes=5),
        stale_after_seconds=30,
        rfq_stale_after_seconds=120,
    )
    actual = SpreadHistory(tmp_path / "data").load_recent_pair_points(
        primary_size_usd=10_000,
        as_of=AS_OF,
        window=timedelta(minutes=5),
        stale_after_seconds=30,
        rfq_stale_after_seconds=120,
    )

    assert set(actual) == set(expected)
    for key in expected:
        assert [sample for sample, _ in actual[key]] == [sample for sample, _ in expected[key]]
        assert [value for _, value in actual[key]] == pytest.approx(
            [value for _, value in expected[key]], abs=1e-9
        )


def test_build_application_hydration_old_schema_matches_new_schema(tmp_path):
    from radar.app import build_application

    old_root = tmp_path / "old-data"
    new_root = tmp_path / "new-data"
    runtime_root = tmp_path / "runtime"
    sample_time = AS_OF - timedelta(seconds=10)
    snapshots = list(make_pair(sample_time, long_buy=100.0, short_sell=101.0))
    write_legacy_markets(old_root, snapshots)
    new_storage = ParquetStorage(new_root)
    for snapshot in snapshots:
        new_storage.append(snapshot)
    new_storage.flush(now=AS_OF)
    config = RadarConfig(
        markets=[
            MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
            MarketConfig(
                venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
            ),
        ],
        monitors={"spread": {"primary_size_usd": 10_000}},
    )

    def hydrated(root: Path, runtime_db: Path):
        app = build_application(
            config,
            data_root=root,
            runtime_db=runtime_db,
            telegram_credentials=("token-not-logged", "chat-id"),
            clock=lambda: AS_OF,
        )
        try:
            monitor = next(item for item in app.monitor_runner.monitors if item.name == "spread")
            key = SpreadPairKey("BTC", "lighter", "BTC", "hyperliquid", "BTC")
            return dict(monitor._basis_by_key[key]._values)
        finally:
            app.runtime_store.close()

    assert hydrated(old_root, runtime_root / "old.sqlite3") == hydrated(
        new_root, runtime_root / "new.sqlite3"
    )


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


def test_load_recent_pair_points_hydrates_all_directional_pairs_and_excludes_asof(
    tmp_path,
):
    sample_time = AS_OF - timedelta(seconds=10)
    long_snapshot, short_snapshot = make_pair(
        sample_time,
        long_buy=100.0,
        short_sell=101.0,
    )
    duplicate_long = long_snapshot.model_copy(
        update={
            "observed_at": sample_time + timedelta(seconds=1),
            "buy_10k_vwap": 99.0,
        }
    )
    current_long, current_short = make_pair(AS_OF, long_buy=50.0, short_sell=75.0)
    write_markets(
        tmp_path,
        [long_snapshot, short_snapshot, duplicate_long, current_long, current_short],
    )

    points = SpreadHistory(tmp_path / "data").load_recent_pair_points(
        primary_size_usd=10_000,
        as_of=AS_OF,
    )

    key = SpreadPairKey("BTC", "lighter", "BTC", "hyperliquid", "BTC")
    assert points[key][0][0] == sample_time
    assert points[key][0][1] == pytest.approx((101.0 / 99.0 - 1.0) * 10_000)
    assert all(point[0] < AS_OF for values in points.values() for point in values)


def test_load_recent_pair_points_can_limit_to_spread_monitor_feeds(tmp_path):
    sample_time = AS_OF - timedelta(seconds=10)
    long_snapshot, short_snapshot = make_pair(sample_time)
    extra = make_market("backpack", sample_time)
    write_markets(tmp_path, [long_snapshot, short_snapshot, extra])
    history = SpreadHistory(tmp_path / "data")

    all_points = history.load_recent_pair_points(
        primary_size_usd=10_000,
        as_of=AS_OF,
    )
    scoped_points = history.load_recent_pair_points(
        primary_size_usd=10_000,
        as_of=AS_OF,
        allowed_feeds={
            ("lighter", "BTC", "BTC"),
            ("hyperliquid", "BTC", "BTC"),
        },
    )

    expected = SpreadPairKey("BTC", "lighter", "BTC", "hyperliquid", "BTC")
    assert set(scoped_points) == {expected, SpreadPairKey("BTC", "hyperliquid", "BTC", "lighter", "BTC")}
    assert scoped_points[expected] == all_points[expected]
    assert all("backpack" not in {key.long_venue, key.short_venue} for key in scoped_points)


def test_v2_hydration_applies_optional_observation_skew_validation(tmp_path):
    sample_time = AS_OF - timedelta(seconds=10)
    long_snapshot, short_snapshot = make_pair(
        sample_time,
        observed_at=sample_time + timedelta(seconds=6),
    )
    write_markets(tmp_path, [long_snapshot, short_snapshot])
    history = SpreadHistory(tmp_path / "data")
    key = SpreadPairKey("BTC", "lighter", "BTC", "hyperliquid", "BTC")

    legacy = history.load_recent_pair_points(
        primary_size_usd=10_000,
        as_of=AS_OF,
    )
    strict = history.load_recent_pair_points(
        primary_size_usd=10_000,
        as_of=AS_OF,
        stale_after_seconds=5,
    )

    assert key in legacy
    assert key not in strict


def test_v2_hydration_keeps_latest_valid_duplicate_and_pair_identity(tmp_path):
    sample_time = AS_OF - timedelta(seconds=10)
    long_snapshot, short_snapshot = make_pair(
        sample_time,
        observed_at=sample_time + timedelta(seconds=1),
        long_buy=100.0,
        short_sell=101.0,
    )
    newer_long = long_snapshot.model_copy(
        update={
            "observed_at": sample_time + timedelta(seconds=2),
            "buy_10k_vwap": 99.0,
        }
    )
    write_markets(tmp_path, [long_snapshot, short_snapshot, newer_long])

    points = SpreadHistory(tmp_path / "data").load_recent_pair_points(
        primary_size_usd=10_000,
        as_of=AS_OF,
        stale_after_seconds=5,
    )

    key = SpreadPairKey("BTC", "lighter", "BTC", "hyperliquid", "BTC")
    assert key in points
    assert points[key][0][0] == sample_time
    assert points[key][0][1] == pytest.approx((101.0 / 99.0 - 1.0) * 10_000)


def test_v2_hydration_mean_matches_replay_mean_on_same_fixture(tmp_path):
    from radar.history.anomaly_replay import _ContextHistory

    values = [100.0, 101.0, 99.5, 102.0, 100.5]
    snapshots: list[MarketSnapshot] = []
    for index, raw_spread in enumerate(values, start=1):
        sample_time = AS_OF - timedelta(seconds=index * 10)
        long_snapshot, short_snapshot = make_pair(
            sample_time,
            long_buy=100.0,
            short_sell=100.0 * (1.0 + raw_spread / 10_000.0),
            observed_at=sample_time + timedelta(seconds=1),
        )
        snapshots.extend((long_snapshot, short_snapshot))
    write_markets(tmp_path, snapshots)

    key = SpreadPairKey("BTC", "lighter", "BTC", "hyperliquid", "BTC")
    points = SpreadHistory(tmp_path / "data").load_recent_pair_points(
        primary_size_usd=10_000,
        as_of=AS_OF,
        window=timedelta(seconds=60),
        stale_after_seconds=5,
    )
    replay = _ContextHistory()
    for sample_time, raw_spread in points[key]:
        replay.append(sample_time, raw_spread)

    live_mean = sum(raw_spread for _, raw_spread in points[key]) / len(points[key])
    replay_mean = replay.stats_before(AS_OF, 60).mean_bps

    assert replay_mean == pytest.approx(live_mean)
