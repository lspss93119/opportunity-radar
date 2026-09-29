from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from radar.config import MarketConfig, MonitorConfig, RadarConfig, SpreadMonitorConfig
from radar.dashboard_data import DashboardQueryService, OpportunitiesFilters
from radar.models import MarketSnapshot
from radar.monitors.spread.basis import MIN_HISTORY_OBSERVATIONS
from radar.storage.parquet import ParquetStorage
from radar.storage.sqlite import SQLiteRuntimeStore

NOW = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
LONG_VENUE = "lighter"
SHORT_VENUE = "hyperliquid"


def make_config(
    *markets: MarketConfig,
    fees_bps: dict[str, float] | None = None,
    stale_after_seconds: int = 30,
) -> RadarConfig:
    return RadarConfig(
        markets=list(markets),
        fees_bps={} if fees_bps is None else fees_bps,
        monitors=MonitorConfig(
            spread=SpreadMonitorConfig(stale_after_seconds=stale_after_seconds)
        ),
    )


def make_market(
    *,
    sample_time: datetime = NOW,
    observed_at: datetime = NOW,
    venue: str = LONG_VENUE,
    venue_symbol: str = "BTC",
    canonical_symbol: str = "BTC",
    buy_10k_vwap: float | None = 100.0,
    sell_10k_vwap: float | None = 101.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=observed_at,
        venue=venue,
        venue_symbol=venue_symbol,
        canonical_symbol=canonical_symbol,
        best_bid=99.0,
        best_bid_size=2.0,
        best_ask=100.0,
        best_ask_size=3.0,
        buy_10k_vwap=buy_10k_vwap,
        sell_10k_vwap=sell_10k_vwap,
    )


def write_markets(
    data_root: Path,
    snapshots: list[MarketSnapshot],
    *,
    now: datetime = NOW,
) -> None:
    storage = ParquetStorage(data_root)
    for snapshot in snapshots:
        storage.append(snapshot)
    storage.flush(now=now)


def write_episodes(
    runtime_db: Path,
    episodes: dict[str, object],
    *,
    updated_at: datetime = NOW,
) -> None:
    with SQLiteRuntimeStore(runtime_db) as store:
        store.set_monitor_state(
            "spread",
            "episodes",
            episodes,
            updated_at=updated_at,
        )


def pair_markets(
    *,
    sample_time: datetime = NOW,
    observed_at: datetime = NOW,
    symbol: str = "BTC",
    long_venue: str = LONG_VENUE,
    short_venue: str = SHORT_VENUE,
    long_symbol: str | None = None,
    short_symbol: str | None = None,
    long_buy: float | None = 100.0,
    long_sell: float | None = 101.0,
    short_buy: float | None = 100.0,
    short_sell: float | None = 101.0,
) -> list[MarketSnapshot]:
    return [
        make_market(
            sample_time=sample_time,
            observed_at=observed_at,
            venue=long_venue,
            venue_symbol=long_symbol or symbol,
            canonical_symbol=symbol,
            buy_10k_vwap=long_buy,
            sell_10k_vwap=long_sell,
        ),
        make_market(
            sample_time=sample_time,
            observed_at=observed_at,
            venue=short_venue,
            venue_symbol=short_symbol or symbol,
            canonical_symbol=symbol,
            buy_10k_vwap=short_buy,
            sell_10k_vwap=short_sell,
        ),
    ]


def pair_config(
    *,
    symbol: str = "BTC",
    long_venue: str = LONG_VENUE,
    short_venue: str = SHORT_VENUE,
    long_symbol: str | None = None,
    short_symbol: str | None = None,
    fees_bps: dict[str, float] | None = None,
    stale_after_seconds: int = 30,
) -> RadarConfig:
    return make_config(
        MarketConfig(
            venue=long_venue,
            venue_symbol=long_symbol or symbol,
            canonical_symbol=symbol,
        ),
        MarketConfig(
            venue=short_venue,
            venue_symbol=short_symbol or symbol,
            canonical_symbol=symbol,
        ),
        fees_bps=fees_bps or {long_venue: 1.0, short_venue: 2.0},
        stale_after_seconds=stale_after_seconds,
    )


def write_prior_pair_history(
    data_root: Path,
    *,
    prior_times: list[datetime],
    symbol: str = "BTC",
    long_venue: str = LONG_VENUE,
    short_venue: str = SHORT_VENUE,
    long_symbol: str | None = None,
    short_symbol: str | None = None,
    current_raw_spread_bps: float = 500.0,
    current_sample_time: datetime = NOW,
    prior_raw_spreads: tuple[float, float] = (100.0, 300.0),
) -> None:
    snapshots: list[MarketSnapshot] = []
    for index, sample_time in enumerate(prior_times):
        raw_spread_bps = prior_raw_spreads[index % 2]
        snapshots.extend(
            pair_markets(
                sample_time=sample_time,
                observed_at=sample_time,
                symbol=symbol,
                long_venue=long_venue,
                short_venue=short_venue,
                long_symbol=long_symbol,
                short_symbol=short_symbol,
                long_buy=100.0,
                short_sell=100.0 * (1.0 + raw_spread_bps / 10_000.0),
            )
        )
    snapshots.extend(
        pair_markets(
            sample_time=current_sample_time,
            observed_at=NOW - timedelta(seconds=2),
            symbol=symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            long_symbol=long_symbol,
            short_symbol=short_symbol,
            long_buy=100.0,
            short_sell=100.0 * (1.0 + current_raw_spread_bps / 10_000.0),
        )
    )
    write_markets(data_root, snapshots)


def full_window_times(*, count: int = MIN_HISTORY_OBSERVATIONS) -> list[datetime]:
    all_times = [NOW - timedelta(days=1) + timedelta(seconds=10 * index) for index in range(8640)]
    return [sample_time for index, sample_time in enumerate(all_times) if index % 5 != 1][:count]


def make_episode(
    *,
    symbol: str = "BTC",
    long_venue: str = LONG_VENUE,
    short_venue: str = SHORT_VENUE,
    long_symbol: str | None = None,
    short_symbol: str | None = None,
    alert_condition_since: datetime | None = NOW - timedelta(seconds=75),
    last_seen_at: datetime = NOW - timedelta(seconds=2),
    candidate_confirmed: bool = True,
    alerted: bool = True,
) -> dict[str, object]:
    return {
        "episode_id": f"{symbol}:episode",
        "key": {
            "canonical_symbol": symbol,
            "long_venue": long_venue,
            "long_venue_symbol": long_symbol or symbol,
            "short_venue": short_venue,
            "short_venue_symbol": short_symbol or symbol,
        },
        "first_seen_at": (NOW - timedelta(seconds=90)).isoformat(),
        "last_seen_at": last_seen_at.isoformat(),
        "candidate": {
            "sample_time": last_seen_at.isoformat(),
            "long_buy_vwap": 100.0,
            "short_sell_vwap": 105.0,
            "long_fee_bps": 1.0,
            "short_fee_bps": 2.0,
            "raw_spread_bps": 500.0,
            "net_spread_bps": 497.0,
        },
        "candidate_confirmed": candidate_confirmed,
        "candidate_confirmed_at": (
            (NOW - timedelta(seconds=60)).isoformat()
            if candidate_confirmed
            else None
        ),
        "alert_condition_since": (
            alert_condition_since.isoformat() if alert_condition_since else None
        ),
        "alerted": alerted,
    }


def test_latest_feed_row_uses_newest_observed_at_for_duplicate_sample_time(tmp_path):
    config = make_config(
        MarketConfig(venue=LONG_VENUE, venue_symbol="BTC", canonical_symbol="BTC")
    )
    sample_time = NOW - timedelta(seconds=5)
    write_markets(
        tmp_path / "data",
        [
            make_market(
                sample_time=sample_time,
                observed_at=NOW - timedelta(seconds=20),
                buy_10k_vwap=90.0,
            ),
            make_market(
                sample_time=sample_time,
                observed_at=NOW - timedelta(seconds=3),
                buy_10k_vwap=100.0,
            ),
        ],
    )

    status = DashboardQueryService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_status()

    feed = status["feeds"][0]
    assert feed["sample_time"] == sample_time.isoformat()
    assert feed["observed_at"] == (NOW - timedelta(seconds=3)).isoformat()
    assert feed["buy_10k_vwap"] == 100.0


def test_opportunity_requires_exact_sample_time_and_distinct_venues(tmp_path):
    mismatch_root = tmp_path / "mismatch"
    mismatch_config = pair_config()
    write_markets(
        mismatch_root / "data",
        [
            make_market(
                sample_time=NOW,
                observed_at=NOW - timedelta(seconds=1),
                venue=LONG_VENUE,
                venue_symbol="BTC",
                canonical_symbol="BTC",
                buy_10k_vwap=100.0,
            ),
        ],
    )
    write_markets(
        mismatch_root / "data",
        [
            make_market(
                sample_time=NOW - timedelta(seconds=10),
                observed_at=NOW - timedelta(seconds=1),
                venue=SHORT_VENUE,
                venue_symbol="BTC",
                canonical_symbol="BTC",
                sell_10k_vwap=101.0,
            )
        ],
    )
    mismatch_result = DashboardQueryService(
        mismatch_config,
        data_root=mismatch_root / "data",
        runtime_db=mismatch_root / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_opportunities(OpportunitiesFilters(), now=NOW)
    assert mismatch_result["rows"] == []

    same_venue_root = tmp_path / "same-venue"
    same_venue_config = make_config(
        MarketConfig(venue=LONG_VENUE, venue_symbol="BTC", canonical_symbol="BTC"),
        MarketConfig(venue=LONG_VENUE, venue_symbol="BTC-PERP", canonical_symbol="BTC"),
        fees_bps={LONG_VENUE: 1.0},
    )
    write_markets(
        same_venue_root / "data",
        pair_markets(
            sample_time=NOW,
            long_venue=LONG_VENUE,
            short_venue=LONG_VENUE,
            long_symbol="BTC",
            short_symbol="BTC-PERP",
        ),
    )
    same_venue_result = DashboardQueryService(
        same_venue_config,
        data_root=same_venue_root / "data",
        runtime_db=same_venue_root / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_opportunities(OpportunitiesFilters(), now=NOW)
    assert same_venue_result["rows"] == []

    exact_root = tmp_path / "exact"
    write_markets(exact_root / "data", pair_markets(sample_time=NOW))
    exact_service = DashboardQueryService(
        mismatch_config,
        data_root=exact_root / "data",
        runtime_db=exact_root / "runtime.sqlite3",
        clock=lambda: NOW,
    )
    exact_pair = exact_service.get_pair(
        canonical_symbol="BTC",
        long_venue=LONG_VENUE,
        long_venue_symbol="BTC",
        short_venue=SHORT_VENUE,
        short_venue_symbol="BTC",
        range_name="1h",
        now=NOW,
    )
    assert exact_pair["identity"] == {
        "canonical_symbol": "BTC",
        "long_venue": LONG_VENUE,
        "long_venue_symbol": "BTC",
        "short_venue": SHORT_VENUE,
        "short_venue_symbol": "BTC",
    }
    assert exact_pair["current"]["sample_time"] == NOW.isoformat()


def test_missing_vwap_stale_future_and_missing_fee_fail_closed(tmp_path):
    scenarios = (
        (
            "missing-vwap",
            pair_markets(long_buy=None, long_sell=None),
            pair_config(),
        ),
        (
            "stale",
            pair_markets(observed_at=NOW - timedelta(seconds=31)),
            pair_config(),
        ),
        (
            "future",
            pair_markets(sample_time=NOW + timedelta(seconds=1)),
            pair_config(),
        ),
        (
            "missing-fee",
            pair_markets(),
            pair_config(fees_bps={LONG_VENUE: 1.0}),
        ),
    )
    for name, snapshots, config in scenarios:
        root = tmp_path / name
        write_markets(root / "data", snapshots)
        result = DashboardQueryService(
            config,
            data_root=root / "data",
            runtime_db=root / "runtime.sqlite3",
            clock=lambda: NOW,
        ).get_opportunities(OpportunitiesFilters(), now=NOW)
        assert result["rows"] == [], name


def test_latest_stale_exact_pair_sample_does_not_carry_forward_older_data(tmp_path):
    config = pair_config()
    write_markets(
        tmp_path / "data",
        pair_markets(
            sample_time=NOW - timedelta(seconds=10),
            observed_at=NOW - timedelta(seconds=1),
        )
        + pair_markets(
            sample_time=NOW,
            observed_at=NOW - timedelta(seconds=31),
        ),
    )

    result = DashboardQueryService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_opportunities(OpportunitiesFilters(), now=NOW)

    assert result["rows"] == []


def test_prior_only_basis_excludes_current_observation_and_uses_population_std(tmp_path):
    config = pair_config()
    write_prior_pair_history(tmp_path / "data", prior_times=full_window_times())

    result = DashboardQueryService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_opportunities(
        OpportunitiesFilters(long_venue=LONG_VENUE, short_venue=SHORT_VENUE),
        now=NOW,
    )

    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["current_raw_spread_bps"] == pytest.approx(500.0)
    assert row["rolling_mean_bps"] == pytest.approx(200.0)
    assert row["rolling_std_bps"] == pytest.approx(100.0)
    assert row["deviation_bps"] == pytest.approx(300.0)
    assert row["round_trip_fee_bps"] == pytest.approx(6.0)
    assert row["theoretical_edge_bps"] == pytest.approx(294.0)
    assert row["basis_eligible"] is True


def test_basis_requires_full_window_and_eighty_percent_coverage(tmp_path):
    below_min_root = tmp_path / "below-min"
    write_prior_pair_history(
        below_min_root / "data",
        prior_times=full_window_times(count=MIN_HISTORY_OBSERVATIONS - 1),
    )
    below_min = DashboardQueryService(
        pair_config(),
        data_root=below_min_root / "data",
        runtime_db=below_min_root / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_pair(
        canonical_symbol="BTC",
        long_venue=LONG_VENUE,
        long_venue_symbol="BTC",
        short_venue=SHORT_VENUE,
        short_venue_symbol="BTC",
        range_name="24h",
        now=NOW,
    )
    assert below_min["basis"]["eligible"] is False
    assert below_min["basis"]["mean_bps"] is None

    short_window_root = tmp_path / "short-window"
    short_start = NOW - timedelta(hours=20)
    short_times = [short_start + timedelta(seconds=10 * index) for index in range(MIN_HISTORY_OBSERVATIONS)]
    write_prior_pair_history(short_window_root / "data", prior_times=short_times)
    short_window = DashboardQueryService(
        pair_config(),
        data_root=short_window_root / "data",
        runtime_db=short_window_root / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_pair(
        canonical_symbol="BTC",
        long_venue=LONG_VENUE,
        long_venue_symbol="BTC",
        short_venue=SHORT_VENUE,
        short_venue_symbol="BTC",
        range_name="24h",
        now=NOW,
    )
    assert short_window["basis"]["eligible"] is False
    assert short_window["basis"]["coverage"] == 0.0


def test_pair_history_downsampling_preserves_first_latest_and_local_extrema(tmp_path):
    start = NOW - timedelta(hours=1)
    snapshots: list[MarketSnapshot] = []
    for index in range(1_000):
        sample_time = start + timedelta(seconds=3 * index)
        raw_spread_bps = 100.0 if index == 501 else 200.0
        snapshots.extend(
            pair_markets(
                sample_time=sample_time,
                observed_at=sample_time,
                long_buy=100.0,
                short_sell=100.0 * (1.0 + raw_spread_bps / 10_000.0),
            )
        )
    write_markets(tmp_path / "data", snapshots)

    result = DashboardQueryService(
        pair_config(),
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_pair(
        canonical_symbol="BTC",
        long_venue=LONG_VENUE,
        long_venue_symbol="BTC",
        short_venue=SHORT_VENUE,
        short_venue_symbol="BTC",
        range_name="1h",
        now=NOW,
    )

    history = result["history"]
    assert len(history) <= 500
    assert history[0]["sample_time"] == start.isoformat()
    assert history[-1]["sample_time"] == (start + timedelta(seconds=3 * 999)).isoformat()
    assert any(
        point["sample_time"] == (start + timedelta(seconds=3 * 501)).isoformat()
        and point["raw_spread_bps"] == pytest.approx(100.0)
        for point in history
    )


def test_data_as_of_is_latest_dataset_sample_time(tmp_path):
    config = make_config(
        MarketConfig(venue=LONG_VENUE, venue_symbol="BTC", canonical_symbol="BTC")
    )
    latest_sample = NOW - timedelta(seconds=7)
    write_markets(
        tmp_path / "data",
        [make_market(sample_time=NOW - timedelta(seconds=20)), make_market(sample_time=latest_sample)],
    )

    result = DashboardQueryService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_status(now=NOW)

    assert result["data_as_of"] == latest_sample.isoformat()
    assert result["generated_at"] == NOW.isoformat()


def test_persisted_episode_controls_signal_duration_and_alert_state_without_monitor_evaluation(
    tmp_path, monkeypatch
):
    from radar.monitors.spread.monitor import SpreadMonitor

    async def fail_if_evaluated(*args, **kwargs):
        raise AssertionError("dashboard must not evaluate SpreadMonitor")

    monkeypatch.setattr(SpreadMonitor, "evaluate", fail_if_evaluated)
    config = pair_config()
    write_markets(tmp_path / "data", pair_markets())
    write_episodes(
        tmp_path / "runtime.sqlite3",
        {"episode": make_episode()},
    )
    service = DashboardQueryService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    )

    persisted = service.get_pair(
        canonical_symbol="BTC",
        long_venue=LONG_VENUE,
        long_venue_symbol="BTC",
        short_venue=SHORT_VENUE,
        short_venue_symbol="BTC",
        range_name="1h",
        now=NOW,
    )
    assert persisted["lifecycle"]["active"] is True
    assert persisted["lifecycle"]["alerted"] is True
    assert persisted["lifecycle"]["candidate_confirmed"] is True
    assert persisted["lifecycle"]["signal_duration_seconds"] == 75

    missing = DashboardQueryService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "missing.sqlite3",
        clock=lambda: NOW,
    ).get_pair(
        canonical_symbol="BTC",
        long_venue=LONG_VENUE,
        long_venue_symbol="BTC",
        short_venue=SHORT_VENUE,
        short_venue_symbol="BTC",
        range_name="1h",
        now=NOW,
    )
    assert missing["lifecycle"]["active"] is False
    assert missing["lifecycle"]["signal_duration_seconds"] is None
    assert missing["lifecycle"]["available"] is False


def test_sqlite_missing_or_unreadable_database_does_not_create_file(tmp_path):
    missing_db = tmp_path / "missing" / "runtime.sqlite3"
    config = make_config()
    missing_status = DashboardQueryService(
        config,
        data_root=tmp_path / "data",
        runtime_db=missing_db,
        clock=lambda: NOW,
    ).get_status(now=NOW)
    assert not missing_db.exists()
    assert missing_status["sqlite"]["status"] == "unavailable"
    assert missing_status["errors"]

    unreadable_db = tmp_path / "unreadable.sqlite3"
    unreadable_db.write_bytes(b"not a sqlite database")
    unreadable_status = DashboardQueryService(
        config,
        data_root=tmp_path / "data",
        runtime_db=unreadable_db,
        clock=lambda: NOW,
    ).get_status(now=NOW)
    assert unreadable_status["sqlite"]["status"] == "unavailable"
    assert unreadable_db.read_bytes() == b"not a sqlite database"


def test_empty_parquet_and_temporary_partition_are_degraded_without_being_read_as_data(
    tmp_path,
):
    partition = tmp_path / "data" / "market" / f"date={NOW.date().isoformat()}"
    partition.mkdir(parents=True)
    (partition / "part-empty.parquet").write_bytes(b"")
    (partition / "part-temporary.parquet.tmp").write_bytes(b"not parquet")
    (partition / ".part-hidden.parquet").write_bytes(b"not parquet")

    status = DashboardQueryService(
        make_config(
            MarketConfig(venue=LONG_VENUE, venue_symbol="BTC", canonical_symbol="BTC")
        ),
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_status(now=NOW)

    assert status["overall"]["status"] == "down"
    assert status["feeds"][0]["sample_time"] is None
    assert any("market data read failed" in error for error in status["errors"])


def test_cache_key_separates_filters_and_expiry_recomputes_result(tmp_path):
    current_time = [NOW]
    data_root = tmp_path / "data"
    config = make_config(
        MarketConfig(venue=LONG_VENUE, venue_symbol="BTC", canonical_symbol="BTC"),
        MarketConfig(venue=SHORT_VENUE, venue_symbol="BTC", canonical_symbol="BTC"),
        MarketConfig(venue=LONG_VENUE, venue_symbol="ETH", canonical_symbol="ETH"),
        MarketConfig(venue=SHORT_VENUE, venue_symbol="ETH", canonical_symbol="ETH"),
        fees_bps={LONG_VENUE: 1.0, SHORT_VENUE: 2.0},
    )
    write_markets(data_root, pair_markets(symbol="BTC"))
    service = DashboardQueryService(
        config,
        data_root=data_root,
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: current_time[0],
        cache_ttl_seconds=5.0,
    )

    directional_filters = OpportunitiesFilters(
        long_venue=LONG_VENUE,
        short_venue=SHORT_VENUE,
    )
    all_rows = service.get_opportunities(directional_filters, now=NOW)
    assert [row["canonical_symbol"] for row in all_rows["rows"]] == ["BTC"]
    assert service.get_opportunities(
        OpportunitiesFilters(
            symbol="ETH",
            long_venue=LONG_VENUE,
            short_venue=SHORT_VENUE,
        ),
        now=NOW,
    )["rows"] == []

    write_markets(
        data_root,
        pair_markets(symbol="ETH", observed_at=NOW),
    )
    assert service.get_opportunities(
        OpportunitiesFilters(
            symbol="ETH",
            long_venue=LONG_VENUE,
            short_venue=SHORT_VENUE,
        ),
        now=NOW,
    )["rows"] == []

    current_time[0] = NOW + timedelta(seconds=6)
    refreshed = service.get_opportunities(
        OpportunitiesFilters(
            symbol="ETH",
            long_venue=LONG_VENUE,
            short_venue=SHORT_VENUE,
        ),
        now=current_time[0],
    )
    assert [row["canonical_symbol"] for row in refreshed["rows"]] == ["ETH"]


def test_episode_fixture_is_json_serializable():
    # Keep the fixture contract explicit: production runtime state is JSON.
    json.dumps(make_episode(), allow_nan=False)


def query_pair(service, range_name="1h", *, now=NOW):
    return service.get_pair(
        canonical_symbol="BTC",
        long_venue=LONG_VENUE,
        long_venue_symbol="BTC",
        short_venue=SHORT_VENUE,
        short_venue_symbol="BTC",
        range_name=range_name,
        now=now,
    )


def test_status_with_active_episode_is_json_and_reports_latest_sample_age(tmp_path):
    write_markets(tmp_path / "data", pair_markets(sample_time=NOW - timedelta(seconds=10)))
    write_episodes(tmp_path / "runtime.sqlite3", {"episode": make_episode()})
    service = DashboardQueryService(
        pair_config(), data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW,
    )
    status = service.get_status()
    decoded = json.loads(json.dumps(status, allow_nan=False))
    assert decoded["episodes"][0]["alert_condition_since"] == (NOW - timedelta(seconds=75)).isoformat()
    assert status["sample_age_seconds"] == 10.0


def test_pair_rolling_series_has_prior_padding_and_current_stats_ignore_range(tmp_path):
    shifted_times = [time - timedelta(hours=1) for time in full_window_times()]
    write_prior_pair_history(tmp_path / "data", prior_times=shifted_times)
    write_markets(tmp_path / "data", pair_markets(
        sample_time=NOW - timedelta(hours=1), observed_at=NOW - timedelta(hours=1),
    ))
    write_episodes(tmp_path / "runtime.sqlite3", {})
    service = DashboardQueryService(
        pair_config(), data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW,
    )
    short = query_pair(service)
    longer = query_pair(service, "3d")
    assert short["basis"] == longer["basis"]
    first = short["rolling_mean_series"][0]
    assert first["sample_time"] == (NOW - timedelta(hours=1)).isoformat()
    assert first["rolling_mean_bps"] == pytest.approx(200.0)


def test_scanner_basis_window_is_anchored_to_current_sample_not_request_time(tmp_path):
    current_sample = NOW - timedelta(seconds=10)
    times = [time - timedelta(seconds=10) for time in full_window_times()]
    write_prior_pair_history(
        tmp_path / "data", prior_times=times, current_sample_time=current_sample,
    )
    service = DashboardQueryService(
        pair_config(), data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW,
    )
    result = service.get_opportunities(OpportunitiesFilters(long_venue=LONG_VENUE))
    assert result["rows"][0]["basis_eligible"] is True
    assert result["rows"][0]["rolling_mean_bps"] == pytest.approx(200.0)


@pytest.mark.parametrize("bad_input", ["stale", "future-observed"])
def test_invalid_historical_observation_cannot_qualify_basis(tmp_path, bad_input):
    times = full_window_times()
    write_prior_pair_history(tmp_path / "data", prior_times=times)
    # Replace the newest duplicate for the oldest required observation.
    # An observation 31 seconds old cannot be rescued by its partner's timestamp.
    if bad_input == "stale":
        snapshots = pair_markets(
            sample_time=times[0], observed_at=times[0] - timedelta(seconds=31),
        )
        # Use a separate dataset so greatest-observed_at dedup does not hide it.
        root = tmp_path / "stale-data"
        write_prior_pair_history(root, prior_times=times[1:])
        write_markets(root, snapshots)
    else:
        root = tmp_path / "data"
        write_markets(root, pair_markets(
            sample_time=times[0], observed_at=NOW + timedelta(seconds=1),
        ))
    result = query_pair(DashboardQueryService(
        pair_config(), data_root=root,
        runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW,
    ))
    assert result["basis"]["eligible"] is False
    assert result["basis"]["mean_bps"] is None


@pytest.mark.parametrize("bad_field,value", [
    ("last_seen_at", None),
    ("last_seen_at", (NOW + timedelta(seconds=1)).isoformat()),
    ("alert_condition_since", (NOW + timedelta(seconds=1)).isoformat()),
    ("candidate_confirmed", "yes"),
    ("alerted", "yes"),
])
def test_malformed_persisted_lifecycle_is_unknown(tmp_path, bad_field, value):
    episode = make_episode()
    episode[bad_field] = value
    write_episodes(tmp_path / "runtime.sqlite3", {"episode": episode})
    write_markets(tmp_path / "data", pair_markets())
    result = query_pair(DashboardQueryService(
        pair_config(), data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW,
    ))
    assert result["lifecycle"]["available"] is False
    assert result["lifecycle"]["signal_duration_seconds"] is None
    assert result["errors"]


def test_empty_dataset_with_healthy_heartbeat_is_degraded(tmp_path):
    write_episodes(tmp_path / "runtime.sqlite3", {})
    service = DashboardQueryService(
        pair_config(), data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW,
    )
    result = service.get_opportunities(OpportunitiesFilters())
    assert result["status"] == "degraded"
    assert result["rows"] == []


def test_current_point_remains_visible_when_observed_after_sample(tmp_path):
    sample = NOW - timedelta(seconds=10)
    write_markets(tmp_path / "data", pair_markets(sample_time=sample))
    result = query_pair(DashboardQueryService(
        pair_config(), data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW,
    ))
    assert result["current"]["sample_time"] == sample.isoformat()
    assert result["history"][-1]["sample_time"] == sample.isoformat()


def test_latest_cycle_missing_feed_is_visible_in_venue_counts(tmp_path):
    write_markets(tmp_path / "data", [
        make_market(),
        make_market(venue=SHORT_VENUE, sample_time=NOW - timedelta(seconds=10)),
    ])
    write_episodes(tmp_path / "runtime.sqlite3", {})
    status = DashboardQueryService(
        pair_config(), data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW,
    ).get_status()
    assert status["venues"][SHORT_VENUE]["latest"] == 0
    assert status["venues"][SHORT_VENUE]["missing"] == 1
    assert status["overall"]["latest_feeds"] == 1


def test_fee_arithmetic_overflow_fails_closed(tmp_path):
    write_markets(tmp_path / "data", pair_markets())
    result = DashboardQueryService(
        pair_config(fees_bps={LONG_VENUE: 1e308, SHORT_VENUE: 1e308}),
        data_root=tmp_path / "data", runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_opportunities(OpportunitiesFilters())
    assert result["rows"] == []
    json.dumps(result, allow_nan=False)


def test_basis_numeric_overflow_is_unavailable_and_json_is_finite(tmp_path):
    write_prior_pair_history(
        tmp_path / "data", prior_times=full_window_times(),
        prior_raw_spreads=(1e200, 2e200),
    )
    result = query_pair(DashboardQueryService(
        pair_config(), data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW,
    ))
    json.dumps(result, allow_nan=False)
    assert result["basis"]["eligible"] is False


def test_pair_read_is_identity_bounded_and_does_not_read_unneeded_partitions(tmp_path):
    import duckdb

    write_markets(tmp_path / "data", pair_markets())
    write_episodes(tmp_path / "runtime.sqlite3", {})
    old = tmp_path / "data" / "market" / "date=2026-09-01"
    old.mkdir(parents=True)
    (old / "part-broken.parquet").write_bytes(b"not parquet")
    # Extra feed in the same date must not be materialized by exact-pair reads.
    write_markets(tmp_path / "data", [make_market(canonical_symbol="ETH", venue_symbol="ETH")])
    service = DashboardQueryService(
        pair_config(), data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW,
    )
    result = query_pair(service)
    assert result["current"]["raw_spread_bps"] == pytest.approx(100.0)
    assert not result["errors"]
    # Capture actual fetched records at the I/O boundary, without replacing SQL.
    original_connect = duckdb.connect
    fetched = []

    class ReadConnection:
        def __enter__(self):
            self.connection = original_connect()
            return self

        def __exit__(self, *args):
            self.connection.close()

        def execute(self, query, parameters):
            self.result = self.connection.execute(query, parameters)
            return self

        def fetchall(self):
            rows = self.result.fetchall()
            fetched.extend(rows)
            return rows

        def fetchone(self):
            return self.result.fetchone()

    from unittest.mock import patch
    with patch("radar.dashboard_data.duckdb.connect", ReadConnection):
        query_pair(service, "6h")
    assert len(fetched) == 2
