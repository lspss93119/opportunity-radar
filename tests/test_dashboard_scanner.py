from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest

from radar.dashboard_data import DashboardQueryService, OpportunitiesFilters
from test_dashboard_data import (
    LONG_VENUE,
    NOW,
    SHORT_VENUE,
    pair_config,
    pair_markets,
    write_markets,
)


def scanner_key():
    from radar.monitors.spread.models import SpreadPairKey

    return SpreadPairKey(
        canonical_symbol="BTC",
        long_venue=LONG_VENUE,
        long_venue_symbol="BTC",
        short_venue=SHORT_VENUE,
        short_venue_symbol="BTC",
    )


def scanner_row(result):
    return next(
        row
        for row in result["rows"]
        if row["long_venue"] == LONG_VENUE and row["short_venue"] == SHORT_VENUE
    )


def make_service(tmp_path, *, clock=lambda: NOW):
    return DashboardQueryService(
        pair_config(),
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=clock,
        cache_ttl_seconds=0,
    )


def test_scanner_hydration_keeps_compact_prior_only_pair_state(tmp_path):
    write_markets(
        tmp_path / "data",
        pair_markets(
            sample_time=NOW - timedelta(seconds=10),
            long_buy=100.0,
            short_sell=101.0,
        )
        + pair_markets(
            sample_time=NOW,
            long_buy=100.0,
            short_sell=105.0,
        ),
    )

    service = make_service(tmp_path)
    result = service.get_opportunities(OpportunitiesFilters())

    assert scanner_row(result)["current_raw_spread_bps"] == pytest.approx(500.0)
    snapshot = service._scanner_snapshot
    state = snapshot.pair_states[scanner_key()]
    assert len(state.points) == 2
    stats = state.stats_before(int(NOW.timestamp()))
    assert stats.sample_count == 1
    assert stats.mean_bps == pytest.approx(100.0)


def test_scanner_incremental_refresh_updates_current_snapshot(tmp_path):
    write_markets(
        tmp_path / "data",
        pair_markets(sample_time=NOW - timedelta(seconds=10), short_sell=101.0),
    )
    service = make_service(tmp_path)
    first = service.get_opportunities(OpportunitiesFilters())

    write_markets(tmp_path / "data", pair_markets(sample_time=NOW, short_sell=103.0))
    second = service.get_opportunities(OpportunitiesFilters())

    assert scanner_row(first)["current_raw_spread_bps"] == pytest.approx(100.0)
    assert scanner_row(second)["current_raw_spread_bps"] == pytest.approx(300.0)


def test_scanner_duplicate_sample_replaces_without_double_counting(tmp_path):
    sample = NOW - timedelta(seconds=10)
    write_markets(
        tmp_path / "data",
        pair_markets(
            sample_time=sample,
            observed_at=NOW - timedelta(seconds=2),
            short_sell=101.0,
        ),
    )
    service = make_service(tmp_path)
    service.get_opportunities(OpportunitiesFilters())

    write_markets(
        tmp_path / "data",
        pair_markets(
            sample_time=sample,
            observed_at=NOW - timedelta(seconds=1),
            short_sell=104.0,
        ),
    )
    result = service.get_opportunities(OpportunitiesFilters())

    state = service._scanner_snapshot.pair_states[scanner_key()]
    assert len(state.points) == 1
    assert scanner_row(result)["current_raw_spread_bps"] == pytest.approx(400.0)


def test_scanner_evicts_values_older_than_rolling_window(tmp_path):
    current = [NOW - timedelta(seconds=10)]
    old_sample = NOW - timedelta(days=1, seconds=5)
    write_markets(
        tmp_path / "data",
        pair_markets(sample_time=old_sample, observed_at=old_sample)
        + pair_markets(sample_time=current[0], observed_at=current[0]),
    )
    service = make_service(tmp_path, clock=lambda: current[0])
    service.get_opportunities(OpportunitiesFilters())
    assert len(service._scanner_snapshot.pair_states[scanner_key()].points) == 2

    current[0] = NOW
    write_markets(tmp_path / "data", pair_markets(sample_time=NOW, observed_at=NOW))
    service.get_opportunities(OpportunitiesFilters())

    state = service._scanner_snapshot.pair_states[scanner_key()]
    assert int(old_sample.timestamp()) not in [point.sample_epoch for point in state.points]
    assert len(state.points) == 2


def test_restart_rehydrates_existing_parquet_without_request_reconstruction(tmp_path):
    write_markets(tmp_path / "data", pair_markets(sample_time=NOW))
    first = make_service(tmp_path)
    first_result = first.get_opportunities(OpportunitiesFilters())

    restarted = make_service(tmp_path)
    second_result = restarted.get_opportunities(OpportunitiesFilters())

    assert second_result["rows"] == first_result["rows"]


def test_http_scanner_paths_do_not_rescan_full_history_after_hydration(tmp_path, monkeypatch):
    write_markets(tmp_path / "data", pair_markets(sample_time=NOW))
    service = make_service(tmp_path)
    service.get_opportunities(OpportunitiesFilters())

    def fail(*args, **kwargs):
        raise AssertionError("HTTP scanner path must not rebuild Parquet history")

    monkeypatch.setattr(service, "_read_market_rows", fail)
    assert service.get_opportunities(OpportunitiesFilters())["rows"]
    assert service.get_status()["overall"]["latest_feeds"] == 2


def test_concurrent_scanner_reads_are_safe_during_incremental_refresh(tmp_path):
    write_markets(tmp_path / "data", pair_markets(sample_time=NOW - timedelta(seconds=10)))
    service = make_service(tmp_path)
    service.get_opportunities(OpportunitiesFilters())
    write_markets(tmp_path / "data", pair_markets(sample_time=NOW, short_sell=102.0))

    def read():
        return service.get_opportunities(OpportunitiesFilters())["status"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        statuses = list(pool.map(lambda _: read(), range(16)))
    assert all(status in {"healthy", "degraded", "down"} for status in statuses)
