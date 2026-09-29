from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest

from radar.dashboard_data import DashboardQueryService, OpportunitiesFilters
from test_dashboard_data import (
    LONG_VENUE,
    MarketConfig,
    NOW,
    SHORT_VENUE,
    make_config,
    make_market,
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


def make_multi_feed_config(*venues: str):
    return make_config(
        *[
            MarketConfig(venue=venue, venue_symbol="BTC", canonical_symbol="BTC")
            for venue in venues
        ],
        fees_bps={venue: 1.0 for venue in venues},
    )


def multi_feed_markets(
    venues: tuple[str, ...],
    *,
    sample_time,
    observed_at=None,
) -> list:
    return [
        make_market(
            sample_time=sample_time,
            observed_at=observed_at or sample_time,
            venue=venue,
            venue_symbol="BTC",
        )
        for venue in venues
    ]


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


def test_scanner_keeps_published_sample_until_new_sample_has_enough_feeds(tmp_path):
    venues = ("lighter", "hyperliquid", "backpack", "arcus")
    previous_sample = NOW - timedelta(seconds=10)
    next_sample = NOW
    write_markets(
        tmp_path / "data",
        multi_feed_markets(venues, sample_time=previous_sample),
    )
    service = DashboardQueryService(
        make_multi_feed_config(*venues),
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
        cache_ttl_seconds=0,
    )
    initial = service.get_opportunities(OpportunitiesFilters())
    assert initial["scanner_data_as_of"] == previous_sample.isoformat()

    # Two of four feeds are visible first.  This is enough to build a pair,
    # but not enough to replace the previous complete scanner sample.
    write_markets(
        tmp_path / "data",
        multi_feed_markets(venues[:2], sample_time=next_sample),
    )
    partial = service.get_opportunities(OpportunitiesFilters())
    assert partial["data_as_of"] == next_sample.isoformat()
    assert partial["latest_dataset_sample_time"] == next_sample.isoformat()
    assert partial["scanner_data_as_of"] == previous_sample.isoformat()
    assert {
        observation.sample_time
        for observation in service._scanner_snapshot.current_observations.values()
    } == {previous_sample}
    status = service.get_status()
    assert status["data_as_of"] == next_sample.isoformat()
    assert status["scanner_data_as_of"] == previous_sample.isoformat()

    # The remaining rows arrive in a later Parquet file.  The scanner now
    # advances atomically and every current pair belongs to the same slot.
    write_markets(
        tmp_path / "data",
        multi_feed_markets(venues[2:], sample_time=next_sample),
    )
    complete = service.get_opportunities(OpportunitiesFilters())
    assert complete["scanner_data_as_of"] == next_sample.isoformat()
    assert {
        observation.sample_time
        for observation in service._scanner_snapshot.current_observations.values()
    } == {next_sample}
    assert len(service._scanner_snapshot.current_observations) == len(venues) * (
        len(venues) - 1
    )


def test_scanner_accepts_legitimate_one_feed_gap(tmp_path):
    venues = ("lighter", "hyperliquid", "backpack")
    sample = NOW - timedelta(seconds=10)
    write_markets(
        tmp_path / "data",
        multi_feed_markets(venues[:2], sample_time=sample),
    )
    service = DashboardQueryService(
        make_multi_feed_config(*venues),
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
        cache_ttl_seconds=0,
    )

    result = service.get_opportunities(OpportunitiesFilters())

    assert result["scanner_data_as_of"] == sample.isoformat()
    assert len(service._scanner_snapshot.current_observations) == 2
    assert all(
        key.long_venue != "backpack" and key.short_venue != "backpack"
        for key in service._scanner_snapshot.current_observations
    )


def test_scanner_opportunities_report_observation_freshness(tmp_path):
    observed_at = NOW - timedelta(seconds=2)
    write_markets(
        tmp_path / "data",
        pair_markets(sample_time=NOW, observed_at=observed_at),
    )

    result = make_service(tmp_path).get_opportunities(OpportunitiesFilters())

    assert scanner_row(result)["freshness_seconds"] == pytest.approx(2.0)


def test_scanner_snapshot_is_not_mutated_by_later_refresh(tmp_path):
    previous_sample = NOW - timedelta(seconds=10)
    next_sample = NOW
    write_markets(
        tmp_path / "data",
        pair_markets(sample_time=previous_sample),
    )
    service = make_service(tmp_path)
    service.get_opportunities(OpportunitiesFilters())
    previous_snapshot = service._scanner_snapshot

    write_markets(tmp_path / "data", pair_markets(sample_time=next_sample))
    service.get_opportunities(OpportunitiesFilters())

    assert previous_snapshot is not None
    assert {
        observation.sample_time
        for observation in previous_snapshot.current_observations.values()
    } == {previous_sample}
    assert {
        observation.sample_time
        for observation in service._scanner_snapshot.current_observations.values()
    } == {next_sample}


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
