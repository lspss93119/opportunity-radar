from copy import deepcopy
from datetime import datetime, timedelta, timezone
import logging

import pytest

from radar.collectors.base import CollectorBatch
from radar.config import SpreadMonitorConfig
from radar.models import FundingSnapshot, MarketSnapshot
from radar.monitors.base import Monitor
from radar.monitors.spread import SpreadMonitor as ExportedSpreadMonitor
from radar.monitors.spread import monitor as monitor_module
from radar.monitors.spread.basis import RollingBasis
from radar.monitors.spread.models import (
    SpreadPairKey,
    build_spread_candidates,
    calculate_net_spread_bps,
    calculate_raw_spread_bps,
    selected_vwap,
)
from radar.monitors.spread.monitor import SpreadMonitor
from radar.state import RadarState
from radar.storage.sqlite import SQLiteRuntimeStore

UTC = timezone.utc
NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)


def make_market(
    venue: str,
    *,
    buy_10k_vwap: float | None = 100.0,
    sell_10k_vwap: float | None = 101.0,
    sample_time: datetime = NOW,
    observed_at: datetime = NOW,
    canonical_symbol: str = "BTC",
    venue_symbol: str = "BTC",
) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=observed_at,
        venue=venue,
        venue_symbol=venue_symbol,
        canonical_symbol=canonical_symbol,
        best_bid=99.0,
        best_bid_size=1.0,
        best_ask=100.0,
        best_ask_size=1.0,
        buy_10k_vwap=buy_10k_vwap,
        sell_10k_vwap=sell_10k_vwap,
    )


def make_state(
    *snapshots: MarketSnapshot,
    funding: tuple[FundingSnapshot, ...] = (),
) -> RadarState:
    state = RadarState()
    state.apply(
        CollectorBatch(
            market_snapshots=tuple(snapshots),
            funding_snapshots=funding,
        ),
        replace_context=bool(funding),
    )
    return state


def make_monitor(
    *,
    runtime_store: SQLiteRuntimeStore | None = None,
    fees_bps: dict[str, float] | None = None,
    **overrides: object,
) -> SpreadMonitor:
    values = {
        "candidate_net_bps": 10.0,
        "candidate_duration_seconds": 30,
        "alert_net_bps": 20.0,
        "alert_duration_seconds": 120,
        "interval_seconds": 10,
        "stale_after_seconds": 300,
        "primary_size_usd": 10_000,
        "top_n": 3,
    }
    values.update(overrides)
    return SpreadMonitor(
        SpreadMonitorConfig.model_validate(values),
        {"long": 0.0, "short": 0.0} if fees_bps is None else fees_bps,
        runtime_store=runtime_store,
        basis_window_seconds=12_000,
        basis_min_observations=4,
        basis_expected_interval_seconds=10,
    )


def prime_monitor(
    monitor: SpreadMonitor,
    state: RadarState,
    *,
    deviations: dict[SpreadPairKey, float] | None = None,
    when: datetime = NOW,
) -> None:
    candidates = build_spread_candidates(
        state.markets,
        when,
        primary_size_usd=monitor.config.primary_size_usd,
        stale_after_seconds=monitor.config.stale_after_seconds,
        fees_bps=monitor._fees_bps,
    )
    deviations = {} if deviations is None else deviations
    history = {
        candidate.key: tuple(
            (
                when - timedelta(seconds=12_000 - index * 10),
                candidate.raw_spread_bps - deviations.get(candidate.key, 0.0),
            )
            for index in range(1_200)
        )
        for candidate in candidates
    }
    monitor.hydrate_history(history)


def prime_triggering_monitor(
    monitor: SpreadMonitor,
    state: RadarState,
    *,
    when: datetime = NOW,
) -> None:
    candidates = build_spread_candidates(
        state.markets,
        when,
        primary_size_usd=monitor.config.primary_size_usd,
        stale_after_seconds=monitor.config.stale_after_seconds,
        fees_bps=monitor._fees_bps,
    )
    prime_monitor(
        monitor,
        state,
        deviations={
            candidate.key: 20.0
            for candidate in candidates
            if candidate.raw_spread_bps >= 20.0
        },
        when=when,
    )


def fail_next_persistence(
    monkeypatch: pytest.MonkeyPatch,
    store: SQLiteRuntimeStore,
) -> None:
    original = store.set_monitor_state_and_append_opportunities
    failed = False

    def persist(*args: object, **kwargs: object) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("injected persistence failure")
        original(*args, **kwargs)

    monkeypatch.setattr(store, "set_monitor_state_and_append_opportunities", persist)


@pytest.mark.asyncio
async def test_runtime_store_evaluation_does_not_deepcopy_basis_histories(
    monkeypatch, tmp_path
):
    database = tmp_path / "runtime.sqlite3"
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(runtime_store=store)
        prime_monitor(monitor, state)
        original_deepcopy = deepcopy

        def reject_basis_history_copy(value: object) -> object:
            if isinstance(value, dict) and any(
                isinstance(item, RollingBasis) for item in value.values()
            ):
                pytest.fail("SpreadMonitor must not deepcopy RollingBasis histories")
            return original_deepcopy(value)

        monkeypatch.setattr(monitor_module, "deepcopy", reject_basis_history_copy)

        assert await monitor.evaluate(NOW, state) == []


@pytest.mark.asyncio
async def test_spread_evaluation_logs_phase_timings(caplog):
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    monitor = make_monitor()
    prime_monitor(monitor, state)

    with caplog.at_level(logging.INFO, logger="radar.monitors.spread.monitor"):
        await monitor.evaluate(NOW, state)

    message = next(record.message for record in caplog.records if "spread cycle" in record.message)
    for field in (
        "candidate_build_ms=",
        "basis_ms=",
        "lifecycle_ms=",
        "sqlite_persistence_ms=",
        "total_ms=",
        "routes=",
        "alerts=",
    ):
        assert field in message


@pytest.mark.asyncio
async def test_persistence_failure_removes_new_basis_routes_and_restores_existing_history(
    monkeypatch, tmp_path
):
    database = tmp_path / "runtime.sqlite3"
    initial_state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    expanded_state = make_state(
        *initial_state.markets,
        make_market(
            "long-two",
            canonical_symbol="ETH",
            venue_symbol="ETH",
            buy_10k_vwap=100.0,
            sell_10k_vwap=99.0,
        ),
        make_market(
            "short-two",
            canonical_symbol="ETH",
            venue_symbol="ETH",
            buy_10k_vwap=102.0,
            sell_10k_vwap=101.0,
        ),
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(runtime_store=store)
        prime_monitor(monitor, initial_state)
        await monitor.evaluate(NOW, initial_state)
        existing_keys = set(monitor._basis_by_key)
        existing_state = {
            key: (
                tuple(basis._times),
                dict(basis._values),
                basis._sum,
                basis._sum_squares,
                basis._last_sample_time,
            )
            for key, basis in monitor._basis_by_key.items()
        }
        fail_next_persistence(monkeypatch, store)

        with pytest.raises(RuntimeError, match="injected persistence failure"):
            await monitor.evaluate(NOW + timedelta(seconds=10), expanded_state)

        assert set(monitor._basis_by_key) == existing_keys
        for key, expected in existing_state.items():
            basis = monitor._basis_by_key[key]
            assert (
                tuple(basis._times),
                dict(basis._values),
                basis._sum,
                basis._sum_squares,
                basis._last_sample_time,
            ) == expected

        await monitor.evaluate(NOW + timedelta(seconds=10), expanded_state)
        assert set(monitor._basis_by_key) == {
            candidate.key
            for candidate in build_spread_candidates(
                expanded_state.markets,
                NOW + timedelta(seconds=10),
                primary_size_usd=monitor.config.primary_size_usd,
                stale_after_seconds=monitor.config.stale_after_seconds,
                fees_bps=monitor._fees_bps,
                require_fees=False,
            )
        }


def test_directional_spread_uses_executable_prices_and_both_taker_fees():
    raw_spread = calculate_raw_spread_bps(100.0, 101.0)

    assert raw_spread == pytest.approx(100.0)
    assert calculate_net_spread_bps(raw_spread, 4.5, 3.5) == pytest.approx(92.0)


def test_both_directional_pairs_use_their_own_executable_prices():
    candidates = build_spread_candidates(
        [
            make_market("alpha", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
            make_market("bravo", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
        ],
        NOW,
        primary_size_usd=10_000,
        top_n=3,
        stale_after_seconds=30,
        fees_bps={"alpha": 0.0, "bravo": 0.0},
    )

    assert candidates[0].key.long_venue == "alpha"
    assert candidates[0].key.short_venue == "bravo"
    assert candidates[0].raw_spread_bps == pytest.approx(100.0)
    assert candidates[1].key.long_venue == "bravo"
    assert candidates[1].key.short_venue == "alpha"
    assert candidates[1].raw_spread_bps == pytest.approx(-294.117647)


def test_selected_vwap_maps_only_the_fixed_supported_sizes():
    snapshot = make_market("lighter")
    snapshot = snapshot.model_copy(
        update={
            "buy_1k_vwap": 1.0,
            "sell_1k_vwap": 2.0,
            "buy_5k_vwap": 5.0,
            "sell_5k_vwap": 6.0,
            "buy_10k_vwap": 10.0,
            "sell_10k_vwap": 11.0,
        }
    )

    assert selected_vwap(snapshot, "buy", 1_000) == 1.0
    assert selected_vwap(snapshot, "sell", 5_000) == 6.0
    assert selected_vwap(snapshot, "buy", 10_000) == 10.0


def test_candidates_evaluate_all_directional_pairs_with_deterministic_keys():
    snapshots = [
        make_market("alpha", buy_10k_vwap=100.0, sell_10k_vwap=110.0),
        make_market("bravo", buy_10k_vwap=101.0, sell_10k_vwap=109.0),
        make_market("charlie", buy_10k_vwap=102.0, sell_10k_vwap=108.0),
        make_market("delta", buy_10k_vwap=103.0, sell_10k_vwap=107.0),
    ]

    candidates = build_spread_candidates(
        snapshots,
        NOW,
        primary_size_usd=10_000,
        top_n=2,
        stale_after_seconds=30,
        fees_bps={
            "alpha": 0.0,
            "bravo": 0.0,
            "charlie": 0.0,
            "delta": 0.0,
        },
    )

    assert len(candidates) == 12
    assert {
        (candidate.key.long_venue, candidate.key.short_venue)
        for candidate in candidates
    } == {
        (long_venue, short_venue)
        for long_venue in ("alpha", "bravo", "charlie", "delta")
        for short_venue in ("alpha", "bravo", "charlie", "delta")
        if long_venue != short_venue
    }
    alpha_to_bravo = next(
        candidate
        for candidate in candidates
        if candidate.key
        == SpreadPairKey("BTC", "alpha", "BTC", "bravo", "BTC")
    )
    assert alpha_to_bravo.long_buy_vwap == 100.0
    assert alpha_to_bravo.short_sell_vwap == 109.0


def test_equal_prices_have_deterministic_venue_tie_breaking():
    snapshots = [
        make_market("zulu", buy_10k_vwap=100.0, sell_10k_vwap=101.0),
        make_market("alpha", buy_10k_vwap=100.0, sell_10k_vwap=101.0),
        make_market("bravo", buy_10k_vwap=99.0, sell_10k_vwap=102.0),
    ]

    candidates = build_spread_candidates(
        snapshots,
        NOW,
        primary_size_usd=10_000,
        top_n=2,
        stale_after_seconds=30,
        fees_bps={venue: 0.0 for venue in ("zulu", "alpha", "bravo")},
    )

    assert len(candidates) == 6
    assert {
        (candidate.key.long_venue, candidate.key.short_venue)
        for candidate in candidates
    } == {
        (long_venue, short_venue)
        for long_venue in ("zulu", "alpha", "bravo")
        for short_venue in ("zulu", "alpha", "bravo")
        if long_venue != short_venue
    }


@pytest.mark.parametrize("venue_count", [3, 5, 6])
def test_all_valid_directional_pairs_are_evaluated(venue_count: int):
    venues = [f"venue-{index}" for index in range(venue_count)]
    snapshots = [make_market(venue) for venue in venues]

    candidates = build_spread_candidates(
        snapshots,
        NOW,
        primary_size_usd=10_000,
        top_n=1,
        stale_after_seconds=30,
        fees_bps={venue: 0.0 for venue in venues},
    )

    assert len(candidates) == venue_count * (venue_count - 1)
    assert all(
        candidate.key.long_venue != candidate.key.short_venue
        for candidate in candidates
    )


def test_same_venue_long_short_pairs_are_never_evaluated():
    candidates = build_spread_candidates(
        [
            make_market("same", venue_symbol="BTC-A"),
            make_market("same", venue_symbol="BTC-B"),
            make_market("other"),
        ],
        NOW,
        primary_size_usd=10_000,
        top_n=1,
        stale_after_seconds=30,
        fees_bps={"same": 0.0, "other": 0.0},
    )

    assert len(candidates) == 4
    assert all(
        candidate.key.long_venue != candidate.key.short_venue
        for candidate in candidates
    )


def test_low_fee_venue_is_not_pruned_by_raw_price_top_n():
    state = make_state(
        make_market("raw-cheap-1", buy_10k_vwap=100.0, sell_10k_vwap=90.0),
        make_market("raw-cheap-2", buy_10k_vwap=100.5, sell_10k_vwap=91.0),
        make_market("low-fee", buy_10k_vwap=101.0, sell_10k_vwap=92.0),
        make_market("short", buy_10k_vwap=110.0, sell_10k_vwap=102.0),
    )
    candidates = build_spread_candidates(
        state.markets,
        NOW,
        primary_size_usd=10_000,
        top_n=2,
        stale_after_seconds=30,
        fees_bps={
            "raw-cheap-1": 250.0,
            "raw-cheap-2": 250.0,
            "low-fee": 0.0,
            "short": 0.0,
        },
    )

    target = next(
        candidate
        for candidate in candidates
        if candidate.key.long_venue == "low-fee"
        and candidate.key.short_venue == "short"
    )
    assert target.net_spread_bps == pytest.approx((102.0 / 101.0 - 1.0) * 10_000)


@pytest.mark.asyncio
async def test_multiple_directional_pair_episodes_coexist():
    state = make_state(
        make_market("alpha", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("bravo", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
        make_market("charlie", buy_10k_vwap=103.0, sell_10k_vwap=102.0),
    )
    monitor = make_monitor(
        candidate_duration_seconds=0,
        alert_net_bps=1_000.0,
        fees_bps={"alpha": 0.0, "bravo": 0.0, "charlie": 0.0},
    )
    candidates = build_spread_candidates(
        state.markets,
        NOW,
        primary_size_usd=10_000,
        stale_after_seconds=30,
        fees_bps={"alpha": 0.0, "bravo": 0.0, "charlie": 0.0},
    )
    deviations = {
        candidate.key: 20.0
        for candidate in candidates
        if candidate.key.long_venue == "alpha"
        and candidate.key.short_venue in {"bravo", "charlie"}
    }
    prime_monitor(monitor, state, deviations=deviations)

    assert await monitor.evaluate(NOW, state) == []

    assert {
        (episode.key.long_venue, episode.key.short_venue)
        for episode in monitor.active_episodes
    } == {("alpha", "bravo"), ("alpha", "charlie")}


@pytest.mark.parametrize(
    "snapshot",
    [
        make_market("lighter", buy_10k_vwap=None),
        make_market("hyperliquid", sell_10k_vwap=None),
        make_market(
            "stale",
            observed_at=NOW - timedelta(seconds=31),
        ),
        make_market(
            "future",
            observed_at=NOW + timedelta(seconds=1),
        ),
    ],
)
def test_invalid_market_side_is_fail_closed(snapshot):
    candidates = build_spread_candidates(
        [snapshot, make_market("other")],
        NOW,
        primary_size_usd=10_000,
        top_n=3,
        stale_after_seconds=30,
        fees_bps={"lighter": 1.0, "hyperliquid": 1.0, "stale": 1.0, "future": 1.0, "other": 1.0},
    )

    if snapshot.buy_10k_vwap is None:
        assert all(snapshot.venue != candidate.key.long_venue for candidate in candidates)
    elif snapshot.sell_10k_vwap is None:
        assert all(snapshot.venue != candidate.key.short_venue for candidate in candidates)
    else:
        assert all(
            snapshot.venue not in (candidate.key.long_venue, candidate.key.short_venue)
            for candidate in candidates
        )


def test_sample_time_mismatch_never_creates_a_candidate():
    candidates = build_spread_candidates(
        [
            make_market("long", sample_time=NOW, buy_10k_vwap=100.0),
            make_market("short", sample_time=NOW + timedelta(seconds=10), sell_10k_vwap=101.0),
        ],
        NOW,
        primary_size_usd=10_000,
        top_n=3,
        stale_after_seconds=30,
        fees_bps={"long": 0.0, "short": 0.0},
    )

    assert candidates == ()


def test_invalid_sample_timestamp_is_fail_closed():
    invalid = make_market("long").model_copy(update={"sample_time": datetime(2026, 9, 15, 12, 0)})

    candidates = build_spread_candidates(
        [invalid, make_market("short")],
        NOW,
        primary_size_usd=10_000,
        top_n=3,
        stale_after_seconds=30,
        fees_bps={"long": 0.0, "short": 0.0},
    )

    assert candidates == ()


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("observed_at", None),
        ("observed_at", "not-a-timestamp"),
        ("sample_time", None),
        ("sample_time", "not-a-timestamp"),
    ],
)
def test_invalid_timestamp_type_is_fail_closed(field_name, invalid_value):
    invalid = make_market("long").model_copy(update={field_name: invalid_value})

    candidates = build_spread_candidates(
        [invalid, make_market("short")],
        NOW,
        primary_size_usd=10_000,
        top_n=3,
        stale_after_seconds=30,
        fees_bps={"long": 0.0, "short": 0.0},
    )

    assert candidates == ()


def test_missing_fee_never_creates_a_candidate():
    candidates = build_spread_candidates(
        [make_market("long"), make_market("short")],
        NOW,
        primary_size_usd=10_000,
        top_n=3,
        stale_after_seconds=30,
        fees_bps={"long": 0.0},
    )

    assert candidates == ()


@pytest.mark.parametrize("invalid_price", [float("nan"), float("inf"), 0.0])
def test_non_positive_or_non_finite_executable_price_is_fail_closed(invalid_price):
    invalid = make_market("long").model_copy(update={"buy_10k_vwap": invalid_price})
    candidates = build_spread_candidates(
        [invalid, make_market("short")],
        NOW,
        primary_size_usd=10_000,
        top_n=3,
        stale_after_seconds=30,
        fees_bps={"long": 0.0, "short": 0.0},
    )

    assert all(candidate.key.long_venue != "long" for candidate in candidates)


def test_extreme_finite_prices_skip_only_the_invalid_pair():
    candidates = build_spread_candidates(
        [
            make_market(
                "tiny",
                canonical_symbol="BTC",
                buy_10k_vwap=1e-320,
                sell_10k_vwap=1e-320,
            ),
            make_market(
                "huge",
                canonical_symbol="BTC",
                buy_10k_vwap=1e308,
                sell_10k_vwap=1e308,
            ),
            make_market(
                "good-long",
                canonical_symbol="ETH",
                buy_10k_vwap=100.0,
                sell_10k_vwap=99.0,
            ),
            make_market(
                "good-short",
                canonical_symbol="ETH",
                buy_10k_vwap=102.0,
                sell_10k_vwap=101.0,
            ),
        ],
        NOW,
        primary_size_usd=10_000,
        top_n=3,
        stale_after_seconds=30,
        fees_bps={venue: 0.0 for venue in ("tiny", "huge", "good-long", "good-short")},
    )

    assert any(
        candidate.key.long_venue == "good-long"
        and candidate.key.short_venue == "good-short"
        for candidate in candidates
    )
    assert all(
        not (
            candidate.key.long_venue == "tiny"
            and candidate.key.short_venue == "huge"
        )
        for candidate in candidates
    )


@pytest.mark.parametrize("invalid_price", [True, False])
def test_boolean_executable_price_is_fail_closed(invalid_price):
    invalid = make_market("long").model_copy(update={"buy_10k_vwap": invalid_price})

    candidates = build_spread_candidates(
        [invalid, make_market("short")],
        NOW,
        primary_size_usd=10_000,
        top_n=3,
        stale_after_seconds=30,
        fees_bps={"long": 0.0, "short": 0.0},
    )

    assert all(candidate.key.long_venue != "long" for candidate in candidates)


@pytest.mark.asyncio
async def test_active_episode_restores_from_sqlite_within_continuity_gap(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(
            runtime_store=store,
            candidate_duration_seconds=30,
            stale_after_seconds=60,
        )
        prime_triggering_monitor(monitor, state)
        await monitor.evaluate(NOW, state)
        await monitor.evaluate(NOW + timedelta(seconds=10), state)
        episode_id = monitor.active_episodes[0].episode_id

    with SQLiteRuntimeStore(database) as reopened_store:
        reopened = make_monitor(
            runtime_store=reopened_store,
            candidate_duration_seconds=30,
            stale_after_seconds=60,
        )
        prime_triggering_monitor(reopened, state)
        assert reopened.active_episodes[0].episode_id == episode_id
        await reopened.evaluate(NOW + timedelta(seconds=20), state)
        assert reopened.active_episodes[0].candidate_confirmed is False


@pytest.mark.asyncio
async def test_large_restart_gap_starts_a_new_episode_without_fake_duration(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    initial_state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(
            runtime_store=store,
            candidate_duration_seconds=30,
            stale_after_seconds=60,
        )
        prime_triggering_monitor(monitor, initial_state)
        await monitor.evaluate(NOW, initial_state)
        old_episode_id = monitor.active_episodes[0].episode_id

    restarted_state = make_state(
        make_market(
            "long",
            buy_10k_vwap=100.0,
            sell_10k_vwap=99.0,
            sample_time=NOW + timedelta(minutes=10),
            observed_at=NOW + timedelta(minutes=10),
        ),
        make_market(
            "short",
            buy_10k_vwap=102.0,
            sell_10k_vwap=101.0,
            sample_time=NOW + timedelta(minutes=10),
            observed_at=NOW + timedelta(minutes=10),
        ),
    )
    with SQLiteRuntimeStore(database) as reopened_store:
        reopened = make_monitor(
            runtime_store=reopened_store,
            candidate_duration_seconds=30,
            stale_after_seconds=60,
        )
        prime_triggering_monitor(
            reopened,
            restarted_state,
            when=NOW + timedelta(minutes=10),
        )
        await reopened.evaluate(NOW + timedelta(minutes=10), restarted_state)
        assert reopened.active_episodes[0].episode_id != old_episode_id
        assert reopened.active_episodes[0].candidate_confirmed is False


@pytest.mark.asyncio
async def test_already_alerted_episode_restores_without_duplicate_alert(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(
            runtime_store=store,
            candidate_duration_seconds=0,
            alert_duration_seconds=0,
            stale_after_seconds=60,
        )
        prime_triggering_monitor(monitor, state)
        for offset in range(0, 60, 10):
            assert await monitor.evaluate(NOW + timedelta(seconds=offset), state) == []
        assert len(await monitor.evaluate(NOW + timedelta(seconds=60), state)) == 1

    with SQLiteRuntimeStore(database) as reopened_store:
        reopened = make_monitor(
            runtime_store=reopened_store,
            candidate_duration_seconds=0,
            alert_duration_seconds=0,
            stale_after_seconds=60,
        )
        prime_triggering_monitor(reopened, state)
        assert await reopened.evaluate(NOW + timedelta(seconds=10), state) == []
        assert reopened.active_episodes[0].alerted is True


@pytest.mark.asyncio
async def test_alert_persistence_failure_restores_unalerted_episode_for_retry(
    tmp_path,
    monkeypatch,
):
    database = tmp_path / "runtime.sqlite3"
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(
            runtime_store=store,
            candidate_duration_seconds=0,
            alert_duration_seconds=10,
            stale_after_seconds=60,
        )
        prime_triggering_monitor(monitor, state)
        assert await monitor.evaluate(NOW, state) == []
        for offset in range(10, 60, 10):
            await monitor.evaluate(NOW + timedelta(seconds=offset), state)
        episode_id = monitor.active_episodes[0].episode_id
        episodes_before_failure = deepcopy(monitor.active_episodes)
        persisted_before_failure = store.get_monitor_state("spread", "episodes")
        fail_next_persistence(monkeypatch, store)

        with pytest.raises(RuntimeError, match="injected persistence failure"):
            await monitor.evaluate(NOW + timedelta(seconds=60), state)

        episode = monitor.active_episodes[0]
        assert monitor.active_episodes == episodes_before_failure
        assert episode.episode_id == episode_id
        assert episode.alerted is False
        assert episode.last_seen_at == NOW + timedelta(seconds=50)
        assert store.get_monitor_state("spread", "episodes") == persisted_before_failure
        assert [event["event_type"] for event in store.list_opportunities(monitor_name="spread")] == [
            "candidate_confirmed"
        ]

        alerts = await monitor.evaluate(NOW + timedelta(seconds=60), state)

        assert len(alerts) == 1
        assert monitor.active_episodes[0].alerted is True
        events = store.list_opportunities(monitor_name="spread")
        assert [event["event_type"] for event in events].count("alert") == 1


@pytest.mark.asyncio
async def test_candidate_confirmation_persistence_failure_restores_unconfirmed_episode(
    tmp_path,
    monkeypatch,
):
    database = tmp_path / "runtime.sqlite3"
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(
            runtime_store=store,
            candidate_duration_seconds=10,
            alert_duration_seconds=120,
            stale_after_seconds=60,
        )
        prime_triggering_monitor(monitor, state)
        await monitor.evaluate(NOW, state)
        episode_id = monitor.active_episodes[0].episode_id
        episodes_before_failure = deepcopy(monitor.active_episodes)
        persisted_before_failure = store.get_monitor_state("spread", "episodes")
        fail_next_persistence(monkeypatch, store)

        with pytest.raises(RuntimeError, match="injected persistence failure"):
            await monitor.evaluate(NOW + timedelta(seconds=10), state)

        episode = monitor.active_episodes[0]
        assert monitor.active_episodes == episodes_before_failure
        assert episode.episode_id == episode_id
        assert episode.candidate_confirmed is False
        assert episode.candidate_confirmed_at is None
        assert episode.last_seen_at == NOW
        assert store.get_monitor_state("spread", "episodes") == persisted_before_failure
        assert store.list_opportunities(monitor_name="spread") == []

        assert await monitor.evaluate(NOW + timedelta(seconds=10), state) == []

        episode = monitor.active_episodes[0]
        assert episode.candidate_confirmed is True
        assert episode.candidate_confirmed_at == NOW + timedelta(seconds=10)
        events = store.list_opportunities(monitor_name="spread")
        assert [event["event_type"] for event in events] == ["candidate_confirmed"]


@pytest.mark.asyncio
async def test_resolution_persistence_failure_restores_active_episode_for_retry(
    tmp_path,
    monkeypatch,
):
    database = tmp_path / "runtime.sqlite3"
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    missing_short_state = make_state(
        make_market(
            "long",
            buy_10k_vwap=100.0,
            sell_10k_vwap=99.0,
            observed_at=NOW + timedelta(seconds=10),
        ),
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(
            runtime_store=store,
            candidate_duration_seconds=0,
            alert_duration_seconds=120,
            stale_after_seconds=60,
        )
        prime_triggering_monitor(monitor, state)
        await monitor.evaluate(NOW, state)
        episode_id = monitor.active_episodes[0].episode_id
        episodes_before_failure = deepcopy(monitor.active_episodes)
        persisted_before_failure = store.get_monitor_state("spread", "episodes")
        fail_next_persistence(monkeypatch, store)

        with pytest.raises(RuntimeError, match="injected persistence failure"):
            await monitor.evaluate(NOW + timedelta(seconds=10), missing_short_state)

        episode = monitor.active_episodes[0]
        assert monitor.active_episodes == episodes_before_failure
        assert episode.episode_id == episode_id
        assert episode.candidate_confirmed is True
        assert episode.last_seen_at == NOW
        assert store.get_monitor_state("spread", "episodes") == persisted_before_failure
        assert [event["event_type"] for event in store.list_opportunities(monitor_name="spread")] == [
            "candidate_confirmed"
        ]

        assert await monitor.evaluate(NOW + timedelta(seconds=10), missing_short_state) == []

        assert monitor.active_episodes == ()
        assert store.get_monitor_state("spread", "episodes") == {}
        events = store.list_opportunities(monitor_name="spread")
        assert [event["event_type"] for event in events] == [
            "candidate_confirmed",
            "resolved",
        ]
        assert [event["event_type"] for event in events].count("resolved") == 1


@pytest.mark.asyncio
async def test_lifecycle_logs_meaningful_events_with_distinct_ids(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    below_state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0, observed_at=NOW + timedelta(seconds=20)),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=100.0, observed_at=NOW + timedelta(seconds=20)),
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(
            runtime_store=store,
            candidate_duration_seconds=10,
            alert_duration_seconds=10,
            stale_after_seconds=60,
        )
        prime_triggering_monitor(monitor, state)
        await monitor.evaluate(NOW, state)
        await monitor.evaluate(NOW + timedelta(seconds=10), state)
        for offset in range(20, 70, 10):
            await monitor.evaluate(NOW + timedelta(seconds=offset), state)
        await monitor.evaluate(NOW + timedelta(seconds=70), below_state)

        events = store.list_opportunities(monitor_name="spread")

    assert {event["event_type"] for event in events} == {
        "candidate_confirmed",
        "alert",
        "resolved",
    }
    assert len({event["event_id"] for event in events}) == 3


@pytest.mark.asyncio
async def test_provisional_episode_that_disappears_is_not_logged(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    below_state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0, observed_at=NOW + timedelta(seconds=10)),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=100.0, observed_at=NOW + timedelta(seconds=10)),
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(runtime_store=store, candidate_duration_seconds=30)
        prime_triggering_monitor(monitor, state)
        await monitor.evaluate(NOW, state)
        await monitor.evaluate(NOW + timedelta(seconds=10), below_state)
        assert store.list_opportunities(monitor_name="spread") == []


@pytest.mark.asyncio
async def test_alert_payload_contains_spread_and_available_funding_context():
    funding_time = NOW - timedelta(minutes=1)
    funding = (
        FundingSnapshot(
            effective_time=funding_time,
            observed_at=NOW,
            venue="long",
            venue_symbol="BTC",
            canonical_symbol="BTC",
            funding_rate=0.001,
            next_funding_time=NOW + timedelta(hours=1),
        ),
    )
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
        funding=funding,
    )
    monitor = make_monitor(
        candidate_duration_seconds=0,
        alert_duration_seconds=0,
        fees_bps={"long": 4.5, "short": 3.5},
    )
    prime_triggering_monitor(monitor, state)

    for offset in range(0, 60, 10):
        await monitor.evaluate(NOW + timedelta(seconds=offset), state)
    alerts = await monitor.evaluate(NOW + timedelta(seconds=60), state)

    assert len(alerts) == 1
    payload = alerts[0].payload
    assert payload["raw_spread_bps"] == pytest.approx(100.0)
    assert payload["net_spread_bps"] == pytest.approx(92.0)
    assert payload["rolling_mean_bps"] == pytest.approx(80.0)
    assert payload["rolling_std_bps"] == pytest.approx(0.0)
    assert payload["deviation_bps"] == pytest.approx(20.0)
    assert payload["signal_duration_seconds"] == 60
    assert payload["round_trip_fee_bps"] == pytest.approx(16.0)
    assert payload["theoretical_edge_bps"] == pytest.approx(4.0)
    assert payload["sample_time"] == NOW.isoformat()
    assert payload["episode_started_at"] == NOW.isoformat()
    assert payload["candidate_confirmed_at"] == NOW.isoformat()
    assert payload["alert_condition_started_at"] == NOW.isoformat()
    assert payload["funding_context"]["long"]["funding_rate"] == pytest.approx(0.001)
    assert payload["funding_context"]["long"]["effective_time"] == funding_time.isoformat()
    assert payload["funding_context"]["short"] is None


@pytest.mark.asyncio
async def test_missing_funding_context_does_not_suppress_valid_alert():
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    monitor = make_monitor(candidate_duration_seconds=0, alert_duration_seconds=0)
    prime_triggering_monitor(monitor, state)

    for offset in range(0, 60, 10):
        await monitor.evaluate(NOW + timedelta(seconds=offset), state)
    alerts = await monitor.evaluate(NOW + timedelta(seconds=60), state)

    assert len(alerts) == 1
    assert alerts[0].payload["funding_context"] == {"long": None, "short": None}


@pytest.mark.asyncio
async def test_funding_context_matches_canonical_symbol_as_well_as_venue_symbol():
    wrong_symbol_funding = (
        FundingSnapshot(
            effective_time=NOW,
            observed_at=NOW,
            venue="long",
            venue_symbol="BTC",
            canonical_symbol="ETH",
            funding_rate=0.001,
        ),
    )
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
        funding=wrong_symbol_funding,
    )
    monitor = make_monitor(candidate_duration_seconds=0, alert_duration_seconds=0)
    prime_triggering_monitor(monitor, state)

    for offset in range(0, 60, 10):
        await monitor.evaluate(NOW + timedelta(seconds=offset), state)
    alerts = await monitor.evaluate(NOW + timedelta(seconds=60), state)

    assert len(alerts) == 1
    assert alerts[0].payload["funding_context"] == {"long": None, "short": None}


def test_spread_monitor_satisfies_existing_monitor_protocol():
    monitor = make_monitor()

    assert isinstance(monitor, Monitor)
    assert ExportedSpreadMonitor is SpreadMonitor


@pytest.mark.asyncio
async def test_disappeared_pair_resolves_and_clears_persisted_active_state(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    missing_short_state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0, observed_at=NOW + timedelta(seconds=10)),
    )
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(
            runtime_store=store,
            candidate_duration_seconds=0,
            stale_after_seconds=60,
        )
        prime_triggering_monitor(monitor, state)
        await monitor.evaluate(NOW, state)
        await monitor.evaluate(NOW + timedelta(seconds=10), missing_short_state)

        assert monitor.active_episodes == ()
        assert store.get_monitor_state("spread", "episodes") == {}
        assert [event["event_type"] for event in store.list_opportunities(monitor_name="spread")] == [
            "candidate_confirmed",
            "resolved",
        ]


@pytest.mark.asyncio
async def test_candidate_is_confirmed_only_after_continuous_candidate_duration():
    monitor = make_monitor(candidate_duration_seconds=30)
    state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    prime_triggering_monitor(monitor, state)

    for seconds in (0, 10, 20):
        assert await monitor.evaluate(NOW + timedelta(seconds=seconds), state) == []
        assert monitor.active_episodes[0].candidate_confirmed is False

    assert await monitor.evaluate(NOW + timedelta(seconds=30), state) == []
    assert monitor.active_episodes[0].candidate_confirmed is True
    assert monitor.active_episodes[0].candidate_confirmed_at == NOW + timedelta(seconds=30)


@pytest.mark.asyncio
async def test_below_candidate_resolves_episode_and_reentry_starts_a_new_one():
    monitor = make_monitor(candidate_duration_seconds=0)
    qualifying_state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    prime_triggering_monitor(monitor, qualifying_state)
    await monitor.evaluate(NOW, qualifying_state)
    first_episode_id = monitor.active_episodes[0].episode_id

    below_candidate_state = make_state(
        make_market("long", buy_10k_vwap=100.0, observed_at=NOW + timedelta(seconds=10)),
        make_market(
            "short",
            buy_10k_vwap=102.0,
            sell_10k_vwap=100.0,
            observed_at=NOW + timedelta(seconds=10),
        ),
    )
    await monitor.evaluate(NOW + timedelta(seconds=10), below_candidate_state)
    target_key = SpreadPairKey("BTC", "long", "BTC", "short", "BTC")
    assert all(episode.key != target_key for episode in monitor.active_episodes)

    await monitor.evaluate(NOW + timedelta(seconds=20), qualifying_state)
    assert monitor.active_episodes[0].episode_id != first_episode_id


@pytest.mark.asyncio
async def test_high_std_after_alert_does_not_rearm_episode():
    monitor = make_monitor(candidate_duration_seconds=0, alert_duration_seconds=20)
    high_state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    prime_triggering_monitor(monitor, high_state)
    for offset in range(0, 60, 10):
        assert await monitor.evaluate(NOW + timedelta(seconds=offset), high_state) == []
    alerts = await monitor.evaluate(NOW + timedelta(seconds=60), high_state)
    assert len(alerts) == 1
    assert monitor.active_episodes[0].alerted is True

    noisy_state = make_state(
        make_market("long", buy_10k_vwap=100.0, observed_at=NOW + timedelta(seconds=70)),
        make_market(
            "short",
            buy_10k_vwap=102.0,
            sell_10k_vwap=103.0,
            observed_at=NOW + timedelta(seconds=70),
        ),
    )
    await monitor.evaluate(NOW + timedelta(seconds=70), noisy_state)
    assert monitor.active_episodes[0].alerted is True
    assert await monitor.evaluate(NOW + timedelta(seconds=80), high_state) == []


@pytest.mark.asyncio
async def test_one_alert_per_episode_and_reentry_can_alert_again():
    monitor = make_monitor(candidate_duration_seconds=0, alert_duration_seconds=0)
    qualifying_state = make_state(
        make_market("long", buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", buy_10k_vwap=102.0, sell_10k_vwap=101.0),
    )
    prime_triggering_monitor(monitor, qualifying_state)

    for offset in range(0, 60, 10):
        assert await monitor.evaluate(NOW + timedelta(seconds=offset), qualifying_state) == []
    first_alerts = await monitor.evaluate(NOW + timedelta(seconds=60), qualifying_state)
    assert len(first_alerts) == 1
    assert await monitor.evaluate(NOW + timedelta(seconds=70), qualifying_state) == []

    below_state = make_state(
        make_market(
            "long",
            buy_10k_vwap=100.0,
            sell_10k_vwap=99.0,
            observed_at=NOW + timedelta(seconds=20),
        ),
        make_market(
            "short",
            buy_10k_vwap=102.0,
            sell_10k_vwap=100.0,
            observed_at=NOW + timedelta(seconds=20),
        ),
    )
    await monitor.evaluate(NOW + timedelta(seconds=80), below_state)
    for offset in range(90, 150, 10):
        assert await monitor.evaluate(NOW + timedelta(seconds=offset), qualifying_state) == []
    second_alerts = await monitor.evaluate(NOW + timedelta(seconds=150), qualifying_state)
    assert len(second_alerts) == 1
    assert second_alerts[0].event_id != first_alerts[0].event_id
