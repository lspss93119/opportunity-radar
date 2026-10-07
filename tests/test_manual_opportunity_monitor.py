from __future__ import annotations

from datetime import UTC, datetime, timedelta
import logging

import pytest

import radar.history.manual_opportunity as history_module
import radar.monitors.manual_opportunity as monitor_module
from radar.collectors.base import CollectorBatch
from radar.alerts.manual_opportunity import ManualOpportunityAlertProcessor
from radar.config import ManualOpportunityConfig
from radar.history.manual_opportunity import BboRollingHistory
from radar.models import HourlyContext, MarketSnapshot
from radar.monitors.manual_opportunity import (
    ManualOpportunityMonitor,
)
from radar.monitors.spread.models import SpreadPairKey
from radar.state import RadarState
from radar.storage.sqlite import SQLiteRuntimeStore


START = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
KEY = SpreadPairKey("QQQ", "arcus", "QQQ-USD", "lighter_robinhood", "QQQ")
FEES = {"arcus": 0.0, "lighter_robinhood": 0.0, "backpack": 0.0}


class _SmallHistory(BboRollingHistory):
    def __init__(self) -> None:
        super().__init__(
            windows_seconds={"2h": 1_000, "24h": 1_000, "3d": 1_000},
            expected_interval_seconds=10,
            minimum_coverage=0.0,
        )


@pytest.fixture
def small_history(monkeypatch):
    monkeypatch.setattr(history_module, "BboRollingHistory", _SmallHistory)


def snapshot(
    venue: str,
    venue_symbol: str,
    when: datetime,
    *,
    best_bid: float = 99.0,
    best_ask: float = 100.0,
    observed_at: datetime | None = None,
    bid_size: float = 1.0,
    ask_size: float = 1.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=when,
        observed_at=when if observed_at is None else observed_at,
        venue=venue,
        venue_symbol=venue_symbol,
        canonical_symbol="QQQ",
        best_bid=best_bid,
        best_bid_size=bid_size,
        best_ask=best_ask,
        best_ask_size=ask_size,
    )


def context(
    venue: str,
    venue_symbol: str,
    when: datetime,
    *,
    volume: float | None = 2_000_000.0,
) -> HourlyContext:
    return HourlyContext(
        sample_time=when - timedelta(hours=1),
        observed_at=when - timedelta(hours=1),
        venue=venue,
        venue_symbol=venue_symbol,
        canonical_symbol="QQQ",
        volume_24h=volume,
    )


def state_for(
    when: datetime,
    *,
    long_observed_at: datetime | None = None,
    short_observed_at: datetime | None = None,
    include_context: bool = True,
    short_bid: float = 101.2,
    short_venue: str = "lighter_robinhood",
    short_symbol: str = "QQQ",
) -> RadarState:
    state = RadarState()
    state.apply_market_batch(
        CollectorBatch(
            market_snapshots=(
                snapshot(
                    "arcus",
                    "QQQ-USD",
                    when,
                    observed_at=long_observed_at,
                ),
                snapshot(
                    short_venue,
                    short_symbol,
                    when,
                    best_bid=short_bid,
                    best_ask=102.0,
                    observed_at=short_observed_at,
                ),
            )
        )
    )
    if include_context:
        state.apply_context_batch(
            CollectorBatch(
                hourly_contexts=(
                    context("arcus", "QQQ-USD", when, volume=2_000_000.0),
                    context(short_venue, short_symbol, when, volume=1_500_000.0),
                )
            )
        )
    return state


def monitor(
    *,
    store: SQLiteRuntimeStore | None = None,
    confirmation_seconds: int = 60,
    fees: dict[str, float] = FEES,
) -> ManualOpportunityMonitor:
    return ManualOpportunityMonitor(
        ManualOpportunityConfig(confirmation_seconds=confirmation_seconds),
        fees,
        runtime_store=store,
    )


def prime(monitor_instance: ManualOpportunityMonitor, key: SpreadPairKey = KEY) -> None:
    monitor_instance.hydrate_history(
        {
            key: tuple(
                (START - timedelta(seconds=offset), 0.0)
                for offset in range(1_000, 0, -10)
            )
        }
    )


@pytest.mark.asyncio
async def test_monitor_evaluates_all_ordered_cross_venue_pairs_only(small_history):
    del small_history
    current = START
    state = RadarState()
    state.apply_market_batch(
        CollectorBatch(
            market_snapshots=(
                snapshot("arcus", "QQQ-USD", current),
                snapshot("lighter_robinhood", "QQQ", current, best_bid=101.0, best_ask=102.0),
                snapshot("backpack", "QQQ.US_USDC_PERP", current, best_bid=102.0, best_ask=103.0),
            )
        )
    )
    state.apply_context_batch(
        CollectorBatch(
            hourly_contexts=(
                context("arcus", "QQQ-USD", current),
                context("lighter_robinhood", "QQQ", current),
                context("backpack", "QQQ.US_USDC_PERP", current),
            )
        )
    )

    instance = monitor()
    assert await instance.evaluate(current, state) == []
    keys = tuple(instance._histories)

    assert len(keys) == 6
    assert all(key.long_venue != key.short_venue for key in keys)


@pytest.mark.asyncio
async def test_monitor_uses_prior_mean_and_exact_hourly_volume(small_history):
    del small_history
    instance = monitor()
    prime(instance)

    first = await instance.evaluate(START, state_for(START))
    assert first == []
    assert instance.active_episodes[0].last_observation.mean_24h_bps == pytest.approx(
        0.0
    )

    alerts = []
    for offset in range(10, 61, 10):
        when = START + timedelta(seconds=offset)
        alerts = await instance.evaluate(when, state_for(when))

    assert len(alerts) == 1
    assert alerts[0].payload["event_kind"] == "manual_initial"
    assert alerts[0].payload["route_volume_24h"] == pytest.approx(1_500_000.0)
    assert alerts[0].payload["signal_duration_seconds"] == 60


@pytest.mark.asyncio
async def test_detector_event_is_persisted_when_repeated_initial_is_telegram_suppressed(
    small_history, tmp_path, caplog
):
    del small_history
    caplog.set_level(logging.INFO, logger=monitor_module.__name__)

    class FakeTelegram:
        async def send_text(self, _text: str) -> None:
            return None

    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        instance = monitor(store=store, confirmation_seconds=0)
        prime(instance)
        processor = ManualOpportunityAlertProcessor(
            FakeTelegram(), notification_gate=instance.notification_gate
        )  # type: ignore[arg-type]

        first = await instance.evaluate(START, state_for(START))
        assert [alert.payload["event_kind"] for alert in first] == ["manual_initial"]
        await processor.process(first[0])

        await instance.evaluate(
            START + timedelta(seconds=10),
            state_for(START + timedelta(seconds=10), short_bid=99.0),
        )
        repeated = await instance.evaluate(
            START + timedelta(seconds=90),
            state_for(START + timedelta(seconds=90)),
        )

        assert repeated == []
        events = store.list_opportunities(monitor_name="manual_opportunity")
        assert [event["event_type"] for event in events] == [
            "manual_initial",
            "manual_initial",
        ]
        suppression_logs = [record.getMessage() for record in caplog.records]
        assert any(
            "manual_telegram_initial_suppressed" in message
            and "reason=quiet_rearm_not_met" in message
            for message in suppression_logs
        )


@pytest.mark.asyncio
async def test_expansion_is_sent_only_for_a_telegram_notified_episode(
    small_history, tmp_path
):
    del small_history

    class FakeTelegram:
        async def send_text(self, _text: str) -> None:
            return None

    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        instance = monitor(store=store, confirmation_seconds=0)
        prime(instance)
        processor = ManualOpportunityAlertProcessor(
            FakeTelegram(), notification_gate=instance.notification_gate
        )  # type: ignore[arg-type]

        first = await instance.evaluate(START, state_for(START))
        await processor.process(first[0])
        expansion = await instance.evaluate(
            START + timedelta(seconds=10),
            state_for(START + timedelta(seconds=10), short_bid=101.9),
        )

        assert [alert.payload["event_kind"] for alert in expansion] == [
            "manual_expansion"
        ]
        await processor.process(expansion[0])
        events = store.list_opportunities(monitor_name="manual_opportunity")
        assert [event["event_type"] for event in events] == [
            "manual_initial",
            "manual_expansion",
        ]


@pytest.mark.asyncio
async def test_suppressed_episode_expansion_remains_persisted_but_not_telegram_sent(
    small_history, tmp_path
):
    del small_history

    class FakeTelegram:
        async def send_text(self, _text: str) -> None:
            return None

    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        instance = monitor(store=store, confirmation_seconds=0)
        prime(instance)
        processor = ManualOpportunityAlertProcessor(
            FakeTelegram(), notification_gate=instance.notification_gate
        )  # type: ignore[arg-type]

        first = await instance.evaluate(START, state_for(START))
        await processor.process(first[0])
        await instance.evaluate(
            START + timedelta(seconds=10),
            state_for(START + timedelta(seconds=10), short_bid=99.0),
        )
        suppressed_initial = await instance.evaluate(
            START + timedelta(seconds=20),
            state_for(START + timedelta(seconds=20)),
        )
        assert suppressed_initial == []

        suppressed_expansion = await instance.evaluate(
            START + timedelta(seconds=30),
            state_for(START + timedelta(seconds=30), short_bid=101.9),
        )
        assert suppressed_expansion == []

        events = store.list_opportunities(monitor_name="manual_opportunity")
        assert [event["event_type"] for event in events] == [
            "manual_initial",
            "manual_initial",
            "manual_expansion",
        ]


@pytest.mark.asyncio
async def test_monitor_logs_manual_lifecycle_transitions(small_history, caplog):
    del small_history
    caplog.set_level(logging.INFO, logger=monitor_module.__name__)
    instance = monitor()
    prime(instance)

    await instance.evaluate(START, state_for(START))
    await instance.evaluate(
        START + timedelta(seconds=10),
        state_for(START + timedelta(seconds=10), short_bid=99.0),
    )
    for offset in range(20, 81, 10):
        when = START + timedelta(seconds=offset)
        await instance.evaluate(when, state_for(when))
    await instance.evaluate(
        START + timedelta(seconds=90),
        state_for(START + timedelta(seconds=90), short_bid=101.9),
    )

    messages = [record.getMessage() for record in caplog.records]
    transition_messages = [
        message
        for message in messages
        if any(
            f"manual opportunity {event}" in message
            for event in (
                "candidate_start",
                "candidate_reset",
                "manual_confirm",
                "manual_expansion",
            )
        )
    ]
    assert any("manual opportunity candidate_start" in message for message in transition_messages)
    assert any("manual opportunity candidate_reset" in message for message in transition_messages)
    assert any("manual opportunity manual_confirm" in message for message in transition_messages)
    assert any("manual opportunity manual_expansion" in message for message in transition_messages)
    assert all("sample_time=" in message for message in transition_messages)
    assert all("available_at=" in message for message in transition_messages)
    assert all("symbol=QQQ" in message for message in transition_messages)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state_kwargs",
    [
        {"include_context": False},
        {"long_observed_at": START - timedelta(seconds=31)},
        {"short_observed_at": START + timedelta(seconds=1)},
    ],
)
async def test_monitor_fails_closed_for_missing_or_temporally_invalid_inputs(
    small_history, state_kwargs
):
    del small_history
    instance = monitor()
    prime(instance)

    assert await instance.evaluate(START, state_for(START, **state_kwargs)) == []
    assert instance.active_episodes == ()


@pytest.mark.asyncio
async def test_monitor_fails_closed_for_missing_fee_and_zero_depth(small_history):
    del small_history
    missing_fee = monitor(fees={"arcus": 0.0})
    prime(missing_fee)
    assert await missing_fee.evaluate(START, state_for(START)) == []
    assert missing_fee.active_episodes == ()

    zero_depth_state = state_for(START)
    zero_depth_state.apply_market_batch(
        CollectorBatch(
            market_snapshots=(
                snapshot("arcus", "QQQ-USD", START, ask_size=0.0),
                snapshot(
                    "lighter_robinhood",
                    "QQQ",
                    START,
                    best_bid=101.2,
                    best_ask=102.0,
                ),
            )
        )
    )
    zero_depth = monitor()
    prime(zero_depth)
    assert await zero_depth.evaluate(START, zero_depth_state) == []
    assert zero_depth.active_episodes == ()


@pytest.mark.asyncio
async def test_monitor_restored_state_does_not_expand_from_stale_bbo(small_history, tmp_path):
    del small_history
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        instance = monitor(store=store, confirmation_seconds=0)
        prime(instance)
        initial = await instance.evaluate(START, state_for(START))
        assert [alert.payload["event_kind"] for alert in initial] == ["manual_initial"]

    with SQLiteRuntimeStore(database) as store:
        restored = monitor(store=store, confirmation_seconds=0)
        prime(restored)
        stale = await restored.evaluate(
            START + timedelta(seconds=31),
            state_for(
                START + timedelta(seconds=31),
                long_observed_at=START,
                short_observed_at=START,
                short_bid=101.5,
            ),
        )
        assert stale == []
        assert restored.active_episodes == ()


@pytest.mark.asyncio
async def test_monitor_persistence_failure_rolls_back_history_and_retries(
    monkeypatch, small_history, tmp_path
):
    del small_history
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        instance = monitor(store=store)
        prime(instance)
        for offset in range(0, 60, 10):
            when = START + timedelta(seconds=offset)
            assert await instance.evaluate(when, state_for(when)) == []

        original = store.set_monitor_state_and_append_opportunities
        failed = True

        def fail_once(*args, **kwargs):
            nonlocal failed
            if failed:
                failed = False
                raise RuntimeError("injected persistence failure")
            return original(*args, **kwargs)

        monkeypatch.setattr(store, "set_monitor_state_and_append_opportunities", fail_once)
        with pytest.raises(RuntimeError, match="injected persistence failure"):
            await instance.evaluate(START + timedelta(seconds=60), state_for(START + timedelta(seconds=60)))
        assert instance.active_episodes[0].confirmed_at is None
        assert instance._histories[KEY]._last_sample_time == START + timedelta(seconds=50)

        retried = await instance.evaluate(
            START + timedelta(seconds=60), state_for(START + timedelta(seconds=60))
        )
    assert [alert.payload["event_kind"] for alert in retried] == ["manual_initial"]


@pytest.mark.asyncio
async def test_stale_bbo_does_not_advance_notification_quiet_timer(
    small_history, tmp_path
):
    del small_history

    class FakeTelegram:
        async def send_text(self, _text: str) -> None:
            return None

    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        instance = monitor(store=store, confirmation_seconds=0)
        prime(instance)
        processor = ManualOpportunityAlertProcessor(
            FakeTelegram(), notification_gate=instance.notification_gate
        )  # type: ignore[arg-type]

        initial = await instance.evaluate(START, state_for(START))
        await processor.process(initial[0])
        await instance.evaluate(
            START + timedelta(seconds=10),
            state_for(START + timedelta(seconds=10), short_bid=99.0),
        )

        stale_time = START + timedelta(seconds=310)
        assert await instance.evaluate(
            stale_time,
            state_for(
                stale_time,
                short_bid=99.0,
                long_observed_at=START + timedelta(seconds=10),
                short_observed_at=START + timedelta(seconds=10),
            ),
        ) == []

        retry_time = START + timedelta(seconds=320)
        assert await instance.evaluate(
            retry_time,
            state_for(retry_time, short_bid=99.0),
        ) == []
        assert not instance.notification_gate.is_armed(KEY)


@pytest.mark.asyncio
async def test_runtime_store_batch_preserves_strategy_outputs(small_history, tmp_path):
    del small_history
    memory_monitor = monitor()
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        persisted_monitor = monitor(store=store)
        prime(memory_monitor)
        prime(persisted_monitor)

        for offset in range(0, 80, 10):
            when = START + timedelta(seconds=offset)
            memory_alerts = await memory_monitor.evaluate(when, state_for(when))
            persisted_alerts = await persisted_monitor.evaluate(when, state_for(when))

            assert [
                (alert.event_id, alert.payload) for alert in persisted_alerts
            ] == [
                (alert.event_id, alert.payload) for alert in memory_alerts
            ]
            assert (
                persisted_monitor._lifecycle._serialize_state()
                == memory_monitor._lifecycle._serialize_state()
            )


@pytest.mark.asyncio
async def test_runtime_store_cycle_does_not_deepcopy_all_histories(
    monkeypatch, small_history, tmp_path
):
    del small_history
    monkeypatch.setattr(
        monitor_module,
        "deepcopy",
        lambda _value: pytest.fail("live monitor must not deepcopy all histories"),
        raising=False,
    )
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        instance = monitor(store=store)
        prime(instance)
        assert await instance.evaluate(START, state_for(START)) == []


@pytest.mark.asyncio
async def test_manual_cycle_uses_at_most_one_runtime_transaction(small_history, tmp_path):
    del small_history
    current = START
    venues = (
        ("arcus", "QQQ-USD"),
        ("lighter_robinhood", "QQQ"),
        ("backpack", "QQQ.US_USDC_PERP"),
    )
    state = RadarState()
    state.apply_market_batch(
        CollectorBatch(
            market_snapshots=tuple(
                snapshot(
                    venue,
                    venue_symbol,
                    current,
                    best_bid=99.0 + index,
                    best_ask=100.0 + index,
                )
                for index, (venue, venue_symbol) in enumerate(venues)
            )
        )
    )
    state.apply_context_batch(
        CollectorBatch(
            hourly_contexts=tuple(
                context(venue, venue_symbol, current)
                for venue, venue_symbol in venues
            )
        )
    )
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        calls = 0
        original = store.set_monitor_state_and_append_opportunities

        def counted(*args, **kwargs):
            nonlocal calls
            calls += 1
            return original(*args, **kwargs)

        store.set_monitor_state_and_append_opportunities = counted  # type: ignore[method-assign]
        instance = monitor(store=store)
        assert await instance.evaluate(current, state) == []
        assert calls <= 1


@pytest.mark.asyncio
async def test_production_scale_shape_keeps_one_transaction_for_420_routes(
    small_history, tmp_path
):
    del small_history
    current = START
    venues = tuple(
        (f"venue_{index:02d}", f"QQQ-{index:02d}") for index in range(21)
    )
    state = RadarState()
    state.apply_market_batch(
        CollectorBatch(
            market_snapshots=tuple(
                snapshot(
                    venue,
                    venue_symbol,
                    current,
                    best_bid=99.0 + index,
                    best_ask=100.0 + index,
                )
                for index, (venue, venue_symbol) in enumerate(venues)
            )
        )
    )
    state.apply_context_batch(
        CollectorBatch(
            hourly_contexts=tuple(
                context(venue, venue_symbol, current)
                for venue, venue_symbol in venues
            )
        )
    )
    fees = {venue: 0.0 for venue, _symbol in venues}
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        calls = 0
        original = store.set_monitor_state_and_append_opportunities

        def counted(*args, **kwargs):
            nonlocal calls
            calls += 1
            return original(*args, **kwargs)

        store.set_monitor_state_and_append_opportunities = counted  # type: ignore[method-assign]
        instance = ManualOpportunityMonitor(
            ManualOpportunityConfig(), fees, runtime_store=store
        )
        assert await instance.evaluate(current, state) == []

        assert len(instance._histories) == 21 * 20
        assert calls == 1


@pytest.mark.asyncio
async def test_manual_monitor_logs_cycle_timing(small_history, tmp_path, caplog):
    del small_history
    caplog.set_level(logging.INFO, logger=monitor_module.__name__)
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        instance = monitor(store=store)
        prime(instance)
        await instance.evaluate(START, state_for(START))

    message = next(
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("manual opportunity cycle")
    )
    for field in (
        "route_preparation_ms=",
        "history_ms=",
        "lifecycle_ms=",
        "sqlite_persistence_ms=",
        "total_ms=",
        "routes_evaluated=",
        "active_episodes=",
        "alerts_emitted=",
    ):
        assert field in message
