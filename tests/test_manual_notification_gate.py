from __future__ import annotations

from datetime import UTC, datetime, timedelta

from radar.config import ManualOpportunityConfig
from radar.monitors.manual_opportunity import ManualOpportunityNotificationGate
from radar.monitors.spread.models import SpreadPairKey
from radar.storage.sqlite import SQLiteRuntimeStore


START = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
KEY = SpreadPairKey("NVDA", "lighter", "NVDA", "arcus", "NVDA-USD")
REVERSE_KEY = SpreadPairKey("NVDA", "arcus", "NVDA-USD", "lighter", "NVDA")


def make_gate(
    store: SQLiteRuntimeStore | None = None,
    *,
    quiet_seconds: int = 300,
) -> ManualOpportunityNotificationGate:
    return ManualOpportunityNotificationGate(
        ManualOpportunityConfig(telegram_rearm_quiet_seconds=quiet_seconds),
        runtime_store=store,
    )


def disarm(
    gate: ManualOpportunityNotificationGate,
    key: SpreadPairKey = KEY,
    episode_id: str = "episode-1",
) -> None:
    assert gate.reserve_initial(key, episode_id)
    gate.mark_sent(key, episode_id, sent_at=START)
    assert not gate.is_armed(key)


def test_first_reserved_initial_becomes_disarmed_after_successful_send():
    gate = make_gate()

    assert gate.reserve_initial(KEY, "episode-1")
    gate.mark_sent(KEY, "episode-1", sent_at=START)

    assert not gate.is_armed(KEY)
    assert gate.notified_episode_id(KEY) == "episode-1"


def test_four_minutes_fifty_nine_seconds_of_quiet_does_not_rearm():
    gate = make_gate()
    disarm(gate)

    gate.observe(KEY, qualifies=False, observed_at=START)
    gate.observe(
        KEY,
        qualifies=False,
        observed_at=START + timedelta(seconds=299),
    )

    assert not gate.is_armed(KEY)


def test_five_minutes_of_continuous_observed_nonqualification_rearms():
    gate = make_gate()
    disarm(gate)

    for seconds in range(0, 301, 10):
        gate.observe(KEY, qualifies=False, observed_at=START + timedelta(seconds=seconds))

    assert gate.is_armed(KEY)


def test_qualifying_observation_resets_quiet_timer():
    gate = make_gate()
    disarm(gate)

    for seconds in range(0, 181, 10):
        gate.observe(KEY, qualifies=False, observed_at=START + timedelta(seconds=seconds))
    gate.observe(KEY, qualifies=True, observed_at=START + timedelta(seconds=180))
    for seconds in range(190, 480, 10):
        gate.observe(KEY, qualifies=False, observed_at=START + timedelta(seconds=seconds))

    assert not gate.is_armed(KEY)
    gate.observe(KEY, qualifies=False, observed_at=START + timedelta(seconds=480))
    assert not gate.is_armed(KEY)
    gate.observe(KEY, qualifies=False, observed_at=START + timedelta(seconds=490))
    assert gate.is_armed(KEY)


def test_data_gap_does_not_count_as_quiet_rearm_time():
    gate = make_gate()
    disarm(gate)

    gate.observe(KEY, qualifies=False, observed_at=START)
    gate.observe(KEY, qualifies=False, observed_at=START + timedelta(seconds=301))

    assert not gate.is_armed(KEY)


def test_directional_routes_have_independent_notification_state():
    gate = make_gate()
    disarm(gate, KEY, "episode-forward")
    assert gate.reserve_initial(REVERSE_KEY, "episode-reverse")
    gate.mark_sent(REVERSE_KEY, "episode-reverse", sent_at=START)

    gate.observe(KEY, qualifies=False, observed_at=START)

    assert not gate.is_armed(KEY)
    assert not gate.is_armed(REVERSE_KEY)
    assert gate.notified_episode_id(KEY) == "episode-forward"
    assert gate.notified_episode_id(REVERSE_KEY) == "episode-reverse"


def test_restart_preserves_disarmed_state(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        first = make_gate(store)
        first.begin_cycle()
        disarm(first)
        first.commit_cycle(START)

    with SQLiteRuntimeStore(database) as store:
        restored = make_gate(store)
        assert not restored.is_armed(KEY)
        assert restored.notified_episode_id(KEY) == "episode-1"


def test_restart_does_not_treat_unobserved_gap_as_continuous_quiet(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        first = make_gate(store)
        first.begin_cycle()
        disarm(first)
        first.observe(KEY, qualifies=False, observed_at=START)
        first.commit_cycle(START)

    with SQLiteRuntimeStore(database) as store:
        restored = make_gate(store)
        restored.begin_cycle()
        restored.observe(
            KEY,
            qualifies=False,
            observed_at=START + timedelta(seconds=120),
        )
        restored.commit_cycle(START + timedelta(seconds=120))

        assert not restored.is_armed(KEY)


def test_rearmed_state_survives_restart(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        first = make_gate(store)
        first.begin_cycle()
        disarm(first)
        for seconds in range(0, 301, 10):
            first.observe(
                KEY,
                qualifies=False,
                observed_at=START + timedelta(seconds=seconds),
            )
        first.commit_cycle(START + timedelta(seconds=300))

    with SQLiteRuntimeStore(database) as store:
        restored = make_gate(store)
        assert restored.is_armed(KEY)


def test_expansion_is_allowed_only_for_successfully_notified_episode():
    gate = make_gate()

    assert gate.allow_expansion(KEY, "unknown-episode") is False
    disarm(gate, KEY, "notified-episode")
    assert gate.allow_expansion(KEY, "notified-episode") is True
