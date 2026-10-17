import sqlite3
from datetime import datetime, timezone

import pytest

from radar.monitors.base import AlertRequest
from radar.storage.sqlite import SQLiteRuntimeStore

UTC = timezone.utc


def make_alert(event_id: str = "alert-1") -> AlertRequest:
    return AlertRequest(
        monitor="spread",
        event_id=event_id,
        created_at=datetime(2026, 9, 15, 12, 0, tzinfo=UTC),
        payload={"event_id": event_id, "event_kind": "legacy"},
    )


def test_monitor_state_updates_and_survives_reopen(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    updated_at = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)

    with SQLiteRuntimeStore(database) as store:
        store.set_monitor_state(
            "future-monitor",
            "BTC",
            {"phase": "candidate", "count": 1},
            updated_at=updated_at,
        )
        assert store.get_monitor_state("future-monitor", "BTC") == {
            "phase": "candidate",
            "count": 1,
        }

        store.set_monitor_state(
            "future-monitor",
            "BTC",
            {"phase": "alert", "count": 2},
            updated_at=updated_at,
        )
        assert store.get_monitor_state("future-monitor", "BTC") == {
            "phase": "alert",
            "count": 2,
        }

    with SQLiteRuntimeStore(database) as reopened:
        assert reopened.get_monitor_state("future-monitor", "BTC") == {
            "phase": "alert",
            "count": 2,
        }


def test_opportunity_log_is_generic_json_and_survives_reopen(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    occurred_at = datetime(2026, 9, 15, 12, 0, 10, tzinfo=UTC)
    event = {
        "symbol": "BTC",
        "venues": ["lighter", "hyperliquid"],
        "metrics": {"net_bps": 21.5},
    }

    with SQLiteRuntimeStore(database) as store:
        store.append_opportunity(
            "future-monitor",
            "event-1",
            "candidate_started",
            event,
            occurred_at=occurred_at,
        )
        assert store.list_opportunities() == [
            {
                "monitor_name": "future-monitor",
                "event_id": "event-1",
                "event_type": "candidate_started",
                "event": event,
                "occurred_at": occurred_at,
            }
        ]

    with SQLiteRuntimeStore(database) as reopened:
        assert reopened.list_opportunities(monitor_name="future-monitor") == [
            {
                "monitor_name": "future-monitor",
                "event_id": "event-1",
                "event_type": "candidate_started",
                "event": event,
                "occurred_at": occurred_at,
            }
        ]


def test_state_and_opportunity_events_commit_atomically(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    occurred_at = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)

    with SQLiteRuntimeStore(database) as store:
        store.append_opportunity(
            "spread",
            "existing-event",
            "alert",
            {"episode_id": "existing"},
            occurred_at=occurred_at,
        )

        with pytest.raises(sqlite3.IntegrityError):
            store.set_monitor_state_and_append_opportunities(
                "spread",
                "episodes",
                {"new": {"alerted": True}},
                updated_at=occurred_at,
                opportunities=(
                    (
                        "existing-event",
                        "resolved",
                        {"episode_id": "new"},
                        occurred_at,
                    ),
                ),
            )

        assert store.get_monitor_state("spread", "episodes") is None
        assert [
            event["event_type"]
            for event in store.list_opportunities(monitor_name="spread")
        ] == [
            "alert"
        ]


def test_multiple_states_and_opportunity_events_commit_atomically(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    occurred_at = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)

    with SQLiteRuntimeStore(database) as store:
        store.set_monitor_states_and_append_opportunities(
            (
                ("spread", "notifications", {"episode": {"sent": True}}, occurred_at),
                ("spread", "clusters", {"QQQ": {"sent_at": occurred_at.isoformat()}}, occurred_at),
            ),
            opportunities=(
                (
                    "delivery-result-1",
                    "anomaly_delivery_result",
                    {"outcome": "success"},
                    occurred_at,
                ),
            ),
        )

        assert store.get_monitor_state("spread", "notifications") == {
            "episode": {"sent": True}
        }
        assert store.get_monitor_state("spread", "clusters") == {
            "QQQ": {"sent_at": occurred_at.isoformat()}
        }
        assert store.list_opportunities(monitor_name="spread")[-1]["event_type"] == (
            "anomaly_delivery_result"
        )


def test_lifecycle_state_event_and_notification_commit_atomically_and_survive_reopen(
    tmp_path,
):
    database = tmp_path / "runtime.sqlite3"
    occurred_at = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    alert = make_alert()

    with SQLiteRuntimeStore(database) as store:
        store.set_monitor_state_and_append_opportunities(
            "spread",
            "episodes",
            {"episode-1": {"alerted": True}},
            updated_at=occurred_at,
            opportunities=(
                ("episode-1:alert", "alert", {"episode_id": "episode-1"}, occurred_at),
            ),
            notifications=(alert,),
        )

        pending = store.get_notification_outbox("spread", alert.event_id)
        assert pending is not None
        assert pending.status == "pending"
        assert pending.alert == alert

    with SQLiteRuntimeStore(database) as reopened:
        pending = reopened.get_notification_outbox("spread", alert.event_id)
        assert pending is not None
        assert pending.alert == alert
        assert reopened.get_monitor_state("spread", "episodes") == {
            "episode-1": {"alerted": True}
        }


def test_notification_outbox_duplicate_event_is_idempotent(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    occurred_at = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    alert = make_alert()

    with SQLiteRuntimeStore(database) as store:
        for state_key in ("episodes", "episodes-copy"):
            store.set_monitor_state_and_append_opportunities(
                "spread",
                state_key,
                {state_key: True},
                updated_at=occurred_at,
                notifications=(alert,),
            )

        rows = store.list_notification_outbox()
        assert len(rows) == 1
        assert rows[0].alert == alert


def test_notification_outbox_retry_and_terminal_failure_are_inspectable(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    occurred_at = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    retry_at = occurred_at + datetime.resolution
    alert = make_alert()

    with SQLiteRuntimeStore(database) as store:
        store.set_monitor_state_and_append_opportunities(
            "spread",
            "episodes",
            {},
            updated_at=occurred_at,
            notifications=(alert,),
        )
        assert store.record_notification_attempt("spread", alert.event_id) == 1
        store.record_notification_failure(
            "spread",
            alert.event_id,
            error_summary="RuntimeError",
            next_attempt_at=retry_at,
            terminal=False,
        )
        assert store.list_notification_outbox(now=occurred_at) == []
        due = store.list_notification_outbox(now=retry_at)
        assert len(due) == 1
        assert due[0].attempt_count == 1
        assert due[0].last_error == "RuntimeError"

        assert store.record_notification_attempt("spread", alert.event_id) == 2
        store.record_notification_failure(
            "spread",
            alert.event_id,
            error_summary="ValueError",
            next_attempt_at=retry_at,
            terminal=True,
        )
        terminal = store.get_notification_outbox("spread", alert.event_id)
        assert terminal is not None
        assert terminal.status == "failed"
        assert store.list_notification_outbox(now=retry_at) == []
