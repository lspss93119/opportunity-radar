import sqlite3
from datetime import datetime, timezone

import pytest

from radar.storage.sqlite import SQLiteRuntimeStore

UTC = timezone.utc


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
