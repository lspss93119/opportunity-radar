from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from radar.monitors.base import AlertRequest

UTC = timezone.utc


def _utc_iso(value: datetime | None, field_name: str) -> str:
    timestamp = datetime.now(UTC) if value is None else value
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return timestamp.astimezone(UTC).isoformat()


def _require_text(value: str, field_name: str) -> str:
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    return value


def _json_text(payload: object) -> str:
    try:
        return json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("payload must be JSON-compatible") from exc


@dataclass(frozen=True)
class NotificationOutboxEntry:
    """Durable notification state used by the single alert worker."""

    alert: AlertRequest
    status: str
    attempt_count: int
    available_at: datetime
    last_error: str | None
    created_at: datetime
    sent_at: datetime | None


class SQLiteRuntimeStore:
    """Small generic SQLite store for runtime state and opportunity events."""

    def __init__(self, database: Path) -> None:
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.database)
        self._connection.row_factory = sqlite3.Row
        self._initialize()

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS monitor_state (
                monitor_name TEXT NOT NULL,
                state_key TEXT NOT NULL,
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (monitor_name, state_key)
            );

            CREATE TABLE IF NOT EXISTS opportunity_log (
                monitor_name TEXT NOT NULL,
                event_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_json TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                PRIMARY KEY (monitor_name, event_id)
            );

            CREATE TABLE IF NOT EXISTS notification_outbox (
                monitor_name TEXT NOT NULL,
                event_id TEXT NOT NULL,
                alert_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending', 'sent', 'failed')),
                attempt_count INTEGER NOT NULL DEFAULT 0,
                available_at TEXT NOT NULL,
                last_error TEXT,
                created_at TEXT NOT NULL,
                sent_at TEXT,
                PRIMARY KEY (monitor_name, event_id)
            );

            CREATE INDEX IF NOT EXISTS idx_notification_outbox_due
            ON notification_outbox (status, available_at);
            """
        )
        self._connection.commit()

    def set_monitor_state(
        self,
        monitor_name: str,
        state_key: str,
        state: object,
        *,
        updated_at: datetime | None = None,
    ) -> None:
        monitor_name = _require_text(monitor_name, "monitor_name")
        state_key = _require_text(state_key, "state_key")
        self._connection.execute(
            """
            INSERT INTO monitor_state (monitor_name, state_key, state_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (monitor_name, state_key) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = excluded.updated_at
            """,
            (monitor_name, state_key, _json_text(state), _utc_iso(updated_at, "updated_at")),
        )
        self._connection.commit()

    def set_monitor_state_and_append_opportunities(
        self,
        monitor_name: str,
        state_key: str,
        state: object,
        *,
        updated_at: datetime | None = None,
        opportunities: Sequence[tuple[str, str, object, datetime | None]] = (),
        additional_states: Sequence[tuple[str, str, object, datetime | None]] = (),
        notifications: Sequence[AlertRequest] = (),
    ) -> None:
        """Commit one runtime-state update and its events as one transaction."""
        self.set_monitor_states_and_append_opportunities(
            ((monitor_name, state_key, state, updated_at), *additional_states),
            opportunity_monitor_name=monitor_name,
            opportunities=opportunities,
            notifications=notifications,
        )

    def set_monitor_states_and_append_opportunities(
        self,
        states: Sequence[tuple[str, str, object, datetime | None]],
        *,
        opportunity_monitor_name: str | None = None,
        opportunities: Sequence[tuple[str, str, object, datetime | None]] = (),
        notifications: Sequence[AlertRequest] = (),
    ) -> None:
        """Commit multiple runtime states and their events atomically."""
        if not states:
            raise ValueError("states must not be empty")
        event_monitor_name = _require_text(
            opportunity_monitor_name or states[0][0],
            "opportunity_monitor_name",
        )
        state_rows = [
            (
                _require_text(monitor_name, "monitor_name"),
                _require_text(state_key, "state_key"),
                _json_text(state),
                _utc_iso(updated_at, "updated_at"),
            )
            for monitor_name, state_key, state, updated_at in states
        ]
        event_rows = [
            (
                _require_text(event_id, "event_id"),
                _require_text(event_type, "event_type"),
                _json_text(event),
                _utc_iso(occurred_at, "occurred_at"),
            )
            for event_id, event_type, event, occurred_at in opportunities
        ]
        notification_rows = [self._notification_row(alert) for alert in notifications]
        with self._connection:
            self._connection.executemany(
                """
                INSERT INTO monitor_state (monitor_name, state_key, state_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (monitor_name, state_key) DO UPDATE SET
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                state_rows,
            )
            self._connection.executemany(
                """
                INSERT INTO opportunity_log
                    (monitor_name, event_id, event_type, event_json, occurred_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (event_monitor_name, event_id, event_type, event_json, occurred_at)
                    for event_id, event_type, event_json, occurred_at in event_rows
                ],
            )
            self._connection.executemany(
                """
                INSERT INTO notification_outbox
                    (monitor_name, event_id, alert_json, status, attempt_count,
                     available_at, last_error, created_at, sent_at)
                VALUES (?, ?, ?, 'pending', 0, ?, NULL, ?, NULL)
                ON CONFLICT (monitor_name, event_id) DO NOTHING
                """,
                notification_rows,
            )

    @staticmethod
    def _notification_row(alert: AlertRequest) -> tuple[str, str, str, str, str]:
        if not isinstance(alert, AlertRequest):
            raise TypeError("notifications must contain AlertRequest values")
        created_at = _utc_iso(alert.created_at, "alert.created_at")
        return (
            alert.monitor,
            alert.event_id,
            _json_text(
                {
                    "monitor": alert.monitor,
                    "event_id": alert.event_id,
                    "created_at": created_at,
                    "payload": alert.payload,
                }
            ),
            created_at,
            created_at,
        )

    @staticmethod
    def _notification_from_row(row: sqlite3.Row) -> NotificationOutboxEntry:
        alert_json = json.loads(row["alert_json"])
        alert = AlertRequest(
            monitor=alert_json["monitor"],
            event_id=alert_json["event_id"],
            created_at=datetime.fromisoformat(alert_json["created_at"]).astimezone(UTC),
            payload=alert_json["payload"],
        )
        available_at = datetime.fromisoformat(row["available_at"]).astimezone(UTC)
        created_at = datetime.fromisoformat(row["created_at"]).astimezone(UTC)
        sent_at = (
            None
            if row["sent_at"] is None
            else datetime.fromisoformat(row["sent_at"]).astimezone(UTC)
        )
        return NotificationOutboxEntry(
            alert=alert,
            status=row["status"],
            attempt_count=int(row["attempt_count"]),
            available_at=available_at,
            last_error=row["last_error"],
            created_at=created_at,
            sent_at=sent_at,
        )

    def get_notification_outbox(
        self, monitor_name: str, event_id: str
    ) -> NotificationOutboxEntry | None:
        row = self._connection.execute(
            """
            SELECT monitor_name, event_id, alert_json, status, attempt_count,
                   available_at, last_error, created_at, sent_at
            FROM notification_outbox
            WHERE monitor_name = ? AND event_id = ?
            """,
            (
                _require_text(monitor_name, "monitor_name"),
                _require_text(event_id, "event_id"),
            ),
        ).fetchone()
        return None if row is None else self._notification_from_row(row)

    def list_notification_outbox(
        self, *, now: datetime | None = None
    ) -> list[NotificationOutboxEntry]:
        now_iso = _utc_iso(now, "now")
        rows = self._connection.execute(
            """
            SELECT monitor_name, event_id, alert_json, status, attempt_count,
                   available_at, last_error, created_at, sent_at
            FROM notification_outbox
            WHERE status = 'pending' AND available_at <= ?
            ORDER BY available_at, created_at, monitor_name, event_id
            """,
            (now_iso,),
        ).fetchall()
        return [self._notification_from_row(row) for row in rows]

    def record_notification_attempt(self, monitor_name: str, event_id: str) -> int:
        cursor = self._connection.execute(
            """
            UPDATE notification_outbox
            SET attempt_count = attempt_count + 1
            WHERE monitor_name = ? AND event_id = ? AND status = 'pending'
            """,
            (
                _require_text(monitor_name, "monitor_name"),
                _require_text(event_id, "event_id"),
            ),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"pending notification not found: {monitor_name}/{event_id}")
        self._connection.commit()
        row = self._connection.execute(
            """
            SELECT attempt_count
            FROM notification_outbox
            WHERE monitor_name = ? AND event_id = ?
            """,
            (monitor_name, event_id),
        ).fetchone()
        assert row is not None
        return int(row["attempt_count"])

    def record_notification_failure(
        self,
        monitor_name: str,
        event_id: str,
        *,
        error_summary: str,
        next_attempt_at: datetime,
        terminal: bool,
    ) -> None:
        status = "failed" if terminal else "pending"
        cursor = self._connection.execute(
            """
            UPDATE notification_outbox
            SET status = ?, available_at = ?, last_error = ?
            WHERE monitor_name = ? AND event_id = ? AND status = 'pending'
            """,
            (
                status,
                _utc_iso(next_attempt_at, "next_attempt_at"),
                _require_text(error_summary, "error_summary"),
                _require_text(monitor_name, "monitor_name"),
                _require_text(event_id, "event_id"),
            ),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"pending notification not found: {monitor_name}/{event_id}")
        self._connection.commit()

    def mark_notification_sent(
        self,
        monitor_name: str,
        event_id: str,
        *,
        sent_at: datetime | None = None,
    ) -> None:
        sent_iso = _utc_iso(sent_at, "sent_at")
        cursor = self._connection.execute(
            """
            UPDATE notification_outbox
            SET status = 'sent', sent_at = ?, available_at = ?, last_error = NULL
            WHERE monitor_name = ? AND event_id = ? AND status = 'pending'
            """,
            (
                sent_iso,
                sent_iso,
                _require_text(monitor_name, "monitor_name"),
                _require_text(event_id, "event_id"),
            ),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"pending notification not found: {monitor_name}/{event_id}")
        self._connection.commit()

    def get_monitor_state(self, monitor_name: str, state_key: str) -> object | None:
        row = self._connection.execute(
            """
            SELECT state_json
            FROM monitor_state
            WHERE monitor_name = ? AND state_key = ?
            """,
            (monitor_name, state_key),
        ).fetchone()
        return None if row is None else json.loads(row["state_json"])

    def append_opportunity(
        self,
        monitor_name: str,
        event_id: str,
        event_type: str,
        event: object,
        *,
        occurred_at: datetime | None = None,
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO opportunity_log
                (monitor_name, event_id, event_type, event_json, occurred_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                _require_text(monitor_name, "monitor_name"),
                _require_text(event_id, "event_id"),
                _require_text(event_type, "event_type"),
                _json_text(event),
                _utc_iso(occurred_at, "occurred_at"),
            ),
        )
        self._connection.commit()

    def list_opportunities(
        self, *, monitor_name: str | None = None
    ) -> list[dict[str, object]]:
        if monitor_name is None:
            rows = self._connection.execute(
                """
                SELECT monitor_name, event_id, event_type, event_json, occurred_at
                FROM opportunity_log
                ORDER BY occurred_at, monitor_name, event_id
                """
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT monitor_name, event_id, event_type, event_json, occurred_at
                FROM opportunity_log
                WHERE monitor_name = ?
                ORDER BY occurred_at, event_id
                """,
                (monitor_name,),
            ).fetchall()
        return [
            {
                "monitor_name": row["monitor_name"],
                "event_id": row["event_id"],
                "event_type": row["event_type"],
                "event": json.loads(row["event_json"]),
                "occurred_at": datetime.fromisoformat(row["occurred_at"]).astimezone(
                    UTC
                ),
            }
            for row in rows
        ]

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "SQLiteRuntimeStore":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
