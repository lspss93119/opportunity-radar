from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

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
