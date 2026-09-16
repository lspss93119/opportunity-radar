from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol, TypeAlias, runtime_checkable

from radar.state import RadarState

UTC = timezone.utc
JSONValue: TypeAlias = (
    None
    | bool
    | int
    | float
    | str
    | list["JSONValue"]
    | dict[str, "JSONValue"]
)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be non-empty")
    return value


def _require_utc(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    return value.astimezone(UTC)


def _validate_json_value(value: object, field_name: str = "payload") -> None:
    if value is None or isinstance(value, (bool, int, str)):
        return
    if isinstance(value, float):
        if math.isfinite(value):
            return
        raise ValueError(f"{field_name} must be JSON-compatible")
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{field_name}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{field_name} must be JSON-compatible")
            _validate_json_value(item, f"{field_name}.{key}")
        return
    raise ValueError(f"{field_name} must be JSON-compatible")


@dataclass(frozen=True)
class AlertRequest:
    monitor: str
    event_id: str
    created_at: datetime
    payload: dict[str, JSONValue]

    def __post_init__(self) -> None:
        object.__setattr__(self, "monitor", _require_text(self.monitor, "monitor"))
        object.__setattr__(self, "event_id", _require_text(self.event_id, "event_id"))
        object.__setattr__(
            self,
            "created_at",
            _require_utc(self.created_at, "created_at"),
        )
        if not isinstance(self.payload, dict):
            raise ValueError("payload must be JSON-compatible")
        payload = dict(self.payload)
        _validate_json_value(payload)
        object.__setattr__(self, "payload", payload)


@runtime_checkable
class Monitor(Protocol):
    name: str
    interval_seconds: int

    async def evaluate(
        self,
        now: datetime,
        state: RadarState,
    ) -> list[AlertRequest]:
        """Evaluate the latest in-memory state without slow historical work."""
