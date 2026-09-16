from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TypeAlias

from radar.monitors.base import AlertRequest, Monitor
from radar.state import RadarState

UTC = timezone.utc
LOGGER = logging.getLogger(__name__)
ErrorHandler: TypeAlias = Callable[[str, Exception], None]


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True)
class _EvaluationResult:
    monitor: Monitor
    alerts: list[AlertRequest]
    error: Exception | None = None


class MonitorRunner:
    """Run enabled monitors on their cadences and enqueue their alerts."""

    def __init__(
        self,
        monitors: Sequence[Monitor],
        state: RadarState,
        queue: asyncio.Queue[AlertRequest] | None = None,
        *,
        error_handler: ErrorHandler | None = None,
    ) -> None:
        self.monitors = tuple(monitors)
        self.state = state
        self.queue = asyncio.Queue() if queue is None else queue
        self.error_handler = error_handler
        self._last_run: dict[str, datetime | None] = {}
        self._running: set[str] = set()
        self._validate_monitors()

    def _validate_monitors(self) -> None:
        names: set[str] = set()
        for monitor in self.monitors:
            if not isinstance(monitor, Monitor):
                raise TypeError("monitors must satisfy the Monitor protocol")
            if not isinstance(monitor.name, str) or not monitor.name.strip():
                raise ValueError("monitor name must be non-empty")
            if (
                isinstance(monitor.interval_seconds, bool)
                or not isinstance(monitor.interval_seconds, int)
                or monitor.interval_seconds <= 0
            ):
                raise ValueError("monitor interval_seconds must be positive")
            if monitor.name in names:
                raise ValueError(f"duplicate monitor name: {monitor.name}")
            names.add(monitor.name)
            self._last_run[monitor.name] = None

    def _is_due(self, monitor: Monitor, now: datetime) -> bool:
        last_run = self._last_run[monitor.name]
        return last_run is None or now >= last_run + timedelta(
            seconds=monitor.interval_seconds
        )

    async def run_cycle(self, now: datetime) -> None:
        current_time = _as_utc(now)
        due_monitors = tuple(
            monitor
            for monitor in self.monitors
            if monitor.name not in self._running and self._is_due(monitor, current_time)
        )
        if not due_monitors:
            return

        for monitor in due_monitors:
            self._running.add(monitor.name)
            self._last_run[monitor.name] = current_time
        try:
            results = await asyncio.gather(
                *(self._evaluate(monitor, current_time) for monitor in due_monitors)
            )
        finally:
            for monitor in due_monitors:
                self._running.discard(monitor.name)

        for result in results:
            if result.error is not None:
                self._report_error(result.monitor.name, result.error)
                continue
            for alert in result.alerts:
                await self.queue.put(alert)

    async def _evaluate(
        self,
        monitor: Monitor,
        now: datetime,
    ) -> _EvaluationResult:
        try:
            alerts = list(await monitor.evaluate(now, self.state))
            if any(not isinstance(alert, AlertRequest) for alert in alerts):
                raise TypeError("monitor evaluate() must return AlertRequest values")
            return _EvaluationResult(monitor, alerts)
        except Exception as error:
            return _EvaluationResult(monitor, [], error)

    def _report_error(self, monitor_name: str, error: Exception) -> None:
        if self.error_handler is not None:
            try:
                self.error_handler(monitor_name, error)
            except Exception as handler_error:
                LOGGER.error(
                    "monitor error handler failed for %s",
                    monitor_name,
                    exc_info=handler_error,
                )
            return
        LOGGER.error("monitor %s evaluation failed", monitor_name, exc_info=error)
