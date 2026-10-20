from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

from radar.monitors.base import AlertRequest
from radar.storage.sqlite import NotificationOutboxEntry, SQLiteRuntimeStore

LOGGER = logging.getLogger(__name__)
DEFAULT_RETRY_DELAYS_SECONDS = (5.0, 30.0, 300.0, 900.0)
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_POLL_INTERVAL_SECONDS = 1.0


class AlertProcessor(Protocol):
    async def process(self, alert: AlertRequest) -> None:
        ...


class AlertRouter:
    """Route alert requests to their existing slow-path processors."""

    def __init__(
        self,
        spread_processor: AlertProcessor,
        manual_processor: AlertProcessor,
    ) -> None:
        self._spread_processor = spread_processor
        self._manual_processor = manual_processor

    async def process(self, alert: AlertRequest) -> None:
        if alert.monitor == "spread":
            await self._spread_processor.process(alert)
        elif alert.monitor == "manual_opportunity":
            await self._manual_processor.process(alert)
        else:
            raise ValueError(f"unknown alert monitor: {alert.monitor}")


class AlertWorker:
    def __init__(
        self,
        queue: asyncio.Queue[AlertRequest],
        processor: AlertProcessor,
        *,
        error_handler: Callable[[AlertRequest, Exception], None] | None = None,
        runtime_store: SQLiteRuntimeStore | None = None,
        retry_delays_seconds: tuple[float, ...] = DEFAULT_RETRY_DELAYS_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> None:
        if not retry_delays_seconds or any(
            delay < 0 for delay in retry_delays_seconds
        ):
            raise ValueError("retry_delays_seconds must contain non-negative delays")
        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        self._queue = queue
        self._processor = processor
        self._error_handler = error_handler
        self._runtime_store = runtime_store
        self._retry_delays_seconds = retry_delays_seconds
        self._max_attempts = max_attempts
        self._poll_interval_seconds = poll_interval_seconds

    async def run_once(self) -> None:
        alert = await self._queue.get()
        try:
            await self._process_queued_alert(alert)
        finally:
            self._queue.task_done()

    async def run_forever(self) -> None:
        if self._runtime_store is None:
            while True:
                await self.run_once()
        while True:
            try:
                alert = await asyncio.wait_for(
                    self._queue.get(), timeout=self._poll_interval_seconds
                )
            except asyncio.TimeoutError:
                await self._process_due_notification()
                continue
            try:
                await self._process_queued_alert(alert)
            finally:
                self._queue.task_done()

    async def _process_queued_alert(self, alert: AlertRequest) -> None:
        if self._runtime_store is None:
            await self._process_without_outbox(alert)
            return
        entry = self._runtime_store.get_notification_outbox(
            alert.monitor, alert.event_id
        )
        if entry is None:
            # Keep compatibility with a monitor that has no runtime store yet.
            await self._process_without_outbox(alert)
            return
        await self._process_outbox_entry(entry)

    async def _process_due_notification(self) -> None:
        assert self._runtime_store is not None
        entries = self._runtime_store.list_notification_outbox()
        if entries:
            await self._process_outbox_entry(entries[0])

    async def _process_outbox_entry(self, entry: NotificationOutboxEntry) -> None:
        if entry.status != "pending":
            return
        if entry.available_at > datetime.now(UTC):
            return
        alert = entry.alert
        try:
            attempt = self._runtime_store.record_notification_attempt(
                alert.monitor,
                alert.event_id,
                max_attempts=self._max_attempts,
            )
        except Exception:  # noqa: BLE001
            LOGGER.exception(
                "notification outbox attempt could not be recorded event_id=%s",
                alert.event_id,
            )
            return
        if attempt is None:
            try:
                self._runtime_store.record_notification_failure(
                    alert.monitor,
                    alert.event_id,
                    error_summary="max_attempts_exhausted",
                    next_attempt_at=datetime.now(UTC),
                    terminal=True,
                )
            except Exception:  # noqa: BLE001
                LOGGER.exception(
                    "notification outbox max-attempt terminal state could not be recorded "
                    "event_id=%s",
                    alert.event_id,
                )
            return
        try:
            await self._processor.process(alert)
        except Exception as error:  # noqa: BLE001
            self._report_failure(alert, error)
            terminal = self._is_terminal_error(error) or attempt >= self._max_attempts
            delay = self._retry_delays_seconds[
                min(attempt - 1, len(self._retry_delays_seconds) - 1)
            ]
            try:
                self._runtime_store.record_notification_failure(
                    alert.monitor,
                    alert.event_id,
                    error_summary=self._error_summary(error),
                    next_attempt_at=datetime.now(UTC) + timedelta(seconds=delay),
                    terminal=terminal,
                )
            except Exception:  # noqa: BLE001
                LOGGER.exception(
                    "notification outbox failure could not be recorded event_id=%s",
                    alert.event_id,
                )
            return
        try:
            self._runtime_store.mark_notification_sent(
                alert.monitor, alert.event_id, sent_at=datetime.now(UTC)
            )
        except Exception:  # noqa: BLE001
            # The send may have succeeded. Leave the durable row pending so a
            # later worker can retry; this is intentionally at-least-once.
            LOGGER.exception(
                "notification outbox acknowledgement failed event_id=%s",
                alert.event_id,
            )

    async def _process_without_outbox(self, alert: AlertRequest) -> None:
        try:
            await self._processor.process(alert)
        except Exception as error:  # noqa: BLE001
            self._report_failure(alert, error)

    def _report_failure(self, alert: AlertRequest, error: Exception) -> None:
        if self._error_handler is None:
            LOGGER.error(
                "alert processing failed for event_id=%s error=%s",
                alert.event_id,
                self._error_summary(error),
            )
            return
        try:
            self._error_handler(alert, error)
        except Exception:  # noqa: BLE001
            LOGGER.error(
                "alert error handler failed for event_id=%s",
                alert.event_id,
            )

    @staticmethod
    def _is_terminal_error(error: Exception) -> bool:
        if isinstance(error, (ValueError, TypeError)):
            return True
        status_code = getattr(error, "status_code", None)
        if isinstance(status_code, int) and 400 <= status_code < 500 and status_code != 429:
            return True
        return False

    @staticmethod
    def _error_summary(error: Exception) -> str:
        error_kind = getattr(error, "error_kind", None)
        status_code = getattr(error, "status_code", None)
        details = [type(error).__name__]
        if isinstance(error_kind, str) and error_kind:
            details.append(error_kind)
        if isinstance(status_code, int):
            details.append(f"status_code={status_code}")
        return ":".join(details)
