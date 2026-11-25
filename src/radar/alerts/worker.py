from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable
from typing import Protocol

from radar.monitors.base import AlertRequest

LOGGER = logging.getLogger(__name__)
_URL_OR_BOT_TOKEN = re.compile(
    r"https?://\S+|\b\d{8,}:[A-Za-z0-9_-]{20,}\b",
    re.IGNORECASE,
)


def _safe_log_value(value: object) -> str:
    return _URL_OR_BOT_TOKEN.sub("[redacted]", str(value))


class AlertProcessor(Protocol):
    async def process(self, alert: AlertRequest) -> None:
        ...


class AlertRouter:
    """Route alert requests to their existing slow-path processors."""

    def __init__(
        self,
        spread_processor: AlertProcessor,
        manual_processor: AlertProcessor,
        funding_processor: AlertProcessor | None = None,
    ) -> None:
        self._spread_processor = spread_processor
        self._manual_processor = manual_processor
        self._funding_processor = funding_processor

    async def process(self, alert: AlertRequest) -> None:
        if alert.monitor == "spread":
            await self._spread_processor.process(alert)
        elif alert.monitor == "manual_opportunity":
            await self._manual_processor.process(alert)
        elif alert.monitor == "funding" and self._funding_processor is not None:
            await self._funding_processor.process(alert)
        else:
            raise ValueError(f"unknown alert monitor: {alert.monitor}")


class AlertWorker:
    def __init__(
        self,
        queue: asyncio.Queue[AlertRequest],
        processor: AlertProcessor,
        *,
        error_handler: Callable[[AlertRequest, Exception], None] | None = None,
    ) -> None:
        self._queue = queue
        self._processor = processor
        self._error_handler = error_handler

    async def run_once(self) -> None:
        alert = await self._queue.get()
        try:
            await self._processor.process(alert)
            payload = alert.payload
            kind = payload.get("kind", payload.get("event_type", "unknown"))
            symbol = payload.get("canonical_symbol", "unknown")
            LOGGER.info(
                "alert sent monitor=%s kind=%s symbol=%s event_id=%s",
                _safe_log_value(alert.monitor),
                _safe_log_value(kind),
                _safe_log_value(symbol),
                _safe_log_value(alert.event_id),
            )
        except Exception as error:  # noqa: BLE001
            if self._error_handler is None:
                LOGGER.error("alert processing failed for event_id=%s", alert.event_id)
            else:
                try:
                    self._error_handler(alert, error)
                except Exception:  # noqa: BLE001
                    LOGGER.error(
                        "alert error handler failed for event_id=%s",
                        alert.event_id,
                    )
        finally:
            self._queue.task_done()

    async def run_forever(self) -> None:
        while True:
            await self.run_once()
