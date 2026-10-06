from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Protocol

from radar.monitors.base import AlertRequest

LOGGER = logging.getLogger(__name__)


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
    ) -> None:
        self._queue = queue
        self._processor = processor
        self._error_handler = error_handler

    async def run_once(self) -> None:
        alert = await self._queue.get()
        try:
            await self._processor.process(alert)
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
