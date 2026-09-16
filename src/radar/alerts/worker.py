from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from radar.alerts.spread import SpreadAlertProcessor
from radar.monitors.base import AlertRequest

LOGGER = logging.getLogger(__name__)


class AlertWorker:
    def __init__(
        self,
        queue: asyncio.Queue[AlertRequest],
        processor: SpreadAlertProcessor,
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
