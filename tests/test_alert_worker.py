from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from radar.monitors.base import AlertRequest

NOW = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


def make_alert(event_id: str) -> AlertRequest:
    return AlertRequest(
        monitor="spread",
        event_id=event_id,
        created_at=NOW,
        payload={"event_id": event_id},
    )


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.processed: list[AlertRequest] = []

    async def process(self, alert: AlertRequest) -> None:
        self.processed.append(alert)
        if self.error is not None and alert.event_id == "failed":
            raise self.error


@pytest.mark.asyncio
async def test_worker_processes_one_item_and_accounts_for_queue_task():
    from radar.alerts.worker import AlertWorker

    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    processor = FakeProcessor()
    alert = make_alert("success")
    queue.put_nowait(alert)
    worker = AlertWorker(queue, processor)  # type: ignore[arg-type]

    await worker.run_once()
    await asyncio.wait_for(queue.join(), timeout=1)

    assert processor.processed == [alert]
    assert queue.empty()


@pytest.mark.asyncio
async def test_worker_reports_failure_continues_and_accounts_for_all_items():
    from radar.alerts.worker import AlertWorker

    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    processor_error = RuntimeError("processor failed")
    processor = FakeProcessor(error=processor_error)
    failed = make_alert("failed")
    succeeding = make_alert("succeeding")
    failures: list[tuple[AlertRequest, Exception]] = []

    def error_handler(alert: AlertRequest, error: Exception) -> None:
        failures.append((alert, error))

    queue.put_nowait(failed)
    queue.put_nowait(succeeding)
    worker = AlertWorker(
        queue,
        processor,  # type: ignore[arg-type]
        error_handler=error_handler,
    )

    await worker.run_once()
    await worker.run_once()
    await asyncio.wait_for(queue.join(), timeout=1)

    assert failures == [(failed, processor_error)]
    assert processor.processed == [failed, succeeding]
    assert queue.empty()


@pytest.mark.asyncio
async def test_worker_does_not_escape_processor_or_error_handler_failures():
    from radar.alerts.worker import AlertWorker

    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    processor = FakeProcessor(error=ValueError("invalid spread payload"))
    handler_calls: list[tuple[AlertRequest, Exception]] = []

    def failing_error_handler(alert: AlertRequest, error: Exception) -> None:
        handler_calls.append((alert, error))
        raise RuntimeError("error hook failed")

    alert = make_alert("failed")
    queue.put_nowait(alert)
    worker = AlertWorker(
        queue,
        processor,  # type: ignore[arg-type]
        error_handler=failing_error_handler,
    )

    await worker.run_once()
    await asyncio.wait_for(queue.join(), timeout=1)

    assert handler_calls == [(alert, processor.error)]
    assert queue.empty()


@pytest.mark.asyncio
async def test_run_forever_processes_items_until_cancelled():
    from radar.alerts.worker import AlertWorker

    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    processor = FakeProcessor()
    alert = make_alert("forever")
    queue.put_nowait(alert)
    worker = AlertWorker(queue, processor)  # type: ignore[arg-type]

    task = asyncio.create_task(worker.run_forever())
    await asyncio.wait_for(queue.join(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert processor.processed == [alert]
