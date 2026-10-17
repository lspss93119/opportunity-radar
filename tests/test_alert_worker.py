from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from radar.monitors.base import AlertRequest
from radar.storage.sqlite import SQLiteRuntimeStore

NOW = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


def make_alert(event_id: str) -> AlertRequest:
    return AlertRequest(
        monitor="spread",
        event_id=event_id,
        created_at=NOW,
        payload={"event_id": event_id},
    )


def make_manual_alert(event_id: str) -> AlertRequest:
    return AlertRequest(
        monitor="manual_opportunity",
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


@pytest.mark.asyncio
async def test_alert_router_dispatches_spread_and_manual_independently():
    from radar.alerts.worker import AlertRouter

    spread = FakeProcessor()
    manual = FakeProcessor()
    router = AlertRouter(spread, manual)
    spread_alert = make_alert("spread")
    manual_alert = make_manual_alert("manual")

    await router.process(spread_alert)
    await router.process(manual_alert)

    assert spread.processed == [spread_alert]
    assert manual.processed == [manual_alert]


@pytest.mark.asyncio
async def test_unknown_monitor_reaches_existing_worker_error_handler():
    from radar.alerts.worker import AlertRouter, AlertWorker

    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    alert = AlertRequest(
        monitor="unknown",
        event_id="unknown",
        created_at=NOW,
        payload={},
    )
    failures: list[tuple[AlertRequest, Exception]] = []
    queue.put_nowait(alert)
    worker = AlertWorker(
        queue,
        AlertRouter(FakeProcessor(), FakeProcessor()),
        error_handler=lambda item, error: failures.append((item, error)),
    )

    await worker.run_once()

    assert failures[0][0] == alert
    assert isinstance(failures[0][1], ValueError)


def seed_outbox(store: SQLiteRuntimeStore, alert: AlertRequest) -> None:
    store.set_monitor_state_and_append_opportunities(
        alert.monitor,
        "episodes",
        {},
        updated_at=alert.created_at,
        notifications=(alert,),
    )


class FlakyProcessor:
    def __init__(self, failures: int = 0, error: Exception | None = None) -> None:
        self.failures = failures
        self.error = RuntimeError("temporary failure") if error is None else error
        self.processed: list[AlertRequest] = []

    async def process(self, alert: AlertRequest) -> None:
        self.processed.append(alert)
        if self.failures:
            self.failures -= 1
            raise self.error


@pytest.mark.asyncio
async def test_durable_worker_recovers_pending_notification_after_restart(tmp_path):
    from radar.alerts.worker import AlertWorker

    database = tmp_path / "runtime.sqlite3"
    alert = make_alert("restart-recovery")
    with SQLiteRuntimeStore(database) as store:
        seed_outbox(store, alert)
        processor = FlakyProcessor()
        worker = AlertWorker(
            asyncio.Queue(),
            processor,  # type: ignore[arg-type]
            runtime_store=store,
            poll_interval_seconds=0.01,
        )
        task = asyncio.create_task(worker.run_forever())
        await asyncio.wait_for(
            _wait_until(lambda: len(processor.processed) == 1),
            timeout=1,
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        row = store.get_notification_outbox("spread", alert.event_id)
        assert row is not None
        assert row.status == "sent"


@pytest.mark.asyncio
async def test_durable_worker_retries_transient_failure_and_records_attempts(tmp_path):
    from radar.alerts.worker import AlertWorker

    database = tmp_path / "runtime.sqlite3"
    alert = make_alert("transient")
    with SQLiteRuntimeStore(database) as store:
        seed_outbox(store, alert)
        processor = FlakyProcessor(failures=1)
        failures: list[tuple[AlertRequest, Exception]] = []
        worker = AlertWorker(
            asyncio.Queue(),
            processor,  # type: ignore[arg-type]
            runtime_store=store,
            error_handler=lambda item, error: failures.append((item, error)),
            retry_delays_seconds=(0.0,),
            poll_interval_seconds=0.01,
        )
        task = asyncio.create_task(worker.run_forever())
        await asyncio.wait_for(
            _wait_until(lambda: len(processor.processed) == 2),
            timeout=1,
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        row = store.get_notification_outbox("spread", alert.event_id)
        assert row is not None
        assert row.status == "sent"
        assert row.attempt_count == 2
        assert len(failures) == 1


@pytest.mark.asyncio
async def test_restart_does_not_bypass_max_attempts_after_inflight_cancellation(tmp_path):
    from radar.alerts.worker import AlertWorker

    class BlockingProcessor:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.processed: list[AlertRequest] = []

        async def process(self, alert: AlertRequest) -> None:
            self.processed.append(alert)
            self.started.set()
            await self.release.wait()

    database = tmp_path / "runtime.sqlite3"
    alert = make_alert("cancelled-in-flight")
    with SQLiteRuntimeStore(database) as store:
        seed_outbox(store, alert)
        first_processor = BlockingProcessor()
        first_worker = AlertWorker(
            asyncio.Queue(),
            first_processor,  # type: ignore[arg-type]
            runtime_store=store,
            max_attempts=1,
            poll_interval_seconds=0.01,
        )
        first_task = asyncio.create_task(first_worker.run_forever())
        await asyncio.wait_for(first_processor.started.wait(), timeout=1)
        first_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_task

        pending = store.get_notification_outbox("spread", alert.event_id)
        assert pending is not None
        assert pending.status == "pending"
        assert pending.attempt_count == 1

    with SQLiteRuntimeStore(database) as restarted_store:
        second_processor = FlakyProcessor()
        restarted_worker = AlertWorker(
            asyncio.Queue(),
            second_processor,  # type: ignore[arg-type]
            runtime_store=restarted_store,
            max_attempts=1,
            poll_interval_seconds=0.01,
        )
        restarted_task = asyncio.create_task(restarted_worker.run_forever())
        await asyncio.wait_for(
            _wait_until(
                lambda: (
                    (row := restarted_store.get_notification_outbox(
                        "spread", alert.event_id
                    ))
                    is not None
                    and row.status in {"failed", "sent"}
                )
            ),
            timeout=1,
        )
        restarted_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await restarted_task

        terminal = restarted_store.get_notification_outbox("spread", alert.event_id)
        assert terminal is not None
        assert terminal.status == "failed"
        assert terminal.attempt_count == 1
        assert terminal.last_error == "max_attempts_exhausted"
        assert second_processor.processed == []


@pytest.mark.asyncio
async def test_durable_worker_marks_permanent_failure_without_requeue(tmp_path):
    from radar.alerts.worker import AlertWorker

    database = tmp_path / "runtime.sqlite3"
    alert = make_alert("permanent")
    with SQLiteRuntimeStore(database) as store:
        seed_outbox(store, alert)
        processor = FlakyProcessor(
            failures=100,
            error=ValueError("invalid alert payload"),
        )
        worker = AlertWorker(
            asyncio.Queue(),
            processor,  # type: ignore[arg-type]
            runtime_store=store,
            retry_delays_seconds=(0.0,),
            poll_interval_seconds=0.01,
        )
        task = asyncio.create_task(worker.run_forever())
        await asyncio.wait_for(
            _wait_until(
                lambda: (
                    (row := store.get_notification_outbox("spread", alert.event_id))
                    is not None
                    and row.status == "failed"
                )
            ),
            timeout=1,
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(processor.processed) == 1


@pytest.mark.asyncio
async def test_durable_worker_deduplicates_already_sent_queue_item(tmp_path):
    from radar.alerts.worker import AlertWorker

    database = tmp_path / "runtime.sqlite3"
    alert = make_alert("already-sent")
    with SQLiteRuntimeStore(database) as store:
        seed_outbox(store, alert)
        store.record_notification_attempt("spread", alert.event_id)
        store.mark_notification_sent("spread", alert.event_id, sent_at=NOW)
        processor = FlakyProcessor()
        queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
        queue.put_nowait(alert)
        worker = AlertWorker(
            queue,
            processor,  # type: ignore[arg-type]
            runtime_store=store,
        )
        await worker.run_once()
        assert processor.processed == []
        assert queue.empty()


@pytest.mark.asyncio
async def test_duplicate_queue_item_respects_retry_backoff(tmp_path):
    from radar.alerts.worker import AlertWorker

    alert = make_alert("duplicate-backoff")
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        seed_outbox(store, alert)
        processor = FlakyProcessor(failures=1)
        queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
        queue.put_nowait(alert)
        queue.put_nowait(alert)
        worker = AlertWorker(
            queue,
            processor,  # type: ignore[arg-type]
            runtime_store=store,
            retry_delays_seconds=(3600.0,),
        )

        await worker.run_once()
        first = store.get_notification_outbox("spread", alert.event_id)
        assert first is not None
        assert first.status == "pending"
        assert first.attempt_count == 1

        await worker.run_once()
        second = store.get_notification_outbox("spread", alert.event_id)
        assert second is not None
        assert second.status == "pending"
        assert second.attempt_count == 1
        assert len(processor.processed) == 1
        assert queue.empty()


async def _wait_until(predicate, interval: float = 0.005) -> None:
    for _ in range(200):
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition did not become true")
