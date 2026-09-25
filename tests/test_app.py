from __future__ import annotations

import asyncio
import logging
import threading
from datetime import UTC, datetime, timedelta

import pytest

from radar.collectors.base import CollectorBatch
from radar.config import MarketConfig, RadarConfig
from radar.market_data import LatestMarketData
from radar.models import MarketSnapshot
from radar.monitors.base import AlertRequest
from radar.pipeline import MarketDataPipeline
from radar.state import RadarState
from radar.storage.parquet import ParquetStorage
from radar.vwap import BookLevel

NOW = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)


def make_market(sample_time: datetime = NOW) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=sample_time + timedelta(milliseconds=100),
        venue="lighter",
        venue_symbol="BTC",
        canonical_symbol="BTC",
        best_bid=99.0,
        best_bid_size=10.0,
        best_ask=100.0,
        best_ask_size=10.0,
        buy_10k_vwap=100.0,
        sell_10k_vwap=99.0,
    )


class FakeCollector:
    venue = "lighter"

    async def collect(
        self, *, sample_time: datetime, include_hourly_context: bool
    ) -> CollectorBatch:
        return CollectorBatch(market_snapshots=(make_market(sample_time),))


class RecordingRunner:
    def __init__(self, queue: asyncio.Queue[AlertRequest]) -> None:
        self.queue = queue
        self.calls: list[tuple[datetime, RadarState]] = []

    async def run_cycle(self, now: datetime) -> None:
        self.calls.append((now, self.state))

    state: RadarState


class FakeWorker:
    def __init__(self, events: list[str] | None = None) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.running = asyncio.Event()
        self.events = events

    async def run_forever(self) -> None:
        self.started.set()
        self.running.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            if self.events is not None:
                self.events.append("worker.stop")
            raise


class FakeRuntimeStore:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.closed = False

    def close(self) -> None:
        self.events.append("runtime.close")
        self.closed = True


class FakeStorage:
    pending_count = 1


class RecordingPipeline:
    sampling_seconds = 10

    def __init__(self, events: list[str]) -> None:
        self.state = RadarState()
        self.events = events
        self.flush_calls: list[datetime] = []

    async def collect_once(
        self,
        *,
        now: datetime,
        sample_time: datetime | None = None,
    ) -> CollectorBatch:
        self.events.append("pipeline.append")
        shutdown_event = getattr(self, "shutdown_event", None)
        if shutdown_event is not None:
            self.events.append("shutdown.requested")
            shutdown_event.set()
        return CollectorBatch()

    async def stop(self) -> None:
        self.events.append("pipeline.stop")

    def flush_storage(self, *, now: datetime | None = None) -> int:
        assert now is not None
        self.events.append("flush")
        self.flush_calls.append(now)
        return 1


class LifecycleRecordingPipeline(RecordingPipeline):
    async def start(self) -> None:
        self.events.append("pipeline.start")

    async def stop(self) -> None:
        self.events.append("pipeline.stop")


class SmokePipeline:
    sampling_seconds = 10

    def __init__(self, state: RadarState, batch: CollectorBatch) -> None:
        self.state = state
        self.batch = batch
        self.collect_calls: list[datetime] = []
        self.flush_calls: list[datetime] = []

    async def start(self) -> None:
        return None

    async def wait_for_market_feeds(
        self,
        canonical_symbol: str,
        *,
        required_venues: int = 2,
        timeout_seconds: float = 30.0,
    ) -> None:
        return None

    async def collect_once(self, *, now: datetime) -> CollectorBatch:
        self.collect_calls.append(now)
        self.state.apply(self.batch, replace_context=True)
        return self.batch

    def flush_storage(self, *, now: datetime | None = None) -> int:
        assert now is not None
        self.flush_calls.append(now)
        return 1

    async def stop(self) -> None:
        return None


class SmokePipelineWithEvents:
    sampling_seconds = 10

    def __init__(
        self,
        state: RadarState,
        batch: CollectorBatch,
        events: list[str],
    ) -> None:
        self.state = state
        self.batch = batch
        self.events = events

    async def start(self) -> None:
        self.events.append("start")

    async def wait_for_market_feeds(
        self,
        canonical_symbol: str,
        *,
        required_venues: int = 2,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.events.append(f"wait_for_market_feeds:{canonical_symbol}")

    async def collect_once(self, *, now: datetime) -> CollectorBatch:
        self.events.append("collect_once")
        self.state.apply(self.batch, replace_context=True)
        return self.batch

    async def stop(self) -> None:
        self.events.append("stop")

    def flush_storage(self, *, now: datetime | None = None) -> int:
        assert now is not None
        self.events.append("flush")
        return 1


class RecordingProcessor:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def process(self, alert: AlertRequest) -> None:
        self.events.append("process")


def make_smoke_application(
    pipeline: SmokePipelineWithEvents,
    events: list[str],
):
    from radar.app import RadarApplication

    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = RecordingRunner(queue)
    runner.state = pipeline.state
    return RadarApplication(
        pipeline=pipeline,  # type: ignore[arg-type]
        monitor_runner=runner,  # type: ignore[arg-type]
        alert_worker=FakeWorker(),  # type: ignore[arg-type]
        storage=FakeStorage(),  # type: ignore[arg-type]
        runtime_store=FakeRuntimeStore(events),  # type: ignore[arg-type]
        processor=RecordingProcessor(events),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )


def make_smoke_market(
    venue: str,
    *,
    sample_time: datetime = NOW,
    buy_10k_vwap: float = 100.0,
    sell_10k_vwap: float = 101.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=sample_time + timedelta(milliseconds=100),
        venue=venue,
        venue_symbol="BTC",
        canonical_symbol="BTC",
        best_bid=99.0,
        best_bid_size=10.0,
        best_ask=100.0,
        best_ask_size=10.0,
        buy_10k_vwap=buy_10k_vwap,
        sell_10k_vwap=sell_10k_vwap,
    )


def make_application_config() -> RadarConfig:
    return RadarConfig(
        markets=[
            MarketConfig(
                venue="lighter",
                venue_symbol="BTC",
                canonical_symbol="BTC",
            )
        ],
    )


def test_missing_telegram_credentials_are_reported_without_values():
    from radar.app import load_telegram_credentials

    with pytest.raises(RuntimeError, match="RADAR_TELEGRAM_BOT_TOKEN") as error:
        load_telegram_credentials({"RADAR_TELEGRAM_CHAT_ID": "chat-secret"})

    assert "chat-secret" not in str(error.value)


@pytest.mark.asyncio
async def test_application_cycle_updates_state_appends_storage_and_runs_monitor(tmp_path):
    from radar.app import RadarApplication

    state = RadarState()
    latest = LatestMarketData()
    latest.update_book(
        venue="lighter",
        venue_symbol="BTC",
        bids=(BookLevel(price=99.0, base_size=200.0),),
        asks=(BookLevel(price=100.0, base_size=200.0),),
        observed_at=NOW,
    )
    storage = ParquetStorage(tmp_path / "data")
    pipeline = MarketDataPipeline(
        [],
        state,
        markets=(
            MarketConfig(
                venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"
            ),
        ),
        latest_market_data=latest,
        clock=lambda: NOW,
        storage=storage,
    )
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = RecordingRunner(queue)
    runner.state = state
    runtime = FakeRuntimeStore([])
    app = RadarApplication(
        pipeline=pipeline,
        monitor_runner=runner,  # type: ignore[arg-type]
        alert_worker=FakeWorker(),  # type: ignore[arg-type]
        storage=storage,
        runtime_store=runtime,  # type: ignore[arg-type]
        processor=object(),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )

    batch = await app.collect_and_evaluate_once(NOW)

    assert batch.market_snapshots[0].venue == "lighter"
    assert state.get_market("lighter", "BTC") is not None
    assert storage.pending_count == 1
    assert runner.calls == [(NOW, state)]


@pytest.mark.asyncio
async def test_scheduler_forwards_authoritative_sample_time_to_pipeline():
    from radar.app import RadarApplication

    actual_now = datetime(2026, 9, 15, 15, 7, 49, 999000, tzinfo=UTC)
    scheduled_sample_time = datetime(2026, 9, 15, 15, 7, 50, tzinfo=UTC)

    class ScheduledPipeline(RecordingPipeline):
        def __init__(self, events: list[str]) -> None:
            super().__init__(events)
            self.collect_calls: list[tuple[datetime, datetime | None]] = []

        async def collect_once(
            self,
            *,
            now: datetime,
            sample_time: datetime | None = None,
        ) -> CollectorBatch:
            self.collect_calls.append((now, sample_time))
            return await super().collect_once(now=now)

    class OneCycleApplication(RadarApplication):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.boundary_calls = 0

        async def _wait_until_next_boundary(self, stop_event: asyncio.Event) -> bool:
            self.boundary_calls += 1
            if self.boundary_calls == 1:
                self._next_scheduled_sample_time = scheduled_sample_time
                return False
            stop_event.set()
            return True

    events: list[str] = []
    pipeline = ScheduledPipeline(events)
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = RecordingRunner(queue)
    runner.state = pipeline.state
    app = OneCycleApplication(
        pipeline=pipeline,  # type: ignore[arg-type]
        monitor_runner=runner,  # type: ignore[arg-type]
        alert_worker=FakeWorker(),  # type: ignore[arg-type]
        storage=FakeStorage(),  # type: ignore[arg-type]
        runtime_store=FakeRuntimeStore(events),  # type: ignore[arg-type]
        processor=object(),  # type: ignore[arg-type]
        clock=lambda: actual_now,
    )

    await app.run(stop_event=asyncio.Event())

    assert pipeline.collect_calls == [(actual_now, scheduled_sample_time)]


@pytest.mark.asyncio
async def test_scheduler_skips_missed_slots_without_backfilling():
    from radar.app import RadarApplication

    actual_now = datetime(2026, 9, 15, 15, 7, 49, 999000, tzinfo=UTC)
    after_cycle = datetime(2026, 9, 15, 15, 8, 1, tzinfo=UTC)
    scheduled_sample_time = datetime(2026, 9, 15, 15, 7, 50, tzinfo=UTC)
    expected_next_slot = datetime(2026, 9, 15, 15, 8, 10, tzinfo=UTC)
    clock_values = [actual_now, after_cycle, after_cycle]

    class ScheduledPipeline(RecordingPipeline):
        def __init__(self, events: list[str]) -> None:
            super().__init__(events)
            self.collect_calls: list[tuple[datetime, datetime | None]] = []

        async def collect_once(
            self,
            *,
            now: datetime,
            sample_time: datetime | None = None,
        ) -> CollectorBatch:
            self.collect_calls.append((now, sample_time))
            return await super().collect_once(now=now)

    class SkipApplication(RadarApplication):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.boundary_calls = 0
            self.next_slot_seen: datetime | None = None

        async def _wait_until_next_boundary(self, stop_event: asyncio.Event) -> bool:
            self.boundary_calls += 1
            if self.boundary_calls == 1:
                self._next_scheduled_sample_time = scheduled_sample_time
                return False
            self.next_slot_seen = self._next_scheduled_sample_time
            stop_event.set()
            return True

    def clock() -> datetime:
        return clock_values.pop(0) if clock_values else after_cycle

    events: list[str] = []
    pipeline = ScheduledPipeline(events)
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = RecordingRunner(queue)
    runner.state = pipeline.state
    app = SkipApplication(
        pipeline=pipeline,  # type: ignore[arg-type]
        monitor_runner=runner,  # type: ignore[arg-type]
        alert_worker=FakeWorker(),  # type: ignore[arg-type]
        storage=FakeStorage(),  # type: ignore[arg-type]
        runtime_store=FakeRuntimeStore(events),  # type: ignore[arg-type]
        processor=object(),  # type: ignore[arg-type]
        clock=clock,
    )

    await app.run(stop_event=asyncio.Event())

    assert pipeline.collect_calls == [(actual_now, scheduled_sample_time)]
    assert app.next_slot_seen == expected_next_slot


@pytest.mark.asyncio
async def test_application_flushes_at_interval_not_after_each_sample():
    from radar.app import RadarApplication

    events: list[str] = []
    pipeline = RecordingPipeline(events)
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = RecordingRunner(queue)
    runner.state = pipeline.state
    app = RadarApplication(
        pipeline=pipeline,  # type: ignore[arg-type]
        monitor_runner=runner,  # type: ignore[arg-type]
        alert_worker=FakeWorker(),  # type: ignore[arg-type]
        storage=FakeStorage(),  # type: ignore[arg-type]
        runtime_store=FakeRuntimeStore(events),  # type: ignore[arg-type]
        processor=object(),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )

    assert app.maybe_flush(NOW) is False
    assert app.maybe_flush(NOW + timedelta(seconds=59)) is False
    assert app.maybe_flush(NOW + timedelta(seconds=60)) is True
    assert app.maybe_flush(NOW + timedelta(seconds=61)) is False
    await app._wait_for_periodic_flush()

    assert pipeline.flush_calls == [NOW + timedelta(seconds=60)]


@pytest.mark.asyncio
async def test_blocked_periodic_flush_does_not_block_samples_or_overlap():
    from radar.app import RadarApplication

    flush_started = threading.Event()
    release_flush = threading.Event()

    class BlockedFlushPipeline(RecordingPipeline):
        def flush_storage(self, *, now: datetime | None = None) -> int:
            assert now is not None
            self.flush_calls.append(now)
            if len(self.flush_calls) == 1:
                flush_started.set()
                assert release_flush.wait(timeout=1.0)
            return 1

    events: list[str] = []
    pipeline = BlockedFlushPipeline(events)
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = RecordingRunner(queue)
    runner.state = pipeline.state
    app = RadarApplication(
        pipeline=pipeline,  # type: ignore[arg-type]
        monitor_runner=runner,  # type: ignore[arg-type]
        alert_worker=FakeWorker(),  # type: ignore[arg-type]
        storage=FakeStorage(),  # type: ignore[arg-type]
        runtime_store=FakeRuntimeStore(events),  # type: ignore[arg-type]
        processor=object(),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )
    app._last_flush_at = NOW - timedelta(seconds=60)

    assert app.maybe_flush(NOW) is True
    for _ in range(100):
        if flush_started.is_set():
            break
        await asyncio.sleep(0.001)
    assert flush_started.is_set()

    await app.collect_and_evaluate_once(NOW + timedelta(seconds=10))
    assert app.maybe_flush(NOW + timedelta(seconds=10)) is False
    await app.collect_and_evaluate_once(NOW + timedelta(seconds=20))

    assert app.stats.collection_cycles == 2
    assert len(pipeline.flush_calls) == 1

    release_flush.set()
    await app._wait_for_periodic_flush()
    assert len(pipeline.flush_calls) == 1


@pytest.mark.asyncio
async def test_shutdown_waits_for_periodic_flush_before_final_flush_and_close():
    from radar.app import RadarApplication

    flush_started = threading.Event()
    release_flush = threading.Event()

    class BlockingFlushPipeline(RecordingPipeline):
        def flush_storage(self, *, now: datetime | None = None) -> int:
            assert now is not None
            call_number = len(self.flush_calls) + 1
            self.flush_calls.append(now)
            events.append(f"flush.{call_number}.start")
            if call_number == 1:
                flush_started.set()
                assert release_flush.wait(timeout=1.0)
            events.append(f"flush.{call_number}.done")
            return 1

    class OneCycleApplication(RadarApplication):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.boundary_calls = 0

        async def _wait_until_next_boundary(self, stop_event: asyncio.Event) -> bool:
            self.boundary_calls += 1
            if self.boundary_calls == 1:
                return False
            stop_event.set()
            return True

    events: list[str] = []
    pipeline = BlockingFlushPipeline(events)
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = RecordingRunner(queue)
    runner.state = pipeline.state
    worker = FakeWorker(events)
    runtime = FakeRuntimeStore(events)
    stop_event = asyncio.Event()
    app = OneCycleApplication(
        pipeline=pipeline,  # type: ignore[arg-type]
        monitor_runner=runner,  # type: ignore[arg-type]
        alert_worker=worker,  # type: ignore[arg-type]
        storage=FakeStorage(),  # type: ignore[arg-type]
        runtime_store=runtime,  # type: ignore[arg-type]
        processor=object(),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )
    app._last_flush_at = NOW - timedelta(seconds=60)

    run_task = asyncio.create_task(app.run(stop_event=stop_event))
    for _ in range(100):
        if flush_started.is_set():
            break
        await asyncio.sleep(0.001)
    assert flush_started.is_set()
    await asyncio.sleep(0)
    assert not run_task.done()

    release_flush.set()
    await run_task

    assert events.index("pipeline.stop") < events.index("flush.1.done")
    assert events.index("flush.1.done") < events.index("flush.2.start")
    assert events.index("flush.2.done") < events.index("worker.stop")
    assert events.index("worker.stop") < events.index("runtime.close")


@pytest.mark.asyncio
async def test_final_flush_preserves_rows_appended_during_periodic_flush(
    tmp_path, monkeypatch
):
    import pyarrow.parquet as parquet

    from radar.app import RadarApplication

    root = tmp_path / "data"
    storage = ParquetStorage(root)
    storage.append(make_market(NOW))

    class StoragePipeline:
        state = RadarState()

        def flush_storage(self, *, now: datetime | None = None) -> int:
            assert now is not None
            return storage.flush(now=now)

    write_started = threading.Event()
    release_write = threading.Event()
    original_write = parquet.write_table

    def blocked_write(*args, **kwargs):
        write_started.set()
        assert release_write.wait(timeout=1.0)
        return original_write(*args, **kwargs)

    monkeypatch.setattr("radar.storage.parquet.pq.write_table", blocked_write)

    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = RecordingRunner(queue)
    runner.state = RadarState()
    app = RadarApplication(
        pipeline=StoragePipeline(),  # type: ignore[arg-type]
        monitor_runner=runner,  # type: ignore[arg-type]
        alert_worker=FakeWorker(),  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        runtime_store=FakeRuntimeStore([]),  # type: ignore[arg-type]
        processor=object(),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )
    app._last_flush_at = NOW - timedelta(seconds=60)

    assert app.maybe_flush(NOW) is True
    for _ in range(100):
        if write_started.is_set():
            break
        await asyncio.sleep(0.001)
    assert write_started.is_set()

    storage.append(make_market(NOW + timedelta(seconds=10)))
    assert storage.pending_count == 1
    release_write.set()
    await app._wait_for_periodic_flush()
    assert storage.pending_count == 1

    await app.flush_now(NOW + timedelta(seconds=10))
    files = tuple((root / "market").glob("date=*/*.parquet"))
    assert len(files) == 2
    assert sum(parquet.ParquetFile(path).metadata.num_rows for path in files) == 2


@pytest.mark.asyncio
async def test_application_shutdown_orders_pipeline_flush_worker_and_runtime(caplog):
    from radar.app import RadarApplication

    caplog.set_level(logging.INFO, logger="radar.app")

    class OneCycleApplication(RadarApplication):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.boundary_calls = 0

        async def _wait_until_next_boundary(self, stop_event: asyncio.Event) -> bool:
            self.boundary_calls += 1
            if self.boundary_calls == 1:
                return False
            stop_event.set()
            return True

    events: list[str] = []
    pipeline = RecordingPipeline(events)
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = RecordingRunner(queue)
    runner.state = pipeline.state
    worker = FakeWorker(events)
    runtime = FakeRuntimeStore(events)
    stop_event = asyncio.Event()
    pipeline.shutdown_event = stop_event
    app = OneCycleApplication(
        pipeline=pipeline,  # type: ignore[arg-type]
        monitor_runner=runner,  # type: ignore[arg-type]
        alert_worker=worker,  # type: ignore[arg-type]
        storage=FakeStorage(),  # type: ignore[arg-type]
        runtime_store=runtime,  # type: ignore[arg-type]
        processor=object(),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )

    await app.run(stop_event=stop_event)

    assert worker.cancelled.is_set()
    assert events == [
        "pipeline.append",
        "shutdown.requested",
        "pipeline.stop",
        "flush",
        "worker.stop",
        "runtime.close",
    ]
    messages = [record.getMessage() for record in caplog.records]
    cycle_message = next(message for message in messages if "market cycle=" in message)
    assert "scheduled_sample_time=2026-09-17 12:00:00+00:00" in cycle_message
    assert "boundary_lateness_ms=" in cycle_message
    assert "cache_collect_ms=" in cycle_message
    assert "monitor_ms=" in cycle_message
    assert "scheduler_critical_ms=" in cycle_message
    assert any("parquet flush start reason=final" in message for message in messages)
    assert any("parquet flush complete reason=final" in message for message in messages)
    append_index = events.index("pipeline.append")
    flush_index = events.index("flush")
    assert append_index < flush_index
    assert "pipeline.append" not in events[flush_index + 1 :]
    assert runtime.closed


@pytest.mark.asyncio
async def test_application_starts_and_stops_pipeline_lifecycle_hooks():
    from radar.app import RadarApplication

    events: list[str] = []
    pipeline = LifecycleRecordingPipeline(events)
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = RecordingRunner(queue)
    runner.state = pipeline.state
    runtime = FakeRuntimeStore(events)
    app = RadarApplication(
        pipeline=pipeline,  # type: ignore[arg-type]
        monitor_runner=runner,  # type: ignore[arg-type]
        alert_worker=FakeWorker(),  # type: ignore[arg-type]
        storage=FakeStorage(),  # type: ignore[arg-type]
        runtime_store=runtime,  # type: ignore[arg-type]
        processor=object(),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )
    stop_event = asyncio.Event()
    stop_event.set()

    await app.run(stop_event=stop_event)

    assert events[:2] == ["pipeline.start", "pipeline.stop"]


def test_build_application_wires_monitor_runner_and_worker_to_one_queue(tmp_path):
    from radar.app import build_application

    app = build_application(
        make_application_config(),
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime" / "radar.sqlite3",
        telegram_credentials=("token-not-logged", "chat-id"),
        clock=lambda: NOW,
    )

    try:
        assert app.monitor_runner.queue is app.alert_worker._queue
    finally:
        app.runtime_store.close()


def test_parser_accepts_run_and_telegram_smoke_commands():
    from radar.app import build_parser

    parser = build_parser()

    run_args = parser.parse_args(["run", "--config", "config/radar.yaml"])
    smoke_args = parser.parse_args(
        ["telegram-smoke", "--config", "config/radar.yaml", "--symbol", "ETH"]
    )

    assert run_args.command == "run"
    assert smoke_args.command == "telegram-smoke"
    assert smoke_args.symbol == "ETH"


@pytest.mark.asyncio
async def test_telegram_smoke_collects_flushes_and_uses_real_processor():
    from radar.alerts.spread import SpreadAlertProcessor
    from radar.app import RadarApplication, run_telegram_smoke
    from radar.history.spread import HistoricalSpreadContext

    state = RadarState()
    batch = CollectorBatch(
        market_snapshots=(
            make_smoke_market("lighter", buy_10k_vwap=100.0, sell_10k_vwap=101.0),
            make_smoke_market("hyperliquid", buy_10k_vwap=100.5, sell_10k_vwap=101.5),
        )
    )
    pipeline = SmokePipeline(state, batch)

    class SmokeHistory:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def query(self, **kwargs: object) -> HistoricalSpreadContext:
            self.calls.append(kwargs)
            return HistoricalSpreadContext.empty()

    history = SmokeHistory()

    class SmokeTelegram:
        def __init__(self) -> None:
            self.text_calls: list[str] = []
            self.chart_calls: list[tuple[bytes, str]] = []

        async def send_text(self, text: str) -> None:
            self.text_calls.append(text)

        async def send_chart(self, png: bytes, caption: str) -> None:
            self.chart_calls.append((png, caption))

    telegram = SmokeTelegram()
    rendered: list[str] = []

    def render(details, context):
        rendered.append(details.canonical_symbol)
        return b"png"

    processor = SpreadAlertProcessor(
        history,
        telegram,
        chart_renderer=render,
    )  # type: ignore[arg-type]
    runtime = FakeRuntimeStore([])
    app = RadarApplication(
        pipeline=pipeline,  # type: ignore[arg-type]
        monitor_runner=RecordingRunner(asyncio.Queue()),  # type: ignore[arg-type]
        alert_worker=FakeWorker(),  # type: ignore[arg-type]
        storage=FakeStorage(),  # type: ignore[arg-type]
        runtime_store=runtime,  # type: ignore[arg-type]
        processor=processor,
        clock=lambda: NOW,
    )

    await run_telegram_smoke(app, symbol="BTC", now=NOW)

    assert pipeline.collect_calls == [NOW]
    assert pipeline.flush_calls == [NOW]
    assert rendered == ["BTC"]
    assert history.calls[0]["canonical_symbol"] == "BTC"
    assert len(telegram.chart_calls) == 1
    assert telegram.text_calls == []
    assert runtime.closed


@pytest.mark.asyncio
async def test_telegram_smoke_starts_waits_samples_and_stops_pipeline(monkeypatch):
    from radar import app as app_module
    from radar.app import run_telegram_smoke

    events: list[str] = []
    state = RadarState()
    batch = CollectorBatch(
        market_snapshots=(
            make_smoke_market("lighter", buy_10k_vwap=100.0, sell_10k_vwap=101.0),
            make_smoke_market("hyperliquid", buy_10k_vwap=100.5, sell_10k_vwap=101.5),
        )
    )
    pipeline = SmokePipelineWithEvents(state, batch, events)
    application = make_smoke_application(pipeline, events)

    real_build_alert = app_module.build_telegram_smoke_alert

    def build_alert(*args, **kwargs):
        events.append("build_alert")
        return real_build_alert(*args, **kwargs)

    monkeypatch.setattr(app_module, "build_telegram_smoke_alert", build_alert)

    await run_telegram_smoke(application, symbol="BTC", now=NOW)

    assert events == [
        "start",
        "wait_for_market_feeds:BTC",
        "collect_once",
        "build_alert",
        "stop",
        "flush",
        "process",
        "runtime.close",
    ]


@pytest.mark.asyncio
async def test_telegram_smoke_cleanup_runs_when_readiness_fails():
    from radar.app import run_telegram_smoke

    class FailingReadinessPipeline(SmokePipelineWithEvents):
        async def wait_for_market_feeds(
            self,
            canonical_symbol: str,
            *,
            required_venues: int = 2,
            timeout_seconds: float = 30.0,
        ) -> None:
            self.events.append(f"wait_for_market_feeds:{canonical_symbol}")
            raise TimeoutError("readiness timeout")

    events: list[str] = []
    state = RadarState()
    batch = CollectorBatch()
    pipeline = FailingReadinessPipeline(state, batch, events)
    application = make_smoke_application(pipeline, events)

    with pytest.raises(TimeoutError, match="readiness timeout"):
        await run_telegram_smoke(
            application,
            symbol="BTC",
            now=NOW,
            readiness_timeout_seconds=0.01,
        )

    assert events == [
        "start",
        "wait_for_market_feeds:BTC",
        "stop",
        "flush",
        "runtime.close",
    ]


def test_main_dispatches_telegram_smoke(monkeypatch, tmp_path):
    import radar.app as app_module

    config = make_application_config()
    fake_application = object()
    calls: list[tuple[object, str]] = []

    async def fake_smoke(application, *, symbol):
        calls.append((application, symbol))

    monkeypatch.setattr(app_module, "load_config", lambda path: config)
    monkeypatch.setattr(
        app_module,
        "build_application",
        lambda loaded_config: fake_application,
    )
    monkeypatch.setattr(app_module, "run_telegram_smoke", fake_smoke)

    assert app_module.main(
        [
            "telegram-smoke",
            "--config",
            str(tmp_path / "radar.yaml"),
            "--symbol",
            "ETH",
        ]
    ) == 0
    assert calls == [(fake_application, "ETH")]


def test_main_configures_info_logging(monkeypatch, tmp_path):
    import logging
    import radar.app as app_module

    config = make_application_config()
    logging_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        app_module.logging,
        "basicConfig",
        lambda **kwargs: logging_calls.append(kwargs),
    )
    monkeypatch.setattr(app_module, "load_config", lambda path: config)
    monkeypatch.setattr(app_module, "build_application", lambda loaded_config: object())

    async def fake_smoke(application, *, symbol):
        return None

    monkeypatch.setattr(app_module, "run_telegram_smoke", fake_smoke)

    assert app_module.main(
        [
            "telegram-smoke",
            "--config",
            str(tmp_path / "radar.yaml"),
        ]
    ) == 0
    assert logging_calls == [{"level": logging.INFO}]
