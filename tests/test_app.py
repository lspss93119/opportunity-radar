from __future__ import annotations

import asyncio
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
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.running = asyncio.Event()

    async def run_forever(self) -> None:
        self.started.set()
        self.running.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
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

    async def collect_once(self, *, now: datetime) -> CollectorBatch:
        self.events.append("collect")
        return CollectorBatch()

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

    async def collect_once(self, *, now: datetime) -> CollectorBatch:
        self.collect_calls.append(now)
        self.state.apply(self.batch, replace_context=True)
        return self.batch

    def flush_storage(self, *, now: datetime | None = None) -> int:
        assert now is not None
        self.flush_calls.append(now)
        return 1


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

    assert await app.maybe_flush(NOW) == 0
    assert await app.maybe_flush(NOW + timedelta(seconds=59)) == 0
    assert await app.maybe_flush(NOW + timedelta(seconds=60)) == 1
    assert await app.maybe_flush(NOW + timedelta(seconds=61)) == 0

    assert pipeline.flush_calls == [NOW + timedelta(seconds=60)]


@pytest.mark.asyncio
async def test_application_shutdown_flushes_then_cancels_worker_then_closes_runtime():
    from radar.app import RadarApplication

    events: list[str] = []
    pipeline = RecordingPipeline(events)
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    runner = RecordingRunner(queue)
    runner.state = pipeline.state
    worker = FakeWorker()
    runtime = FakeRuntimeStore(events)
    app = RadarApplication(
        pipeline=pipeline,  # type: ignore[arg-type]
        monitor_runner=runner,  # type: ignore[arg-type]
        alert_worker=worker,  # type: ignore[arg-type]
        storage=FakeStorage(),  # type: ignore[arg-type]
        runtime_store=runtime,  # type: ignore[arg-type]
        processor=object(),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )
    stop_event = asyncio.Event()
    stop_event.set()

    await app.run(stop_event=stop_event)

    assert worker.cancelled.is_set()
    assert events == ["flush", "runtime.close"]
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
