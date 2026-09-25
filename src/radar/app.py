from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from radar.alerts.spread import SpreadAlertProcessor
from radar.alerts.telegram import TelegramTransport
from radar.alerts.worker import AlertWorker
from radar.collectors.base import CollectorBatch
from radar.config import RadarConfig, load_config
from radar.history.spread import SpreadHistory
from radar.models import FundingSnapshot, MarketSnapshot
from radar.monitors.base import AlertRequest, JSONValue
from radar.monitors.registry import build_enabled_monitors
from radar.monitors.runner import MonitorRunner
from radar.pipeline import MarketDataPipeline, aligned_sample_time, utc_now
from radar.state import RadarState
from radar.storage.parquet import ParquetStorage
from radar.storage.sqlite import SQLiteRuntimeStore

LOGGER = logging.getLogger(__name__)
DEFAULT_DATA_ROOT = Path("data")
DEFAULT_RUNTIME_DB = Path("runtime/radar.sqlite3")
DEFAULT_PARQUET_FLUSH_SECONDS = 60


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def load_telegram_credentials(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    values = os.environ if environ is None else environ
    token = values.get("RADAR_TELEGRAM_BOT_TOKEN", "")
    chat_id = values.get("RADAR_TELEGRAM_CHAT_ID", "")
    missing = [
        name
        for name, value in (
            ("RADAR_TELEGRAM_BOT_TOKEN", token),
            ("RADAR_TELEGRAM_CHAT_ID", chat_id),
        )
        if not value.strip()
    ]
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(
            f"Telegram is enabled but required environment variable(s) are missing: {names}"
        )
    return token, chat_id


@dataclass
class PilotStats:
    collection_cycles: int = 0
    collector_failures: int = 0
    monitor_errors: int = 0
    alerts_queued: int = 0
    alert_processing_errors: int = 0
    parquet_flushes: int = 0
    latest_sample_time: datetime | None = None


@dataclass(frozen=True)
class _CycleTiming:
    cache_collect_ms: float
    monitor_ms: float


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000.0


class RadarApplication:
    def __init__(
        self,
        *,
        pipeline: MarketDataPipeline,
        monitor_runner: MonitorRunner,
        alert_worker: AlertWorker,
        storage: ParquetStorage,
        runtime_store: SQLiteRuntimeStore,
        processor: SpreadAlertProcessor,
        config: RadarConfig | None = None,
        clock: Callable[[], datetime] = utc_now,
        flush_interval_seconds: int = DEFAULT_PARQUET_FLUSH_SECONDS,
        stats: PilotStats | None = None,
    ) -> None:
        if flush_interval_seconds <= 0:
            raise ValueError("flush_interval_seconds must be positive")
        self.pipeline = pipeline
        self.monitor_runner = monitor_runner
        self.alert_worker = alert_worker
        self.storage = storage
        self.runtime_store = runtime_store
        self.processor = processor
        self.config = RadarConfig() if config is None else config
        self.clock = clock
        self.flush_interval_seconds = flush_interval_seconds
        self.stats = PilotStats() if stats is None else stats
        self._last_flush_at: datetime | None = None
        self._periodic_flush_task: asyncio.Task[None] | None = None
        self._next_scheduled_sample_time: datetime | None = None
        self._last_cycle_timing: _CycleTiming | None = None

    async def collect_and_evaluate_once(
        self,
        now: datetime,
        *,
        sample_time: datetime | None = None,
    ) -> CollectorBatch:
        current_time = _as_utc(now, "now")
        collect_started = time.perf_counter()
        if sample_time is None:
            batch = await self.pipeline.collect_once(now=current_time)
        else:
            batch = await self.pipeline.collect_once(
                now=current_time,
                sample_time=sample_time,
            )
        cache_collect_ms = _elapsed_ms(collect_started)
        self.stats.collection_cycles += 1
        if batch.market_snapshots:
            self.stats.latest_sample_time = max(
                snapshot.sample_time for snapshot in batch.market_snapshots
            )
        monitor_started = time.perf_counter()
        await self.monitor_runner.run_cycle(current_time)
        monitor_ms = _elapsed_ms(monitor_started)
        self._last_cycle_timing = _CycleTiming(
            cache_collect_ms=cache_collect_ms,
            monitor_ms=monitor_ms,
        )
        self.stats.alerts_queued = self.monitor_runner.queue.qsize()
        return batch

    def maybe_flush(self, now: datetime) -> bool:
        """Schedule one periodic flush without awaiting storage I/O."""
        current_time = _as_utc(now, "now")
        if self._last_flush_at is None:
            self._last_flush_at = current_time
            return False
        if current_time < self._last_flush_at + timedelta(
            seconds=self.flush_interval_seconds
        ):
            return False
        if (
            self._periodic_flush_task is not None
            and not self._periodic_flush_task.done()
        ):
            return False

        self._last_flush_at = current_time
        task = asyncio.create_task(
            self._run_periodic_flush(current_time),
            name="radar-periodic-parquet-flush",
        )
        self._periodic_flush_task = task
        task.add_done_callback(self._periodic_flush_done)
        return True

    def _periodic_flush_done(self, task: asyncio.Task[None]) -> None:
        if self._periodic_flush_task is task:
            self._periodic_flush_task = None

    async def _run_periodic_flush(self, scheduled_at: datetime) -> None:
        try:
            await self._flush_now(scheduled_at, reason="periodic")
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            LOGGER.error("periodic Parquet flush failed", exc_info=error)

    async def _wait_for_periodic_flush(self) -> None:
        task = self._periodic_flush_task
        if task is None:
            return
        await task

    async def flush_now(self, now: datetime | None = None) -> int:
        current_time = _as_utc(
            self.clock() if now is None else now,
            "now",
        )
        return await self._flush_now(current_time, reason="final")

    async def _flush_now(self, current_time: datetime, *, reason: str) -> int:
        started = time.perf_counter()
        LOGGER.info("parquet flush start reason=%s at=%s", reason, current_time)
        files_written = await asyncio.to_thread(
            self.pipeline.flush_storage,
            now=current_time,
        )
        self._last_flush_at = current_time
        self.stats.parquet_flushes += 1
        LOGGER.info(
            "parquet flush complete reason=%s duration_ms=%.3f files=%d pending=%d",
            reason,
            _elapsed_ms(started),
            files_written,
            self.storage.pending_count,
        )
        return files_written

    async def _wait_until_next_boundary(
        self,
        stop_event: asyncio.Event,
    ) -> bool:
        now = _as_utc(self.clock(), "now")
        next_sample = self._next_scheduled_sample_time
        if next_sample is None:
            next_sample = aligned_sample_time(
                now, self.pipeline.sampling_seconds
            ) + timedelta(seconds=self.pipeline.sampling_seconds)
            self._next_scheduled_sample_time = next_sample
        delay = max(0.0, (next_sample - now).total_seconds())
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
        except TimeoutError:
            return False
        return stop_event.is_set()

    async def run(self, *, stop_event: asyncio.Event | None = None) -> None:
        event = asyncio.Event() if stop_event is None else stop_event
        worker_task = asyncio.create_task(self.alert_worker.run_forever())
        await asyncio.sleep(0)
        self._next_scheduled_sample_time = None
        try:
            start_pipeline = getattr(self.pipeline, "start", None)
            if callable(start_pipeline):
                await start_pipeline()
            LOGGER.info("radar pilot startup")
            while not event.is_set():
                if await self._wait_until_next_boundary(event):
                    break
                if event.is_set():
                    break
                cycle_time = _as_utc(self.clock(), "now")
                scheduled_sample_time = self._next_scheduled_sample_time
                if scheduled_sample_time is None:
                    scheduled_sample_time = aligned_sample_time(
                        cycle_time, self.pipeline.sampling_seconds
                    )
                boundary_lateness_ms = max(
                    0.0,
                    (cycle_time - scheduled_sample_time).total_seconds() * 1000.0,
                )
                critical_started = time.perf_counter()
                try:
                    batch = await self.collect_and_evaluate_once(
                        cycle_time,
                        sample_time=scheduled_sample_time,
                    )
                    self.maybe_flush(cycle_time)
                    scheduler_critical_ms = _elapsed_ms(critical_started)
                    timing = self._last_cycle_timing
                    if timing is None:
                        raise RuntimeError("cycle timing was not recorded")
                    LOGGER.info(
                        "market cycle=%d scheduled_sample_time=%s "
                        "actual_cycle_start=%s boundary_lateness_ms=%.3f "
                        "cache_collect_ms=%.3f monitor_ms=%.3f "
                        "scheduler_critical_ms=%.3f markets=%d funding=%d "
                        "hourly_context=%d queue_size=%d",
                        self.stats.collection_cycles,
                        scheduled_sample_time,
                        cycle_time,
                        boundary_lateness_ms,
                        timing.cache_collect_ms,
                        timing.monitor_ms,
                        scheduler_critical_ms,
                        len(batch.market_snapshots),
                        len(batch.funding_snapshots),
                        len(batch.hourly_contexts),
                        self.monitor_runner.queue.qsize(),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001
                    LOGGER.error("application cycle failed", exc_info=error)
                finally:
                    next_sample_time = scheduled_sample_time + timedelta(
                        seconds=self.pipeline.sampling_seconds
                    )
                    after_cycle = _as_utc(self.clock(), "now")
                    if after_cycle >= next_sample_time:
                        next_sample_time = aligned_sample_time(
                            after_cycle, self.pipeline.sampling_seconds
                        ) + timedelta(seconds=self.pipeline.sampling_seconds)
                    self._next_scheduled_sample_time = next_sample_time
        finally:
            event.set()
            try:
                stop_pipeline = getattr(self.pipeline, "stop", None)
                if callable(stop_pipeline):
                    await stop_pipeline()
            except Exception as error:  # noqa: BLE001
                LOGGER.error("shutdown collectors stop failed", exc_info=error)
            try:
                await self._wait_for_periodic_flush()
            except Exception as error:  # noqa: BLE001
                LOGGER.error("waiting for periodic Parquet flush failed", exc_info=error)
            try:
                await self.flush_now()
            except Exception as error:  # noqa: BLE001
                LOGGER.error("shutdown Parquet flush failed", exc_info=error)
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
            self.runtime_store.close()
            LOGGER.info(
                "radar pilot shutdown cycles=%d collector_failures=%d "
                "monitor_errors=%d alert_errors=%d parquet_flushes=%d queue_size=%d",
                self.stats.collection_cycles,
                self.stats.collector_failures,
                self.stats.monitor_errors,
                self.stats.alert_processing_errors,
                self.stats.parquet_flushes,
                self.monitor_runner.queue.qsize(),
            )
            self._next_scheduled_sample_time = None


def _configured_fee_bps(config: RadarConfig, venue: str) -> float:
    for configured_venue, fee in config.fees_bps.items():
        if configured_venue.lower() == venue.lower():
            return fee
    return 0.0


def _find_smoke_pair(
    state: RadarState,
    symbol: str,
) -> tuple[MarketSnapshot, MarketSnapshot]:
    canonical_symbol = symbol.strip().upper()
    if not canonical_symbol:
        raise ValueError("symbol must be non-empty")
    snapshots = tuple(
        snapshot
        for snapshot in state.markets
        if snapshot.canonical_symbol.upper() == canonical_symbol
    )
    for long_snapshot in snapshots:
        if long_snapshot.buy_10k_vwap is None:
            continue
        for short_snapshot in snapshots:
            if (
                short_snapshot is long_snapshot
                or short_snapshot.venue.lower() == long_snapshot.venue.lower()
                or short_snapshot.sell_10k_vwap is None
                or short_snapshot.sample_time != long_snapshot.sample_time
            ):
                continue
            return long_snapshot, short_snapshot
    raise RuntimeError(
        f"Telegram smoke requires two venues with current $10k VWAP data for {canonical_symbol}"
    )


def _funding_payload(
    funding: FundingSnapshot | None,
) -> dict[str, JSONValue] | None:
    if funding is None:
        return None
    return {
        "venue": funding.venue,
        "venue_symbol": funding.venue_symbol,
        "canonical_symbol": funding.canonical_symbol,
        "effective_time": funding.effective_time.isoformat(),
        "observed_at": funding.observed_at.isoformat(),
        "funding_rate": funding.funding_rate,
        "next_funding_time": (
            funding.next_funding_time.isoformat()
            if funding.next_funding_time is not None
            else None
        ),
    }


def _find_funding(
    state: RadarState,
    snapshot: MarketSnapshot,
) -> FundingSnapshot | None:
    for funding in state.funding:
        if (
            funding.venue == snapshot.venue
            and funding.venue_symbol == snapshot.venue_symbol
            and funding.canonical_symbol == snapshot.canonical_symbol
        ):
            return funding
    return None


def build_telegram_smoke_alert(
    config: RadarConfig,
    state: RadarState,
    *,
    symbol: str,
    now: datetime,
) -> AlertRequest:
    current_time = _as_utc(now, "now")
    long_snapshot, short_snapshot = _find_smoke_pair(state, symbol)
    long_buy_vwap = long_snapshot.buy_10k_vwap
    short_sell_vwap = short_snapshot.sell_10k_vwap
    if long_buy_vwap is None or short_sell_vwap is None:
        raise RuntimeError("Telegram smoke requires current $10k VWAP data")

    sample_time = long_snapshot.sample_time
    payload: dict[str, JSONValue] = {
        "canonical_symbol": long_snapshot.canonical_symbol,
        "long_venue": long_snapshot.venue,
        "long_venue_symbol": long_snapshot.venue_symbol,
        "short_venue": short_snapshot.venue,
        "short_venue_symbol": short_snapshot.venue_symbol,
        "primary_size_usd": 10_000,
        "long_buy_vwap": long_buy_vwap,
        "short_sell_vwap": short_sell_vwap,
        # These are deliberately fixed smoke values. The app must not calculate
        # or assert an opportunity; spread logic remains in SpreadMonitor.
        "raw_spread_bps": 0.0,
        "long_fee_bps": _configured_fee_bps(config, long_snapshot.venue),
        "short_fee_bps": _configured_fee_bps(config, short_snapshot.venue),
        "net_spread_bps": 0.0,
        "sample_time": sample_time.isoformat(),
        "candidate_duration_seconds": 0,
        "alert_duration_seconds": 0,
        "funding_context": {
            "long": _funding_payload(_find_funding(state, long_snapshot)),
            "short": _funding_payload(_find_funding(state, short_snapshot)),
        },
    }
    return AlertRequest(
        monitor="spread",
        event_id=f"telegram-smoke:{long_snapshot.canonical_symbol}:{sample_time.isoformat()}",
        created_at=current_time,
        payload=payload,
    )


async def run_telegram_smoke(
    application: RadarApplication,
    *,
    symbol: str = "BTC",
    now: datetime | None = None,
    readiness_timeout_seconds: float = 30.0,
) -> None:
    current_time: datetime | None = None
    alert: AlertRequest | None = None
    try:
        await application.pipeline.start()
        canonical_symbol = symbol.strip().upper()
        await application.pipeline.wait_for_market_feeds(
            canonical_symbol,
            required_venues=2,
            timeout_seconds=readiness_timeout_seconds,
        )
        current_time = _as_utc(
            application.clock() if now is None else now,
            "now",
        )
        batch = await application.pipeline.collect_once(now=current_time)
        application.stats.collection_cycles += 1
        if batch.market_snapshots:
            application.stats.latest_sample_time = max(
                snapshot.sample_time for snapshot in batch.market_snapshots
            )
        alert = build_telegram_smoke_alert(
            application.config,
            application.pipeline.state,
            symbol=symbol,
            now=current_time,
        )
        LOGGER.info(
            "telegram smoke sending synthetic alert symbol=%s sample_time=%s",
            alert.payload["canonical_symbol"],
            alert.payload["sample_time"],
        )
    finally:
        try:
            await application.pipeline.stop()
        except Exception as error:  # noqa: BLE001
            LOGGER.error("telegram smoke pipeline stop failed", exc_info=error)
        try:
            await application.flush_now(current_time)
        except Exception as error:  # noqa: BLE001
            LOGGER.error("telegram smoke Parquet flush failed", exc_info=error)
        try:
            if alert is not None:
                await application.processor.process(alert)
        finally:
            application.runtime_store.close()


def build_application(
    config: RadarConfig,
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    runtime_db: Path = DEFAULT_RUNTIME_DB,
    telegram_credentials: tuple[str, str] | None = None,
    clock: Callable[[], datetime] = utc_now,
) -> RadarApplication:
    token, chat_id = (
        load_telegram_credentials()
        if telegram_credentials is None
        else telegram_credentials
    )
    stats = PilotStats()

    def collector_error_handler(venue: str, error: Exception) -> None:
        stats.collector_failures += 1
        LOGGER.error(
            "collector %s failed: %s: %s",
            venue,
            type(error).__name__,
            error,
        )

    def monitor_error_handler(name: str, error: Exception) -> None:
        stats.monitor_errors += 1
        LOGGER.error("monitor %s evaluation failed", name, exc_info=error)

    def alert_error_handler(alert: AlertRequest, error: Exception) -> None:
        stats.alert_processing_errors += 1
        LOGGER.error(
            "alert worker failed event_id=%s: %s: %s",
            alert.event_id,
            type(error).__name__,
            error,
        )

    state = RadarState()
    storage = ParquetStorage(Path(data_root))
    runtime_store = SQLiteRuntimeStore(Path(runtime_db))
    queue: asyncio.Queue[AlertRequest] = asyncio.Queue()
    pipeline = MarketDataPipeline.from_config(
        config,
        state=state,
        clock=clock,
        storage=storage,
        collector_error_handler=collector_error_handler,
    )
    monitors = build_enabled_monitors(config, runtime_store=runtime_store)
    monitor_runner = MonitorRunner(
        monitors,
        state,
        queue,
        error_handler=monitor_error_handler,
    )
    history = SpreadHistory(Path(data_root))
    telegram = TelegramTransport(token, chat_id)
    processor = SpreadAlertProcessor(history, telegram)
    alert_worker = AlertWorker(
        queue,
        processor,
        error_handler=alert_error_handler,
    )
    return RadarApplication(
        pipeline=pipeline,
        monitor_runner=monitor_runner,
        alert_worker=alert_worker,
        storage=storage,
        runtime_store=runtime_store,
        processor=processor,
        config=config,
        clock=clock,
        stats=stats,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Opportunity Radar")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run", help="run the live pilot")
    run_parser.add_argument("--config", type=Path, required=True)
    smoke_parser = subparsers.add_parser(
        "telegram-smoke",
        help="send a synthetic alert through the real slow path",
    )
    smoke_parser.add_argument("--config", type=Path, required=True)
    smoke_parser.add_argument("--symbol", default="BTC")
    opportunities_parser = subparsers.add_parser(
        "opportunities",
        help="report historical opportunities from Parquet",
    )
    opportunities_parser.add_argument("--config", type=Path, required=True)
    opportunities_parser.add_argument("--hours", type=float, default=6.0)
    opportunities_parser.add_argument("--top", type=int, default=20)
    opportunities_parser.add_argument(
        "--size",
        dest="size_usd",
        type=int,
        choices=(1_000, 5_000, 10_000),
    )
    opportunities_parser.add_argument("--symbol")
    opportunities_parser.add_argument("--min-net-bps", type=float)
    variational_parser = subparsers.add_parser(
        "variational-opportunities",
        help="report read-only Variational $1k discovery opportunities",
    )
    variational_parser.add_argument("--config", type=Path, required=True)
    variational_parser.add_argument("--hours", type=float, default=24.0)
    variational_parser.add_argument("--top", type=int, default=30)
    variational_parser.add_argument("--symbol")
    variational_parser.add_argument("--min-net-bps", type=float)
    variational_parser.add_argument("--other-venue")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.command == "opportunities":
            from radar.history.opportunities import (
                build_opportunity_report,
                format_opportunity_report,
            )

            report = build_opportunity_report(
                DEFAULT_DATA_ROOT,
                config,
                hours=args.hours,
                top=args.top,
                size_usd=args.size_usd,
                symbol=args.symbol,
                min_net_bps=args.min_net_bps,
            )
            print(format_opportunity_report(report, top=args.top))
        elif args.command == "variational-opportunities":
            from radar.history.variational_opportunities import (
                build_variational_opportunity_report,
                format_variational_opportunity_report,
            )

            variational_report = build_variational_opportunity_report(
                DEFAULT_DATA_ROOT,
                config,
                hours=args.hours,
                top=args.top,
                symbol=args.symbol,
                min_net_bps=args.min_net_bps,
                other_venue=args.other_venue,
            )
            print(
                format_variational_opportunity_report(
                    variational_report,
                    top=args.top,
                )
            )
        elif args.command == "run":
            application = build_application(config)
            asyncio.run(application.run())
        else:
            application = build_application(config)
            asyncio.run(
                run_telegram_smoke(
                    application,
                    symbol=args.symbol,
                )
            )
    except KeyboardInterrupt:
        return 130
    except (OSError, RuntimeError, ValueError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
