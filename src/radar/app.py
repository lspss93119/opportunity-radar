from __future__ import annotations

import argparse
import asyncio
import logging
import os
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

    async def collect_and_evaluate_once(self, now: datetime) -> CollectorBatch:
        current_time = _as_utc(now, "now")
        batch = await self.pipeline.collect_once(now=current_time)
        self.stats.collection_cycles += 1
        if batch.market_snapshots:
            self.stats.latest_sample_time = max(
                snapshot.sample_time for snapshot in batch.market_snapshots
            )
        await self.monitor_runner.run_cycle(current_time)
        self.stats.alerts_queued = self.monitor_runner.queue.qsize()
        return batch

    async def maybe_flush(self, now: datetime) -> int:
        current_time = _as_utc(now, "now")
        if self._last_flush_at is None:
            self._last_flush_at = current_time
            return 0
        if current_time < self._last_flush_at + timedelta(
            seconds=self.flush_interval_seconds
        ):
            return 0
        return await self.flush_now(current_time)

    async def flush_now(self, now: datetime | None = None) -> int:
        current_time = _as_utc(
            self.clock() if now is None else now,
            "now",
        )
        files_written = await asyncio.to_thread(
            self.pipeline.flush_storage,
            now=current_time,
        )
        self._last_flush_at = current_time
        self.stats.parquet_flushes += 1
        LOGGER.info(
            "parquet flush files=%d pending=%d",
            files_written,
            self.storage.pending_count,
        )
        return files_written

    async def _wait_until_next_boundary(
        self,
        stop_event: asyncio.Event,
    ) -> bool:
        now = _as_utc(self.clock(), "now")
        next_sample = aligned_sample_time(now, self.pipeline.sampling_seconds) + timedelta(
            seconds=self.pipeline.sampling_seconds
        )
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
                try:
                    batch = await self.collect_and_evaluate_once(cycle_time)
                    await self.maybe_flush(cycle_time)
                    LOGGER.info(
                        "collection cycle=%d sample_time=%s markets=%d funding=%d "
                        "hourly_context=%d queue_size=%d",
                        self.stats.collection_cycles,
                        self.stats.latest_sample_time,
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
            event.set()
            try:
                stop_pipeline = getattr(self.pipeline, "stop", None)
                if callable(stop_pipeline):
                    await stop_pipeline()
            except Exception as error:  # noqa: BLE001
                LOGGER.error("shutdown collectors stop failed", exc_info=error)
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
