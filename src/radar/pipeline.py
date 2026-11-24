from __future__ import annotations

import asyncio
import inspect
import logging
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, TypedDict

from radar.collectors.base import (
    CollectorBatch,
    CollectorErrorHandler,
    CollectorLike,
    markets_for_venue,
    report_collector_error,
)
from radar.collectors.variational import (
    VariationalCollector,
    variational_funding_snapshot,
    variational_hourly_context,
    variational_market_snapshot,
)
from radar.config import MarketConfig, RadarConfig
from radar.market_data import LatestMarketData, MarketAvailabilityDiagnostic
from radar.models import MarketSnapshot, QuotedMarketSnapshot
from radar.state import RadarState
from radar.storage.parquet import ParquetStorage

UTC = timezone.utc
SAMPLE_INTERVAL_SECONDS = 10
HOURLY_CONTEXT_GRACE_SECONDS = 60
MARKET_AVAILABILITY_FEED_SUMMARY_INTERVAL = 60
LOGGER = logging.getLogger(__name__)


class _CollectorOptions(TypedDict, total=False):
    latest_market_data: LatestMarketData
    request_json: Any


def utc_now() -> datetime:
    return datetime.now(UTC)


def aligned_sample_time(
    now: datetime, sampling_seconds: int = SAMPLE_INTERVAL_SECONDS
) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if sampling_seconds <= 0:
        raise ValueError("sampling_seconds must be positive")
    if sampling_seconds != SAMPLE_INTERVAL_SECONDS:
        raise ValueError("sampling_seconds must be a 10-second interval")

    utc_value = now.astimezone(UTC)
    epoch_seconds = int(utc_value.timestamp())
    aligned_epoch = epoch_seconds - epoch_seconds % sampling_seconds
    return datetime.fromtimestamp(aligned_epoch, tz=UTC)


def hourly_sample_time(sample_time: datetime) -> datetime:
    if sample_time.tzinfo is None or sample_time.utcoffset() is None:
        raise ValueError("sample_time must be timezone-aware")
    return sample_time.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def context_sample_time(sample_time: datetime, period_seconds: int) -> datetime:
    """Floor a UTC sample time to the start of its context polling period."""
    if sample_time.tzinfo is None or sample_time.utcoffset() is None:
        raise ValueError("sample_time must be timezone-aware")
    if period_seconds <= 0 or 3600 % period_seconds:
        raise ValueError("period_seconds must evenly divide 3600")
    epoch_seconds = int(sample_time.astimezone(UTC).timestamp())
    return datetime.fromtimestamp(
        epoch_seconds - epoch_seconds % period_seconds, tz=UTC
    )


def should_collect_hourly_context(
    now: datetime,
    hour: datetime,
    last_hourly_sample: datetime | None,
    *,
    grace_seconds: int = HOURLY_CONTEXT_GRACE_SECONDS,
) -> bool:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if hour.tzinfo is None or hour.utcoffset() is None:
        raise ValueError("hour must be timezone-aware")
    if grace_seconds < 0:
        raise ValueError("grace_seconds must be non-negative")

    current_hour = hour.astimezone(UTC)
    if last_hourly_sample == current_hour:
        return False
    return now.astimezone(UTC) >= current_hour + timedelta(seconds=grace_seconds)


def merge_batches(batches: Iterable[CollectorBatch]) -> CollectorBatch:
    batches = tuple(batches)
    return CollectorBatch(
        market_snapshots=tuple(
            snapshot for batch in batches for snapshot in batch.market_snapshots
        ),
        funding_snapshots=tuple(
            snapshot for batch in batches for snapshot in batch.funding_snapshots
        ),
        hourly_contexts=tuple(
            context for batch in batches for context in batch.hourly_contexts
        ),
    )


def _storage_cadence(
    markets: Iterable[MarketConfig],
) -> dict[tuple[str, str], int]:
    return {
        (market.venue, market.venue_symbol): market.sample_seconds
        for market in markets
        if market.enabled
    }


class MarketDataPipeline:
    def __init__(
        self,
        collectors: Sequence[CollectorLike],
        state: RadarState,
        *,
        markets: Sequence[MarketConfig] = (),
        latest_market_data: LatestMarketData | None = None,
        sampling_seconds: int = SAMPLE_INTERVAL_SECONDS,
        clock: Callable[[], datetime] = utc_now,
        storage: ParquetStorage | None = None,
        collector_error_handler: CollectorErrorHandler | None = None,
        stale_after_seconds: int = 30,
        quoted_market_collector: VariationalCollector | None = None,
        quoted_market_poll_interval_seconds: float = 30.0,
        context_poll_seconds: int = 3600,
        rfq_stale_after_seconds: int = 120,
        quoted_markets: Sequence[MarketConfig] = (),
        live_depth_usd: int = 10_000,
    ) -> None:
        if sampling_seconds != SAMPLE_INTERVAL_SECONDS:
            raise ValueError("sampling_seconds must be a 10-second interval")
        if isinstance(stale_after_seconds, bool) or stale_after_seconds < 0:
            raise ValueError("stale_after_seconds must be non-negative")
        if quoted_market_poll_interval_seconds <= 0:
            raise ValueError("quoted_market_poll_interval_seconds must be positive")
        if (
            isinstance(context_poll_seconds, bool)
            or context_poll_seconds <= 0
            or 3600 % context_poll_seconds
        ):
            raise ValueError("context_poll_seconds must evenly divide 3600")
        self._context_poll_seconds = context_poll_seconds
        if isinstance(rfq_stale_after_seconds, bool) or rfq_stale_after_seconds <= 0:
            raise ValueError("rfq_stale_after_seconds must be positive")
        self._rfq_stale_after_seconds = rfq_stale_after_seconds
        self._latest_quotes: dict[str, QuotedMarketSnapshot] = {}
        self._storage_cadence = _storage_cadence((*markets, *quoted_markets))
        if live_depth_usd not in (1_000, 5_000, 10_000):
            raise ValueError("live_depth_usd must be 1000, 5000, or 10000")
        # Feeds reach live state only when both sides fill the evaluated size.
        self._live_depth_usd = live_depth_usd
        self._collectors = tuple(collectors)
        self.state = state
        self._markets = tuple(markets)
        self._latest_market_data = (
            LatestMarketData()
            if latest_market_data is None
            else latest_market_data
        )
        self.sampling_seconds = sampling_seconds
        self._clock = clock
        self.storage = storage
        self._collector_error_handler = collector_error_handler
        self._stale_after_seconds = stale_after_seconds
        self._last_hourly_sample: datetime | None = None
        self._hourly_task: asyncio.Task[None] | None = None
        self._hourly_stop_event: asyncio.Event | None = None
        self._quoted_market_collector = quoted_market_collector
        self._quoted_market_poll_interval_seconds = (
            quoted_market_poll_interval_seconds
        )
        self._quoted_market_task: asyncio.Task[None] | None = None
        self._started = False
        self._market_availability_reasons: dict[tuple[str, str, str], str] = {}
        self._market_availability_counts: dict[
            tuple[str, str, str], Counter[str]
        ] = {}
        self._market_availability_cycles = 0

    @classmethod
    def from_config(
        cls,
        config: RadarConfig,
        *,
        state: RadarState | None = None,
        request_json=None,
        clock: Callable[[], datetime] = utc_now,
        storage: ParquetStorage | None = None,
        collector_error_handler: CollectorErrorHandler | None = None,
    ) -> "MarketDataPipeline":
        from radar.collectors.arcus import ArcusCollector
        from radar.collectors.backpack import BackpackCollector
        from radar.collectors.hyperliquid import HyperliquidCollector, ReconnectBudget
        from radar.collectors.ondo import OndoCollector
        from radar.collectors.lighter import (
            LIGHTER_ROBINHOOD_BASE_URL,
            LighterCollector,
        )

        latest_market_data = LatestMarketData()
        # One budget for every Hyperliquid feed: they share the public IP.
        hyperliquid_budget = ReconnectBudget(config.hyperliquid_reconnect_spacing_seconds)
        collector_kwargs: _CollectorOptions = {
            "latest_market_data": latest_market_data,
        }
        if request_json is not None:
            collector_kwargs["request_json"] = request_json
        collectors: list[CollectorLike] = [
            LighterCollector(
                config.markets,
                clock=clock,
                error_handler=collector_error_handler,
                **collector_kwargs,
            ),
        ]
        discovered_venues = (
            set(config.discovery.venues) if config.discovery.enabled else set()
        )
        if (
            markets_for_venue(config.markets, "lighter_robinhood")
            or "lighter_robinhood" in discovered_venues
        ):
            collectors.append(
                LighterCollector(
                    config.markets,
                    venue="lighter_robinhood",
                    base_url=LIGHTER_ROBINHOOD_BASE_URL,
                    clock=clock,
                    error_handler=collector_error_handler,
                    **collector_kwargs,
                )
            )
        collectors.append(
            HyperliquidCollector(
                config.markets,
                reconnect_budget=hyperliquid_budget,
                clock=clock,
                error_handler=collector_error_handler,
                **collector_kwargs,
            )
        )
        if (
            markets_for_venue(config.markets, "trade_xyz")
            or "trade_xyz" in discovered_venues
        ):
            collectors.append(
                HyperliquidCollector(
                    config.markets,
                    reconnect_budget=hyperliquid_budget,
                    venue="trade_xyz",
                    dex="xyz",
                    clock=clock,
                    error_handler=collector_error_handler,
                    **collector_kwargs,
                )
            )
        if markets_for_venue(config.markets, "entropy"):
            collectors.append(
                HyperliquidCollector(
                    config.markets,
                    reconnect_budget=hyperliquid_budget,
                    venue="entropy",
                    dex="io",
                    clock=clock,
                    error_handler=collector_error_handler,
                    **collector_kwargs,
                )
            )
        if markets_for_venue(config.markets, "arcus"):
            collectors.append(
                ArcusCollector(
                    config.markets,
                    clock=clock,
                    error_handler=collector_error_handler,
                    source=config.arcus.source,
                    ws_url=config.arcus.ws_url,
                    ws_levels=config.arcus.ws_levels,
                    ws_silence_seconds=config.arcus.ws_silence_seconds,
                    ws_reconnect_delay_seconds=config.arcus.ws_reconnect_delay_seconds,
                    rest_fallback=config.arcus.rest_fallback,
                    **collector_kwargs,
                )
            )
        if markets_for_venue(config.markets, "backpack"):
            collectors.append(
                BackpackCollector(
                    config.markets,
                    clock=clock,
                    error_handler=collector_error_handler,
                    **collector_kwargs,
                )
            )
        if markets_for_venue(config.markets, "ondo"):
            collectors.append(
                OndoCollector(
                    config.markets,
                    clock=clock,
                    error_handler=collector_error_handler,
                    **collector_kwargs,
                )
            )
        quoted_market_collector = None
        if config.quoted_markets or "variational" in discovered_venues:
            if request_json is None:
                quoted_market_collector = VariationalCollector(
                    config.quoted_markets,
                    clock=clock,
                    error_handler=collector_error_handler,
                )
            else:
                quoted_market_collector = VariationalCollector(
                    config.quoted_markets,
                    request_json=request_json,
                    clock=clock,
                    error_handler=collector_error_handler,
                )
        return cls(
            collectors,
            RadarState() if state is None else state,
            markets=config.markets,
            latest_market_data=latest_market_data,
            sampling_seconds=config.sampling_seconds,
            clock=clock,
            storage=storage,
            collector_error_handler=collector_error_handler,
            stale_after_seconds=config.monitors.spread.stale_after_seconds,
            quoted_market_collector=quoted_market_collector,
            context_poll_seconds=config.context_poll_seconds,
            rfq_stale_after_seconds=config.monitors.spread.rfq_stale_after_seconds,
            quoted_markets=config.quoted_markets,
            live_depth_usd=config.monitors.spread.primary_size_usd,
        )

    @property
    def collectors(self) -> tuple[CollectorLike, ...]:
        return self._collectors

    async def start(self) -> None:
        if self._started:
            return
        self._hourly_stop_event = asyncio.Event()
        lifecycle_hooks = [
            hook
            for collector in self._collectors
            if (hook := getattr(collector, "start", None)) is not None
        ]
        await asyncio.gather(*(hook() for hook in lifecycle_hooks))
        self._started = True
        self._hourly_task = asyncio.create_task(
            self._run_hourly(),
            name="radar-hourly-context",
        )
        if self._quoted_market_collector is not None:
            self._quoted_market_task = asyncio.create_task(
                self._run_quoted_market(),
                name="radar-variational-quoted-market",
            )

    async def stop(self) -> None:
        self._started = False
        if self._hourly_stop_event is not None:
            self._hourly_stop_event.set()
        hourly_task = self._hourly_task
        if hourly_task is not None and not hourly_task.done():
            hourly_task.cancel()
            await asyncio.gather(hourly_task, return_exceptions=True)
        self._hourly_task = None
        self._hourly_stop_event = None
        quoted_market_task = self._quoted_market_task
        if quoted_market_task is not None and not quoted_market_task.done():
            quoted_market_task.cancel()
            await asyncio.gather(quoted_market_task, return_exceptions=True)
        self._quoted_market_task = None
        lifecycle_hooks = [
            hook
            for collector in self._collectors
            if (hook := getattr(collector, "stop", None)) is not None
        ]
        results = await asyncio.gather(
            *(hook() for hook in lifecycle_hooks),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError
            ):
                raise result

    def flush_storage(
        self,
        *,
        now: datetime | None = None,
        flush_id: str | None = None,
    ) -> int:
        """Flush the optional storage buffer at an application-owned boundary."""
        return (
            0
            if self.storage is None
            else self.storage.flush(now=now, flush_id=flush_id)
        )

    async def collect_once(
        self,
        *,
        now: datetime | None = None,
        sample_time: datetime | None = None,
    ) -> CollectorBatch:
        current_time = self._clock() if now is None else now
        if sample_time is None:
            resolved_sample_time = aligned_sample_time(
                current_time, self.sampling_seconds
            )
        else:
            if sample_time.tzinfo is None or sample_time.utcoffset() is None:
                raise ValueError("sample_time must be timezone-aware")
            resolved_sample_time = sample_time.astimezone(UTC)
            if aligned_sample_time(
                resolved_sample_time, self.sampling_seconds
            ) != resolved_sample_time:
                raise ValueError("sample_time must be aligned to a 10-second boundary")
        for collector in self._collectors:
            prepare_sample = getattr(collector, "prepare_sample", None)
            if callable(prepare_sample):
                prepare_sample()
        candidate_batch, diagnostics = (
            self._latest_market_data.build_batch_with_diagnostics(
                self._markets,
                sample_time=resolved_sample_time,
                now=current_time,
                stale_after_seconds=self._stale_after_seconds,
                live_depth_usd=self._live_depth_usd,
            )
        )
        included_keys = {
            (
                diagnostic.venue,
                diagnostic.venue_symbol,
                diagnostic.canonical_symbol,
            )
            for diagnostic in diagnostics
            if diagnostic.reason == "included" and diagnostic.live_signal_eligible
        }
        self._record_market_availability(diagnostics)
        rfq_snapshots = self._fresh_rfq_snapshots(resolved_sample_time, current_time)
        batch = CollectorBatch(
            market_snapshots=tuple(
                snapshot
                for snapshot in candidate_batch.market_snapshots
                if (
                    snapshot.venue,
                    snapshot.venue_symbol,
                    snapshot.canonical_symbol,
                )
                in included_keys
            )
            + rfq_snapshots
        )
        # Keep the live depth gate (the evaluated size) for monitoring, but archive every
        # fresh, valid BBO even when only the smaller VWAP tiers are executable.
        # Historical research must not lose $1k/$5k observations merely because
        # the same book cannot fill $10k on both sides.
        self.state.apply_market_batch(batch)
        if self.storage is not None:
            self.storage.append(
                self._on_storage_cadence(
                    CollectorBatch(
                        market_snapshots=candidate_batch.market_snapshots + rfq_snapshots
                    ),
                    resolved_sample_time,
                )
            )
        return batch

    def _on_storage_cadence(
        self, batch: CollectorBatch, sample_time: datetime
    ) -> CollectorBatch:
        """Long-tail feeds keep every sample live but persist only on cadence."""
        epoch_seconds = int(sample_time.timestamp())
        return CollectorBatch(
            market_snapshots=tuple(
                snapshot
                for snapshot in batch.market_snapshots
                if epoch_seconds
                % self._storage_cadence.get((snapshot.venue, snapshot.venue_symbol), 1)
                == 0
            )
        )

    async def set_universe(
        self,
        markets: Sequence[MarketConfig],
        quoted_markets: Sequence[MarketConfig],
    ) -> None:
        """Swap the collected market universe without restarting the pipeline."""
        self._markets = tuple(markets)
        self._storage_cadence = _storage_cadence((*markets, *quoted_markets))
        for collector in self._collectors:
            set_markets = getattr(collector, "set_markets", None)
            if not callable(set_markets):
                continue
            try:
                result = set_markets(markets)
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001
                report_collector_error(
                    self._collector_error_handler, collector.venue, error
                )
        collector = self._quoted_market_collector
        if collector is not None:
            collector.set_markets(quoted_markets)
            kept = {
                market.venue_symbol
                for market in quoted_markets
                if market.enabled and market.venue == collector.venue
            }
            self._latest_quotes = {
                symbol: quote
                for symbol, quote in self._latest_quotes.items()
                if symbol in kept
            }

    def _rfq_context_batch(self, sample_time: datetime, now: datetime) -> CollectorBatch:
        """Funding and OI for every RFQ quote still within its freshness limit."""
        quotes = [
            quote
            for quote in self._latest_quotes.values()
            if 0 <= (now - quote.quote_time).total_seconds() <= self._rfq_stale_after_seconds
        ]
        return CollectorBatch(
            funding_snapshots=tuple(variational_funding_snapshot(q) for q in quotes),
            hourly_contexts=tuple(
                variational_hourly_context(q, sample_time=sample_time) for q in quotes
            ),
        )

    def _fresh_rfq_snapshots(
        self, sample_time: datetime, now: datetime
    ) -> tuple[MarketSnapshot, ...]:
        """Expose each RFQ quote while its own ``updated_at`` is still fresh."""
        snapshots: list[MarketSnapshot] = []
        for venue_symbol in sorted(self._latest_quotes):
            quote = self._latest_quotes[venue_symbol]
            age_seconds = (now - quote.quote_time).total_seconds()
            if not 0 <= age_seconds <= self._rfq_stale_after_seconds:
                continue
            try:
                snapshots.append(
                    variational_market_snapshot(quote, sample_time=sample_time)
                )
            except ValueError:
                # A crossed or otherwise invalid quote is skipped, never repaired.
                continue
        return tuple(snapshots)

    def _record_market_availability(
        self, diagnostics: Sequence[MarketAvailabilityDiagnostic]
    ) -> None:
        counts = Counter(diagnostic.reason for diagnostic in diagnostics)
        self._market_availability_cycles += 1
        LOGGER.info(
            "market availability cycle configured=%d included=%d missing_view=%d "
            "unready=%d stale=%d live_depth_usd=%d insufficient_buy=%d "
            "insufficient_sell=%d insufficient_both=%d",
            len(diagnostics),
            counts["included"],
            counts["missing_view"],
            counts["unready"],
            counts["stale"],
            self._live_depth_usd,
            counts["insufficient_buy_depth"],
            counts["insufficient_sell_depth"],
            counts["insufficient_both_depth"],
        )

        for diagnostic in diagnostics:
            key = (
                diagnostic.venue,
                diagnostic.venue_symbol,
                diagnostic.canonical_symbol,
            )
            feed_counts = self._market_availability_counts.setdefault(key, Counter())
            feed_counts[diagnostic.reason] += 1
            previous_reason = self._market_availability_reasons.get(key)
            if previous_reason != diagnostic.reason and (
                diagnostic.reason != "included" or previous_reason is not None
            ):
                LOGGER.info(
                    "market availability transition venue=%s venue_symbol=%s "
                    "canonical_symbol=%s previous=%s reason=%s",
                    diagnostic.venue,
                    diagnostic.venue_symbol,
                    diagnostic.canonical_symbol,
                    previous_reason or "none",
                    diagnostic.reason,
                )
            self._market_availability_reasons[key] = diagnostic.reason

        if self._market_availability_cycles % MARKET_AVAILABILITY_FEED_SUMMARY_INTERVAL:
            return
        for key in sorted(self._market_availability_counts):
            venue, venue_symbol, canonical_symbol = key
            feed_counts = self._market_availability_counts[key]
            if not any(
                reason != "included" and count > 0
                for reason, count in feed_counts.items()
            ):
                continue
            LOGGER.info(
                "market availability feed summary venue=%s venue_symbol=%s "
                "canonical_symbol=%s included=%d missing_view=%d unready=%d "
                "stale=%d insufficient_buy=%d insufficient_sell=%d "
                "insufficient_both=%d",
                venue,
                venue_symbol,
                canonical_symbol,
                feed_counts["included"],
                feed_counts["missing_view"],
                feed_counts["unready"],
                feed_counts["stale"],
                feed_counts["insufficient_buy_depth"],
                feed_counts["insufficient_sell_depth"],
                feed_counts["insufficient_both_depth"],
            )

    async def collect_hourly_once(
        self, *, now: datetime | None = None
    ) -> CollectorBatch | None:
        current_time = self._clock() if now is None else now
        sample_time = aligned_sample_time(current_time, self.sampling_seconds)
        hour = context_sample_time(sample_time, self._context_poll_seconds)
        if not should_collect_hourly_context(
            current_time, hour, self._last_hourly_sample
        ):
            return None

        results = await asyncio.gather(
            *(self._collect_hourly(collector, hour) for collector in self._collectors),
            return_exceptions=True,
        )
        batches: list[CollectorBatch] = []
        for collector, result in zip(self._collectors, results, strict=True):
            if isinstance(result, CollectorBatch):
                batches.append(result)
            elif isinstance(result, asyncio.CancelledError):
                raise result
            elif isinstance(result, Exception):
                report_collector_error(
                    self._collector_error_handler,
                    collector.venue,
                    result,
                )
            else:
                report_collector_error(
                    self._collector_error_handler,
                    collector.venue,
                    TypeError("collector returned a non-CollectorBatch value"),
                )
        batch = merge_batches((*batches, self._rfq_context_batch(hour, current_time)))
        self.state.apply_context_batch(batch)
        if self.storage is not None:
            self.storage.append(batch)
        self._last_hourly_sample = hour
        return batch

    async def wait_for_market_feeds(
        self,
        canonical_symbol: str,
        *,
        required_venues: int = 2,
        timeout_seconds: float = 30.0,
    ) -> None:
        if not canonical_symbol:
            raise ValueError("canonical_symbol must be non-empty")
        if required_venues <= 0:
            raise ValueError("required_venues must be positive")
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")

        async def wait_for_ready() -> None:
            while True:
                now = self._clock()
                ready_venues = self._latest_market_data.ready_venues(
                    markets=self._markets,
                    canonical_symbol=canonical_symbol,
                    now=now,
                    stale_after_seconds=self._stale_after_seconds,
                    primary_size_usd=self._live_depth_usd,
                )
                if len(ready_venues) >= required_venues:
                    return
                await asyncio.sleep(0.05)

        try:
            async with asyncio.timeout(timeout_seconds):
                await wait_for_ready()
        except TimeoutError as error:
            raise TimeoutError(
                f"timed out waiting for {required_venues} ready market feeds "
                f"for {canonical_symbol}"
            ) from error

    async def _collect_hourly(
        self,
        collector: CollectorLike,
        sample_time: datetime,
    ) -> CollectorBatch:
        collect_hourly = getattr(collector, "collect_hourly", None)
        if callable(collect_hourly):
            result = await collect_hourly(sample_time=sample_time)
            if not isinstance(result, CollectorBatch):
                raise TypeError("collector returned a non-CollectorBatch value")
            return result

        collect = getattr(collector, "collect", None)
        if callable(collect):
            result = await collect(
                sample_time=sample_time,
                include_hourly_context=True,
            )
            if not isinstance(result, CollectorBatch):
                raise TypeError("collector returned a non-CollectorBatch value")
            return CollectorBatch(
                funding_snapshots=result.funding_snapshots,
                hourly_contexts=result.hourly_contexts,
            )
        raise TypeError("collector does not provide an hourly collection method")

    async def _run_quoted_market(self) -> None:
        collector = self._quoted_market_collector
        if collector is None:
            return
        while True:
            try:
                snapshots = await collector.poll_once()
                for snapshot in snapshots:
                    self._latest_quotes[snapshot.venue_symbol] = snapshot
                if snapshots and self.storage is not None:
                    self.storage.append_quoted(snapshots)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001
                report_collector_error(
                    self._collector_error_handler,
                    collector.venue,
                    error,
                )
            await asyncio.sleep(self._quoted_market_poll_interval_seconds)

    async def _run_hourly(self) -> None:
        stop_event = self._hourly_stop_event
        if stop_event is None:
            return
        while not stop_event.is_set():
            now = self._clock()
            sample_time = aligned_sample_time(now, self.sampling_seconds)
            hour = context_sample_time(sample_time, self._context_poll_seconds)
            if should_collect_hourly_context(
                now, hour, self._last_hourly_sample
            ):
                await self.collect_hourly_once(now=now)
                continue

            next_due = hour + timedelta(seconds=HOURLY_CONTEXT_GRACE_SECONDS)
            if self._last_hourly_sample == hour:
                next_due += timedelta(seconds=self._context_poll_seconds)
            delay = max(0.0, (next_due - now.astimezone(UTC)).total_seconds())
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except TimeoutError:
                pass

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> None:
        while True:
            now = self._clock()
            next_sample = aligned_sample_time(now, self.sampling_seconds) + timedelta(
                seconds=self.sampling_seconds
            )
            delay = max(0.0, (next_sample - now).total_seconds())
            if stop_event is None:
                await asyncio.sleep(delay)
            else:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                except TimeoutError:
                    pass
                if stop_event.is_set():
                    return
            await self.collect_once()
