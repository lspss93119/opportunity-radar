from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Sequence
from datetime import datetime, timedelta, timezone

from radar.collectors.base import Collector, CollectorBatch
from radar.config import RadarConfig
from radar.state import RadarState

UTC = timezone.utc
SAMPLE_INTERVAL_SECONDS = 10
HOURLY_CONTEXT_GRACE_SECONDS = 60


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


class MarketDataPipeline:
    def __init__(
        self,
        collectors: Sequence[Collector],
        state: RadarState,
        *,
        sampling_seconds: int = SAMPLE_INTERVAL_SECONDS,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if sampling_seconds != SAMPLE_INTERVAL_SECONDS:
            raise ValueError("sampling_seconds must be a 10-second interval")
        self._collectors = tuple(collectors)
        self.state = state
        self.sampling_seconds = sampling_seconds
        self._clock = clock
        self._last_hourly_sample: datetime | None = None

    @classmethod
    def from_config(
        cls,
        config: RadarConfig,
        *,
        state: RadarState | None = None,
        request_json=None,
        clock: Callable[[], datetime] = utc_now,
    ) -> "MarketDataPipeline":
        from radar.collectors.hyperliquid import HyperliquidCollector
        from radar.collectors.lighter import LighterCollector

        collector_kwargs = {} if request_json is None else {"request_json": request_json}
        collectors = (
            LighterCollector(config.markets, clock=clock, **collector_kwargs),
            HyperliquidCollector(config.markets, clock=clock, **collector_kwargs),
        )
        return cls(
            collectors,
            RadarState() if state is None else state,
            sampling_seconds=config.sampling_seconds,
            clock=clock,
        )

    @property
    def collectors(self) -> tuple[Collector, ...]:
        return self._collectors

    async def collect_once(self, *, now: datetime | None = None) -> CollectorBatch:
        current_time = self._clock() if now is None else now
        sample_time = aligned_sample_time(current_time, self.sampling_seconds)
        hour = hourly_sample_time(sample_time)
        include_hourly_context = should_collect_hourly_context(
            current_time, hour, self._last_hourly_sample
        )

        results = await asyncio.gather(
            *(
                collector.collect(
                    sample_time=sample_time,
                    include_hourly_context=include_hourly_context,
                )
                for collector in self._collectors
            ),
            return_exceptions=True,
        )
        batches = tuple(result for result in results if isinstance(result, CollectorBatch))
        batch = merge_batches(batches)
        self.state.apply(batch, replace_context=include_hourly_context)
        if include_hourly_context:
            self._last_hourly_sample = hour
        return batch

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
