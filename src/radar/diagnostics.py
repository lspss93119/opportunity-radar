from __future__ import annotations

import asyncio
import gc
import logging
import os
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

LAG_WARNING_THRESHOLDS_MS = (100.0, 500.0, 1_000.0, 5_000.0)
DIAGNOSTIC_LOG_MAX_BYTES = 20 * 1024 * 1024
DIAGNOSTIC_LOG_BACKUP_COUNT = 3


@dataclass(frozen=True)
class ResourceSnapshot:
    rss_bytes: int | None = None
    process_cpu_ms: float | None = None
    load_average_1m: float | None = None
    available_memory_bytes: int | None = None
    swap_used_bytes: int | None = None


@dataclass(frozen=True)
class SchedulerAdvance:
    previous_sample_time: datetime
    after_cycle_time: datetime
    next_sample_time: datetime
    skipped_slots: tuple[datetime, ...]


def _require_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def advance_scheduled_sample_time(
    previous_sample_time: datetime,
    after_cycle_time: datetime,
    *,
    sampling_seconds: int,
) -> SchedulerAdvance:
    if sampling_seconds <= 0:
        raise ValueError("sampling_seconds must be positive")
    previous = _require_utc(previous_sample_time, "previous_sample_time")
    after_cycle = _require_utc(after_cycle_time, "after_cycle_time")
    next_sample = previous + timedelta(seconds=sampling_seconds)
    if after_cycle < next_sample:
        return SchedulerAdvance(previous, after_cycle, next_sample, ())

    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    elapsed_seconds = (after_cycle - epoch).total_seconds()
    aligned_seconds = (
        int(elapsed_seconds // sampling_seconds) * sampling_seconds
    )
    next_sample = epoch + timedelta(
        seconds=aligned_seconds + sampling_seconds
    )
    skipped: list[datetime] = []
    cursor = previous + timedelta(seconds=sampling_seconds)
    while cursor < next_sample:
        skipped.append(cursor)
        cursor += timedelta(seconds=sampling_seconds)
    return SchedulerAdvance(previous, after_cycle, next_sample, tuple(skipped))


class EventLoopLagWatchdog:
    def __init__(
        self,
        *,
        logger: logging.Logger,
        resource_snapshot: Callable[[], ResourceSnapshot] | None = None,
        cycle_number: Callable[[], int | None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        interval_seconds: float = 0.1,
        summary_interval_seconds: float = 60.0,
        on_tick: Callable[[], None] | None = None,
        on_summary: Callable[[dict[str, float | int | None]], None] | None = None,
    ) -> None:
        if interval_seconds <= 0 or summary_interval_seconds <= 0:
            raise ValueError("watchdog intervals must be positive")
        self.logger = logger
        self._resource_snapshot = resource_snapshot
        self._cycle_number = cycle_number
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self.interval_seconds = interval_seconds
        self.summary_interval_seconds = summary_interval_seconds
        self._on_tick = on_tick
        self._on_summary = on_summary
        self._lags_ms: deque[float] = deque(maxlen=6_000)
        self._samples = 0
        self._over_100ms = 0
        self._over_500ms = 0
        self._over_1s = 0
        self._over_5s = 0
        self._max_lag_ms = 0.0
        self._in_lag_episode = False
        self._last_summary_monotonic: float | None = None

    def observe(
        self,
        *,
        expected_monotonic: float,
        actual_monotonic: float,
        wall_time: datetime | None = None,
        cycle_number: int | None = None,
    ) -> float:
        lag_ms = max(0.0, actual_monotonic - expected_monotonic) * 1_000.0
        self._samples += 1
        self._lags_ms.append(lag_ms)
        self._max_lag_ms = max(self._max_lag_ms, lag_ms)
        self._over_100ms += int(lag_ms >= 100.0)
        self._over_500ms += int(lag_ms >= 500.0)
        self._over_1s += int(lag_ms >= 1_000.0)
        self._over_5s += int(lag_ms >= 5_000.0)

        if lag_ms >= 100.0 and not self._in_lag_episode:
            crossed_threshold = max(
                threshold
                for threshold in LAG_WARNING_THRESHOLDS_MS
                if lag_ms >= threshold
            )
            resources = (
                None
                if self._resource_snapshot is None
                else self._resource_snapshot()
            )
            self.logger.warning(
                "event loop lag episode lag_ms=%.3f threshold_ms=%.0f "
                "wall_time=%s expected_monotonic=%.6f actual_monotonic=%.6f "
                "cycle=%s rss_bytes=%s process_cpu_ms=%s "
                "load_average_1m=%s",
                lag_ms,
                crossed_threshold,
                wall_time or self._wall_clock(),
                expected_monotonic,
                actual_monotonic,
                cycle_number
                if cycle_number is not None
                else (None if self._cycle_number is None else self._cycle_number()),
                None if resources is None else resources.rss_bytes,
                None if resources is None else resources.process_cpu_ms,
                None if resources is None else resources.load_average_1m,
            )
            self._in_lag_episode = True
        elif lag_ms < 100.0 and self._in_lag_episode:
            self.logger.info(
                "event loop lag recovered lag_ms=%.3f wall_time=%s",
                lag_ms,
                wall_time or self._wall_clock(),
            )
            self._in_lag_episode = False
        return lag_ms

    def summary(self) -> dict[str, float | int | None]:
        values = tuple(self._lags_ms)
        return {
            "samples": self._samples,
            "p50_lag_ms": _percentile(values, 0.50),
            "p95_lag_ms": _percentile(values, 0.95),
            "p99_lag_ms": _percentile(values, 0.99),
            "max_lag_ms": self._max_lag_ms,
            "over_100ms": self._over_100ms,
            "over_500ms": self._over_500ms,
            "over_1s": self._over_1s,
            "over_5s": self._over_5s,
        }

    def maybe_log_summary(self, now_monotonic: float | None = None) -> bool:
        now = self._monotonic() if now_monotonic is None else now_monotonic
        if (
            self._last_summary_monotonic is not None
            and now - self._last_summary_monotonic < self.summary_interval_seconds
        ):
            return False
        self._last_summary_monotonic = now
        summary = self.summary()
        self.logger.info(
            "event loop lag summary samples=%d p50_ms=%s p95_ms=%s "
            "p99_ms=%s max_ms=%.3f over_100ms=%d over_500ms=%d "
            "over_1s=%d over_5s=%d",
            summary["samples"],
            summary["p50_lag_ms"],
            summary["p95_lag_ms"],
            summary["p99_lag_ms"],
            summary["max_lag_ms"],
            summary["over_100ms"],
            summary["over_500ms"],
            summary["over_1s"],
            summary["over_5s"],
        )
        return True

    async def run(self, stop_event: asyncio.Event) -> None:
        started = self._monotonic()
        self._last_summary_monotonic = started
        expected = started + self.interval_seconds
        while not stop_event.is_set():
            timeout = max(0.0, expected - self._monotonic())
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=timeout)
            except TimeoutError:
                pass
            if stop_event.is_set():
                return
            actual = self._monotonic()
            self.observe(
                expected_monotonic=expected,
                actual_monotonic=actual,
            )
            if self._on_tick is not None:
                self._on_tick()
            if self.maybe_log_summary(actual) and self._on_summary is not None:
                self._on_summary(self.summary())
            expected += self.interval_seconds
            if actual >= expected:
                expected = actual + self.interval_seconds


@dataclass(frozen=True)
class GCPause:
    generation: int
    duration_ms: float
    collected: int
    uncollectable: int


@dataclass
class _GCGenerationStats:
    count: int = 0
    total_ms: float = 0.0
    max_ms: float = 0.0
    collected: int = 0
    uncollectable: int = 0


class GCTelemetry:
    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        logger: logging.Logger | None = None,
        warning_threshold_ms: float = 100.0,
    ) -> None:
        self._clock = clock
        self.logger = logger
        self.warning_threshold_ms = warning_threshold_ms
        self._active: dict[int, float] = {}
        self._stats = {generation: _GCGenerationStats() for generation in range(3)}
        self._long_pauses: list[GCPause] = []
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        gc.callbacks.append(self.callback)
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        try:
            gc.callbacks.remove(self.callback)
        except ValueError:
            pass
        self._started = False
        self._active.clear()

    def callback(self, phase: str, info: dict[str, Any]) -> None:
        generation = int(info.get("generation", -1))
        if generation not in self._stats:
            return
        if phase == "start":
            self._active[generation] = self._clock()
            return
        if phase != "stop":
            return
        started = self._active.pop(generation, None)
        if started is None:
            return
        duration_ms = max(0.0, self._clock() - started) * 1_000.0
        collected = int(info.get("collected", 0))
        uncollectable = int(info.get("uncollectable", 0))
        stats = self._stats[generation]
        stats.count += 1
        stats.total_ms += duration_ms
        stats.max_ms = max(stats.max_ms, duration_ms)
        stats.collected += collected
        stats.uncollectable += uncollectable
        if duration_ms >= self.warning_threshold_ms:
            self._long_pauses.append(
                GCPause(generation, duration_ms, collected, uncollectable)
            )

    def drain_long_pauses(self) -> tuple[GCPause, ...]:
        pauses = tuple(self._long_pauses)
        self._long_pauses.clear()
        return pauses

    def emit_long_pause_warnings(self) -> None:
        if self.logger is None:
            self._long_pauses.clear()
            return
        for pause in self.drain_long_pauses():
            self.logger.warning(
                "gc pause generation=%d duration_ms=%.3f collected=%d "
                "uncollectable=%d",
                pause.generation,
                pause.duration_ms,
                pause.collected,
                pause.uncollectable,
            )

    def summary(self) -> dict[str, dict[str, float | int]]:
        return {
            f"generation_{generation}": {
                "count": stats.count,
                "total_ms": stats.total_ms,
                "max_ms": stats.max_ms,
                "collected": stats.collected,
                "uncollectable": stats.uncollectable,
            }
            for generation, stats in self._stats.items()
        }


@dataclass(frozen=True)
class DiagnosticLogSession:
    path: Path
    handler: RotatingFileHandler
    logger: logging.Logger
    session_id: str
    previous_level: int

    def close(self) -> None:
        self.logger.info("diagnostic session end session_id=%s", self.session_id)
        self.handler.flush()
        self.logger.removeHandler(self.handler)
        self.handler.close()
        self.logger.setLevel(self.previous_level)


def install_diagnostic_logging(
    path: Path,
    *,
    logger: logging.Logger,
    session_id: str | None = None,
) -> DiagnosticLogSession:
    path.parent.mkdir(parents=True, exist_ok=True)
    actual_session_id = session_id or (
        f"pid-{os.getpid()}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S.%fZ')}"
    )
    previous_level = logger.level
    if logger.level == logging.NOTSET or logger.level > logging.INFO:
        logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(
        path,
        maxBytes=DIAGNOSTIC_LOG_MAX_BYTES,
        backupCount=DIAGNOSTIC_LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    logger.addHandler(handler)
    logger.info(
        "diagnostic session start session_id=%s log_path=%s",
        actual_session_id,
        path,
    )
    return DiagnosticLogSession(
        path,
        handler,
        logger,
        actual_session_id,
        previous_level,
    )


@dataclass
class _BackpackSymbolStats:
    messages: int = 0
    bytes_received: int = 0
    handler_invocations: int = 0
    handler_wall_ns: int = 0
    handler_cpu_ns: int = 0
    max_handler_ns: int = 0
    updates_applied: int = 0
    publishes: int = 0
    rebuilds: int = 0
    gaps: int = 0


class BackpackWorkloadStats:
    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        thread_time_ns: Callable[[], int] = time.thread_time_ns,
    ) -> None:
        self._monotonic = monotonic
        self._thread_time_ns = thread_time_ns
        self._started_at: float | None = None
        self._last_now: float | None = None
        self._global = _BackpackSymbolStats()
        self._symbols: dict[str, _BackpackSymbolStats] = {}
        self._parse_ns = 0
        self._apply_update_ns = 0
        self._snapshot_ns = 0
        self._publish_callback_ns = 0

    def _symbol(self, symbol: str) -> _BackpackSymbolStats:
        return self._symbols.setdefault(symbol, _BackpackSymbolStats())

    def record_handler(
        self,
        *,
        symbol: str | None,
        payload_bytes: int,
        handler_wall_ns: int,
        handler_cpu_ns: int,
        parse_ns: int,
        apply_update_ns: int,
        snapshot_ns: int,
        publish_callback_ns: int,
    ) -> None:
        now = self._monotonic()
        if self._started_at is None:
            self._started_at = now
        self._last_now = now
        target = self._global if symbol is None else self._symbol(symbol)
        for stats in (self._global, target) if symbol is not None else (target,):
            stats.messages += 1
            stats.bytes_received += payload_bytes
            stats.handler_invocations += 1
            stats.handler_wall_ns += handler_wall_ns
            stats.handler_cpu_ns += handler_cpu_ns
            stats.max_handler_ns = max(stats.max_handler_ns, handler_wall_ns)
        self._parse_ns += parse_ns
        self._apply_update_ns += apply_update_ns
        self._snapshot_ns += snapshot_ns
        self._publish_callback_ns += publish_callback_ns

    def record_update_applied(self, symbol: str) -> None:
        self._global.updates_applied += 1
        self._symbol(symbol).updates_applied += 1

    def record_publish(self, symbol: str) -> None:
        self._global.publishes += 1
        self._symbol(symbol).publishes += 1

    def record_rebuild(self, symbol: str) -> None:
        self._global.rebuilds += 1
        self._symbol(symbol).rebuilds += 1

    def record_gap(self, symbol: str) -> None:
        self._global.gaps += 1
        self._symbol(symbol).gaps += 1

    def summary(self, now: float | None = None) -> dict[str, Any]:
        current = self._monotonic() if now is None else now
        elapsed = max(0.0, current - (self._started_at or current))
        global_stats = self._global
        wall_ms = global_stats.handler_wall_ns / 1_000_000.0
        cpu_ms = global_stats.handler_cpu_ns / 1_000_000.0

        def symbol_summary(stats: _BackpackSymbolStats) -> dict[str, Any]:
            return {
                "messages": stats.messages,
                "bytes": stats.bytes_received,
                "handler_invocations": stats.handler_invocations,
                "msg_per_sec": stats.messages / elapsed if elapsed else 0.0,
                "bytes_per_sec": stats.bytes_received / elapsed if elapsed else 0.0,
                "handler_wall_ms": stats.handler_wall_ns / 1_000_000.0,
                "handler_cpu_ms": stats.handler_cpu_ns / 1_000_000.0,
                "max_handler_ms": stats.max_handler_ns / 1_000_000.0,
                "updates_applied": stats.updates_applied,
                "publishes": stats.publishes,
                "rebuilds": stats.rebuilds,
                "gaps": stats.gaps,
            }

        return {
            "messages": global_stats.messages,
            "bytes": global_stats.bytes_received,
            "handler_invocations": global_stats.handler_invocations,
            "msg_per_sec": global_stats.messages / elapsed if elapsed else 0.0,
            "bytes_per_sec": global_stats.bytes_received / elapsed if elapsed else 0.0,
            "handler_wall_ms": wall_ms,
            "handler_cpu_ms": cpu_ms,
            "handler_utilization_pct": (
                cpu_ms / (elapsed * 1_000.0) * 100.0 if elapsed else 0.0
            ),
            "max_handler_ms": global_stats.max_handler_ns / 1_000_000.0,
            "updates_applied": global_stats.updates_applied,
            "publishes": global_stats.publishes,
            "rebuilds": global_stats.rebuilds,
            "gaps": global_stats.gaps,
            "parse_ms": self._parse_ns / 1_000_000.0,
            "apply_update_ms": self._apply_update_ns / 1_000_000.0,
            "snapshot_ms": self._snapshot_ns / 1_000_000.0,
            "publish_callback_ms": self._publish_callback_ns / 1_000_000.0,
            "elapsed_seconds": elapsed,
            "symbols": {
                symbol: symbol_summary(stats)
                for symbol, stats in self._symbols.items()
            },
        }


def _percentile(values: tuple[float, ...], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (
        position - lower
    )
