from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from radar.collectors.base import CollectorBatch
from radar.models import (
    FundingSnapshot,
    HourlyContext,
    MarketSnapshot,
    QuotedMarketSnapshot,
)

UTC = timezone.utc
LOGGER = logging.getLogger(__name__)
DATASET_NAMES = ("market", "funding", "hourly_context", "quoted_market")
NormalizedRecord = (
    MarketSnapshot
    | FundingSnapshot
    | HourlyContext
    | QuotedMarketSnapshot
)
PendingKey = tuple[str, date]
UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")

MARKET_SCHEMA = pa.schema(
    [
        pa.field("sample_time", UTC_TIMESTAMP, nullable=False),
        pa.field("observed_at", UTC_TIMESTAMP, nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("venue_symbol", pa.string(), nullable=False),
        pa.field("canonical_symbol", pa.string(), nullable=False),
        pa.field("best_bid", pa.float64(), nullable=False),
        pa.field("best_bid_size", pa.float64(), nullable=False),
        pa.field("best_ask", pa.float64(), nullable=False),
        pa.field("best_ask_size", pa.float64(), nullable=False),
        pa.field("mark_price", pa.float64()),
        pa.field("index_price", pa.float64()),
        pa.field("buy_1k_vwap", pa.float64()),
        pa.field("sell_1k_vwap", pa.float64()),
        pa.field("buy_5k_vwap", pa.float64()),
        pa.field("sell_5k_vwap", pa.float64()),
        pa.field("buy_10k_vwap", pa.float64()),
        pa.field("sell_10k_vwap", pa.float64()),
    ]
)

FUNDING_SCHEMA = pa.schema(
    [
        pa.field("effective_time", UTC_TIMESTAMP, nullable=False),
        pa.field("observed_at", UTC_TIMESTAMP, nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("venue_symbol", pa.string(), nullable=False),
        pa.field("canonical_symbol", pa.string(), nullable=False),
        pa.field("funding_rate", pa.float64(), nullable=False),
        pa.field("next_funding_time", UTC_TIMESTAMP),
    ]
)

HOURLY_CONTEXT_SCHEMA = pa.schema(
    [
        pa.field("sample_time", UTC_TIMESTAMP, nullable=False),
        pa.field("observed_at", UTC_TIMESTAMP, nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("venue_symbol", pa.string(), nullable=False),
        pa.field("canonical_symbol", pa.string(), nullable=False),
        pa.field("open_interest", pa.float64()),
        pa.field("volume_24h", pa.float64()),
    ]
)

QUOTED_MARKET_SCHEMA = pa.schema(
    [
        pa.field("quote_time", UTC_TIMESTAMP, nullable=False),
        pa.field("fetched_at", UTC_TIMESTAMP, nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("venue_symbol", pa.string(), nullable=False),
        pa.field("canonical_symbol", pa.string(), nullable=False),
        pa.field("mark_price", pa.float64(), nullable=False),
        pa.field("bid_1k", pa.float64(), nullable=False),
        pa.field("ask_1k", pa.float64(), nullable=False),
        pa.field("bid_100k", pa.float64(), nullable=False),
        pa.field("ask_100k", pa.float64(), nullable=False),
        pa.field("bid_1m", pa.float64()),
        pa.field("ask_1m", pa.float64()),
        pa.field("funding_rate", pa.float64(), nullable=False),
        pa.field("funding_interval_seconds", pa.int64(), nullable=False),
        pa.field("volume_24h", pa.float64()),
        pa.field("long_open_interest", pa.float64()),
        pa.field("short_open_interest", pa.float64()),
    ]
)

DATASET_SCHEMAS = {
    "market": MARKET_SCHEMA,
    "funding": FUNDING_SCHEMA,
    "hourly_context": HOURLY_CONTEXT_SCHEMA,
    "quoted_market": QUOTED_MARKET_SCHEMA,
}


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass
class _DatasetFlushTelemetry:
    rows: int = 0
    rows_by_partition: dict[str, int] = field(default_factory=dict)
    mkdir_ms: float = 0.0
    table_from_pylist_ms: float = 0.0
    parquet_write_ms: float = 0.0

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


class ParquetStorage:
    """Buffered date-partitioned Parquet storage for normalized snapshots."""

    def __init__(self, root: Path, *, retention_days: int = 90) -> None:
        if retention_days <= 0:
            raise ValueError("retention_days must be positive")
        self.root = Path(root)
        self.retention_days = retention_days
        self._pending: dict[PendingKey, list[dict[str, object]]] = {}
        self._pending_lock = threading.Lock()

    @property
    def pending_count(self) -> int:
        with self._pending_lock:
            return sum(len(rows) for rows in self._pending.values())

    def append(
        self,
        value: (
            CollectorBatch
            | MarketSnapshot
            | FundingSnapshot
            | HourlyContext
            | QuotedMarketSnapshot
        ),
    ) -> None:
        with self._pending_lock:
            if isinstance(value, CollectorBatch):
                for market_snapshot in value.market_snapshots:
                    self._append_record("market", market_snapshot)
                for funding_snapshot in value.funding_snapshots:
                    self._append_record("funding", funding_snapshot)
                for context in value.hourly_contexts:
                    self._append_record("hourly_context", context)
                return
            if isinstance(value, MarketSnapshot):
                self._append_record("market", value)
                return
            if isinstance(value, FundingSnapshot):
                self._append_record("funding", value)
                return
            if isinstance(value, HourlyContext):
                self._append_record("hourly_context", value)
                return
            if isinstance(value, QuotedMarketSnapshot):
                self._append_record("quoted_market", value)
                return
            raise TypeError("value must be a CollectorBatch or normalized snapshot")

    def append_quoted(self, snapshots: Iterable[QuotedMarketSnapshot]) -> None:
        with self._pending_lock:
            for snapshot in snapshots:
                self._append_record("quoted_market", snapshot)

    def _append_record(self, dataset: str, record: NormalizedRecord) -> None:
        if dataset == "funding":
            if not isinstance(record, FundingSnapshot):
                raise TypeError("funding dataset requires FundingSnapshot")
            timestamp = record.effective_time
        elif dataset == "quoted_market":
            if not isinstance(record, QuotedMarketSnapshot):
                raise TypeError("quoted_market dataset requires QuotedMarketSnapshot")
            timestamp = record.quote_time
        else:
            if not isinstance(record, (MarketSnapshot, HourlyContext)):
                raise TypeError("market datasets require a sample_time")
            timestamp = record.sample_time
        partition_date = _as_utc(timestamp, "record timestamp").date()
        key = (dataset, partition_date)
        self._pending.setdefault(key, []).append(record.model_dump(mode="python"))

    def flush(
        self,
        *,
        now: datetime | None = None,
        flush_id: str | None = None,
    ) -> int:
        """Write pending rows with atomic file replacement, then prune."""
        resolved_flush_id = uuid.uuid4().hex if flush_id is None else flush_id
        worker_started = time.perf_counter()
        current_time = _as_utc(
            datetime.now(UTC) if now is None else now,
            "now",
        )
        lock_started = time.perf_counter()
        with self._pending_lock:
            lock_acquired = time.perf_counter()
            if not self._pending:
                pending = None
            else:
                pending = self._pending
                self._pending = {}
            swap_finished = time.perf_counter()
        lock_wait_ms = (lock_acquired - lock_started) * 1000.0
        swap_hold_ms = (swap_finished - lock_acquired) * 1000.0

        if pending is None:
            prune_started = time.perf_counter()
            try:
                self.prune(now=current_time)
            except Exception as error:
                self._log_phase_failure(
                    flush_id=resolved_flush_id,
                    phase="prune",
                    dataset=None,
                    rows=0,
                    worker_started=worker_started,
                    error=error,
                )
                raise
            prune_ms = (time.perf_counter() - prune_started) * 1000.0
            self._log_telemetry(
                flush_id=resolved_flush_id,
                worker_started=worker_started,
                lock_wait_ms=lock_wait_ms,
                swap_hold_ms=swap_hold_ms,
                rows_total=0,
                dataset_telemetry={},
                files_written=0,
                replace_ms=0.0,
                prune_ms=prune_ms,
                pending_empty=True,
            )
            return 0

        dataset_telemetry: dict[str, _DatasetFlushTelemetry] = {}
        rows_total = 0
        for (dataset, partition_date), rows in pending.items():
            metrics = dataset_telemetry.setdefault(
                dataset,
                _DatasetFlushTelemetry(),
            )
            row_count = len(rows)
            rows_total += row_count
            metrics.rows += row_count
            metrics.rows_by_partition[partition_date.isoformat()] = row_count

        staged: list[tuple[str, date, int, Path, Path]] = []
        replaced: list[Path] = []
        phase = "pending_swap"
        phase_dataset: str | None = None
        phase_rows = rows_total
        try:
            for (dataset, partition_date), rows in pending.items():
                phase_dataset = dataset
                phase_rows = len(rows)
                metrics = dataset_telemetry[dataset]
                mkdir_started = time.perf_counter()
                phase = "mkdir"
                partition = self.root / dataset / f"date={partition_date.isoformat()}"
                partition.mkdir(parents=True, exist_ok=True)
                metrics.mkdir_ms += (time.perf_counter() - mkdir_started) * 1000.0
                filename = f"part-{uuid.uuid4().hex}.parquet"
                final_path = partition / filename
                temp_path = partition / f".{filename}.tmp"
                staged.append((dataset, partition_date, len(rows), temp_path, final_path))
                phase = "table_from_pylist"
                table_started = time.perf_counter()
                table = pa.Table.from_pylist(rows, schema=DATASET_SCHEMAS[dataset])
                metrics.table_from_pylist_ms += (
                    time.perf_counter() - table_started
                ) * 1000.0
                phase = "parquet_write"
                write_started = time.perf_counter()
                pq.write_table(table, temp_path, compression="zstd")
                metrics.parquet_write_ms += (
                    time.perf_counter() - write_started
                ) * 1000.0

            phase = "os_replace"
            replace_started = time.perf_counter()
            for dataset, partition_date, row_count, temp_path, final_path in staged:
                phase_dataset = dataset
                phase_rows = row_count
                os.replace(temp_path, final_path)
                replaced.append(final_path)
            replace_ms = (time.perf_counter() - replace_started) * 1000.0
        except Exception as error:
            self._log_phase_failure(
                flush_id=resolved_flush_id,
                phase=phase,
                dataset=phase_dataset,
                rows=phase_rows,
                worker_started=worker_started,
                error=error,
            )
            for _, _, _, temp_path, _ in staged:
                temp_path.unlink(missing_ok=True)
            for final_path in replaced:
                final_path.unlink(missing_ok=True)
            with self._pending_lock:
                restored = {
                    key: list(rows)
                    for key, rows in pending.items()
                }
                for key, rows in self._pending.items():
                    restored.setdefault(key, []).extend(rows)
                self._pending = restored
            raise

        prune_started = time.perf_counter()
        try:
            self.prune(now=current_time)
        except Exception as error:
            self._log_phase_failure(
                flush_id=resolved_flush_id,
                phase="prune",
                dataset=None,
                rows=rows_total,
                worker_started=worker_started,
                error=error,
            )
            raise
        prune_ms = (time.perf_counter() - prune_started) * 1000.0
        self._log_telemetry(
            flush_id=resolved_flush_id,
            worker_started=worker_started,
            lock_wait_ms=lock_wait_ms,
            swap_hold_ms=swap_hold_ms,
            rows_total=rows_total,
            dataset_telemetry=dataset_telemetry,
            files_written=len(staged),
            replace_ms=replace_ms,
            prune_ms=prune_ms,
            pending_empty=False,
        )
        return len(staged)

    def _log_telemetry(
        self,
        *,
        flush_id: str,
        worker_started: float,
        lock_wait_ms: float,
        swap_hold_ms: float,
        rows_total: int,
        dataset_telemetry: dict[str, _DatasetFlushTelemetry],
        files_written: int,
        replace_ms: float,
        prune_ms: float,
        pending_empty: bool,
    ) -> None:
        worker_total_ms = (time.perf_counter() - worker_started) * 1000.0
        dataset_metrics = json.dumps(
            {
                dataset: metrics.as_dict()
                for dataset, metrics in dataset_telemetry.items()
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        LOGGER.info(
            "parquet flush telemetry flush_id=%s pending_empty=%s "
            "worker_total_ms=%.3f pending_lock_wait_ms=%.3f "
            "pending_swap_hold_ms=%.3f rows_total=%d files_written=%d "
            "dataset_metrics=%s replace_ms=%.3f prune_ms=%.3f",
            flush_id,
            str(pending_empty).lower(),
            worker_total_ms,
            lock_wait_ms,
            swap_hold_ms,
            rows_total,
            files_written,
            dataset_metrics,
            replace_ms,
            prune_ms,
        )

    def _log_phase_failure(
        self,
        *,
        flush_id: str,
        phase: str,
        dataset: str | None,
        rows: int,
        worker_started: float,
        error: BaseException | None,
    ) -> None:
        LOGGER.error(
            "parquet flush phase failure flush_id=%s phase=%s dataset=%s "
            "rows=%d elapsed_worker_ms=%.3f error_type=%s",
            flush_id,
            phase,
            "-" if dataset is None else dataset,
            rows,
            (time.perf_counter() - worker_started) * 1000.0,
            type(error).__name__ if error is not None else "UnknownError",
        )

    def prune(self, *, now: datetime | None = None) -> int:
        """Delete complete date partitions older than the retention cutoff."""
        current_time = _as_utc(
            datetime.now(UTC) if now is None else now,
            "now",
        )
        cutoff_date = (current_time - timedelta(days=self.retention_days)).date()
        removed = 0
        for dataset in DATASET_NAMES:
            dataset_root = self.root / dataset
            if not dataset_root.is_dir():
                continue
            for partition in sorted(dataset_root.glob("date=*")):
                if not partition.is_dir() or partition.is_symlink():
                    continue
                try:
                    partition_date = date.fromisoformat(
                        partition.name.removeprefix("date=")
                    )
                except ValueError:
                    continue
                if partition_date >= cutoff_date:
                    continue
                for child in partition.iterdir():
                    if child.is_file() or child.is_symlink():
                        child.unlink()
                try:
                    partition.rmdir()
                except OSError:
                    continue
                removed += 1
        return removed
