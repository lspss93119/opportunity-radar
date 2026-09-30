from __future__ import annotations

import argparse
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from radar.analysis_export import (
    DATASET_NAMES,
    DATASET_TIMESTAMP_FIELDS,
    REMOTE_NAME,
    DatasetName,
    ExportResult,
    export_dataset_date,
)


EXPECTED_SAMPLE_SLOTS = 8640
SAMPLE_INTERVAL_SECONDS = 10
SAMPLE_SCAN_BATCH_SIZE = 8192
DEFAULT_MIN_COVERAGE_PCT = 99.8
Outcome = Literal["work_completed", "no_work", "incomplete_only", "error"]
Quality = Literal["complete", "partial", "rejected"]
Exporter = Callable[..., ExportResult]


@dataclass(frozen=True)
class DateInspection:
    target_date: date
    quality: Quality
    eligible: bool
    slot_count: int
    expected_slots: int
    missing_slots: tuple[datetime, ...]
    coverage_pct: float
    first_sample: datetime | None
    last_sample: datetime | None
    reason: str | None
    fragment_count: int
    row_count: int
    source_bytes: int

    @property
    def complete(self) -> bool:
        return self.quality == "complete"


@dataclass(frozen=True)
class DatasetInspection:
    dataset: DatasetName
    target_date: date
    valid: bool
    timestamp_field: str
    fragment_count: int
    row_count: int
    source_bytes: int
    first_timestamp: datetime | None
    last_timestamp: datetime | None
    reason: str | None


@dataclass(frozen=True)
class DatasetRun:
    dataset: DatasetName
    target_date: date
    source_rows: int
    result: ExportResult


@dataclass(frozen=True)
class DailyRunResult:
    current_date: date
    lookback_days: int
    min_coverage_pct: float
    inspections: tuple[DateInspection, ...]
    outcome: Outcome
    selected_date: date | None = None
    export_result: ExportResult | None = None
    error: str | None = None
    dataset_inspections: tuple[DatasetInspection, ...] = ()
    dataset_runs: tuple[DatasetRun, ...] = ()

    def summary(self) -> str:
        lines = [
            f"current_utc_date={self.current_date.isoformat()}",
            f"lookback_days={self.lookback_days}",
            f"min_coverage_pct={self.min_coverage_pct:g}",
        ]
        for inspection in self.inspections:
            reason = inspection.reason or "none"
            lines.append(
                f"date={inspection.target_date.isoformat()} "
                f"quality={inspection.quality} "
                f"eligible={str(inspection.eligible).lower()} "
                f"slots={inspection.slot_count} "
                f"expected_slots={inspection.expected_slots} "
                f"missing_slots={len(inspection.missing_slots)} "
                f"coverage_pct={inspection.coverage_pct:.6f} "
                f"fragments={inspection.fragment_count} "
                f"rows={inspection.row_count} "
                f"first_sample={_format_sample(inspection.first_sample)} "
                f"last_sample={_format_sample(inspection.last_sample)} "
                f"reason={reason}"
            )
            if inspection.quality == "partial":
                missing = ",".join(
                    _format_sample(sample) for sample in inspection.missing_slots
                )
                lines.append(f"missing_slot_timestamps={missing}")

        for dataset_inspection in self.dataset_inspections:
            lines.append(
                f"dataset={dataset_inspection.dataset} "
                f"date={dataset_inspection.target_date.isoformat()} "
                f"valid={str(dataset_inspection.valid).lower()} "
                f"source_rows={dataset_inspection.row_count} "
                f"fragments={dataset_inspection.fragment_count} "
                f"first_timestamp={_format_sample(dataset_inspection.first_timestamp)} "
                f"last_timestamp={_format_sample(dataset_inspection.last_timestamp)} "
                f"reason={dataset_inspection.reason or 'none'}"
            )

        for dataset_run in self.dataset_runs:
            result = dataset_run.result
            lines.append(
                f"dataset={dataset_run.dataset} "
                f"date={dataset_run.target_date.isoformat()} "
                f"source_rows={dataset_run.source_rows} "
                f"local={result.local_path} "
                f"converted={str(result.converted).lower()} "
                f"uploaded={str(result.uploaded).lower()} "
                f"remote_verified={str(result.remote_verified).lower()}"
            )

        selected = (
            self.selected_date.isoformat() if self.selected_date is not None else "none"
        )
        lines.append(f"selected_date={selected}")
        if self.export_result is None:
            lines.extend(
                [
                    "converted=none",
                    "uploaded=none",
                    "remote_verified=none",
                ]
            )
        else:
            lines.extend(
                [
                    f"converted={str(self.export_result.converted).lower()}",
                    f"uploaded={str(self.export_result.uploaded).lower()}",
                    f"remote_verified={str(self.export_result.remote_verified).lower()}",
                ]
            )
        if self.error is not None:
            lines.append(f"error={self.error}")
        lines.append(f"outcome={self.outcome}")
        return "\n".join(lines)


def _partition(data_root: Path, target_date: date) -> Path:
    return data_root / "market" / f"date={target_date.isoformat()}"


def _inspection(
    target_date: date,
    *,
    quality: Quality,
    eligible: bool,
    slot_count: int,
    slots: set[datetime] | None = None,
    first_sample: datetime | None = None,
    last_sample: datetime | None = None,
    reason: str | None,
    fragment_count: int = 0,
    row_count: int = 0,
    source_bytes: int = 0,
) -> DateInspection:
    actual_slots = slots or set()
    expected_start = datetime.combine(target_date, datetime.min.time(), tzinfo=UTC)
    expected = {
        expected_start + timedelta(seconds=SAMPLE_INTERVAL_SECONDS * index)
        for index in range(EXPECTED_SAMPLE_SLOTS)
    }
    return DateInspection(
        target_date=target_date,
        quality=quality,
        eligible=eligible,
        slot_count=slot_count,
        expected_slots=EXPECTED_SAMPLE_SLOTS,
        missing_slots=tuple(sorted(expected - actual_slots)),
        coverage_pct=slot_count / EXPECTED_SAMPLE_SLOTS * 100,
        first_sample=first_sample,
        last_sample=last_sample,
        reason=reason,
        fragment_count=fragment_count,
        row_count=row_count,
        source_bytes=source_bytes,
    )


def _format_sample(sample: datetime | None) -> str:
    if sample is None:
        return "none"
    return sample.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _validate_min_coverage_pct(min_coverage_pct: float) -> None:
    if not 0 < min_coverage_pct <= 100:
        raise ValueError("min_coverage_pct must be greater than 0 and at most 100")


def minimum_required_slots(min_coverage_pct: float) -> int:
    _validate_min_coverage_pct(min_coverage_pct)
    return math.ceil(EXPECTED_SAMPLE_SLOTS * min_coverage_pct / 100)


def _day_quality(
    target_date: date,
    slots: set[datetime],
    *,
    minimum_slots: int,
    structural_reason: str | None,
) -> tuple[Quality, bool, str | None]:
    expected_start = datetime.combine(target_date, datetime.min.time(), tzinfo=UTC)
    expected_end = expected_start + timedelta(
        seconds=(EXPECTED_SAMPLE_SLOTS - 1) * SAMPLE_INTERVAL_SECONDS
    )
    reasons: list[str] = []
    if structural_reason is not None:
        reasons.append(structural_reason)
    if expected_start not in slots:
        reasons.append("missing_start")
    if expected_end not in slots:
        reasons.append("missing_end")
    if len(slots) < minimum_slots:
        reasons.append("coverage_below_threshold")
    if reasons:
        return "rejected", False, ",".join(dict.fromkeys(reasons))
    if len(slots) == EXPECTED_SAMPLE_SLOTS:
        return "complete", True, None
    return "partial", True, "partial_coverage"


def inspect_market_date(
    data_root: Path,
    target_date: date,
    *,
    min_coverage_pct: float = DEFAULT_MIN_COVERAGE_PCT,
) -> DateInspection:
    minimum_slots = minimum_required_slots(min_coverage_pct)
    return _inspect_market_date(
        data_root,
        target_date,
        minimum_slots=minimum_slots,
    )


def _inspect_market_date(
    data_root: Path,
    target_date: date,
    *,
    minimum_slots: int,
) -> DateInspection:
    partition = _partition(data_root, target_date)
    if not partition.is_dir():
        return _inspection(
            target_date,
            quality="rejected",
            eligible=False,
            slot_count=0,
            reason="partition_missing",
        )

    try:
        files = tuple(
            path for path in sorted(partition.glob("part-*.parquet")) if path.is_file()
        )
        if not files:
            return _inspection(
                target_date,
                quality="rejected",
                eligible=False,
                slot_count=0,
                reason="no_parquet_fragments",
            )

        first_parquet = pq.ParquetFile(files[0])
        try:
            first_schema = first_parquet.schema_arrow
            row_count = first_parquet.metadata.num_rows
        finally:
            close = getattr(first_parquet, "close", None)
            if callable(close):
                close()
        source_bytes = sum(path.stat().st_size for path in files)
        schema_mismatch = False
        for path in files[1:]:
            parquet = pq.ParquetFile(path)
            try:
                row_count += parquet.metadata.num_rows
                schema_mismatch = schema_mismatch or not parquet.schema_arrow.equals(
                    first_schema,
                    check_metadata=True,
                )
            finally:
                close = getattr(parquet, "close", None)
                if callable(close):
                    close()
        if schema_mismatch:
            return _inspection(
                target_date,
                quality="rejected",
                eligible=False,
                slot_count=0,
                reason="schema_mismatch",
                fragment_count=len(files),
                row_count=row_count,
                source_bytes=source_bytes,
            )

        field_index = first_schema.get_field_index("sample_time")
        if field_index < 0:
            return _inspection(
                target_date,
                quality="rejected",
                eligible=False,
                slot_count=0,
                reason="sample_time_missing",
                fragment_count=len(files),
                row_count=row_count,
                source_bytes=source_bytes,
            )
        sample_field = first_schema.field(field_index)
        if not pa.types.is_timestamp(sample_field.type) or sample_field.type.tz != "UTC":
            return _inspection(
                target_date,
                quality="rejected",
                eligible=False,
                slot_count=0,
                reason="sample_time_not_utc_timestamp",
                fragment_count=len(files),
                row_count=row_count,
                source_bytes=source_bytes,
            )

        slots: set[datetime] = set()
        first_sample: datetime | None = None
        last_sample: datetime | None = None
        structural_reasons: list[str] = []
        for path in files:
            parquet = pq.ParquetFile(path)
            try:
                for batch in parquet.iter_batches(
                    columns=["sample_time"],
                    batch_size=SAMPLE_SCAN_BATCH_SIZE,
                ):
                    for value in batch.column(0).to_pylist():
                        if not isinstance(value, datetime):
                            if "sample_time_not_datetime" not in structural_reasons:
                                structural_reasons.append("sample_time_not_datetime")
                            continue
                        if value.tzinfo is None or value.utcoffset() is None:
                            if (
                                "sample_time_not_timezone_aware"
                                not in structural_reasons
                            ):
                                structural_reasons.append(
                                    "sample_time_not_timezone_aware"
                                )
                            continue
                        sample_time = value.astimezone(UTC)
                        if first_sample is None or sample_time < first_sample:
                            first_sample = sample_time
                        if last_sample is None or sample_time > last_sample:
                            last_sample = sample_time
                        if sample_time.date() != target_date:
                            if "sample_time_wrong_date" not in structural_reasons:
                                structural_reasons.append("sample_time_wrong_date")
                            continue
                        if (
                            sample_time.microsecond != 0
                            or sample_time.second % SAMPLE_INTERVAL_SECONDS != 0
                        ):
                            if "sample_time_unaligned" not in structural_reasons:
                                structural_reasons.append("sample_time_unaligned")
                            continue
                        slots.add(sample_time)
            finally:
                close = getattr(parquet, "close", None)
                if callable(close):
                    close()

        quality, eligible, reason = _day_quality(
            target_date,
            slots,
            minimum_slots=minimum_slots,
            structural_reason=",".join(structural_reasons) or None,
        )
        return _inspection(
            target_date,
            quality=quality,
            eligible=eligible,
            slot_count=len(slots),
            slots=slots,
            reason=reason,
            first_sample=first_sample,
            last_sample=last_sample,
            fragment_count=len(files),
            row_count=row_count,
            source_bytes=source_bytes,
        )
    except Exception as error:
        return _inspection(
            target_date,
            quality="rejected",
            eligible=False,
            slot_count=0,
            reason=f"unreadable_parquet:{type(error).__name__}",
        )


def inspect_dataset_date(
    data_root: Path,
    dataset: DatasetName,
    target_date: date,
) -> DatasetInspection:
    partition = data_root / dataset / f"date={target_date.isoformat()}"
    timestamp_field = DATASET_TIMESTAMP_FIELDS[dataset]
    if not partition.is_dir():
        return DatasetInspection(
            dataset=dataset,
            target_date=target_date,
            valid=False,
            timestamp_field=timestamp_field,
            fragment_count=0,
            row_count=0,
            source_bytes=0,
            first_timestamp=None,
            last_timestamp=None,
            reason="partition_missing",
        )

    try:
        files = tuple(
            path for path in sorted(partition.glob("part-*.parquet")) if path.is_file()
        )
        if not files:
            return DatasetInspection(
                dataset=dataset,
                target_date=target_date,
                valid=False,
                timestamp_field=timestamp_field,
                fragment_count=0,
                row_count=0,
                source_bytes=0,
                first_timestamp=None,
                last_timestamp=None,
                reason="no_parquet_fragments",
            )

        first_schema = None
        row_count = 0
        source_bytes = 0
        schema_mismatch = False
        missing_timestamp = False
        invalid_timestamp_schema = False
        first_timestamp: datetime | None = None
        last_timestamp: datetime | None = None
        structural_reasons: list[str] = []

        for path in files:
            source_bytes += path.stat().st_size
            parquet = pq.ParquetFile(path)
            try:
                schema = parquet.schema_arrow
                if first_schema is None:
                    first_schema = schema
                elif not schema.equals(first_schema, check_metadata=True):
                    schema_mismatch = True
                row_count += parquet.metadata.num_rows
                field_index = schema.get_field_index(timestamp_field)
                if field_index < 0:
                    missing_timestamp = True
                    continue
                field = schema.field(field_index)
                if not pa.types.is_timestamp(field.type) or field.type.tz != "UTC":
                    invalid_timestamp_schema = True
                    continue
                for batch in parquet.iter_batches(
                    columns=[timestamp_field],
                    batch_size=SAMPLE_SCAN_BATCH_SIZE,
                ):
                    for value in batch.column(0).to_pylist():
                        if value is None:
                            if "timestamp_null" not in structural_reasons:
                                structural_reasons.append("timestamp_null")
                            continue
                        if not isinstance(value, datetime):
                            if "timestamp_not_datetime" not in structural_reasons:
                                structural_reasons.append("timestamp_not_datetime")
                            continue
                        if value.tzinfo is None or value.utcoffset() is None:
                            if "timestamp_not_timezone_aware" not in structural_reasons:
                                structural_reasons.append("timestamp_not_timezone_aware")
                            continue
                        timestamp = value.astimezone(UTC)
                        if first_timestamp is None or timestamp < first_timestamp:
                            first_timestamp = timestamp
                        if last_timestamp is None or timestamp > last_timestamp:
                            last_timestamp = timestamp
                        if timestamp.date() != target_date:
                            if "timestamp_wrong_date" not in structural_reasons:
                                structural_reasons.append("timestamp_wrong_date")
            finally:
                close = getattr(parquet, "close", None)
                if callable(close):
                    close()

        if row_count == 0:
            reason = "empty_partition"
        elif schema_mismatch:
            reason = "schema_mismatch"
        elif missing_timestamp:
            reason = f"{timestamp_field}_missing"
        elif invalid_timestamp_schema:
            reason = f"{timestamp_field}_not_utc_timestamp"
        elif structural_reasons:
            reason = ",".join(structural_reasons)
        else:
            reason = None
        return DatasetInspection(
            dataset=dataset,
            target_date=target_date,
            valid=reason is None,
            timestamp_field=timestamp_field,
            fragment_count=len(files),
            row_count=row_count,
            source_bytes=source_bytes,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            reason=reason,
        )
    except Exception as error:
        return DatasetInspection(
            dataset=dataset,
            target_date=target_date,
            valid=False,
            timestamp_field=timestamp_field,
            fragment_count=0,
            row_count=0,
            source_bytes=0,
            first_timestamp=None,
            last_timestamp=None,
            reason=f"unreadable_parquet:{type(error).__name__}",
        )


def _market_dataset_inspection(inspection: DateInspection) -> DatasetInspection:
    return DatasetInspection(
        dataset="market",
        target_date=inspection.target_date,
        valid=inspection.eligible,
        timestamp_field="sample_time",
        fragment_count=inspection.fragment_count,
        row_count=inspection.row_count,
        source_bytes=inspection.source_bytes,
        first_timestamp=inspection.first_sample,
        last_timestamp=inspection.last_sample,
        reason=None if inspection.eligible else inspection.reason,
    )


def _validate_now(now: datetime | None) -> datetime:
    current = datetime.now(UTC) if now is None else now
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return current.astimezone(UTC)


def run_daily(
    *,
    data_root: Path,
    output_root: Path,
    remote: str,
    rclone: Path,
    lookback_days: int = 7,
    min_coverage_pct: float = DEFAULT_MIN_COVERAGE_PCT,
    now: datetime | None = None,
    exporter: Exporter = export_dataset_date,
) -> DailyRunResult:
    if remote != REMOTE_NAME:
        raise ValueError(f"remote must be exactly {REMOTE_NAME}")
    if lookback_days < 1:
        raise ValueError("lookback_days must be at least 1")
    minimum_slots = minimum_required_slots(min_coverage_pct)

    current_date = _validate_now(now).date()
    candidate_dates = tuple(
        current_date - timedelta(days=offset)
        for offset in range(lookback_days, 0, -1)
    )
    inspections: list[DateInspection] = []
    dataset_inspections: list[DatasetInspection] = []
    dataset_runs: list[DatasetRun] = []
    for target_date in candidate_dates:
        inspection = _inspect_market_date(
            data_root,
            target_date,
            minimum_slots=minimum_slots,
        )
        inspections.append(inspection)
        if not inspection.eligible:
            continue

        bundle_inspections = (
            _market_dataset_inspection(inspection),
            *(
                inspect_dataset_date(data_root, dataset, target_date)
                for dataset in DATASET_NAMES
                if dataset != "market"
            ),
        )
        dataset_inspections.extend(bundle_inspections)
        invalid_dataset = next(
            (dataset_inspection for dataset_inspection in bundle_inspections
             if not dataset_inspection.valid),
            None,
        )
        if invalid_dataset is not None:
            return DailyRunResult(
                current_date=current_date,
                lookback_days=lookback_days,
                min_coverage_pct=min_coverage_pct,
                inspections=tuple(inspections),
                outcome="error",
                selected_date=target_date,
                error=(
                    f"{invalid_dataset.dataset} dataset invalid: "
                    f"{invalid_dataset.reason or 'unknown error'}"
                ),
                dataset_inspections=tuple(dataset_inspections),
                dataset_runs=tuple(dataset_runs),
            )

        selected_bundle_runs: list[DatasetRun] = []
        actual_work = False
        for dataset in DATASET_NAMES:
            try:
                result = exporter(
                    data_root=data_root,
                    output_root=output_root,
                    target_date=target_date,
                    dataset=dataset,
                    remote=remote,
                    rclone=rclone,
                )
            except Exception as error:
                return DailyRunResult(
                    current_date=current_date,
                    lookback_days=lookback_days,
                    min_coverage_pct=min_coverage_pct,
                    inspections=tuple(inspections),
                    outcome="error",
                    selected_date=target_date,
                    error=f"{dataset} export failed: {error}",
                    dataset_inspections=tuple(dataset_inspections),
                    dataset_runs=tuple(dataset_runs),
                )
            run = DatasetRun(
                dataset=dataset,
                target_date=target_date,
                source_rows=result.row_count,
                result=result,
            )
            selected_bundle_runs.append(run)
            dataset_runs.append(run)
            actual_work = actual_work or result.converted or result.uploaded

        if actual_work:
            return DailyRunResult(
                current_date=current_date,
                lookback_days=lookback_days,
                min_coverage_pct=min_coverage_pct,
                inspections=tuple(inspections),
                outcome="work_completed",
                selected_date=target_date,
                export_result=selected_bundle_runs[0].result,
                dataset_inspections=tuple(dataset_inspections),
                dataset_runs=tuple(dataset_runs),
            )

    outcome: Outcome = "no_work" if any(
        inspection.eligible for inspection in inspections
    ) else "incomplete_only"
    return DailyRunResult(
        current_date=current_date,
        lookback_days=lookback_days,
        min_coverage_pct=min_coverage_pct,
        inspections=tuple(inspections),
        outcome=outcome,
        dataset_inspections=tuple(dataset_inspections),
        dataset_runs=tuple(dataset_runs),
    )


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("lookback-days must be an integer") from error
    if parsed < 1:
        raise argparse.ArgumentTypeError("lookback-days must be at least 1")
    return parsed


def _coverage_pct(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "min-coverage-pct must be a number"
        ) from error
    try:
        _validate_min_coverage_pct(parsed)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one daily market analysis export")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--rclone", type=Path, required=True)
    parser.add_argument("--lookback-days", type=_positive_int, default=7)
    parser.add_argument(
        "--min-coverage-pct",
        type=_coverage_pct,
        default=DEFAULT_MIN_COVERAGE_PCT,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_daily(
            data_root=args.data_root,
            output_root=args.output_root,
            remote=args.remote,
            rclone=args.rclone,
            lookback_days=args.lookback_days,
            min_coverage_pct=args.min_coverage_pct,
        )
    except (OSError, ValueError) as error:
        print(f"outcome=error error={error}")
        return 2

    print(result.summary())
    return 0 if result.outcome != "error" else 1


if __name__ == "__main__":
    raise SystemExit(main())
