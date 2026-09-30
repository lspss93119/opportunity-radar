from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from radar.analysis_export import REMOTE_NAME, ExportResult, export_market_date


EXPECTED_SAMPLE_SLOTS = 8640
SAMPLE_INTERVAL_SECONDS = 10
SAMPLE_SCAN_BATCH_SIZE = 8192
Outcome = Literal["work_completed", "no_work", "incomplete_only", "error"]
Exporter = Callable[..., ExportResult]


@dataclass(frozen=True)
class DateInspection:
    target_date: date
    complete: bool
    slot_count: int
    reason: str | None
    fragment_count: int
    row_count: int
    source_bytes: int


@dataclass(frozen=True)
class DailyRunResult:
    current_date: date
    lookback_days: int
    inspections: tuple[DateInspection, ...]
    outcome: Outcome
    selected_date: date | None = None
    export_result: ExportResult | None = None
    error: str | None = None

    def summary(self) -> str:
        lines = [
            f"current_utc_date={self.current_date.isoformat()}",
            f"lookback_days={self.lookback_days}",
        ]
        for inspection in self.inspections:
            reason = inspection.reason or "none"
            lines.append(
                f"date={inspection.target_date.isoformat()} "
                f"complete={str(inspection.complete).lower()} "
                f"slots={inspection.slot_count} "
                f"fragments={inspection.fragment_count} "
                f"rows={inspection.row_count} "
                f"reason={reason}"
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
    complete: bool,
    slot_count: int,
    reason: str | None,
    fragment_count: int = 0,
    row_count: int = 0,
    source_bytes: int = 0,
) -> DateInspection:
    return DateInspection(
        target_date=target_date,
        complete=complete,
        slot_count=slot_count,
        reason=reason,
        fragment_count=fragment_count,
        row_count=row_count,
        source_bytes=source_bytes,
    )


def _complete_day_reason(target_date: date, slots: set[datetime]) -> str | None:
    expected_start = datetime.combine(target_date, datetime.min.time(), tzinfo=UTC)
    expected_end = expected_start + timedelta(
        seconds=(EXPECTED_SAMPLE_SLOTS - 1) * SAMPLE_INTERVAL_SECONDS
    )
    reasons: list[str] = []
    if expected_start not in slots:
        reasons.append("missing_start")
    if expected_end not in slots:
        reasons.append("missing_end")
    if len(slots) != EXPECTED_SAMPLE_SLOTS:
        reasons.append(f"slot_count={len(slots)}")
    return ",".join(reasons) or None


def inspect_market_date(data_root: Path, target_date: date) -> DateInspection:
    partition = _partition(data_root, target_date)
    if not partition.is_dir():
        return _inspection(
            target_date,
            complete=False,
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
                complete=False,
                slot_count=0,
                reason="no_parquet_fragments",
            )

        parquets = [pq.ParquetFile(path) for path in files]
        first_schema = parquets[0].schema_arrow
        row_count = sum(parquet.metadata.num_rows for parquet in parquets)
        source_bytes = sum(path.stat().st_size for path in files)
        for parquet in parquets[1:]:
            if not parquet.schema_arrow.equals(first_schema, check_metadata=True):
                return _inspection(
                    target_date,
                    complete=False,
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
                complete=False,
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
                complete=False,
                slot_count=0,
                reason="sample_time_not_utc_timestamp",
                fragment_count=len(files),
                row_count=row_count,
                source_bytes=source_bytes,
            )

        slots: set[datetime] = set()
        for parquet in parquets:
            for batch in parquet.iter_batches(
                columns=["sample_time"],
                batch_size=SAMPLE_SCAN_BATCH_SIZE,
            ):
                for value in batch.column(0).to_pylist():
                    if not isinstance(value, datetime):
                        return _inspection(
                            target_date,
                            complete=False,
                            slot_count=len(slots),
                            reason="sample_time_not_datetime",
                            fragment_count=len(files),
                            row_count=row_count,
                            source_bytes=source_bytes,
                        )
                    if value.tzinfo is None or value.utcoffset() is None:
                        return _inspection(
                            target_date,
                            complete=False,
                            slot_count=len(slots),
                            reason="sample_time_not_timezone_aware",
                            fragment_count=len(files),
                            row_count=row_count,
                            source_bytes=source_bytes,
                        )
                    sample_time = value.astimezone(UTC)
                    if sample_time.date() != target_date:
                        return _inspection(
                            target_date,
                            complete=False,
                            slot_count=len(slots),
                            reason="sample_time_wrong_date",
                            fragment_count=len(files),
                            row_count=row_count,
                            source_bytes=source_bytes,
                        )
                    if (
                        sample_time.microsecond != 0
                        or sample_time.second % SAMPLE_INTERVAL_SECONDS != 0
                    ):
                        return _inspection(
                            target_date,
                            complete=False,
                            slot_count=len(slots),
                            reason="sample_time_unaligned",
                            fragment_count=len(files),
                            row_count=row_count,
                            source_bytes=source_bytes,
                        )
                    slots.add(sample_time)

        reason = _complete_day_reason(target_date, slots)
        return _inspection(
            target_date,
            complete=reason is None,
            slot_count=len(slots),
            reason=reason,
            fragment_count=len(files),
            row_count=row_count,
            source_bytes=source_bytes,
        )
    except Exception as error:
        return _inspection(
            target_date,
            complete=False,
            slot_count=0,
            reason=f"unreadable_parquet:{type(error).__name__}",
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
    now: datetime | None = None,
    exporter: Exporter = export_market_date,
) -> DailyRunResult:
    if remote != REMOTE_NAME:
        raise ValueError(f"remote must be exactly {REMOTE_NAME}")
    if lookback_days < 1:
        raise ValueError("lookback_days must be at least 1")

    current_date = _validate_now(now).date()
    candidate_dates = tuple(
        current_date - timedelta(days=offset)
        for offset in range(lookback_days, 0, -1)
    )
    inspections: list[DateInspection] = []
    for target_date in candidate_dates:
        inspection = inspect_market_date(data_root, target_date)
        inspections.append(inspection)
        if not inspection.complete:
            continue

        try:
            result = exporter(
                data_root=data_root,
                output_root=output_root,
                target_date=target_date,
                remote=remote,
                rclone=rclone,
            )
        except Exception as error:
            return DailyRunResult(
                current_date=current_date,
                lookback_days=lookback_days,
                inspections=tuple(inspections),
                outcome="error",
                selected_date=target_date,
                error=str(error),
            )

        if result.converted or result.uploaded:
            return DailyRunResult(
                current_date=current_date,
                lookback_days=lookback_days,
                inspections=tuple(inspections),
                outcome="work_completed",
                selected_date=target_date,
                export_result=result,
            )

    outcome: Outcome = "no_work" if any(
        inspection.complete for inspection in inspections
    ) else "incomplete_only"
    return DailyRunResult(
        current_date=current_date,
        lookback_days=lookback_days,
        inspections=tuple(inspections),
        outcome=outcome,
    )


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("lookback-days must be an integer") from error
    if parsed < 1:
        raise argparse.ArgumentTypeError("lookback-days must be at least 1")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one daily market analysis export")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--rclone", type=Path, required=True)
    parser.add_argument("--lookback-days", type=_positive_int, default=7)
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
        )
    except (OSError, ValueError) as error:
        print(f"outcome=error error={error}")
        return 2

    print(result.summary())
    return 0 if result.outcome != "error" else 1


if __name__ == "__main__":
    raise SystemExit(main())
