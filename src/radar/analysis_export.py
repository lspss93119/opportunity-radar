from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import math
import os
import subprocess
import sys
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

REMOTE_NAME = "opportunity-drive-own"
REMOTE_ROOT = "OpportunityRadar/analysis_csv/market"
CSV_BATCH_SIZE = 8192
RCLONE_TIMEOUT_SECONDS = 120.0
RCLONE_CONTIMEOUT_SECONDS = 30.0
RCLONE_RETRIES = 1
RCLONE_LOW_LEVEL_RETRIES = 1


class ExportError(RuntimeError):
    """Raised when a local export or remote verification cannot complete."""


class RemoteMismatchError(ExportError):
    """Raised when the formal remote file is present but differs locally."""


@dataclass(frozen=True)
class ExportResult:
    local_path: Path
    remote_path: str
    row_count: int
    converted: bool
    uploaded: bool
    remote_verified: bool


@dataclass(frozen=True)
class _RemoteFile:
    size: int
    md5: str


def _validate_remote(remote: str) -> None:
    if remote != REMOTE_NAME:
        raise ValueError(f"remote must be exactly {REMOTE_NAME}")


def _source_partition(data_root: Path, target_date: date) -> Path:
    return data_root / "market" / f"date={target_date.isoformat()}"


def _source_files(data_root: Path, target_date: date) -> tuple[Path, ...]:
    partition = _source_partition(data_root, target_date)
    files = tuple(path for path in sorted(partition.glob("part-*.parquet")) if path.is_file())
    if not files:
        raise ExportError(f"no market Parquet files found in {partition}")
    return files


def _source_schema_and_rows(files: Sequence[Path]) -> tuple[pa.Schema, int]:
    first = pq.ParquetFile(files[0])
    schema = first.schema_arrow
    row_count = first.metadata.num_rows
    for path in files[1:]:
        parquet = pq.ParquetFile(path)
        if not parquet.schema_arrow.equals(schema, check_metadata=True):
            raise ExportError(f"source Parquet schema mismatch: {path.name}")
        row_count += parquet.metadata.num_rows
    return schema, row_count


def _csv_value(field: pa.Field, value: object) -> str:
    if value is None:
        return ""
    if pa.types.is_timestamp(field.type):
        if not isinstance(value, datetime):
            raise ExportError(f"timestamp field {field.name} is not datetime")
        if value.tzinfo is None or value.utcoffset() is None:
            raise ExportError(f"timestamp field {field.name} is not timezone-aware")
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        )
    if pa.types.is_floating(field.type):
        if not isinstance(value, (int, float)):
            raise ExportError(f"floating field {field.name} is not numeric")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ExportError(f"non-finite float in {field.name}")
        return repr(numeric)
    return str(value)


def _validate_csv(path: Path, schema: pa.Schema, expected_rows: int) -> int:
    fields = list(schema)
    try:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as compressed:
            reader = csv.reader(compressed)
            header = next(reader, None)
            if header != schema.names:
                raise ExportError(f"CSV header mismatch in {path.name}")

            row_count = 0
            for row_number, row in enumerate(reader, start=2):
                if len(row) != len(fields):
                    raise ExportError(
                        f"CSV field count mismatch at row {row_number} in {path.name}"
                    )
                for field, text in zip(fields, row, strict=True):
                    if not text:
                        if not field.nullable:
                            raise ExportError(
                                f"non-null field {field.name} is empty at row {row_number}"
                            )
                        continue
                    if pa.types.is_timestamp(field.type):
                        if not text.endswith("Z"):
                            raise ExportError(
                                f"timestamp field {field.name} is not UTC ISO-8601"
                            )
                        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
                        if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(None):
                            raise ExportError(
                                f"timestamp field {field.name} is not UTC"
                            )
                    elif pa.types.is_floating(field.type):
                        if not math.isfinite(float(text)):
                            raise ExportError(
                                f"non-finite float in {field.name} at row {row_number}"
                            )
                    elif pa.types.is_integer(field.type):
                        int(text)
                row_count += 1
    except ExportError:
        raise
    except (OSError, EOFError, csv.Error, TypeError, ValueError) as error:
        raise ExportError(f"invalid CSV.GZ artifact {path.name}") from error

    if row_count != expected_rows:
        raise ExportError(
            f"CSV row count mismatch: expected {expected_rows}, got {row_count}"
        )
    return row_count


def _convert_partition(
    files: Sequence[Path],
    destination: Path,
    schema: pa.Schema,
) -> int:
    fields = list(schema)
    row_count = 0
    with gzip.open(destination, "wt", encoding="utf-8", newline="") as compressed:
        writer = csv.writer(compressed, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(schema.names)
        for path in files:
            parquet = pq.ParquetFile(path)
            if not parquet.schema_arrow.equals(schema, check_metadata=True):
                raise ExportError(f"source Parquet schema mismatch: {path.name}")
            for batch in parquet.iter_batches(batch_size=CSV_BATCH_SIZE):
                for row in batch.to_pylist():
                    writer.writerow(
                        [_csv_value(field, row[field.name]) for field in fields]
                    )
                    row_count += 1
    return row_count


def _publish_local(
    files: Sequence[Path],
    schema: pa.Schema,
    expected_rows: int,
    output: Path,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{uuid.uuid4().hex}.tmp")
    try:
        converted_rows = _convert_partition(files, temporary, schema)
        if converted_rows != expected_rows:
            raise ExportError(
                f"conversion row count mismatch: expected {expected_rows}, "
                f"got {converted_rows}"
            )
        _validate_csv(temporary, schema, expected_rows)
        os.replace(temporary, output)
    except ExportError:
        temporary.unlink(missing_ok=True)
        raise
    except (OSError, ValueError, TypeError) as error:
        temporary.unlink(missing_ok=True)
        raise ExportError("local CSV.GZ conversion failed") from error


def _md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rate_limited(stderr: str) -> bool:
    lowered = stderr.lower()
    return "ratelimitexceeded" in lowered or "rate_limit_exceeded" in lowered


def _run_rclone(
    argv: list[str],
    *,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(
            argv,
            shell=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except FileNotFoundError as error:
        raise ExportError("rclone executable was not found") from error
    except subprocess.TimeoutExpired as error:
        operation = argv[1] if len(argv) > 1 else "command"
        raise ExportError(f"rclone {operation} timed out") from error

    if completed.returncode != 0:
        operation = argv[1] if len(argv) > 1 else "command"
        detail = "rate-limited" if _rate_limited(completed.stderr or "") else (
            f"exit code {completed.returncode}"
        )
        raise ExportError(f"rclone {operation} failed ({detail})")
    return completed


def _remote_file(
    rclone: Path,
    remote_parent: str,
    filename: str,
) -> _RemoteFile | None:
    listing = _run_rclone(
        [
            str(rclone),
            "lsl",
            remote_parent,
            "--include",
            filename,
        ],
        timeout_seconds=RCLONE_TIMEOUT_SECONDS,
    )
    remote_size: int | None = None
    for line in (listing.stdout or "").splitlines():
        parts = line.split(maxsplit=3)
        if len(parts) == 4 and parts[3] == filename:
            try:
                remote_size = int(parts[0])
            except ValueError as error:
                raise ExportError("invalid remote size listing") from error
            break
    if remote_size is None:
        return None

    hashes = _run_rclone(
        [
            str(rclone),
            "md5sum",
            remote_parent,
            "--include",
            filename,
        ],
        timeout_seconds=RCLONE_TIMEOUT_SECONDS,
    )
    remote_md5: str | None = None
    for line in (hashes.stdout or "").splitlines():
        parts = line.split(maxsplit=1)
        if len(parts) == 2 and parts[1] == filename:
            remote_md5 = parts[0]
            break
    if remote_md5 is None:
        raise ExportError("remote MD5 listing did not contain the formal file")
    return _RemoteFile(size=remote_size, md5=remote_md5)


def _copy_to_remote(
    rclone: Path,
    local_path: Path,
    remote_path: str,
) -> None:
    _run_rclone(
        [
            str(rclone),
            "copyto",
            str(local_path),
            remote_path,
            "--immutable",
            "--timeout",
            f"{RCLONE_TIMEOUT_SECONDS:g}s",
            "--contimeout",
            f"{RCLONE_CONTIMEOUT_SECONDS:g}s",
            "--retries",
            str(RCLONE_RETRIES),
            "--low-level-retries",
            str(RCLONE_LOW_LEVEL_RETRIES),
        ],
        timeout_seconds=RCLONE_TIMEOUT_SECONDS,
    )


def export_market_date(
    *,
    data_root: Path,
    output_root: Path,
    target_date: date,
    remote: str,
    rclone: Path,
) -> ExportResult:
    _validate_remote(remote)
    files = _source_files(data_root, target_date)
    schema, expected_rows = _source_schema_and_rows(files)
    filename = f"date={target_date.isoformat()}.csv.gz"
    output = output_root / "market" / filename
    remote_parent = f"{remote}:{REMOTE_ROOT}"
    remote_path = f"{remote_parent}/{filename}"

    converted = False
    if output.exists():
        try:
            _validate_csv(output, schema, expected_rows)
        except ExportError:
            _publish_local(files, schema, expected_rows, output)
            converted = True
    else:
        _publish_local(files, schema, expected_rows, output)
        converted = True

    local_size = output.stat().st_size
    local_md5 = _md5(output)
    existing_remote = _remote_file(rclone, remote_parent, filename)
    if existing_remote is not None:
        if existing_remote.size != local_size or existing_remote.md5 != local_md5:
            raise RemoteMismatchError(
                "formal remote file exists with a different size or checksum"
            )
        return ExportResult(
            local_path=output,
            remote_path=remote_path,
            row_count=expected_rows,
            converted=converted,
            uploaded=False,
            remote_verified=True,
        )

    _copy_to_remote(rclone, output, remote_path)
    verified_remote = _remote_file(rclone, remote_parent, filename)
    if verified_remote is None:
        raise ExportError("uploaded formal remote file was not found")
    if verified_remote.size != local_size or verified_remote.md5 != local_md5:
        raise RemoteMismatchError(
            "uploaded formal remote file has a different size or checksum"
        )
    return ExportResult(
        local_path=output,
        remote_path=remote_path,
        row_count=expected_rows,
        converted=converted,
        uploaded=True,
        remote_verified=True,
    )


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from error


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export market Parquet to CSV.GZ")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--date", type=_parse_date, required=True)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--rclone", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = export_market_date(
            data_root=args.data_root,
            output_root=args.output_root,
            target_date=args.date,
            remote=args.remote,
            rclone=args.rclone,
        )
    except (ExportError, OSError, ValueError) as error:
        print(f"analysis export failed: {error}", file=sys.stderr)
        return 2
    print(
        f"rows={result.row_count} local={result.local_path} "
        f"converted={result.converted} uploaded={result.uploaded} "
        f"remote_verified={result.remote_verified}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
