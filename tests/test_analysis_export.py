from __future__ import annotations

import csv
import gzip
import hashlib
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from radar import analysis_export
from radar.storage.parquet import (
    DATASET_SCHEMAS,
    FUNDING_SCHEMA,
    HOURLY_CONTEXT_SCHEMA,
    MARKET_SCHEMA,
    QUOTED_MARKET_SCHEMA,
)


DATE_TEXT = "2026-09-29"
TARGET_DATE = datetime(2026, 9, 29, tzinfo=UTC).date()
REMOTE_PREFIX = "opportunity-drive-own:OpportunityRadar/analysis_csv/market/"


def _market_row(
    *,
    sample_time: datetime,
    observed_at: datetime,
    venue: str,
    venue_symbol: str,
    canonical_symbol: str,
    buy_10k_vwap: float | None,
    sell_10k_vwap: float | None,
) -> dict[str, object]:
    return {
        "sample_time": sample_time,
        "observed_at": observed_at,
        "venue": venue,
        "venue_symbol": venue_symbol,
        "canonical_symbol": canonical_symbol,
        "best_bid": 736.25,
        "best_bid_size": 12.5,
        "best_ask": 736.75,
        "best_ask_size": 11.5,
        "mark_price": None,
        "index_price": 736.5,
        "buy_1k_vwap": None,
        "sell_1k_vwap": 736.1,
        "buy_5k_vwap": 736.8,
        "sell_5k_vwap": None,
        "buy_10k_vwap": buy_10k_vwap,
        "sell_10k_vwap": sell_10k_vwap,
    }


def _write_market_parts(data_root: Path, parts: list[list[dict[str, object]]]) -> None:
    partition = data_root / "market" / f"date={DATE_TEXT}"
    partition.mkdir(parents=True)
    for index, rows in enumerate(parts, start=1):
        table = pa.Table.from_pylist(rows, schema=MARKET_SCHEMA)
        pq.write_table(table, partition / f"part-{index:02d}.parquet", compression="zstd")


def _write_dataset_parts(
    data_root: Path,
    dataset: str,
    parts: list[list[dict[str, object]]],
) -> None:
    partition = data_root / dataset / f"date={DATE_TEXT}"
    partition.mkdir(parents=True)
    for index, rows in enumerate(parts, start=1):
        table = pa.Table.from_pylist(rows, schema=DATASET_SCHEMAS[dataset])
        pq.write_table(table, partition / f"part-{index:02d}.parquet", compression="zstd")


def _ancillary_rows(dataset: str) -> list[list[dict[str, object]]]:
    base = datetime(2026, 9, 29, tzinfo=UTC)
    if dataset == "funding":
        rows = [
            {
                "effective_time": base,
                "observed_at": base + timedelta(microseconds=1),
                "venue": "lighter",
                "venue_symbol": "QQQ",
                "canonical_symbol": "QQQ",
                "funding_rate": 0.0001,
                "next_funding_time": None,
            },
            {
                "effective_time": base + timedelta(hours=1),
                "observed_at": base + timedelta(hours=1, microseconds=2),
                "venue": "arcus",
                "venue_symbol": "QQQ-USD",
                "canonical_symbol": "QQQ",
                "funding_rate": -0.0002,
                "next_funding_time": base + timedelta(hours=2),
            },
        ]
    elif dataset == "hourly_context":
        rows = [
            {
                "sample_time": base,
                "observed_at": base + timedelta(microseconds=3),
                "venue": "lighter",
                "venue_symbol": "QQQ",
                "canonical_symbol": "QQQ",
                "open_interest": None,
                "volume_24h": 10.5,
            },
            {
                "sample_time": base + timedelta(hours=1),
                "observed_at": base + timedelta(hours=1, microseconds=4),
                "venue": "arcus",
                "venue_symbol": "QQQ-USD",
                "canonical_symbol": "QQQ",
                "open_interest": 12.5,
                "volume_24h": None,
            },
        ]
    elif dataset == "quoted_market":
        rows = [
            {
                "quote_time": base + timedelta(microseconds=5),
                "fetched_at": base + timedelta(microseconds=6),
                "venue": "variational",
                "venue_symbol": "QQQ",
                "canonical_symbol": "QQQ",
                "mark_price": 100.0,
                "bid_1k": 99.9,
                "ask_1k": 100.1,
                "bid_100k": 99.5,
                "ask_100k": 100.5,
                "bid_1m": None,
                "ask_1m": 101.0,
                "funding_rate": 0.0003,
                "funding_interval_seconds": 3600,
                "volume_24h": None,
                "long_open_interest": 50.0,
                "short_open_interest": None,
            },
            {
                "quote_time": base + timedelta(seconds=30),
                "fetched_at": base + timedelta(seconds=31),
                "venue": "variational",
                "venue_symbol": "QQQ",
                "canonical_symbol": "QQQ",
                "mark_price": 100.2,
                "bid_1k": 100.1,
                "ask_1k": 100.3,
                "bid_100k": 99.7,
                "ask_100k": 100.7,
                "bid_1m": 99.0,
                "ask_1m": None,
                "funding_rate": -0.0001,
                "funding_interval_seconds": 7200,
                "volume_24h": 1000.0,
                "long_open_interest": None,
                "short_open_interest": 60.0,
            },
        ]
    else:
        raise AssertionError(dataset)
    midpoint = len(rows) // 2
    return [rows[:midpoint], rows[midpoint:]]


def _source_rows() -> list[list[dict[str, object]]]:
    base = datetime(2026, 9, 29, tzinfo=UTC)
    return [
        [
            _market_row(
                sample_time=base,
                observed_at=base + timedelta(microseconds=123456),
                venue="arcus",
                venue_symbol="QQQ-USD",
                canonical_symbol="QQQ",
                buy_10k_vwap=None,
                sell_10k_vwap=None,
            ),
            _market_row(
                sample_time=base + timedelta(seconds=10),
                observed_at=base + timedelta(seconds=10, microseconds=654321),
                venue="lighter",
                venue_symbol="QQQ",
                canonical_symbol="QQQ",
                buy_10k_vwap=736.1234567890123,
                sell_10k_vwap=735.9876543210987,
            ),
        ],
        [
            _market_row(
                sample_time=base + timedelta(seconds=20),
                observed_at=base + timedelta(seconds=20, microseconds=7),
                venue="backpack",
                venue_symbol="SNDK.US_USDC_PERP",
                canonical_symbol="SNDK",
                buy_10k_vwap=101.0000000000001,
                sell_10k_vwap=100.9999999999999,
            )
        ],
    ]


class FakeRclone:
    def __init__(self, local_path: Path) -> None:
        self.local_path = local_path
        self.calls: list[tuple[str, ...]] = []
        self.remote_exists = False
        self.remote_size = 0
        self.remote_md5 = ""

    def __call__(
        self,
        argv: list[str],
        *,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        call = tuple(argv)
        self.calls.append(call)
        operation = argv[1]
        if operation == "lsl":
            stdout = ""
            if self.remote_exists:
                stdout = (
                    f"{self.remote_size} 2026-09-30 00:00:00 "
                    "date=2026-09-29.csv.gz\n"
                )
            return subprocess.CompletedProcess(argv, 0, stdout, "")
        if operation == "md5sum":
            return subprocess.CompletedProcess(
                argv,
                0,
                f"{self.remote_md5} date=2026-09-29.csv.gz\n",
                "",
            )
        if operation == "copyto":
            self.remote_exists = True
            self.remote_size = self.local_path.stat().st_size
            self.remote_md5 = hashlib.md5(self.local_path.read_bytes()).hexdigest()
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(f"unexpected rclone operation: {operation}")


def _run_export(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_rclone: FakeRclone | None = None,
) -> tuple[analysis_export.ExportResult, Path, FakeRclone]:
    data_root = tmp_path / "data"
    output_root = tmp_path / "analysis"
    _write_market_parts(data_root, _source_rows())
    output = output_root / "market" / f"date={DATE_TEXT}.csv.gz"
    fake = FakeRclone(output) if fake_rclone is None else fake_rclone
    monkeypatch.setattr(analysis_export, "_run_rclone", fake)
    result = analysis_export.export_market_date(
        data_root=data_root,
        output_root=output_root,
        target_date=TARGET_DATE,
        remote="opportunity-drive-own",
        rclone=Path("/opt/homebrew/bin/rclone"),
    )
    return result, output, fake


def test_streaming_conversion_preserves_schema_nulls_timestamps_and_float_round_trip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_iter_batches = pq.ParquetFile.iter_batches
    batch_sizes: list[int | None] = []

    def tracked_iter_batches(self: pq.ParquetFile, *args: object, **kwargs: object):
        batch_sizes.append(kwargs.get("batch_size"))
        return original_iter_batches(self, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", tracked_iter_batches)
    result, output, fake = _run_export(tmp_path, monkeypatch)

    assert result.converted is True
    assert result.uploaded is True
    assert result.row_count == 3
    assert batch_sizes
    assert all(size is not None and size <= 8192 for size in batch_sizes)
    assert all("opportunity-drive:" not in " ".join(call) for call in fake.calls)

    with gzip.open(output, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert reader.fieldnames == list(MARKET_SCHEMA.names)
    assert len(rows) == 3
    assert rows[0]["sample_time"] == "2026-09-29T00:00:00.000000Z"
    assert rows[0]["observed_at"] == "2026-09-29T00:00:00.123456Z"
    assert rows[0]["buy_10k_vwap"] == ""
    assert rows[1]["buy_10k_vwap"] == "736.1234567890123"
    assert float(rows[1]["buy_10k_vwap"]) == 736.1234567890123
    assert float(rows[1]["sell_10k_vwap"]) == 735.9876543210987

    copy_calls = [call for call in fake.calls if call[1] == "copyto"]
    assert len(copy_calls) == 1
    assert copy_calls[0][3] == (
        f"{REMOTE_PREFIX}date={DATE_TEXT}.csv.gz"
    )


def test_valid_local_artifact_skips_rebuild_and_matching_remote_skips_upload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_result, output, first_fake = _run_export(tmp_path, monkeypatch)
    assert first_result.converted is True
    assert first_result.uploaded is True
    original_bytes = output.read_bytes()

    second_fake = FakeRclone(output)
    second_fake.remote_exists = True
    second_fake.remote_size = output.stat().st_size
    second_fake.remote_md5 = hashlib.md5(original_bytes).hexdigest()
    monkeypatch.setattr(analysis_export, "_run_rclone", second_fake)

    def fail_rebuild(*args: object, **kwargs: object) -> None:
        raise AssertionError("valid local CSV must not be rebuilt")

    monkeypatch.setattr(analysis_export, "_convert_partition", fail_rebuild)
    result = analysis_export.export_market_date(
        data_root=tmp_path / "data",
        output_root=tmp_path / "analysis",
        target_date=TARGET_DATE,
        remote="opportunity-drive-own",
        rclone=Path("/opt/homebrew/bin/rclone"),
    )

    assert result.converted is False
    assert result.uploaded is False
    assert output.read_bytes() == original_bytes
    assert not any(call[1] == "copyto" for call in second_fake.calls)


def test_atomic_output_preserves_valid_final_when_new_source_schema_is_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_result, output, _ = _run_export(tmp_path, monkeypatch)
    assert first_result.converted is True
    original_bytes = output.read_bytes()

    bad_schema = MARKET_SCHEMA.append(pa.field("unexpected", pa.string()))
    bad_partition = tmp_path / "data" / "market" / f"date={DATE_TEXT}"
    bad_table = pa.Table.from_pylist(
        [{**_source_rows()[0][0], "unexpected": "bad"}],
        schema=bad_schema,
    )
    pq.write_table(bad_table, bad_partition / "part-99.parquet")

    with pytest.raises(analysis_export.ExportError):
        analysis_export.export_market_date(
            data_root=tmp_path / "data",
            output_root=tmp_path / "analysis",
            target_date=TARGET_DATE,
            remote="opportunity-drive-own",
            rclone=Path("/opt/homebrew/bin/rclone"),
        )

    assert output.read_bytes() == original_bytes
    assert not tuple(output.parent.glob(".*.tmp"))


def test_remote_mismatch_fails_closed_without_copying(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, output, _ = _run_export(tmp_path, monkeypatch)
    original_bytes = output.read_bytes()
    mismatch = FakeRclone(output)
    mismatch.remote_exists = True
    mismatch.remote_size = output.stat().st_size + 1
    mismatch.remote_md5 = "00000000000000000000000000000000"
    monkeypatch.setattr(analysis_export, "_run_rclone", mismatch)

    with pytest.raises(analysis_export.RemoteMismatchError):
        analysis_export.export_market_date(
            data_root=tmp_path / "data",
            output_root=tmp_path / "analysis",
            target_date=TARGET_DATE,
            remote="opportunity-drive-own",
            rclone=Path("/opt/homebrew/bin/rclone"),
        )

    assert output.read_bytes() == original_bytes
    assert not any(call[1] == "copyto" for call in mismatch.calls)


def test_rate_limit_failure_returns_nonzero_and_preserves_local_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "analysis"
    _write_market_parts(data_root, _source_rows())

    def fake_run(
        argv: list[str],
        *,
        shell: bool,
        capture_output: bool,
        text: bool,
        timeout: float,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert shell is False
        assert capture_output is True
        assert text is True
        assert check is False
        assert isinstance(timeout, float)
        if argv[1] == "lsl":
            return subprocess.CompletedProcess(argv, 0, "", "")
        if argv[1] == "copyto":
            return subprocess.CompletedProcess(
                argv,
                1,
                "",
                "403 rateLimitExceeded token=DO_NOT_PRINT",
            )
        raise AssertionError(f"unexpected operation {argv[1]}")

    monkeypatch.setattr(analysis_export.subprocess, "run", fake_run)
    exit_code = analysis_export.main(
        [
            "--data-root",
            str(data_root),
            "--output-root",
            str(output_root),
            "--date",
            DATE_TEXT,
            "--remote",
            "opportunity-drive-own",
            "--rclone",
            "/opt/homebrew/bin/rclone",
        ]
    )

    assert exit_code != 0
    assert (output_root / "market" / f"date={DATE_TEXT}.csv.gz").exists()
    stderr = capsys.readouterr().err
    assert "DO_NOT_PRINT" not in stderr
    assert "rate-limited" in stderr


def test_old_remote_is_rejected_without_fallback(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="opportunity-drive-own"):
        analysis_export.export_market_date(
            data_root=tmp_path / "data",
            output_root=tmp_path / "analysis",
            target_date=TARGET_DATE,
            remote="opportunity-drive",
            rclone=Path("/opt/homebrew/bin/rclone"),
        )


@pytest.mark.parametrize(
    ("dataset", "schema"),
    [
        ("funding", FUNDING_SCHEMA),
        ("hourly_context", HOURLY_CONTEXT_SCHEMA),
        ("quoted_market", QUOTED_MARKET_SCHEMA),
    ],
)
def test_export_dataset_streams_and_preserves_ancillary_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dataset: str,
    schema: pa.Schema,
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "analysis"
    parts = _ancillary_rows(dataset)
    _write_dataset_parts(data_root, dataset, parts)
    output = output_root / dataset / f"date={DATE_TEXT}.csv.gz"
    fake = FakeRclone(output)
    monkeypatch.setattr(analysis_export, "_run_rclone", fake)

    result = analysis_export.export_dataset_date(
        data_root=data_root,
        output_root=output_root,
        target_date=TARGET_DATE,
        dataset=dataset,
        remote="opportunity-drive-own",
        rclone=Path("/opt/homebrew/bin/rclone"),
    )

    assert result.converted is True
    assert result.uploaded is True
    assert result.row_count == 2
    with gzip.open(output, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    assert reader.fieldnames == list(schema.names)
    assert len(rows) == 2
    timestamp_field = schema.field(0).name
    assert rows[0][timestamp_field].endswith("Z")
    assert any(value == "" for value in rows[0].values())
    assert all("opportunity-drive:" not in " ".join(call) for call in fake.calls)
    copy_calls = [call for call in fake.calls if call[1] == "copyto"]
    assert len(copy_calls) == 1
    assert copy_calls[0][3] == (
        f"opportunity-drive-own:OpportunityRadar/analysis_csv/{dataset}/"
        f"date={DATE_TEXT}.csv.gz"
    )


def test_quoted_market_conversion_reads_high_fragment_count_in_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "analysis"
    rows = [
        row
        for part in _ancillary_rows("quoted_market")
        for row in part
    ]
    _write_dataset_parts(
        data_root,
        "quoted_market",
        [[row] for row in rows * 12],
    )
    output = output_root / "quoted_market" / f"date={DATE_TEXT}.csv.gz"
    fake = FakeRclone(output)
    monkeypatch.setattr(analysis_export, "_run_rclone", fake)
    original_iter_batches = pq.ParquetFile.iter_batches
    batch_sizes: list[int | None] = []

    def tracked_iter_batches(self: pq.ParquetFile, *args: object, **kwargs: object):
        batch_sizes.append(kwargs.get("batch_size"))
        return original_iter_batches(self, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", tracked_iter_batches)
    result = analysis_export.export_dataset_date(
        data_root=data_root,
        output_root=output_root,
        target_date=TARGET_DATE,
        dataset="quoted_market",
        remote="opportunity-drive-own",
        rclone=Path("/opt/homebrew/bin/rclone"),
    )

    assert result.row_count == 24
    assert batch_sizes
    assert all(size is not None and size <= 8192 for size in batch_sizes)
