from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from radar import analysis_daily
from radar.analysis_export import ExportError, ExportResult, RemoteMismatchError


SAMPLE_SCHEMA = pa.schema(
    [
        pa.field("sample_time", pa.timestamp("us", tz="UTC")),
        pa.field("value", pa.int64()),
    ]
)
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)


def _slots(target_date: date) -> list[datetime]:
    start = datetime.combine(target_date, datetime.min.time(), tzinfo=UTC)
    return [start + timedelta(seconds=10 * index) for index in range(8640)]


def _write_partition(
    data_root: Path,
    target_date: date,
    sample_times: list[datetime],
    *,
    fragments: int = 2,
) -> Path:
    partition = data_root / "market" / f"date={target_date.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    fragment_count = max(1, min(fragments, len(sample_times)))
    chunk_size = (len(sample_times) + fragment_count - 1) // fragment_count
    for fragment_index in range(fragment_count):
        start = fragment_index * chunk_size
        values = sample_times[start : start + chunk_size]
        if not values:
            continue
        table = pa.Table.from_pydict(
            {
                "sample_time": values,
                "value": list(range(start, start + len(values))),
            },
            schema=SAMPLE_SCHEMA,
        )
        pq.write_table(table, partition / f"part-{fragment_index:04d}.parquet")
    return partition


def _complete_partition(data_root: Path, target_date: date) -> Path:
    return _write_partition(data_root, target_date, _slots(target_date))


def _result(target_date: date, *, converted: bool, uploaded: bool) -> ExportResult:
    return ExportResult(
        local_path=Path(f"/tmp/date={target_date.isoformat()}.csv.gz"),
        remote_path=(
            "opportunity-drive-own:OpportunityRadar/analysis_csv/market/"
            f"date={target_date.isoformat()}.csv.gz"
        ),
        row_count=0,
        converted=converted,
        uploaded=uploaded,
        remote_verified=True,
    )


def _run_kwargs(
    tmp_path: Path,
    *,
    now: datetime = NOW,
    lookback_days: int = 7,
    exporter: Callable[..., ExportResult],
    remote: str = "opportunity-drive-own",
) -> analysis_daily.DailyRunResult:
    return analysis_daily.run_daily(
        data_root=tmp_path / "data",
        output_root=tmp_path / "analysis",
        remote=remote,
        rclone=Path("/opt/homebrew/bin/rclone"),
        lookback_days=lookback_days,
        now=now,
        exporter=exporter,
    )


def test_exactly_8640_aligned_slots_are_complete(tmp_path: Path) -> None:
    target_date = date(2026, 9, 29)
    _complete_partition(tmp_path / "data", target_date)

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.complete is True
    assert inspection.slot_count == 8640
    assert inspection.reason is None
    assert inspection.fragment_count == 2


@pytest.mark.parametrize("slot_count", [8639, 8630])
def test_incomplete_slot_counts_are_rejected(tmp_path: Path, slot_count: int) -> None:
    target_date = date(2026, 9, 29)
    _write_partition(tmp_path / "data", target_date, _slots(target_date)[:slot_count])

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.complete is False
    assert inspection.slot_count == slot_count
    assert inspection.reason is not None
    assert "slot_count" in inspection.reason


def test_missing_beginning_slot_is_rejected(tmp_path: Path) -> None:
    target_date = date(2026, 9, 29)
    sample_times = _slots(target_date)[1:]
    _write_partition(tmp_path / "data", target_date, sample_times)

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.complete is False
    assert inspection.slot_count == 8639
    assert inspection.reason is not None
    assert "missing_start" in inspection.reason


def test_missing_ending_slot_is_rejected(tmp_path: Path) -> None:
    target_date = date(2026, 9, 29)
    sample_times = _slots(target_date)[:-1]
    _write_partition(tmp_path / "data", target_date, sample_times)

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.complete is False
    assert inspection.slot_count == 8639
    assert inspection.reason is not None
    assert "missing_end" in inspection.reason


def test_unaligned_sample_time_is_rejected(tmp_path: Path) -> None:
    target_date = date(2026, 9, 29)
    sample_times = _slots(target_date)
    sample_times[100] += timedelta(seconds=1)
    _write_partition(tmp_path / "data", target_date, sample_times)

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.complete is False
    assert inspection.reason is not None
    assert "unaligned" in inspection.reason


def test_current_utc_date_is_never_eligible(tmp_path: Path) -> None:
    _complete_partition(tmp_path / "data", NOW.date())
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        calls.append(kwargs["target_date"])  # type: ignore[arg-type]
        raise AssertionError("current UTC date must not be exported")

    result = _run_kwargs(tmp_path, lookback_days=1, exporter=exporter)

    assert [inspection.target_date for inspection in result.inspections] == [
        NOW.date() - timedelta(days=1)
    ]
    assert result.outcome == "incomplete_only"
    assert calls == []


def test_lookback_is_oldest_to_newest_and_current_date_is_excluded(
    tmp_path: Path,
) -> None:
    dates = [NOW.date() - timedelta(days=offset) for offset in (3, 2, 1)]
    for target_date in dates:
        _complete_partition(tmp_path / "data", target_date)
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        target_date = kwargs["target_date"]
        assert isinstance(target_date, date)
        calls.append(target_date)
        return _result(target_date, converted=False, uploaded=False)

    result = _run_kwargs(tmp_path, lookback_days=3, exporter=exporter)

    assert [inspection.target_date for inspection in result.inspections] == dates
    assert calls == dates
    assert result.outcome == "no_work"


def test_previously_incomplete_date_can_become_eligible_later(tmp_path: Path) -> None:
    target_date = NOW.date() - timedelta(days=2)
    incomplete = _slots(target_date)[:-1]
    _write_partition(tmp_path / "data", target_date, incomplete)
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        assert isinstance(target, date)
        calls.append(target)
        return _result(target, converted=True, uploaded=True)

    first = _run_kwargs(tmp_path, lookback_days=2, exporter=exporter)
    assert first.outcome == "incomplete_only"
    assert calls == []

    _write_partition(tmp_path / "data", target_date, _slots(target_date), fragments=3)
    second = _run_kwargs(tmp_path, lookback_days=2, exporter=exporter)

    assert second.outcome == "work_completed"
    assert calls == [target_date]


def test_already_exported_complete_date_is_passed_over_to_newer_work(
    tmp_path: Path,
) -> None:
    older = NOW.date() - timedelta(days=2)
    newer = NOW.date() - timedelta(days=1)
    _complete_partition(tmp_path / "data", older)
    _complete_partition(tmp_path / "data", newer)
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        assert isinstance(target, date)
        calls.append(target)
        return _result(target, converted=False, uploaded=target == newer)

    result = _run_kwargs(tmp_path, lookback_days=2, exporter=exporter)

    assert result.outcome == "work_completed"
    assert calls == [older, newer]
    assert result.selected_date == newer
    assert result.export_result is not None
    assert result.export_result.uploaded is True


def test_only_one_date_performs_actual_work(tmp_path: Path) -> None:
    dates = [NOW.date() - timedelta(days=offset) for offset in (3, 2, 1)]
    for target_date in dates:
        _complete_partition(tmp_path / "data", target_date)
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        assert isinstance(target, date)
        calls.append(target)
        return _result(target, converted=target == dates[0], uploaded=target == dates[0])

    result = _run_kwargs(tmp_path, lookback_days=3, exporter=exporter)

    assert result.outcome == "work_completed"
    assert calls == [dates[0]]
    assert result.selected_date == dates[0]


@pytest.mark.parametrize("failure", [ExportError("failed"), RemoteMismatchError("mismatch")])
def test_exporter_failure_stops_subsequent_processing(
    tmp_path: Path,
    failure: Exception,
) -> None:
    older = NOW.date() - timedelta(days=2)
    newer = NOW.date() - timedelta(days=1)
    _complete_partition(tmp_path / "data", older)
    _complete_partition(tmp_path / "data", newer)
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        assert isinstance(target, date)
        calls.append(target)
        raise failure

    result = _run_kwargs(tmp_path, lookback_days=2, exporter=exporter)

    assert result.outcome == "error"
    assert result.error == str(failure)
    assert calls == [older]


def test_remote_name_is_rejected_without_fallback(tmp_path: Path) -> None:
    def exporter(**kwargs: object) -> ExportResult:
        pytest.fail("exporter must not be called")

    with pytest.raises(ValueError, match="opportunity-drive-own"):
        _run_kwargs(
            tmp_path,
            exporter=exporter,
            remote="opportunity-drive",
        )


def test_source_is_not_modified_by_completeness_check(tmp_path: Path) -> None:
    target_date = NOW.date() - timedelta(days=1)
    partition = _complete_partition(tmp_path / "data", target_date)
    source_bytes = {
        path: path.read_bytes() for path in partition.glob("*.parquet")
    }

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.complete is True
    assert {path: path.read_bytes() for path in partition.glob("*.parquet")} == source_bytes


def test_completeness_reads_only_sample_time_in_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_date = NOW.date() - timedelta(days=1)
    _complete_partition(tmp_path / "data", target_date)
    original_iter_batches = pq.ParquetFile.iter_batches
    calls: list[tuple[object, object]] = []

    def tracked_iter_batches(
        parquet: pq.ParquetFile,
        *args: object,
        **kwargs: object,
    ) -> object:
        calls.append((kwargs.get("columns"), kwargs.get("batch_size")))
        return original_iter_batches(parquet, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", tracked_iter_batches)

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.complete is True
    assert calls
    assert all(columns == ["sample_time"] for columns, _ in calls)
    assert all(isinstance(batch_size, int) for _, batch_size in calls)
