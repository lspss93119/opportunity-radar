from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from radar import analysis_daily
from radar.analysis_daily import DailyRunResult
from radar.analysis_export import ExportError, ExportResult, RemoteMismatchError
from radar.storage.parquet import DATASET_SCHEMAS


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


def _closed_partial_slots(target_date: date, slot_count: int) -> list[datetime]:
    """Return a slot set with both day boundaries preserved."""
    if slot_count == 8640:
        return _slots(target_date)
    return _slots(target_date)[: slot_count - 1] + [_slots(target_date)[-1]]


def _result(
    target_date: date,
    *,
    converted: bool,
    uploaded: bool,
    dataset: str = "market",
) -> ExportResult:
    return ExportResult(
        local_path=Path(f"/tmp/date={target_date.isoformat()}.csv.gz"),
        remote_path=(
            f"opportunity-drive-own:OpportunityRadar/analysis_csv/{dataset}/"
            f"date={target_date.isoformat()}.csv.gz"
        ),
        row_count=0,
        converted=converted,
        uploaded=uploaded,
        remote_verified=True,
    )


def _write_ancillary_partition(
    data_root: Path,
    dataset: str,
    target_date: date,
    *,
    malformed: bool = False,
) -> None:
    base = datetime.combine(target_date, datetime.min.time(), tzinfo=UTC)
    if dataset == "funding":
        rows = [
            {
                "effective_time": base,
                "observed_at": base,
                "venue": "lighter",
                "venue_symbol": "BTC",
                "canonical_symbol": "BTC",
                "funding_rate": 0.0,
                "next_funding_time": None,
            }
        ]
        if malformed:
            rows[0]["effective_time"] = base + timedelta(days=1)
    elif dataset == "hourly_context":
        rows = [
            {
                "sample_time": base,
                "observed_at": base,
                "venue": "lighter",
                "venue_symbol": "BTC",
                "canonical_symbol": "BTC",
                "open_interest": None,
                "volume_24h": 1.0,
            }
        ]
    elif dataset == "quoted_market":
        rows = [
            {
                "quote_time": base,
                "fetched_at": base,
                "venue": "variational",
                "venue_symbol": "BTC",
                "canonical_symbol": "BTC",
                "mark_price": 100.0,
                "bid_1k": 99.0,
                "ask_1k": 101.0,
                "bid_100k": 98.0,
                "ask_100k": 102.0,
                "bid_1m": None,
                "ask_1m": None,
                "funding_rate": 0.0,
                "funding_interval_seconds": 3600,
                "volume_24h": None,
                "long_open_interest": None,
                "short_open_interest": None,
            }
        ]
    else:
        raise AssertionError(dataset)
    partition = data_root / dataset / f"date={target_date.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=DATASET_SCHEMAS[dataset])
    pq.write_table(table, partition / "part-0000.parquet")


def _write_complete_bundle(data_root: Path, target_date: date) -> None:
    _complete_partition(data_root, target_date)
    for dataset in ("funding", "hourly_context", "quoted_market"):
        _write_ancillary_partition(data_root, dataset, target_date)


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
    assert inspection.quality == "complete"
    assert inspection.eligible is True
    assert inspection.slot_count == 8640
    assert inspection.missing_slots == ()
    assert inspection.reason is None
    assert inspection.fragment_count == 2


@pytest.mark.parametrize("slot_count", [8639, 8630, 8623])
def test_threshold_eligible_slot_counts_are_partial(
    tmp_path: Path,
    slot_count: int,
) -> None:
    target_date = date(2026, 9, 29)
    _write_partition(
        tmp_path / "data",
        target_date,
        _closed_partial_slots(target_date, slot_count),
    )

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.complete is False
    assert inspection.quality == "partial"
    assert inspection.eligible is True
    assert inspection.slot_count == slot_count
    assert inspection.coverage_pct == pytest.approx(slot_count / 8640 * 100)


def test_below_threshold_is_rejected(tmp_path: Path) -> None:
    target_date = date(2026, 9, 29)
    _write_partition(
        tmp_path / "data",
        target_date,
        _closed_partial_slots(target_date, 8622),
    )

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.quality == "rejected"
    assert inspection.eligible is False
    assert inspection.slot_count == 8622
    assert inspection.reason == "coverage_below_threshold"


def test_threshold_calculation_uses_ceil() -> None:
    assert analysis_daily.minimum_required_slots(99.8) == 8623
    assert analysis_daily.minimum_required_slots(99.79) == 8622


def test_missing_beginning_slot_is_rejected(tmp_path: Path) -> None:
    target_date = date(2026, 9, 29)
    sample_times = _slots(target_date)[1:]
    _write_partition(tmp_path / "data", target_date, sample_times)

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.complete is False
    assert inspection.quality == "rejected"
    assert inspection.eligible is False
    assert inspection.slot_count == 8639
    assert inspection.reason is not None
    assert "missing_start" in inspection.reason


def test_missing_ending_slot_is_rejected(tmp_path: Path) -> None:
    target_date = date(2026, 9, 29)
    sample_times = _slots(target_date)[:-1]
    _write_partition(tmp_path / "data", target_date, sample_times)

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.complete is False
    assert inspection.quality == "rejected"
    assert inspection.eligible is False
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
    assert inspection.quality == "rejected"
    assert inspection.eligible is False
    assert inspection.reason is not None
    assert "unaligned" in inspection.reason


def test_outside_date_sample_time_is_rejected(tmp_path: Path) -> None:
    target_date = date(2026, 9, 29)
    sample_times = _slots(target_date)
    sample_times[100] = datetime(2026, 9, 30, tzinfo=UTC)
    _write_partition(tmp_path / "data", target_date, sample_times)

    inspection = analysis_daily.inspect_market_date(tmp_path / "data", target_date)

    assert inspection.quality == "rejected"
    assert inspection.eligible is False
    assert inspection.reason == "sample_time_wrong_date"


def test_custom_min_coverage_percentage(tmp_path: Path) -> None:
    target_date = date(2026, 9, 29)
    _write_partition(
        tmp_path / "data",
        target_date,
        _closed_partial_slots(target_date, 8630),
    )

    inspection = analysis_daily.inspect_market_date(
        tmp_path / "data",
        target_date,
        min_coverage_pct=99.9,
    )

    assert inspection.quality == "rejected"
    assert inspection.eligible is False
    assert inspection.reason == "coverage_below_threshold"


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
        _write_complete_bundle(tmp_path / "data", target_date)
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        target_date = kwargs["target_date"]
        dataset = kwargs["dataset"]
        assert isinstance(target_date, date)
        assert isinstance(dataset, str)
        if dataset == "market":
            calls.append(target_date)
        return _result(target_date, converted=False, uploaded=False, dataset=dataset)

    result = _run_kwargs(tmp_path, lookback_days=3, exporter=exporter)

    assert [inspection.target_date for inspection in result.inspections] == dates
    assert calls == dates
    assert result.outcome == "no_work"


def test_already_exported_partial_date_can_be_passed_over_idempotently(
    tmp_path: Path,
) -> None:
    target_date = NOW.date() - timedelta(days=2)
    incomplete = _closed_partial_slots(target_date, 8639)
    _write_partition(tmp_path / "data", target_date, incomplete)
    for dataset in ("funding", "hourly_context", "quoted_market"):
        _write_ancillary_partition(tmp_path / "data", dataset, target_date)
    calls: list[date] = []
    market_calls = 0

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        dataset = kwargs["dataset"]
        assert isinstance(target, date)
        assert isinstance(dataset, str)
        nonlocal market_calls
        if dataset == "market":
            market_calls += 1
            calls.append(target)
        return _result(
            target,
            converted=market_calls > 1,
            uploaded=market_calls > 1,
            dataset=dataset,
        )

    first = _run_kwargs(tmp_path, lookback_days=2, exporter=exporter)
    assert first.outcome == "no_work"
    assert calls == [target_date]
    assert first.inspections[0].quality == "partial"
    assert first.inspections[0].eligible is True

    _write_partition(
        tmp_path / "data",
        target_date,
        _slots(target_date),
        fragments=3,
    )
    second = _run_kwargs(tmp_path, lookback_days=2, exporter=exporter)

    assert second.outcome == "work_completed"
    assert calls == [target_date, target_date]
    assert second.export_result is not None
    assert second.export_result.converted is True


def test_rejected_date_never_invokes_exporter(tmp_path: Path) -> None:
    target_date = NOW.date() - timedelta(days=1)
    _write_partition(
        tmp_path / "data",
        target_date,
        _closed_partial_slots(target_date, 8622),
    )
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        calls.append(kwargs["target_date"])  # type: ignore[arg-type]
        raise AssertionError("rejected date must not be exported")

    result = _run_kwargs(tmp_path, lookback_days=1, exporter=exporter)

    assert result.outcome == "incomplete_only"
    assert result.inspections[0].quality == "rejected"
    assert calls == []


def test_partial_date_can_invoke_exporter(tmp_path: Path) -> None:
    target_date = NOW.date() - timedelta(days=1)
    _write_partition(
        tmp_path / "data",
        target_date,
        _closed_partial_slots(target_date, 8630),
    )
    for dataset in ("funding", "hourly_context", "quoted_market"):
        _write_ancillary_partition(tmp_path / "data", dataset, target_date)
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        dataset = kwargs["dataset"]
        assert isinstance(target, date)
        assert isinstance(dataset, str)
        if dataset == "market":
            calls.append(target)
        return _result(target, converted=False, uploaded=False, dataset=dataset)

    result = _run_kwargs(tmp_path, lookback_days=1, exporter=exporter)

    assert result.outcome == "no_work"
    assert calls == [target_date]
    assert result.inspections[0].quality == "partial"


def test_already_exported_partial_date_is_passed_over_to_newer_work(
    tmp_path: Path,
) -> None:
    older = NOW.date() - timedelta(days=2)
    newer = NOW.date() - timedelta(days=1)
    _write_partition(
        tmp_path / "data",
        older,
        _closed_partial_slots(older, 8630),
    )
    _write_complete_bundle(tmp_path / "data", newer)
    for dataset in ("funding", "hourly_context", "quoted_market"):
        _write_ancillary_partition(tmp_path / "data", dataset, older)
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        dataset = kwargs["dataset"]
        assert isinstance(target, date)
        assert isinstance(dataset, str)
        if dataset == "market":
            calls.append(target)
        return _result(
            target,
            converted=False,
            uploaded=target == newer,
            dataset=dataset,
        )

    result = _run_kwargs(tmp_path, lookback_days=2, exporter=exporter)

    assert result.outcome == "work_completed"
    assert calls == [older, newer]
    assert result.selected_date == newer
    assert result.export_result is not None
    assert result.export_result.uploaded is True


def test_only_one_date_performs_actual_work(tmp_path: Path) -> None:
    dates = [NOW.date() - timedelta(days=offset) for offset in (3, 2, 1)]
    for target_date in dates:
        _write_complete_bundle(tmp_path / "data", target_date)
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        dataset = kwargs["dataset"]
        assert isinstance(target, date)
        assert isinstance(dataset, str)
        if dataset == "market":
            calls.append(target)
        return _result(
            target,
            converted=target == dates[0],
            uploaded=target == dates[0],
            dataset=dataset,
        )

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
    _write_complete_bundle(tmp_path / "data", older)
    _write_complete_bundle(tmp_path / "data", newer)
    calls: list[date] = []

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        dataset = kwargs["dataset"]
        assert isinstance(target, date)
        assert isinstance(dataset, str)
        if dataset == "market":
            calls.append(target)
        raise failure

    result = _run_kwargs(tmp_path, lookback_days=2, exporter=exporter)

    assert result.outcome == "error"
    assert result.error == f"market export failed: {failure}"
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


@pytest.mark.parametrize("coverage", [0.0, 100.01])
def test_min_coverage_percentage_is_validated(
    tmp_path: Path,
    coverage: float,
) -> None:
    with pytest.raises(ValueError, match="min_coverage_pct"):
        analysis_daily.run_daily(
            data_root=tmp_path / "data",
            output_root=tmp_path / "analysis",
            remote="opportunity-drive-own",
            rclone=Path("/opt/homebrew/bin/rclone"),
            lookback_days=1,
            min_coverage_pct=coverage,
        )


def test_summary_reports_quality_and_partial_missing_slots(tmp_path: Path) -> None:
    target_date = NOW.date() - timedelta(days=1)
    _write_partition(
        tmp_path / "data",
        target_date,
        _closed_partial_slots(target_date, 8630),
    )

    result = _run_kwargs(
        tmp_path,
        lookback_days=1,
        exporter=lambda **kwargs: _result(
            kwargs["target_date"],  # type: ignore[arg-type]
            converted=False,
            uploaded=False,
        ),
    )

    summary = result.summary()
    assert "quality=partial" in summary
    assert "eligible=true" in summary
    assert "expected_slots=8640" in summary
    assert "missing_slots=10" in summary
    assert "missing_slot_timestamps=" in summary


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


def test_bundle_processes_all_datasets_for_one_selected_date(tmp_path: Path) -> None:
    target_date = NOW.date() - timedelta(days=1)
    _write_complete_bundle(tmp_path / "data", target_date)
    calls: list[tuple[date, str]] = []

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        dataset = kwargs["dataset"]
        assert isinstance(target, date)
        assert isinstance(dataset, str)
        calls.append((target, dataset))
        return _result(target, converted=True, uploaded=True, dataset=dataset)

    result = _run_kwargs(tmp_path, lookback_days=1, exporter=exporter)

    assert result.outcome == "work_completed"
    assert result.selected_date == target_date
    assert calls == [
        (target_date, "market"),
        (target_date, "funding"),
        (target_date, "hourly_context"),
        (target_date, "quoted_market"),
    ]
    assert len(result.dataset_runs) == 4
    assert {run.dataset for run in result.dataset_runs} == set(
        analysis_daily.DATASET_NAMES
    )


def test_partial_market_date_can_export_full_bundle(tmp_path: Path) -> None:
    target_date = NOW.date() - timedelta(days=1)
    data_root = tmp_path / "data"
    _write_partition(
        data_root,
        target_date,
        _closed_partial_slots(target_date, 8630),
    )
    for dataset in ("funding", "hourly_context", "quoted_market"):
        _write_ancillary_partition(data_root, dataset, target_date)
    calls: list[str] = []

    def exporter(**kwargs: object) -> ExportResult:
        dataset = kwargs["dataset"]
        assert isinstance(dataset, str)
        calls.append(dataset)
        return _result(
            kwargs["target_date"],  # type: ignore[arg-type]
            converted=True,
            uploaded=True,
            dataset=dataset,
        )

    result = _run_kwargs(tmp_path, lookback_days=1, exporter=exporter)

    assert result.outcome == "work_completed"
    assert calls == list(analysis_daily.DATASET_NAMES)


def test_rejected_market_never_exports_or_validates_ancillary(
    tmp_path: Path,
) -> None:
    target_date = NOW.date() - timedelta(days=1)
    _write_partition(
        tmp_path / "data",
        target_date,
        _closed_partial_slots(target_date, 8622),
    )
    calls: list[str] = []

    def exporter(**kwargs: object) -> ExportResult:
        calls.append(str(kwargs.get("dataset")))
        raise AssertionError("rejected market date must not export a bundle")

    result = _run_kwargs(tmp_path, lookback_days=1, exporter=exporter)

    assert result.outcome == "incomplete_only"
    assert calls == []


def test_malformed_ancillary_partition_fails_closed_before_export(
    tmp_path: Path,
) -> None:
    target_date = NOW.date() - timedelta(days=1)
    data_root = tmp_path / "data"
    _complete_partition(data_root, target_date)
    _write_ancillary_partition(
        data_root,
        "funding",
        target_date,
        malformed=True,
    )
    for dataset in ("hourly_context", "quoted_market"):
        _write_ancillary_partition(data_root, dataset, target_date)
    calls: list[str] = []

    def exporter(**kwargs: object) -> ExportResult:
        calls.append(str(kwargs.get("dataset")))
        raise AssertionError("malformed bundle must fail before export")

    result = _run_kwargs(tmp_path, lookback_days=1, exporter=exporter)

    assert result.outcome == "error"
    assert result.selected_date == target_date
    assert result.error is not None
    assert "funding" in result.error
    assert calls == []


def test_missing_ancillary_partition_fails_closed(tmp_path: Path) -> None:
    target_date = NOW.date() - timedelta(days=1)
    data_root = tmp_path / "data"
    _complete_partition(data_root, target_date)
    calls: list[str] = []

    def exporter(**kwargs: object) -> ExportResult:
        calls.append(str(kwargs.get("dataset")))
        raise AssertionError("missing ancillary data must fail before export")

    result = _run_kwargs(tmp_path, lookback_days=1, exporter=exporter)

    assert result.outcome == "error"
    assert result.error is not None
    assert "funding" in result.error
    assert calls == []


def test_only_one_date_can_perform_work_but_all_bundle_datasets_run(
    tmp_path: Path,
) -> None:
    older = NOW.date() - timedelta(days=2)
    newer = NOW.date() - timedelta(days=1)
    _write_complete_bundle(tmp_path / "data", older)
    _write_complete_bundle(tmp_path / "data", newer)
    calls: list[tuple[date, str]] = []

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        dataset = kwargs["dataset"]
        assert isinstance(target, date)
        assert isinstance(dataset, str)
        calls.append((target, dataset))
        actual_work = target == newer
        return _result(
            target,
            converted=actual_work,
            uploaded=actual_work,
            dataset=dataset,
        )

    result = _run_kwargs(tmp_path, lookback_days=2, exporter=exporter)

    assert result.outcome == "work_completed"
    assert result.selected_date == newer
    assert calls == [
        (older, "market"),
        (older, "funding"),
        (older, "hourly_context"),
        (older, "quoted_market"),
        (newer, "market"),
        (newer, "funding"),
        (newer, "hourly_context"),
        (newer, "quoted_market"),
    ]


def test_bundle_failure_stops_later_datasets_and_dates(tmp_path: Path) -> None:
    older = NOW.date() - timedelta(days=2)
    newer = NOW.date() - timedelta(days=1)
    _write_complete_bundle(tmp_path / "data", older)
    _write_complete_bundle(tmp_path / "data", newer)
    calls: list[tuple[date, str]] = []

    def exporter(**kwargs: object) -> ExportResult:
        target = kwargs["target_date"]
        dataset = kwargs["dataset"]
        assert isinstance(target, date)
        assert isinstance(dataset, str)
        calls.append((target, dataset))
        if dataset == "funding":
            raise RemoteMismatchError("mismatch")
        return _result(target, converted=False, uploaded=False, dataset=dataset)

    result = _run_kwargs(tmp_path, lookback_days=2, exporter=exporter)

    assert result.outcome == "error"
    assert result.selected_date == older
    assert calls == [(older, "market"), (older, "funding")]


def test_bundle_retry_resumes_after_partial_success_without_newer_date(
    tmp_path: Path,
) -> None:
    target_date = NOW.date() - timedelta(days=1)
    _write_complete_bundle(tmp_path / "data", target_date)
    phase = 0
    calls: list[str] = []

    def exporter(**kwargs: object) -> ExportResult:
        nonlocal phase
        dataset = kwargs["dataset"]
        assert isinstance(dataset, str)
        calls.append(dataset)
        if phase == 0 and dataset == "funding":
            raise ExportError("funding upload interrupted")
        actual_work = phase == 0 and dataset == "market" or (
            phase == 1 and dataset == "funding"
        )
        return _result(
            kwargs["target_date"],  # type: ignore[arg-type]
            converted=actual_work,
            uploaded=actual_work,
            dataset=dataset,
        )

    first = _run_kwargs(tmp_path, lookback_days=1, exporter=exporter)
    assert first.outcome == "error"
    assert calls == ["market", "funding"]

    phase = 1
    second = _run_kwargs(tmp_path, lookback_days=1, exporter=exporter)
    assert second.outcome == "work_completed"
    assert second.selected_date == target_date
    assert calls == [
        "market",
        "funding",
        "market",
        "funding",
        "hourly_context",
        "quoted_market",
    ]


def test_bundle_summary_includes_source_rows_and_dataset_results(
    tmp_path: Path,
) -> None:
    target_date = NOW.date() - timedelta(days=1)
    _write_complete_bundle(tmp_path / "data", target_date)

    def exporter(**kwargs: object) -> ExportResult:
        dataset = kwargs["dataset"]
        assert isinstance(dataset, str)
        return _result(
            kwargs["target_date"],  # type: ignore[arg-type]
            converted=False,
            uploaded=False,
            dataset=dataset,
        )

    summary = _run_kwargs(tmp_path, lookback_days=1, exporter=exporter).summary()

    assert "dataset=market" in summary
    assert "dataset=funding" in summary
    assert "dataset=hourly_context" in summary
    assert "dataset=quoted_market" in summary
    assert "source_rows=" in summary


def _main_args(tmp_path: Path) -> list[str]:
    return [
        "--data-root",
        str(tmp_path / "data"),
        "--output-root",
        str(tmp_path / "analysis"),
        "--remote",
        "opportunity-drive-own",
        "--rclone",
        "/opt/homebrew/bin/rclone",
        "--lookback-days",
        "1",
    ]


def _timing_values(output: str) -> tuple[datetime, datetime, float]:
    values = dict(
        line.split("=", 1)
        for line in output.splitlines()
        if line.startswith(("run_started_at=", "run_finished_at=", "run_duration_seconds="))
    )
    started = datetime.fromisoformat(values["run_started_at"].replace("Z", "+00:00"))
    finished = datetime.fromisoformat(values["run_finished_at"].replace("Z", "+00:00"))
    return started, finished, float(values["run_duration_seconds"])


def test_main_emits_timing_and_preserves_summary_for_no_work(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = analysis_daily.main(_main_args(tmp_path))

    assert exit_code == 0
    output = capsys.readouterr().out
    started, finished, duration = _timing_values(output)
    assert started.tzinfo is not None
    assert finished.tzinfo is not None
    assert finished >= started
    assert duration >= 0
    assert "current_utc_date=" in output
    assert "outcome=incomplete_only" in output


def test_main_emits_finish_timing_for_handled_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(**kwargs: object) -> DailyRunResult:
        raise ValueError("synthetic failure")

    monkeypatch.setattr(analysis_daily, "run_daily", fail)

    exit_code = analysis_daily.main(_main_args(tmp_path))

    assert exit_code == 2
    output = capsys.readouterr().out
    started, finished, duration = _timing_values(output)
    assert finished >= started
    assert duration >= 0
    assert "outcome=error error=synthetic failure" in output


def test_main_preserves_result_error_exit_code(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error_result = DailyRunResult(
        current_date=date(2026, 9, 30),
        lookback_days=1,
        min_coverage_pct=99.8,
        inspections=(),
        outcome="error",
        error="runner error",
    )
    monkeypatch.setattr(analysis_daily, "run_daily", lambda **kwargs: error_result)

    exit_code = analysis_daily.main(_main_args(tmp_path))

    assert exit_code == 1
    output = capsys.readouterr().out
    started, finished, duration = _timing_values(output)
    assert finished >= started
    assert duration >= 0
    assert "outcome=error" in output
    assert "error=runner error" in output
