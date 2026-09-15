import os
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from radar.collectors.base import CollectorBatch
from radar.models import FundingSnapshot, HourlyContext, MarketSnapshot
from radar.storage.parquet import ParquetStorage

UTC = timezone.utc


def make_market(sample_time: datetime, symbol: str = "BTC") -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=sample_time.replace(microsecond=123000),
        venue="lighter",
        venue_symbol=symbol,
        canonical_symbol=symbol,
        best_bid=99.0,
        best_bid_size=2.0,
        best_ask=100.0,
        best_ask_size=3.0,
        mark_price=99.5,
        index_price=99.4,
        buy_1k_vwap=100.1,
        sell_1k_vwap=98.9,
        buy_5k_vwap=100.2,
        sell_5k_vwap=98.8,
        buy_10k_vwap=100.3,
        sell_10k_vwap=98.7,
    )


def make_funding(effective_time: datetime, symbol: str = "BTC") -> FundingSnapshot:
    return FundingSnapshot(
        effective_time=effective_time,
        observed_at=effective_time.replace(microsecond=456000),
        venue="lighter",
        venue_symbol=symbol,
        canonical_symbol=symbol,
        funding_rate=0.000123,
        next_funding_time=None,
    )


def make_hourly(sample_time: datetime, symbol: str = "BTC") -> HourlyContext:
    return HourlyContext(
        sample_time=sample_time,
        observed_at=sample_time.replace(microsecond=789000),
        venue="lighter",
        venue_symbol=symbol,
        canonical_symbol=symbol,
        open_interest=123.4,
        volume_24h=567890.1,
    )


def query_dataset(root, dataset: str, columns: str):
    glob = str(root / dataset / "date=*" / "*.parquet").replace("'", "''")
    with duckdb.connect() as connection:
        return connection.execute(
            f"SELECT {columns} FROM read_parquet('{glob}') ORDER BY venue_symbol"
        ).fetchall()


def test_batch_append_flushes_separate_queryable_datasets_without_tiny_files(tmp_path):
    root = tmp_path / "data"
    first_time = datetime(2026, 9, 15, 12, 0, 0, tzinfo=UTC)
    second_time = first_time + timedelta(seconds=10)
    store = ParquetStorage(root)

    store.append(
        CollectorBatch(
            market_snapshots=(make_market(first_time),),
            funding_snapshots=(make_funding(first_time),),
            hourly_contexts=(make_hourly(first_time.replace(minute=0)),),
        )
    )
    store.append(
        CollectorBatch(
            market_snapshots=(make_market(second_time, "ETH"),),
            funding_snapshots=(make_funding(second_time, "ETH"),),
            hourly_contexts=(make_hourly(first_time.replace(minute=0), "ETH"),),
        )
    )

    assert store.pending_count == 6
    assert store.flush(now=second_time) == 3
    assert store.pending_count == 0
    assert all(
        len(tuple((root / dataset).glob("date=*/*.parquet"))) == 1
        for dataset in ("market", "funding", "hourly_context")
    )

    market_rows = query_dataset(
        root,
        "market",
        "sample_time, observed_at, venue, venue_symbol, buy_10k_vwap",
    )
    assert len(market_rows) == 2
    assert market_rows[0][0].astimezone(UTC) == first_time
    assert market_rows[0][1].astimezone(UTC) == first_time.replace(microsecond=123000)
    assert market_rows[0][2:] == ("lighter", "BTC", 100.3)
    assert market_rows[1][2:] == ("lighter", "ETH", 100.3)

    funding_rows = query_dataset(
        root,
        "funding",
        "effective_time, observed_at, venue, venue_symbol, funding_rate",
    )
    assert len(funding_rows) == 2
    assert funding_rows[0][0].astimezone(UTC) == first_time
    assert funding_rows[0][2:] == ("lighter", "BTC", pytest.approx(0.000123))

    hourly_rows = query_dataset(
        root,
        "hourly_context",
        "sample_time, observed_at, venue, venue_symbol, open_interest, volume_24h",
    )
    assert len(hourly_rows) == 2
    assert hourly_rows[0][0].astimezone(UTC) == first_time.replace(minute=0)
    assert hourly_rows[0][2:] == (
        "lighter",
        "BTC",
        pytest.approx(123.4),
        pytest.approx(567890.1),
    )


def test_individual_normalized_models_are_accepted_and_existing_parquet_reopens(tmp_path):
    root = tmp_path / "data"
    timestamp = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    store = ParquetStorage(root)
    store.append(make_market(timestamp))
    store.append(make_funding(timestamp))
    store.append(make_hourly(timestamp))
    store.flush(now=timestamp)

    reopened = ParquetStorage(root)
    assert reopened.pending_count == 0
    assert len(query_dataset(root, "market", "*")) == 1
    assert len(query_dataset(root, "funding", "*")) == 1
    assert len(query_dataset(root, "hourly_context", "*")) == 1


def test_failed_write_leaves_existing_parquet_untouched(tmp_path, monkeypatch):
    root = tmp_path / "data"
    timestamp = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    store = ParquetStorage(root)
    store.append(make_market(timestamp))
    store.flush(now=timestamp)
    existing_file = next((root / "market").glob("date=*/*.parquet"))
    existing_bytes = existing_file.read_bytes()

    store.append(make_market(timestamp + timedelta(seconds=10), "ETH"))

    def fail_write(*args, **kwargs):
        raise OSError("simulated parquet write failure")

    monkeypatch.setattr("radar.storage.parquet.pq.write_table", fail_write)
    with pytest.raises(OSError, match="simulated"):
        store.flush(now=timestamp + timedelta(seconds=10))

    assert existing_file.read_bytes() == existing_bytes
    assert store.pending_count == 1
    assert not tuple((root / "market").glob("date=*/*.tmp"))
    assert len(query_dataset(root, "market", "*")) == 1


def test_partial_file_commit_rolls_back_without_duplicate_retry_rows(tmp_path, monkeypatch):
    root = tmp_path / "data"
    timestamp = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    store = ParquetStorage(root)
    store.append(make_market(timestamp))
    store.append(make_funding(timestamp))

    original_replace = os.replace
    replace_calls = 0

    def fail_on_second_replace(source, destination):
        nonlocal replace_calls
        replace_calls += 1
        if replace_calls == 2:
            raise OSError("simulated replace failure")
        original_replace(source, destination)

    monkeypatch.setattr("radar.storage.parquet.os.replace", fail_on_second_replace)
    with pytest.raises(OSError, match="simulated replace failure"):
        store.flush(now=timestamp)

    assert store.pending_count == 2
    assert not tuple(root.glob("**/*.tmp"))
    assert not tuple(root.glob("**/*.parquet"))

    monkeypatch.setattr("radar.storage.parquet.os.replace", original_replace)
    assert store.flush(now=timestamp) == 2
    assert len(query_dataset(root, "market", "*")) == 1
    assert len(query_dataset(root, "funding", "*")) == 1


def test_retention_removes_expired_date_partitions_and_keeps_boundary_date(tmp_path):
    root = tmp_path / "data"
    now = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    old_time = now - timedelta(days=91)
    boundary_time = now - timedelta(days=90)
    inside_time = now - timedelta(days=1)
    store = ParquetStorage(root)
    for timestamp, symbol in (
        (old_time, "OLD"),
        (boundary_time, "BOUNDARY"),
        (inside_time, "INSIDE"),
    ):
        store.append(make_market(timestamp, symbol))

    store.flush(now=old_time)
    assert store.prune(now=now) == 1

    market_partitions = {
        path.name.removeprefix("date=")
        for path in (root / "market").glob("date=*")
    }
    assert old_time.date().isoformat() not in market_partitions
    assert boundary_time.date().isoformat() in market_partitions
    assert inside_time.date().isoformat() in market_partitions
    assert {row[0] for row in query_dataset(root, "market", "venue_symbol")} == {
        "BOUNDARY",
        "INSIDE",
    }
