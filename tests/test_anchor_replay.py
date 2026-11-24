from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from radar.history.anchor_replay import (
    PairArrays,
    ReplayParams,
    load_symbol,
    pair_arrays,
    simulate,
    summarize,
)

T0 = datetime(2026, 11, 1, tzinfo=UTC)


def arrays_from_spread(spread_bps, *, anchor_bps, step=10.0, times=None) -> PairArrays:
    """Long leg flat at 100 (no bid/ask gap); the short leg carries the spread."""
    spread = np.asarray(spread_bps, dtype=float)
    n = len(spread)
    long_px = np.full(n, 100.0)
    short_px = 100.0 * (1.0 + spread / 10_000.0)
    anchor = np.broadcast_to(np.asarray(anchor_bps, dtype=float), (n,)).copy()
    return PairArrays(
        times=np.arange(n) * step if times is None else np.asarray(times, dtype=float),
        long_buy=long_px,
        long_sell=long_px,
        short_sell=short_px,
        short_buy=short_px,
        anchors={"mean_24h": anchor, "median_2h": anchor},
    )


def sim(arrays, threshold=15.0, **kwargs):
    options = dict(fee_bps_total=0.0, return_band_bps=5.0, max_hold_seconds=3_600.0)
    options.update(kwargs)
    return simulate(arrays, "mean_24h", threshold, **options)


def test_entry_exit_and_pnl_formula():
    # Entry at 20 bps over a 0 anchor, returns to 4 bps (within the 5 bps band).
    arrays = arrays_from_spread([0, 0, 20, 25, 10, 4, 0, 0], anchor_bps=0.0)

    stats = sim(arrays, fee_bps_total=13.0)

    assert stats.trades == 1 and stats.returned == 1
    # short leg: 1 - 100.04/100.20 ; long leg flat
    expected = (1 - 100.04 / 100.20) * 10_000 - 13.0
    assert stats.pnl_bps[0] == pytest.approx(expected)
    assert stats.return_seconds == [30.0]
    assert stats.adverse_bps == [pytest.approx(5.0)]


def test_no_reentry_until_deviation_drops_below_threshold():
    # One long excursion above threshold that returns, then rises again without
    # dipping below the threshold in between counts once; a fresh rise re-arms.
    spread = [0, 20, 30, 3, 0, 18, 18, 2, 0]
    stats = sim(arrays_from_spread(spread, anchor_bps=0.0))
    assert stats.trades == 2

    # Exit band above the threshold: the exit sample is still "above", so the
    # next entry must wait for a sample below the threshold.
    stats = sim(
        arrays_from_spread([0, 20, 18, 18, 18, 0, 20, 0], anchor_bps=0.0),
        return_band_bps=19.0,
    )
    assert stats.trades == 2


def test_anchor_frozen_at_entry():
    spread = [0, 20, 20, 20, 20]
    anchor = [0, 0, 30, 30, 30]  # moving anchor must not trigger an exit
    stats = sim(arrays_from_spread(spread, anchor_bps=anchor), max_hold_seconds=1e9)
    assert stats.trades == 0 and stats.censored == 1


def test_not_returned_after_max_hold_records_adverse_excursion():
    spread = [0, 20, 35, 50, 30, 30, 30, 30, 30, 30, 30]
    stats = sim(arrays_from_spread(spread, anchor_bps=0.0), max_hold_seconds=40.0)

    assert stats.trades == 1 and stats.returned == 0
    assert stats.not_returned_adverse_bps == [pytest.approx(30.0)]
    summary = summarize(stats)
    assert summary["return_rate"] == 0.0
    assert summary["not_returned_max_adverse_bps"] == pytest.approx(30.0)


def test_missing_anchor_and_gaps_block_entries():
    nan = float("nan")
    stats = sim(arrays_from_spread([0, 20, 0], anchor_bps=[nan, nan, nan]))
    assert stats.trades == 0

    gapped = arrays_from_spread([0, 20, 0, 0], anchor_bps=0.0, times=[0, 60, 70, 80])
    assert sim(gapped).trades == 0


def _write_market(root: Path, rows: list[dict]) -> None:
    part = root / "market" / "date=2026-11-01"
    part.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), part / "part-0.parquet")


def test_anchors_use_prior_points_only_and_require_coverage(tmp_path):
    rows = []
    n = 900  # 2.5h of 10s samples
    for i in range(n):
        t = T0 + timedelta(seconds=10 * i)
        # B is 10 bps rich, except the last sample, which spikes to 60 bps.
        b_px = 100.6 if i == n - 1 else 100.1
        for venue, buy, sell in (("a", 100.0, 100.0), ("b", b_px, b_px)):
            rows.append(
                dict(
                    canonical_symbol="X",
                    venue=venue,
                    venue_symbol="X",
                    sample_time=t,
                    observed_at=t,
                    buy=buy,
                    sell=sell,
                )
            )
    path = tmp_path / "replay.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)

    params = ReplayParams()
    with duckdb.connect() as connection:
        load_symbol(connection, path, "X", params)
        arrays = pair_arrays(connection, ("a", "X"), ("b", "X"), params)

    assert arrays is not None and len(arrays.times) == n
    median = arrays.anchors["median_2h"]
    # Needs 80% of a 2h window (576 samples) before the anchor exists.
    assert np.isnan(median[575]) and not np.isnan(median[577])
    # The spike is excluded from its own anchor.
    assert median[-1] == pytest.approx(10.0)
    assert np.all(np.isnan(arrays.anchors["mean_24h"]))  # never 80% of 24h


def test_cli_runs_once_and_writes_outputs(tmp_path):
    from radar.history.anchor_replay import main

    rows = []
    for i in range(3):
        t = T0 + timedelta(seconds=10 * i)
        for venue in ("lighter", "hyperliquid"):
            rows.append(
                dict(
                    sample_time=t,
                    observed_at=t,
                    venue=venue,
                    venue_symbol="BTC",
                    canonical_symbol="BTC",
                    buy_1k_vwap=100.0,
                    sell_1k_vwap=99.9,
                )
            )
    _write_market(tmp_path / "data", rows)
    config = tmp_path / "radar.yaml"
    config.write_text(
        "fees_bps: {lighter: 0.0, hyperliquid: 4.5}\n"
        "markets:\n"
        "  - {venue: lighter, venue_symbol: BTC, canonical_symbol: BTC}\n"
        "  - {venue: hyperliquid, venue_symbol: BTC, canonical_symbol: BTC}\n",
        encoding="utf-8",
    )

    assert (
        main(
            [
                "--config",
                str(config),
                "--data-root",
                str(tmp_path / "data"),
                "--work-dir",
                str(tmp_path / "work"),
                "--output-csv",
                str(tmp_path / "out.csv"),
                "--output-json",
                str(tmp_path / "out.json"),
            ]
        )
        == 0
    )

    summary = json.loads((tmp_path / "out.json").read_text())
    assert summary["pairs"] == 2
    assert {(p["anchor"], p["threshold_bps"]) for p in summary["pooled"]} == {
        (a, t) for a in ("mean_24h", "median_2h") for t in (15.0, 25.0, 40.0)
    }
    assert (tmp_path / "out.csv").read_text().startswith("symbol,")
