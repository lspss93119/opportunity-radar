from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import radar.history.manual_opportunity as replay_module
from radar.config import ManualOpportunityConfig, MarketConfig, RadarConfig
from radar.history.manual_opportunity import BboWindowStats, replay_manual_opportunity
from radar.models import HourlyContext, MarketSnapshot
from radar.storage.parquet import HOURLY_CONTEXT_SCHEMA, MARKET_SCHEMA


START = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)


class _StableHistory:
    def observe(self, sample_time: datetime, raw_spread_bps: float):
        del sample_time, raw_spread_bps
        return {
            name: BboWindowStats(100, 1.0, 20.0, True)
            for name in ("2h", "24h", "3d")
        }


def _write_dataset(root: Path) -> None:
    market_rows: list[dict[str, object]] = []
    for offset in range(-60, 71, 10):
        when = START + timedelta(seconds=offset)
        high_spread = offset >= 0
        short_bid = 100.5 if high_spread else 100.2
        market_rows.extend(
            [
                MarketSnapshot(
                    sample_time=when,
                    observed_at=when,
                    venue="arcus",
                    venue_symbol="QQQ-USD",
                    canonical_symbol="QQQ",
                    best_bid=99.0,
                    best_bid_size=1.0,
                    best_ask=100.0,
                    best_ask_size=1.0,
                ).model_dump(mode="python"),
                MarketSnapshot(
                    sample_time=when,
                    observed_at=when,
                    venue="lighter_robinhood",
                    venue_symbol="QQQ",
                    canonical_symbol="QQQ",
                    best_bid=short_bid,
                    best_bid_size=1.0,
                    best_ask=101.0,
                    best_ask_size=1.0,
                ).model_dump(mode="python"),
            ]
        )
    market_partition = root / "market" / "date=2026-01-01"
    market_partition.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(market_rows, schema=MARKET_SCHEMA),
        market_partition / "part-0000.parquet",
    )

    context_rows = [
        HourlyContext(
            sample_time=START - timedelta(seconds=60),
            observed_at=START - timedelta(seconds=60),
            venue=venue,
            venue_symbol=symbol,
            canonical_symbol="QQQ",
            volume_24h=2_000_000.0,
        ).model_dump(mode="python")
        for venue, symbol in (("arcus", "QQQ-USD"), ("lighter_robinhood", "QQQ"))
    ]
    context_partition = root / "hourly_context" / "date=2026-01-01"
    context_partition.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(context_rows, schema=HOURLY_CONTEXT_SCHEMA),
        context_partition / "part-0000.parquet",
    )


def test_bbo_replay_recalculates_route_and_reports_confirmation(monkeypatch, tmp_path):
    _write_dataset(tmp_path / "data")
    monkeypatch.setattr(replay_module, "BboRollingHistory", _StableHistory)
    config = RadarConfig(
        fees_bps={"arcus": 2.25, "lighter_robinhood": 0.0},
        markets=[
            MarketConfig(
                venue="arcus",
                venue_symbol="QQQ-USD",
                canonical_symbol="QQQ",
            ),
            MarketConfig(
                venue="lighter_robinhood",
                venue_symbol="QQQ",
                canonical_symbol="QQQ",
            ),
        ],
        manual_opportunity=ManualOpportunityConfig(),
    )

    result = replay_manual_opportunity(
        data_root=tmp_path / "data",
        config=config,
        start=START,
        end=START + timedelta(seconds=70),
        output_dir=tmp_path / "out",
    )

    assert result.report["episodes"]["initial_notifications"] == 1
    assert result.report["episodes"]["final_unique_manual_opportunity_episodes"] == 1
    assert result.report["sanity"]["runtime_store_writes"] is False
    assert (tmp_path / "out" / "report.json").exists()
