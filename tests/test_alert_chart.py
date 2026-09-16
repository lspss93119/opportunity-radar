from __future__ import annotations

from datetime import UTC, datetime, timedelta

import matplotlib.pyplot as plt

from radar.alerts.models import SpreadAlertDetails
from radar.history.spread import (
    HistoricalSpreadContext,
    HistoricalSpreadPoint,
    WindowStats,
)

SAMPLE_TIME = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


def make_details():
    return SpreadAlertDetails(
        canonical_symbol="BTC",
        long_venue="lighter",
        long_venue_symbol="BTC",
        short_venue="hyperliquid",
        short_venue_symbol="BTC",
        primary_size_usd=10_000,
        long_buy_vwap=100.0,
        short_sell_vwap=101.0,
        raw_spread_bps=100.0,
        long_fee_bps=4.5,
        short_fee_bps=3.5,
        net_spread_bps=92.0,
        sample_time=SAMPLE_TIME,
        candidate_duration_seconds=30,
        alert_duration_seconds=120,
        long_funding=None,
        short_funding=None,
    )


def make_context(point_count: int) -> HistoricalSpreadContext:
    points = tuple(
        HistoricalSpreadPoint(
            sample_time=SAMPLE_TIME - timedelta(days=point_count - index - 1),
            raw_spread_bps=50.0 + index * 10.0,
        )
        for index in range(point_count)
    )
    return HistoricalSpreadContext(
        points_7d=points,
        stats_7d=WindowStats(point_count, 60.0),
        stats_30d=WindowStats(4, 70.0),
        stats_90d=WindowStats(8, 80.0),
    )


def test_render_spread_chart_returns_png_and_closes_figure(tmp_path, monkeypatch):
    from radar.alerts.chart import render_spread_chart

    monkeypatch.chdir(tmp_path)
    before = set(plt.get_fignums())

    chart = render_spread_chart(make_details(), make_context(3))

    assert chart is not None
    assert chart[:8] == b"\x89PNG\r\n\x1a\n"
    assert set(plt.get_fignums()) == before
    assert tuple(tmp_path.iterdir()) == ()


def test_render_spread_chart_returns_none_for_empty_or_one_point_context():
    from radar.alerts.chart import render_spread_chart

    details = make_details()

    assert render_spread_chart(details, HistoricalSpreadContext.empty()) is None
    assert render_spread_chart(details, make_context(1)) is None
