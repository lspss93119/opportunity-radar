from __future__ import annotations

from dataclasses import replace
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
        rolling_mean_bps=80.0,
        rolling_std_bps=2.5,
        deviation_bps=20.0,
        signal_duration_seconds=120,
        observed_at_skew_seconds=0.5,
        round_trip_fee_bps=16.0,
        theoretical_edge_bps=4.0,
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


def test_chart_downsampling_preserves_endpoints_and_caps_history_points():
    from radar.alerts.chart import _downsample_points

    values = [10.0] * 601
    values[487] = 100.0
    values[510] = -100.0
    points = tuple(
        HistoricalSpreadPoint(
            SAMPLE_TIME + timedelta(seconds=index),
            value,
        )
        for index, value in enumerate(values)
    )
    sampled_points = _downsample_points(points)

    assert len(sampled_points) == 500
    assert sampled_points[0] == points[0]
    assert sampled_points[-1] == points[-1]
    assert points[487] in sampled_points
    assert points[510] in sampled_points


def test_render_spread_chart_supports_threshold_reference_lines_with_dense_history():
    from radar.alerts.chart import render_spread_chart

    chart = render_spread_chart(
        make_details(),
        make_context(601),
        candidate_net_bps=12.5,
        alert_net_bps=25.0,
    )

    assert chart is not None
    assert chart[:8] == b"\x89PNG\r\n\x1a\n"


def test_render_spread_chart_includes_all_reference_lines(monkeypatch):
    from matplotlib.axes import Axes

    from radar.alerts.chart import render_spread_chart

    lines: list[float] = []
    labels: list[str] = []
    original_axhline = Axes.axhline
    original_text = Axes.text

    def capture_axhline(self, y=0, *args, **kwargs):
        lines.append(float(y))
        return original_axhline(self, y, *args, **kwargs)

    def capture_text(self, x, y, s, *args, **kwargs):
        labels.append(str(s))
        return original_text(self, x, y, s, *args, **kwargs)

    monkeypatch.setattr(Axes, "axhline", capture_axhline)
    monkeypatch.setattr(Axes, "text", capture_text)

    assert (
        render_spread_chart(
            make_details(),
            HistoricalSpreadContext(
                points_7d=make_context(3).points_7d,
                stats_7d=WindowStats(3, 60.0),
                stats_30d=WindowStats(4, 70.0),
                stats_90d=WindowStats(8, 80.0),
            ),
            candidate_net_bps=12.5,
            alert_net_bps=25.0,
        )
        is not None
    )

    assert lines == [100.0, 80.0, 60.0, 70.0, 80.0]
    assert {"Current", "24h mean", "7d median", "30d median", "90d median"} <= set(
        labels
    )
    assert "Candidate" not in labels
    assert "Alert" not in labels


def test_chart_displays_only_the_most_recent_24_hours():
    from radar.alerts.chart import _recent_points

    points = tuple(
        HistoricalSpreadPoint(
            SAMPLE_TIME - timedelta(hours=48) + timedelta(hours=index),
            float(index),
        )
        for index in range(49)
    )

    displayed = _recent_points(points)

    assert len(displayed) == 25
    assert displayed[0].sample_time == SAMPLE_TIME - timedelta(hours=24)
    assert displayed[-1].sample_time == SAMPLE_TIME


def test_chart_collapses_nearly_identical_historical_medians_and_omits_legend(
    monkeypatch,
):
    from matplotlib.axes import Axes

    from radar.alerts.chart import render_spread_chart

    lines: list[float] = []
    labels: list[str] = []
    legend_calls: list[bool] = []
    original_axhline = Axes.axhline
    original_text = Axes.text

    def capture_axhline(self, y=0, *args, **kwargs):
        lines.append(float(y))
        return original_axhline(self, y, *args, **kwargs)

    def capture_text(self, x, y, s, *args, **kwargs):
        labels.append(str(s))
        return original_text(self, x, y, s, *args, **kwargs)

    def capture_legend(self, *args, **kwargs):
        legend_calls.append(True)
        return None

    monkeypatch.setattr(Axes, "axhline", capture_axhline)
    monkeypatch.setattr(Axes, "text", capture_text)
    monkeypatch.setattr(Axes, "legend", capture_legend)

    context = HistoricalSpreadContext(
        points_7d=make_context(3).points_7d,
        stats_7d=WindowStats(3, 60.0),
        stats_30d=WindowStats(4, 60.005),
        stats_90d=WindowStats(8, 59.997),
    )
    assert render_spread_chart(make_details(), context) is not None

    assert sum(abs(value - 60.0) < 0.01 for value in lines) == 1
    assert "7d/30d/90d median" in labels
    assert not legend_calls


def test_chart_summary_prioritizes_baseline_and_deviation(monkeypatch):
    from matplotlib.axes import Axes

    from radar.alerts.chart import render_spread_chart

    summaries: list[str] = []
    original_text = Axes.text

    def capture_text(self, x, y, s, *args, **kwargs):
        if "Current raw" in str(s):
            summaries.append(str(s))
        return original_text(self, x, y, s, *args, **kwargs)

    monkeypatch.setattr(Axes, "text", capture_text)

    assert render_spread_chart(make_details(), make_context(3)) is not None

    assert summaries == [
        "\n".join(
            (
                "Current raw: 100.00 bps",
                "Baseline: 80.00 bps",
                "Deviation: +20.00 bps",
                "Net spread: 92.00 bps",
            )
        )
    ]


def test_chart_stacks_close_right_edge_reference_labels(monkeypatch):
    from matplotlib.axes import Axes

    from radar.alerts.chart import render_spread_chart

    positions: dict[str, float] = {}
    original_text = Axes.text

    def capture_text(self, x, y, s, *args, **kwargs):
        if str(s) in {"Current", "7d/30d/90d median"}:
            positions[str(s)] = float(y)
        return original_text(self, x, y, s, *args, **kwargs)

    monkeypatch.setattr(Axes, "text", capture_text)

    details = replace(make_details(), raw_spread_bps=60.2)
    context = HistoricalSpreadContext(
        points_7d=make_context(3).points_7d,
        stats_7d=WindowStats(3, 60.0),
        stats_30d=WindowStats(4, 60.005),
        stats_90d=WindowStats(8, 59.997),
    )
    assert render_spread_chart(details, context) is not None

    assert positions["Current"] - positions["7d/30d/90d median"] >= 2.0
