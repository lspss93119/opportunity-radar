from __future__ import annotations

from datetime import UTC, datetime, timedelta

from matplotlib.axes import Axes
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from radar.alerts.manual_chart import (
    _configure_time_axis,
    _downsample_manual_points,
    _manual_reference_lines,
    _manual_line_style,
    _series_with_gap_breaks,
    render_manual_opportunity_chart,
)
from radar.alerts.manual_opportunity import ManualOpportunityAlertDetails
from radar.history.manual_opportunity import (
    ManualOpportunityHistoryContext,
    ManualOpportunityHistoryPoint,
)


START = datetime(2026, 9, 28, 10, 30, tzinfo=UTC)


def _details() -> ManualOpportunityAlertDetails:
    return ManualOpportunityAlertDetails(
        event_kind="manual_initial",
        episode_id="episode-1",
        canonical_symbol="QQQ",
        long_venue="arcus",
        long_venue_symbol="QQQ-USD",
        short_venue="lighter_robinhood",
        short_venue_symbol="QQQ",
        signal_duration_seconds=60,
        current_spread_bps=30.2,
        reference_a_bps=9.08,
        deviation_bps=21.12,
        round_trip_fee_bps=4.5,
        expected_net_at_a_bps=16.62,
        mean_2h_bps=9.72,
        mean_24h_bps=9.08,
        mean_3d_bps=8.9,
        baseline_range_bps=0.82,
        long_volume_24h=2_000_000.0,
        short_volume_24h=3_000_000.0,
        route_volume_24h=2_000_000.0,
        long_best_ask=100.0,
        short_best_bid=100.302,
        candidate_started_at=START,
        confirmed_at=START + timedelta(seconds=60),
        sample_time=START + timedelta(seconds=60),
        expansion_level_bps=None,
    )


def _context() -> ManualOpportunityHistoryContext:
    return ManualOpportunityHistoryContext(
        points=tuple(
            ManualOpportunityHistoryPoint(START + timedelta(seconds=offset), value)
            for offset, value in (
                (0, 9.0),
                (10, 8.5),
                (20, 31.0),
                (30, 9.5),
                (40, 30.2),
                (60, 30.2),
            )
        )
    )


def test_manual_chart_uses_fee_adjusted_entry_threshold():
    details = _details()

    lines = _manual_reference_lines(details, min_profit_bps=10.0)

    assert ("Frozen a (24h mean)", 9.08) in lines
    assert ("2h mean", 9.72) in lines
    assert ("24h mean", 9.08) not in lines
    assert ("3d mean", 8.9) in lines
    assert ("Entry threshold", 23.58) in lines


def test_manual_chart_keeps_distinct_current_24h_mean_subtle():
    details = _details()
    details = details.__class__(
        **{
            **details.__dict__,
            "mean_24h_bps": 10.0,
        }
    )

    lines = _manual_reference_lines(details)

    assert ("24h mean (current)", 10.0) in lines
    assert _manual_line_style("24h mean (current)")["linewidth"] < _manual_line_style(
        "Frozen a (24h mean)"
    )["linewidth"]


def test_manual_chart_visual_hierarchy_prioritizes_current_entry_and_frozen_a():
    assert _manual_line_style("Current")["linewidth"] > _manual_line_style("2h mean")["linewidth"]
    assert _manual_line_style("Entry threshold")["linewidth"] > _manual_line_style("3d mean")["linewidth"]
    assert _manual_line_style("Frozen a (24h mean)")["linewidth"] > _manual_line_style("2h mean")["linewidth"]


def test_manual_chart_uses_one_compact_reference_legend(monkeypatch):
    legend_labels: list[list[str]] = []
    original_legend = Axes.legend

    def capture_legend(self, *args, **kwargs):
        handles = kwargs["handles"]
        legend_labels.append([handle.get_label() for handle in handles])
        return original_legend(self, *args, **kwargs)

    monkeypatch.setattr(Axes, "legend", capture_legend)

    png = render_manual_opportunity_chart(_details(), _context())

    assert png is not None
    assert legend_labels == [
        [
            "Current",
            "Frozen a (24h mean)",
            "2h mean",
            "3d mean",
            "Entry threshold",
        ]
    ]


def test_manual_chart_uses_seconds_for_short_windows_and_concise_24h_format():
    figure, axis = plt.subplots()
    try:
        _configure_time_axis(
            axis,
            (START, START + timedelta(minutes=1)),
        )
        axis.set_xlim(
            mdates.date2num(START),
            mdates.date2num(START + timedelta(minutes=1)),
        )
        figure.canvas.draw()
        short_formatter = axis.xaxis.get_major_formatter()
        assert getattr(short_formatter, "fmt", None) == "%H:%M:%S"
        short_labels = [
            tick.get_text()
            for tick in axis.get_xticklabels()
            if tick.get_text()
        ]
        assert len(short_labels) == len(set(short_labels))

        _configure_time_axis(
            axis,
            (START - timedelta(hours=24), START),
        )
        assert isinstance(
            axis.xaxis.get_major_formatter(),
            mdates.ConciseDateFormatter,
        )
    finally:
        plt.close(figure)


def test_manual_chart_renders_directional_bbo_png_and_event_markers(monkeypatch):
    labels: list[tuple[str, dict[str, object]]] = []
    original_annotate = Axes.annotate

    def capture_annotate(self, text, *args, **kwargs):
        labels.append((str(text), kwargs))
        return original_annotate(self, text, *args, **kwargs)

    monkeypatch.setattr(Axes, "annotate", capture_annotate)

    png = render_manual_opportunity_chart(
        _details(),
        _context(),
        min_profit_bps=10.0,
    )

    assert png is not None
    assert png.startswith(b"\x89PNG\r\n\x1a\n")
    assert {label for label, _kwargs in labels} >= {
        "Candidate start",
        "Confirmation",
        "Current",
    }
    current_kwargs = dict(next(kwargs for label, kwargs in labels if label == "Current"))
    confirmation_kwargs = dict(
        next(kwargs for label, kwargs in labels if label == "Confirmation")
    )
    assert current_kwargs["annotation_clip"] is False
    assert confirmation_kwargs["annotation_clip"] is False
    assert current_kwargs["ha"] == "right"
    assert confirmation_kwargs["ha"] == "right"


def test_manual_chart_returns_none_without_enough_history():
    context = ManualOpportunityHistoryContext(
        points=(ManualOpportunityHistoryPoint(START, 9.0),)
    )

    assert render_manual_opportunity_chart(_details(), context) is None


def test_manual_chart_renders_full_recent_24h_history_not_only_candidate_window(
    monkeypatch,
):
    history_start = START - timedelta(hours=24)
    points = tuple(
        ManualOpportunityHistoryPoint(
            history_start + timedelta(minutes=10 * index),
            9.0 + (index % 7) * 0.2,
        )
        for index in range(145)
    )
    context = ManualOpportunityHistoryContext(points=points)
    plotted: list[tuple[list[float], list[float]]] = []
    original_plot = Axes.plot

    def capture_plot(self, x, y, *args, **kwargs):
        plotted.append((list(x), list(y)))
        return original_plot(self, x, y, *args, **kwargs)

    monkeypatch.setattr(Axes, "plot", capture_plot)
    png = render_manual_opportunity_chart(_details(), context)

    assert png is not None
    assert plotted
    first_x, last_x = plotted[0][0][0], plotted[0][0][-1]
    assert first_x <= mdates.date2num(history_start)
    assert last_x >= mdates.date2num(history_start + timedelta(hours=24))


def test_manual_downsample_bounds_dense_day_and_preserves_event_and_threshold_points():
    points = tuple(
        ManualOpportunityHistoryPoint(
            START + timedelta(seconds=10 * index),
            0.0 if index not in {321, 4321, 8000} else 25.0,
        )
        for index in range(8_641)
    )
    event_times = tuple(points[index].sample_time for index in (321, 4_321, 8_000))

    selected = _downsample_manual_points(
        points,
        max_points=1_200,
        important_timestamps=event_times,
        preserve_levels=(20.0,),
    )

    assert len(selected) <= 1_200
    assert selected[0] == points[0]
    assert selected[-1] == points[-1]
    assert tuple(point.sample_time for point in selected) == tuple(
        sorted(point.sample_time for point in selected)
    )
    assert all(timestamp in {point.sample_time for point in selected} for timestamp in event_times)
    assert all(
        any(point.sample_time == target for point in selected)
        for target in (points[321].sample_time, points[4_321].sample_time, points[8_000].sample_time)
    )


def test_manual_downsample_preserves_separate_spike_and_trough_extrema():
    points = tuple(
        ManualOpportunityHistoryPoint(
            START + timedelta(seconds=10 * index),
            100.0 if index == 75 else -100.0 if index == 425 else 0.0,
        )
        for index in range(500)
    )

    selected = _downsample_manual_points(points, max_points=40)
    selected_values = {point.raw_spread_bps for point in selected}

    assert 100.0 in selected_values
    assert -100.0 in selected_values


def test_manual_series_breaks_line_only_for_actual_large_source_gaps():
    points = tuple(
        ManualOpportunityHistoryPoint(START + timedelta(seconds=offset), float(offset))
        for offset in (0, 10, 20, 30, 130, 140)
    )

    x_values, y_values = _series_with_gap_breaks(points, points)

    assert len(x_values) == len(y_values)
    assert any(value != value for value in y_values)


def test_manual_series_does_not_break_for_display_sampling_gaps_without_source_gap():
    raw_points = tuple(
        ManualOpportunityHistoryPoint(START + timedelta(seconds=10 * index), float(index))
        for index in range(100)
    )
    display_points = raw_points[::10]

    _x_values, y_values = _series_with_gap_breaks(display_points, raw_points)

    assert not any(value != value for value in y_values)


def test_manual_downsample_uses_only_existing_prior_points_for_event_anchors():
    points = tuple(
        ManualOpportunityHistoryPoint(
            START + timedelta(seconds=10 * index),
            float(index),
        )
        for index in range(300)
    )

    selected = _downsample_manual_points(
        points,
        max_points=40,
        important_timestamps=(START + timedelta(seconds=255),),
    )

    assert points[25] in selected
    assert all(points[0].sample_time <= point.sample_time <= points[-1].sample_time for point in selected)
