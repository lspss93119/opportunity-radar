from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
import math
from typing import cast

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import Formatter

from radar.alerts.manual_opportunity import ManualOpportunityAlertDetails
from radar.history.manual_opportunity import (
    ManualOpportunityHistoryContext,
    ManualOpportunityHistoryPoint,
)

MEAN_DUPLICATE_TOLERANCE_BPS = 0.01
SHORT_WINDOW = timedelta(hours=2)
MAX_MANUAL_CHART_POINTS = 1_200
MIN_SOURCE_GAP_SECONDS = 30.0
SOURCE_GAP_MULTIPLIER = 3.0


def _downsample_manual_points(
    points: tuple[ManualOpportunityHistoryPoint, ...],
    *,
    max_points: int = MAX_MANUAL_CHART_POINTS,
    important_timestamps: tuple[datetime, ...] = (),
    preserve_levels: tuple[float, ...] = (),
) -> tuple[ManualOpportunityHistoryPoint, ...]:
    """Reduce display density while retaining time-local extrema and anchors."""
    if max_points < 2:
        raise ValueError("max_points must be at least 2")
    ordered = tuple(sorted(points, key=lambda point: point.sample_time))
    if len(ordered) <= max_points:
        return ordered

    last_index = len(ordered) - 1
    values = [point.raw_spread_bps for point in ordered]
    important = {0, last_index}

    for timestamp in important_timestamps:
        prior_indices = [
            index
            for index, point in enumerate(ordered)
            if point.sample_time <= timestamp
        ]
        if prior_indices:
            important.add(prior_indices[-1])

    for level in preserve_levels:
        level_indices = [index for index, value in enumerate(values) if value >= level]
        if len(important) + len(level_indices) <= max_points:
            important.update(level_indices)
            continue
        run_start: int | None = None
        for index, value in enumerate(values + [float("-inf")]):
            if value >= level:
                if run_start is None:
                    run_start = index
                continue
            if run_start is None:
                continue
            run_end = index - 1
            important.add(run_start)
            important.add(run_end)
            peak = max(
                range(run_start, run_end + 1),
                key=lambda candidate: abs(values[candidate] - level),
            )
            important.add(peak)
            run_start = None

    available_budget = max_points - len(important)
    if available_budget > 1:
        bucket_count = max(1, available_budget // 2)
        for bucket in range(bucket_count):
            start = bucket * len(ordered) // bucket_count
            end = (bucket + 1) * len(ordered) // bucket_count
            if end <= start:
                continue
            window = range(start, end)
            important.add(min(window, key=values.__getitem__))
            important.add(max(window, key=values.__getitem__))

    indices = sorted(important)
    remaining = max_points - len(indices)
    if remaining > 0:
        for position in range(remaining):
            index = round(position * last_index / max(1, remaining - 1))
            if index not in important:
                indices.append(index)
    if len(indices) < max_points:
        for index in range(len(ordered)):
            if index in important or index in indices:
                continue
            indices.append(index)
            if len(indices) >= max_points:
                break
    selected = sorted(set(indices))[:max_points]
    return tuple(ordered[index] for index in selected)


def _source_gap_ranges(
    source_points: tuple[ManualOpportunityHistoryPoint, ...],
) -> tuple[tuple[datetime, datetime], ...]:
    ordered = tuple(sorted(source_points, key=lambda point: point.sample_time))
    if len(ordered) < 2:
        return ()
    intervals = tuple(
        (right.sample_time - left.sample_time).total_seconds()
        for left, right in zip(ordered, ordered[1:])
    )
    positive_intervals = tuple(interval for interval in intervals if interval > 0)
    if not positive_intervals:
        return ()
    positive_intervals = tuple(sorted(positive_intervals))
    midpoint = len(positive_intervals) // 2
    median_interval = (
        positive_intervals[midpoint]
        if len(positive_intervals) % 2
        else (positive_intervals[midpoint - 1] + positive_intervals[midpoint]) / 2
    )
    threshold = max(MIN_SOURCE_GAP_SECONDS, median_interval * SOURCE_GAP_MULTIPLIER)
    return tuple(
        (left.sample_time, right.sample_time)
        for left, right, interval in zip(ordered, ordered[1:], intervals)
        if interval > threshold
    )


def _series_with_gap_breaks(
    display_points: tuple[ManualOpportunityHistoryPoint, ...],
    source_points: tuple[ManualOpportunityHistoryPoint, ...],
) -> tuple[tuple[float, ...], tuple[float, ...]]:
    """Return chart coordinates without bridging actual source-data gaps."""
    ordered = tuple(sorted(display_points, key=lambda point: point.sample_time))
    gap_ranges = _source_gap_ranges(source_points)
    x_values: list[float] = []
    y_values: list[float] = []
    for index, point in enumerate(ordered):
        if index:
            previous = ordered[index - 1]
            crosses_gap = any(
                gap_start >= previous.sample_time
                and gap_end <= point.sample_time
                for gap_start, gap_end in gap_ranges
            )
            if crosses_gap:
                x_values.append(float("nan"))
                y_values.append(float("nan"))
        x_values.append(float(mdates.date2num(point.sample_time)))
        y_values.append(point.raw_spread_bps)
    return tuple(x_values), tuple(y_values)


def _manual_reference_lines(
    details: ManualOpportunityAlertDetails,
    *,
    min_profit_bps: float = 10.0,
) -> tuple[tuple[str, float], ...]:
    lines: list[tuple[str, float]] = [("Frozen a (24h mean)", details.reference_a_bps)]
    if details.mean_2h_bps is not None:
        lines.append(("2h mean", details.mean_2h_bps))
    if (
        details.mean_24h_bps is not None
        and not math.isclose(
            details.mean_24h_bps,
            details.reference_a_bps,
            rel_tol=0.0,
            abs_tol=MEAN_DUPLICATE_TOLERANCE_BPS,
        )
    ):
        lines.append(("24h mean (current)", details.mean_24h_bps))
    if details.mean_3d_bps is not None:
        lines.append(("3d mean", details.mean_3d_bps))
    lines.append(
        (
            "Entry threshold",
            details.reference_a_bps + details.round_trip_fee_bps + min_profit_bps,
        )
    )
    return tuple(lines)


def _manual_line_style(label: str) -> dict[str, object]:
    styles: dict[str, dict[str, object]] = {
        "Current": {
            "color": "black",
            "linestyle": "-",
            "linewidth": 1.8,
            "alpha": 0.95,
        },
        "Entry threshold": {
            "color": "tab:red",
            "linestyle": "--",
            "linewidth": 1.6,
            "alpha": 0.9,
        },
        "Frozen a (24h mean)": {
            "color": "tab:green",
            "linestyle": "-",
            "linewidth": 1.5,
            "alpha": 0.9,
        },
        "2h mean": {
            "color": "tab:orange",
            "linestyle": ":",
            "linewidth": 0.9,
            "alpha": 0.65,
        },
        "24h mean (current)": {
            "color": "tab:gray",
            "linestyle": "-.",
            "linewidth": 0.8,
            "alpha": 0.5,
        },
        "3d mean": {
            "color": "tab:purple",
            "linestyle": ":",
            "linewidth": 0.9,
            "alpha": 0.65,
        },
    }
    try:
        return dict(styles[label])
    except KeyError as exc:
        raise ValueError(f"unsupported manual chart line: {label}") from exc


def _configure_time_axis(axis, sample_times: tuple[datetime, ...]) -> None:
    """Use readable UTC ticks for both short fixtures and real-day history."""
    ordered = tuple(sorted(sample_times))
    if not ordered:
        return
    locator = mdates.AutoDateLocator(
        minticks=3,
        maxticks=6,
        interval_multiples=True,
    )
    axis.xaxis.set_major_locator(locator)
    span = ordered[-1] - ordered[0]
    if span <= SHORT_WINDOW:
        formatter: Formatter = mdates.DateFormatter("%H:%M:%S", tz="UTC")
    else:
        formatter = mdates.ConciseDateFormatter(
            locator,
            tz="UTC",
            show_offset=False,
        )
    axis.xaxis.set_major_formatter(formatter)


def _draw_reference_line(axis, value: float, label: str) -> None:
    style = _manual_line_style(label)
    axis.axhline(
        value,
        color=cast(str, style["color"]),
        linestyle=cast(str, style["linestyle"]),
        linewidth=cast(float, style["linewidth"]),
        alpha=cast(float, style["alpha"]),
    )


def _nearest_point(
    points: tuple[ManualOpportunityHistoryPoint, ...],
    timestamp: datetime,
) -> ManualOpportunityHistoryPoint | None:
    if not points:
        return None
    if timestamp < points[0].sample_time or timestamp > points[-1].sample_time:
        return None
    return min(points, key=lambda point: abs(point.sample_time - timestamp))


def render_manual_opportunity_chart(
    details: ManualOpportunityAlertDetails,
    context: ManualOpportunityHistoryContext,
    *,
    min_profit_bps: float = 10.0,
) -> bytes | None:
    """Render one Manual Opportunity directional BBO route."""
    if len(context.points) < 2:
        return None
    if min_profit_bps < 0:
        raise ValueError("min_profit_bps must be non-negative")

    all_points = tuple(sorted(context.points, key=lambda point: point.sample_time))
    entry_threshold = details.reference_a_bps + details.round_trip_fee_bps + min_profit_bps
    display_points = _downsample_manual_points(
        all_points,
        important_timestamps=tuple(
            timestamp
            for timestamp in (
                details.candidate_started_at,
                details.confirmed_at,
                details.sample_time,
            )
            if timestamp is not None
        ),
        preserve_levels=(entry_threshold,),
    )
    if len(display_points) < 2:
        return None

    figure, axis = plt.subplots(figsize=(9, 4.8))
    try:
        x_values, y_values = _series_with_gap_breaks(display_points, all_points)
        axis.plot(x_values, y_values, color="tab:blue", linewidth=1.2)

        current_point = ManualOpportunityHistoryPoint(
            details.sample_time,
            details.current_spread_bps,
        )
        right_edge_start = all_points[-1].sample_time - max(
            (all_points[-1].sample_time - all_points[0].sample_time) * 0.15,
            timedelta(seconds=30),
        )
        markers = (
            (
                details.candidate_started_at,
                "Candidate start",
                "tab:purple",
                "o",
                (4, 6),
                "left",
            ),
            (
                details.confirmed_at,
                "Confirmation",
                "tab:red",
                "D",
                (-6, -14),
                "right",
            ),
            (
                details.sample_time,
                "Current",
                "black",
                "X",
                (-6, 8),
                "right",
            ),
        )
        for timestamp, label, color, marker, label_offset, horizontal_alignment in markers:
            if timestamp is None:
                continue
            point = (
                current_point
                if timestamp == details.sample_time
                else _nearest_point(all_points, timestamp)
            )
            if point is None:
                continue
            if point.sample_time >= right_edge_start:
                horizontal_alignment = "right"
                label_offset = {
                    "Candidate start": (-6, 14),
                    "Confirmation": (-6, -14),
                    "Current": (-6, 8),
                }[label]
            x_value = mdates.date2num(point.sample_time)
            axis.scatter(
                [x_value],
                [point.raw_spread_bps],
                color=color,
                marker=marker,
                s=42,
                zorder=6,
            )
            axis.annotate(
                label,
                (x_value, point.raw_spread_bps),
                xytext=label_offset,
                textcoords="offset points",
                fontsize=7,
                color=color,
                ha=horizontal_alignment,
                annotation_clip=False,
            )

        reference_lines: list[tuple[str, float]] = [
            ("Current", details.current_spread_bps)
        ]
        _draw_reference_line(axis, details.current_spread_bps, "Current")
        for label, value in _manual_reference_lines(
            details,
            min_profit_bps=min_profit_bps,
        ):
            _draw_reference_line(axis, value, label)
            reference_lines.append((label, value))
        legend_handles = []
        for label, _value in reference_lines:
            style = _manual_line_style(label)
            legend_handles.append(
                Line2D(
                    [0],
                    [0],
                    color=cast(str, style["color"]),
                    linestyle=cast(str, style["linestyle"]),
                    linewidth=cast(float, style["linewidth"]),
                    alpha=cast(float, style["alpha"]),
                    label=label,
                )
            )
        axis.legend(
            handles=legend_handles,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.26),
            ncol=3,
            fontsize=7,
            frameon=False,
            handlelength=2.0,
            columnspacing=1.3,
        )

        axis.text(
            0.01,
            0.99,
            "\n".join(
                (
                    f"Current spread: {details.current_spread_bps:+.2f} bps",
                    f"Frozen a: {details.reference_a_bps:+.2f} bps",
                    f"Deviation: {details.deviation_bps:+.2f} bps",
                    f"Round-trip fee: {details.round_trip_fee_bps:+.2f} bps",
                    f"Expected net at a: {details.expected_net_at_a_bps:+.2f} bps",
                    f"Baseline range: {_optional_bps(details.baseline_range_bps)}",
                    f"Route volume: {_usd(details.route_volume_24h)}",
                )
            ),
            transform=axis.transAxes,
            horizontalalignment="left",
            verticalalignment="top",
            fontsize=8,
            bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
        )
        axis.set_title(
            f"{details.canonical_symbol} | {details.long_venue}:{details.long_venue_symbol} "
            f"→ {details.short_venue}:{details.short_venue_symbol} | {details.event_kind}",
            fontsize=10,
        )
        axis.set_xlabel("Time (UTC)", fontsize=9)
        axis.set_ylabel("Directional BBO spread (bps)", fontsize=9)
        axis.tick_params(axis="both", labelsize=8)
        axis.grid(axis="y", alpha=0.25)
        _configure_time_axis(
            axis,
            tuple(point.sample_time for point in all_points),
        )
        figure.subplots_adjust(right=0.98, bottom=0.34)
        figure.autofmt_xdate(rotation=30, ha="right")
        output = BytesIO()
        figure.savefig(output, format="png", dpi=120, bbox_inches="tight")
        return output.getvalue()
    finally:
        plt.close(figure)


def _optional_bps(value: float | None) -> str:
    return "n/a" if value is None else f"{value:+.2f} bps"


def _usd(value: float | None) -> str:
    return "n/a" if value is None else f"${value:,.0f}"
