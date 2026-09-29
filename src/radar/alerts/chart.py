from __future__ import annotations

import math
from collections import deque
from io import BytesIO
from datetime import datetime, timedelta

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from radar.alerts.models import SpreadAlertDetails
from radar.history.spread import HistoricalSpreadContext, HistoricalSpreadPoint

MAX_CHART_POINTS = 500
DISPLAY_WINDOW = timedelta(hours=24)
HISTORICAL_MEDIAN_TOLERANCE_BPS = 0.01
REFERENCE_LABEL_MIN_GAP_BPS = 2.0


def _downsample_points(
    points: tuple[HistoricalSpreadPoint, ...],
    *,
    max_points: int = MAX_CHART_POINTS,
) -> tuple[HistoricalSpreadPoint, ...]:
    if max_points < 2:
        raise ValueError("max_points must be at least 2")
    if len(points) <= max_points:
        return points
    last_index = len(points) - 1
    values = [point.raw_spread_bps for point in points]
    important = {
        0,
        last_index,
        min(range(len(values)), key=values.__getitem__),
        max(range(len(values)), key=values.__getitem__),
    }
    extrema_budget = max(0, max_points // 5 - len(important))
    value_range = max(values) - min(values)
    prominence_floor = max(value_range * 0.02, 1e-9)
    extrema: list[tuple[float, int]] = []
    for index in range(1, last_index):
        previous = values[index - 1]
        current = values[index]
        following = values[index + 1]
        if current > previous and current > following:
            prominence = current - max(previous, following)
        elif current < previous and current < following:
            prominence = min(previous, following) - current
        else:
            continue
        if prominence >= prominence_floor:
            extrema.append((prominence, index))
    important.update(
        index
        for _, index in sorted(extrema, reverse=True)[:extrema_budget]
    )

    remaining = max_points - len(important)
    uniform = (
        [int(index * last_index / (remaining - 1)) for index in range(remaining)]
        if remaining > 1
        else []
    )
    indices = list(important)
    for index in uniform:
        if index not in important:
            indices.append(index)
        if len(indices) == max_points:
            break
    if len(indices) < max_points:
        indices.extend(
            index
            for index in range(len(points))
            if index not in important and index not in indices
        )
    indices = sorted(indices[:max_points])
    return tuple(points[index] for index in indices)


def _format_median(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "unavailable"


def _recent_points(
    points: tuple[HistoricalSpreadPoint, ...],
    *,
    window: timedelta = DISPLAY_WINDOW,
) -> tuple[HistoricalSpreadPoint, ...]:
    if not points:
        return ()
    ordered_points = tuple(sorted(points, key=lambda point: point.sample_time))
    cutoff = ordered_points[-1].sample_time - window
    return tuple(point for point in ordered_points if point.sample_time >= cutoff)


def _baseline_median(context: HistoricalSpreadContext) -> float | None:
    for stats in (context.stats_7d, context.stats_30d, context.stats_90d):
        if stats.median_raw_spread_bps is not None:
            return stats.median_raw_spread_bps
    return None


def _historical_median_lines(
    context: HistoricalSpreadContext,
) -> list[tuple[str, float]]:
    medians = [
        ("7d", context.stats_7d.median_raw_spread_bps),
        ("30d", context.stats_30d.median_raw_spread_bps),
        ("90d", context.stats_90d.median_raw_spread_bps),
    ]
    available = [
        (label, value) for label, value in medians if value is not None
    ]
    if not available:
        return []
    first_value = available[0][1]
    if all(
        math.isclose(
            value,
            first_value,
            rel_tol=0.0,
            abs_tol=HISTORICAL_MEDIAN_TOLERANCE_BPS,
        )
        for _, value in available
    ):
        return [("/".join(label for label, _ in available) + " median", first_value)]
    return [(f"{label} median", value) for label, value in available]


def _rolling_mean_series(
    points: tuple[HistoricalSpreadPoint, ...],
    *,
    window: timedelta = DISPLAY_WINDOW,
) -> dict[datetime, float]:
    """Return prior-only 24-hour means for display without look-ahead."""
    ordered = tuple(sorted(points, key=lambda point: point.sample_time))
    history: deque[HistoricalSpreadPoint] = deque()
    total = 0.0
    means: dict[datetime, float] = {}
    for point in ordered:
        cutoff = point.sample_time - window
        while history and history[0].sample_time < cutoff:
            total -= history.popleft().raw_spread_bps
        if history:
            means[point.sample_time] = total / len(history)
        history.append(point)
        total += point.raw_spread_bps
    return means


def _label_reference_line(axis, value: float, label: str, color: str) -> None:
    axis.text(
        0.995,
        value,
        label,
        transform=axis.get_yaxis_transform(),
        horizontalalignment="right",
        verticalalignment="bottom",
        fontsize=7,
        color=color,
        bbox={"boxstyle": "round,pad=0.15", "facecolor": "white", "alpha": 0.7},
    )


def _reference_label_positions(
    reference_lines: list[tuple[str, float, str]],
) -> dict[str, float]:
    positions: dict[str, float] = {}
    previous_position: float | None = None
    for label, value, _ in sorted(reference_lines, key=lambda item: item[1]):
        position = value
        if previous_position is not None:
            position = max(position, previous_position + REFERENCE_LABEL_MIN_GAP_BPS)
        positions[label] = position
        previous_position = position
    return positions


def render_spread_chart(
    details: SpreadAlertDetails,
    context: HistoricalSpreadContext,
    *,
    candidate_net_bps: float = 10.0,
    alert_net_bps: float = 20.0,
) -> bytes | None:
    if len(context.points_7d) < 2:
        return None

    figure, axis = plt.subplots(figsize=(9, 4.5))
    try:
        all_points = tuple(sorted(context.points_7d, key=lambda point: point.sample_time))
        rolling_means = _rolling_mean_series(all_points)
        display_points = _recent_points(all_points)
        display_points = _downsample_points(display_points)
        timestamps = [point.sample_time for point in display_points]
        spreads = [point.raw_spread_bps for point in display_points]
        axis.plot(
            mdates.date2num(timestamps),
            spreads,
            color="tab:blue",
            linewidth=1.2,
        )
        mean_points = [
            point for point in display_points if point.sample_time in rolling_means
        ]
        if mean_points:
            axis.plot(
                mdates.date2num([point.sample_time for point in mean_points]),
                [rolling_means[point.sample_time] for point in mean_points],
                color="tab:orange",
                linewidth=1.0,
            )
        latest_point = display_points[-1]
        axis.scatter(
            [mdates.date2num(latest_point.sample_time)],
            [latest_point.raw_spread_bps],
            color="black",
            s=34,
            zorder=5,
        )
        axis.scatter(
            [mdates.date2num(details.sample_time)],
            [details.raw_spread_bps],
            color="tab:red",
            marker="D",
            s=28,
            zorder=6,
        )
        reference_lines: list[tuple[str, float, str]] = [
            ("Current", details.raw_spread_bps, "black")
        ]
        axis.axhline(details.raw_spread_bps, color="black", linestyle="--")
        axis.axhline(
            details.rolling_mean_bps,
            color="tab:orange",
            linestyle="-.",
        )
        reference_lines.append(("24h mean", details.rolling_mean_bps, "tab:orange"))
        median_colors = ("tab:blue", "tab:orange", "tab:purple")
        for (label, median), color in zip(
            _historical_median_lines(context), median_colors, strict=False
        ):
            axis.axhline(
                median,
                color=color,
                linestyle=":",
            )
            reference_lines.append((label, median, color))
        label_positions = _reference_label_positions(reference_lines)
        for label, value, color in reference_lines:
            _label_reference_line(axis, label_positions[label], label, color)
        baseline = (
            details.rolling_mean_bps
            if details.rolling_mean_bps is not None
            else _baseline_median(context)
        )
        deviation = details.deviation_bps if baseline is not None else None
        axis.text(
            0.01,
            0.99,
            "\n".join(
                (
                    f"Current raw: {details.raw_spread_bps:.2f} bps",
                    (
                        f"Baseline: {_format_median(baseline)} bps"
                        if baseline is not None
                        else "Baseline: unavailable"
                    ),
                    (
                        f"Deviation: {deviation:+.2f} bps"
                        if deviation is not None
                        else "Deviation: unavailable"
                    ),
                    f"Net spread: {_format_median(details.net_spread_bps)} bps",
                )
            ),
            transform=axis.transAxes,
            horizontalalignment="left",
            verticalalignment="top",
            fontsize=8,
            bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
        )
        axis.set_title(
            f"{details.canonical_symbol} | {details.long_venue} → "
            f"{details.short_venue} | ${details.primary_size_usd:,}",
            fontsize=11,
        )
        axis.set_xlabel("Time (UTC)", fontsize=9)
        axis.set_ylabel("Raw spread (bps)", fontsize=9)
        axis.tick_params(axis="both", labelsize=8)
        axis.grid(axis="y", alpha=0.25)
        axis.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=8))
        axis.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M", tz="UTC"))
        figure.subplots_adjust(right=0.98, bottom=0.22)
        figure.autofmt_xdate(rotation=30, ha="right")
        output = BytesIO()
        figure.savefig(output, format="png", dpi=120, bbox_inches="tight")
        return output.getvalue()
    finally:
        plt.close(figure)
